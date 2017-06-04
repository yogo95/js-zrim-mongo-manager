/**
 * Generic MongoDB Manager
 */

const ConnectableObject = require('js-zrim-core').ConnectableObject,
  _ = require('lodash'),
  util = require('util'),
  mongojs = require('mongojs'),
  exceptions = require("js-zrim-core").exceptions,
  Joi = require("joi");

const DEFAULT_TIMEOUT_CONNECTION_MS = 2000;

/**
 * @description A generic mongo DB manager
 * @implements {ConnectableObject}
 * @return {MongoDbManager} The instance
 * @property {Object|undefined} collections The known collections
 * @property {Number} connectionTimeoutMs The connection timeout in milliseconds
 * @property {Object|undefined} nativeInstance The native instance
 * @constructor
 */
function MongoDbManager() {
  if (!(this instanceof MongoDbManager)) {
    return new (Function.prototype.bind.apply(MongoDbManager, Array.prototype.concat.apply([null], arguments)))();
  }

  ConnectableObject.apply(this, arguments);
  this.properties.mongoDbDriver = mongojs; // Set the driver
  this.properties.connectionTimeoutMs = DEFAULT_TIMEOUT_CONNECTION_MS; // The timeout in ms
}
ConnectableObject._applyPrototypeTo(MongoDbManager);

MongoDbManager._defineProperty(MongoDbManager, 'connectionTimeoutMs', {
  configurable: false,
  set: function (connectionTimeoutMs) {
    if (this.properties.connectionTimeoutMs === connectionTimeoutMs) {
      return;
    } else if (_.isNil(connectionTimeoutMs)) {
      this.properties.connectionTimeoutMs = DEFAULT_TIMEOUT_CONNECTION_MS;
      return true;
    } else if (_.isNumber(connectionTimeoutMs) && connectionTimeoutMs > 0) {
      this.properties.connectionTimeoutMs = connectionTimeoutMs;
      return true;
    }
  },
  get: function () {
    return this.properties.connectionTimeoutMs;
  }
});

MongoDbManager._defineProperty(MongoDbManager, 'collections', {
  get: function () {
    return this.properties.mongoDbCollections;
  }
});

MongoDbManager._defineProperty(MongoDbManager, 'nativeInstance', {
  get: function () {
    return this.properties.mongoDbInstance;
  }
});

/**
 * @description Returns the collection
 * @param {String} collectionName The collection name to retrieved
 * @return {Object|undefined} The collection if exists
 */
MongoDbManager.prototype.getCollectionByName = function (collectionName) {
  if (!_.isString(collectionName)) {
    return undefined;
  }

  return this.mongoDbCollections || this.mongoDbCollections[collectionName] || undefined;
};

const _handleInitializationOptionsSchema = Joi.object().keys({
  connectionString: Joi.string().trim().min(1).required(),
  collections: Joi.array().items(
    Joi.object().keys({
      name: Joi.string().trim().min(1).required(),
      index: Joi.array().items(
        Joi.object().keys({
          native: Joi.object().keys({
            keys: Joi.array().items(
              Joi.string().trim().min(1).required()
            ).required(),
            options: Joi.object()
          }).unknown().required()
        }).unknown()
      ).required()
    }).unknown().required()
  ).required()
}).unknown().required();
/**
 * @typedef {Object} MongoDbManager.initialize~CollectionIndexNativeOption
 * @description Contains information about a collection index to ensure
 * @property {Object} keys The keys
 * @property {Object|undefined} options The options to apply
 */
/**
 * @typedef {Object} MongoDbManager.initialize~CollectionIndexOption
 * @description Contains information about a collection index to ensure
 * @property {MongoDbManager.initialize~CollectionIndexNativeOption} native The native index information to use
 */
/**
 * @typedef {Object} MongoDbManager.initialize~CollectionOption
 * @description Contains information about a collection to use
 * @property {string} name The collection name to get
 * @property {MongoDbManager.initialize~CollectionIndexOption[]|MongoDbManager.initialize~CollectionIndexOption|undefined} index The indexes to apply
 */
/**
 * @typedef {Object} MongoDbManager.initialize~Options
 * @description The options
 * @property {string} connectionString The connection info
 * @property {MongoDbManager.initialize~CollectionOption[]} collections The collections to use
 */

/**
 * @inheritDoc
 * @see MongoDbManager.initialize~Options
 */
MongoDbManager.prototype._handleInitialization = function (options) {
  const __pretty_name__ = '_handleInitialization';

  return new Promise((resolve, reject) => {
    ConnectableObject.prototype._handleInitialization.call(this, options)
      .then(() => {
        Joi.validate(options, _handleInitializationOptionsSchema, error => {
          if (error) {
            this.logger.error("[%s] Invalid options: %s\n%s", __pretty_name__, error.message, error.stack);
            return setImmediate(reject, new exceptions.IllegalArgumentException(util.format("Invalid options: %s", error.message)));
          }

          // Now copy the options
          this.properties.mongoDbOptions = {
            connectionString: options.connectionString,
            collections: _.cloneDeep(options.collections)
          };

          setImmediate(resolve);
        });
      })
      .catch(reject);
  });
};

/**
 * @inheritDoc
 */
MongoDbManager.prototype._handleConnection = function () {
  const  __pretty_name__ = '_handleConnection';

  return new Promise((resolve, reject) => {

    /**
     * Handle the mondodb error event after connexion succeed
     * @param {Error} error The error
     * @private
     */
    const _handleMongoDbGlobalError = error => {
      if (this.currentState === MongoDbManager.States.Disconnecting) {
        this.logger.debug("[%s][%s] rule=ignore -> States = Disconnecting", __pretty_name__, '_handleMongoDbGlobalError');
        return;
      }
      this.logger.error("[%s][%s] Received error: %s\n%s", __pretty_name__, '_handleMongoDbGlobalError', error.toString(), error.stack, error);
      this._onConnectionLost()
        .then(() => {
          this.logger.debug("[%s][%s] Connection lost handled", __pretty_name__, '_handleMongoDbGlobalError');
        }, error => {
          this.logger.error("[%s][%s] Connection lost handled with error: %s\n%s",
            __pretty_name__, '_handleMongoDbGlobalError', error.message, error.stack);
        });
    };

    /**
     * Handle a reconnection
     * @private
     */
    const _handleReconnection = () => {
      if (this.currentState === MongoDbManager.States.Disconnecting) {
        this.logger.debug("[%s][%s] rule=ignore -> States = Disconnecting", __pretty_name__, '_handleMongoDbGlobalError');
        return;
      }

      this.logger.warn("[%s][%s] Connection retrieved", __pretty_name__, '_handleReconnection');
      this._onReconnected()
        .then(() => {
          this.logger.debug("[%s][%s] Reconnected handled", __pretty_name__, '_handleReconnection');
        }, error => {
          this.logger.error("[%s][%s] Reconnected handled with error: %s\n%s", __pretty_name__,
            '_handleReconnection', error.message, error.stack);
        });
    };

    /**
     * Free the previous context
     * @param {Object} context The previous context
     * @private
     */
    const _freePreviousContext = (context) => {
      if (_.isObjectLike(context)) {
        this.logger.debug("[%s][%s] Remove listeners", __pretty_name__, '_freePreviousContext');
        context.mongoDbInstance.removeListener('error', context.handleErrorFunction);
        context.mongoDbInstance.removeListener('connect', context.handleReconnection);
        context.mongoDbInstance.removeListener('reconnect', context.handleReconnection);
      }
    };

    if (this.properties.currentConnexionContext) {
      this.logger.debug("[%s] Previous connection found : Free the context", __pretty_name__);
      this.properties.currentConnexionContext.freeContext();
      this.logger.debug("[%s] Delete the previous context", __pretty_name__);
      this.properties.currentConnexionContext = undefined;
      delete this.properties.currentConnexionContext;
    }

    // Prepare the context object
    this.logger.debug("[%s] Create the context object", __pretty_name__);
    const context = {
      manager: this,
      mongoDbOptions: this.properties.mongoDbOptions,
      mongoDbDriver: this.properties.mongoDbDriver,
      connectionTimeoutMs: this.properties.connectionTimeoutMs,
      logger: this.logger
    };

    // Create the tasks
    let tasks = [
      this._handleConnection.Steps.connection,
      this._handleConnection.Steps.initializeCollectionIndex,
      this._handleConnection.Steps.exportVariables
    ];

    this.logger.debug("[%s] Create tasks with context inside", __pretty_name__);
    tasks = _.map(tasks, (task) => {
      return task.bind(this, context);
    });

    this.logger.debug("[%s][Tasks][Start]", __pretty_name__);
    async.waterfall(tasks, error => {
      this.logger.debug("[%s][%s][Tasks][End]", __pretty_name__, '_handleTasksDone');

      if (error) {
        this.logger.error("[%s][%s][Tasks][End][Error] %s\n%s", __pretty_name__, '_handleTasksDone', error.message, error.stack);
        return setImmediate(reject, error);
      }

      this.logger.debug("[%s][%s] Expose the mongodb instance", __pretty_name__, '_handleTasksDone');
      this.properties.mongoDbInstance = context.mongoDbInstance;

      // This will keep necessary information in case of connection last and manually connection
      const currentConnexionContext = {
        handleErrorFunction: _handleMongoDbGlobalError,
        handleReconnection: _handleReconnection,
        freeContext: _freePreviousContext.bind(this, currentConnexionContext),
        mongoDbInstance: this.properties.mongoDbInstance
      };

      this.logger.debug("[%s][%s] Save the current connection context", __pretty_name__, '_handleTasksDone');
      this.properties.currentConnexionContext = currentConnexionContext;

      // Handle the disconnect error
      this.logger.debug("[%s][%s] Connect signal '%s'", __pretty_name__, '_handleTasksDone', 'error');
      this.properties.mongoDbInstance.on('error', _handleMongoDbGlobalError);
      // Handle reconnection
      this.logger.debug("[%s][%s] Connect signal '%s'", __pretty_name__, '_handleTasksDone', 'connect');
      this.properties.mongoDbInstance.on('connect', _handleReconnection);
      this.logger.debug("[%s][%s] Connect signal '%s'", __pretty_name__, '_handleTasksDone', 'reconnect');
      this.properties.mongoDbInstance.on('reconnect', _handleReconnection);

      setImmediate(resolve);
    });
  });
};

/**
 * @typedef {Object} MongoDbManager._handleConnection~Context
 * @description Contains the data for each step
 * @property {MongoDbManager} manager The manager instance
 * @property {MongoDbManager.init~Options} mongoDbOptions The mongoDB options
 * @property {Function} mongoDbDriver The mongoDB Driver to use
 * @property {number} connectionTimeoutMs The connection timeout
 * @property {Object} logger The logger to use
 * @property {Object|undefined} mongoDbInstance The mongoDB Driver instance (when connection succeed)
 */
/**
 * @typedef {Function} MongoDbManager._handleConnection~Step
 * @description A step
 * @property {MongoDbManager._handleConnection~Context} context The context
 * @property {Function} stepDone The callback to tell the step finished with or without error
 */

/**
 * @description Contains step to handle the connection
 * @type {Object}
 * @property {MongoDbManager._handleConnection~Step} connection The first step is to do the mongoDB connection
 * @property {MongoDbManager._handleConnection~Step} initializeCollectionIndex Create the index
 * @property {MongoDbManager._handleConnection~Step} exportVariables Export the public variables
 */
MongoDbManager.prototype._handleConnection.Steps = {
  connection: function (context, stepDone) {
    const __pretty_name__ = '_handleConnection', __step_name__ = 'connection';

    // Generate the collection names
    const collectionNames = _.map(context.mongoDbOptions.collections, function (collection) {
      return collection.name;
    });

    let connectionTimeoutId,
      mongoDbInstance;

    /**
     * Remove the listener in the instance
     * @private
     */
    function _removeListeners() {
      mongoDbInstance.removeListener('connect', _mongoOnConnectOnce);
      mongoDbInstance.removeListener('error', _mongoOnErrorOnce);
    }

    /**
     * @description Handle the post connection response
     * @see MongoDbManager._handlePostConnection~Callback
     * @param {Error|undefined} [userError] The error
     * @private
     */
    function _handlePostConnectionCallback(userError) {
      context.logger.debug("[%s][Step:%s][%s][Enter]", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
      if (userError) {
        context.logger.error("[%s][Step:%s][%s] User function return an error: %s\n%s",
          __pretty_name__, __step_name__, '_handlePostConnectionCallback', userError.toString(), userError.stack);

        let error;
        if (!(userError instanceof Error)) {
          error = new Error(util.format("Post connection failed"));
          error.cause = userError;
          context.logger.debug("[%s][Step:%s][%s] User error was not instance of Error. New error created.", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
        } else {
          error = userError;
        }

        context.logger.debug("[%s][Step:%s][%s][Exit]", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
        return stepDone(error);
      }

      // Accept connection
      context.logger.debug("[%s][Step:%s][%s] Save the mongodb instance into the context", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
      context.mongoDbInstance = mongoDbInstance;
      context.logger.debug("[%s][Step:%s][%s][Exit]", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
      stepDone();
    }

    function _mongoOnConnectOnce() {
      context.logger.debug("[%s][Step:%s][%s][Connected]", __pretty_name__, __step_name__, '_mongoOnConnectOnce');
      _removeListeners();

      if (!connectionTimeoutId) {
        // Ignore because the timeout already append
        context.logger.debug("[%s][Step:%s][%s][Connected] Timeout already reached. Rule=ignore connected", __pretty_name__, __step_name__, '_mongoOnConnectOnce');
        return;
      }

      context.logger.debug("[%s][Step:%s][%s][Connected] Clear the timeout", __pretty_name__, __step_name__, '_mongoOnConnectOnce');
      clearTimeout(connectionTimeoutId);
      connectionTimeoutId = undefined;

      /** @type {MongoDbManager._handlePostConnection~Context} */
      const postConnectionContext = {
        mongoDbInstance: mongoDbInstance
      };

      // Call post connection
      context.logger.debug("[%s][Step:%s][%s] Call the handle post connection", __pretty_name__, __step_name__, '_mongoOnConnectOnce');
      context.manager._handlePostConnection(postConnectionContext)
        .then(() => {
          setImmediate(_handlePostConnectionCallback);
        })
        .catch(error => {
          context.logger.debug("[%s][Step:%s][%s] Received error from _handlePostConnection: %s\n%s",
            __pretty_name__, __step_name__, '_mongoOnConnectOnce', error.message, error.stack);
          setImmediate(stepDone, error);
        });
    }

    function _mongoOnErrorOnce(error) {
      context.logger.debug("[%s][Step:%s][%s][Connection][Error] %s\n%s", __pretty_name__, __step_name__, '_mongoOnErrorOnce', error.toString(), error.stack);
      _removeListeners();

      if (!connectionTimeoutId) {
        // Ignore because the timeout already append
        context.logger.debug("[%s][Step:%s][%s][Connection][Error] Timeout already reached Rule=ignore", '_mongoOnErrorOnce', __pretty_name__, __step_name__);
        return;
      }

      context.logger.error("[%s][Step:%s][%s][Connection][Error] %s\n%s", __pretty_name__, __step_name__, '_mongoOnErrorOnce', error.toString(), error.stack);
      context.logger.debug("[%s][Step:%s][%s][Connection][Error] Clear the timeout", __pretty_name__, __step_name__, '_mongoOnErrorOnce');
      clearTimeout(connectionTimeoutId);
      connectionTimeoutId = undefined;
      const errorToReturn = new Error(util.format("Error while connecting : %s", error.toString()));
      errorToReturn.cause = error;

      /** @type {MongoDbManager._handlePostConnection~Context} */
      const postConnectionContext = {
        mongoDbInstance: mongoDbInstance
      };

      // Call post connection
      context.logger.debug("[%s][Step:%s][%s] Call the post connection", __pretty_name__, __step_name__, '_mongoOnErrorOnce');
      context.manager._handlePostConnection(postConnectionContext, errorToReturn)
        .then(() => {
          context.logger.debug("[%s][Step:%s][%s][%s] Done -> Next step", __pretty_name__, __step_name__, '_mongoOnErrorOnce', '_handlePostConnection');
          setImmediate(stepDone, errorToReturn);
        })
        .then(error => {
          context.logger.debug("[%s][Step:%s][%s][%s] Done -> Next step", __pretty_name__, __step_name__, '_mongoOnErrorOnce', '_handlePostConnection');
          setImmediate(stepDone, error);
        });
    }

    function _connectionTimeout() {
      _removeListeners();
      if (!connectionTimeoutId) {
        context.logger.debug("[%s][Step:%s][%s][Timeout] No timeout. rule=ignore", __pretty_name__, __step_name__, '_connectionTimeout');
        return; // Ignore it
      }

      context.logger.error("[%s][Step:%s][%s][Timeout] Reach the timeout after %d ms", __pretty_name__, __step_name__, '_connectionTimeout', context.connectionTimeoutMs);
      connectionTimeoutId = null;
      return stepDone(new Error(util.format("Connection timeout. Reached after %d ms", context.connectionTimeoutMs)));
    }

    // Start the connection
    connectionTimeoutId = setTimeout(_connectionTimeout, context.connectionTimeoutMs);
    mongoDbInstance = context.mongoDbDriver(context.mongoDbOptions.connectionString, collectionNames);
    mongoDbInstance.once('connect', _mongoOnConnectOnce);
    mongoDbInstance.once('error', _mongoOnErrorOnce);
  },
  initializeCollectionIndex: function (context, stepDone) {
    const __pretty_name__ = '_handleConnection', __step_name__ = 'initializeCollectionIndex';

    // Create the index
    const collectionsWithIndex = [];
    _.each(context.mongoDbOptions.collections, function (collection) {
      if (!collection.index || collection.index.length <= 0) {
        return;
      }

      const collectionWithIndex = {
        collectionName: collection.name,
        indexes: _.map(collection.index, function (index) {
          return {
            keys: index.native.keys,
            options: index.native.options
          };
        })
      };

      collectionsWithIndex.push(collectionWithIndex);
    });

    if (collectionsWithIndex.length === 0) {
      return;
    }

    /**
     * @description Returns the task to create the index for the specified collection
     * @param {String} collectionName The collection name
     * @param {Object} index The index
     * @return {Function} The task function
     * @private
     */
    function _createTaskCreateIndex(collectionName, index) {
      return function _taskCreateIndex(taskDone) {
        if (!context.mongoDbInstance[collectionName]) {
          const error = new Error(util.format("Cannot find the collection '%s'", collectionName));
          context.logger.error("[%s][Step:%s] Cannot find the collection '%s'",
            __pretty_name__, __step_name__, collectionName);
          return taskDone(error);
        } else if (!_.isFunction(context.mongoDbInstance[collectionName].ensureIndex)) {
          const error = new Error(util.format("The collection '%s' does not have the function 'ensureIndex'", collectionName));
          context.logger.error("[%s][Step:%s][Collection:%s] The collection does not have the function 'ensureIndex'",
            __pretty_name__, __step_name__, collectionName);
          return taskDone(error);
        }

        context.logger.debug("[%s][Step:%s][Collection:%s] Create the index '%s'",
          __pretty_name__, __step_name__, collectionName, JSON.stringify(index));
        context.mongoDbInstance[collectionName].ensureIndex(index.keys, index.options, function _ensureIndexCallback(error) {
          if (error) {
            context.logger.error("[%s][Step:%s][Collection:%s] ensureIndex failed : %s",
              __pretty_name__, __step_name__, collectionName, error.toString(), error);
            return taskDone(error);
          }

          context.logger.debug("[%s][Step:%s][Collection:%s] Create the index succeed : '%s'",
            __pretty_name__, __step_name__, collectionName, JSON.stringify(index));
          return taskDone();
        });
      };
    }

    // create the tasks
    const tasks = [];
    _.each(collectionsWithIndex, function (collectionWithIndex) {
      // For each index
      _.each(collectionWithIndex.indexes, function (index) {
        tasks.push(_createTaskCreateIndex(collectionWithIndex.name, index));
      });
    });

    // Tasks created now
    async.parallel(tasks, function _parallelDone(error) {
      if (error) {
        context.logger.error("[%s][Step:%s][Tasks:end][Error] %s",
          __pretty_name__, __step_name__, error.toString(), error);

        return stepDone(error);
      }

      return stepDone();
    });
  },
  exportVariables: function (context, stepDone) {
    const __pretty_name__ = '_handleConnection', __step_name__ = 'exportVariables';

    context.logger.debug("[%s][Step:%s][Enter]", __pretty_name__, __step_name__);

    // Create the collections object
    function _createGetCollection(manager, collectionName) {
      return function _getCollection() {
        return manager.getCollectionByName(collectionName);
      };
    }

    function _setCollection() { }

    const objProperties = {};
    _.each(context.mongoDbOptions.collections, function (collection) {
      context.logger.debug("[%s][Step:%s] Export collection '%s'",
        __pretty_name__, __step_name__, collection);

      objProperties[collection.name] = {
        configurable: false,
        enumerable: true,
        set: _setCollection,
        get: _createGetCollection(context.manager, collection.name) // By using a function we are able to override the getCollectionByName
      };
    });

    context.logger.debug("[%s][Step:%s] Create the collection container", __pretty_name__, __step_name__);
    const collections = Object.create(Object.prototype, objProperties);
    context.logger.debug("[%s][Step:%s] Apply the container by setting the manager property", __pretty_name__, __step_name__);
    context.manager.properties.mongoDbCollections = collections;

    context.logger.debug("[%s][Step:%s][Exit]", __pretty_name__, __step_name__);
    return stepDone();
  }
};


/**
 * @typedef {Object} MongoDbManager._handlePostConnection~Context
 * @description The context for this post connection
 * @property {Object} mongoDbInstance The instance with the connection
 */
/**
 * @typedef {callback} MongoDbManager._handlePostConnection~Callback
 * @param {Error|undefined} [error] Return an error to change the behaviour.
 *  In case of success returning an error will close the connection and return an error to the client
 */

/**
 * @description Override this function to handle post connection.
 * @param {MongoDbManager._handlePostConnection~Context} context The context
 * @param {Error|undefined} [error] If the connection failed, the error
 * @return {Promise} The promise object
 */
MongoDbManager.prototype._handlePostConnection = function (context, error) {

  return new Promise((resolve, reject) => {
    if (error) {
      setImmediate(reject, error);
    } else {
      setImmediate(resolve);
    }
  });
};


/**
 * @inheritDoc
 */
MongoDbManager.prototype._handleDisconnection = function () {
  const __pretty_name__ = '_handleDisconnection';

  return new Promise((resolve, reject) => {

    const _handleError = (error) => {
      this.logger.warn("[%s][%s][Error] %s\n%s", __pretty_name__, '_handleClosed', error.message, error.stack);
      _removeListeners();

      setImmediate(reject, error);
    };

    const _handleClosed = () => {
      this.logger.warn("[%s][%s] Disconnected", __pretty_name__, '_handleClosed');
      _removeListeners();


      if (this.properties.currentConnexionContext) {
        this.logger.debug("[%s] Free the connection context", __pretty_name__);
        this.properties.currentConnexionContext.freeContext();
        this.logger.debug("[%s] Destroy the connection context", __pretty_name__);
        this.properties.currentConnexionContext = undefined;
        delete this.properties.currentConnexionContext;
      }

      setImmediate(resolve);
    };

    const _removeListeners = () => {
      this.logger.debug("[%s][%s] Remove listeners", __pretty_name__, '_removeListeners');
      this.properties.mongoDbInstance.removeListener('error', _handleError);
      this.properties.mongoDbInstance.removeListener('close', _handleClosed);
    };

    this.properties.mongoDbInstance.once('error', _handleError);
    this.properties.mongoDbInstance.once('close', _handleClosed);

    this.properties.mongoDbInstance.close();
  });
};


exports = module.exports = MongoDbManager;
