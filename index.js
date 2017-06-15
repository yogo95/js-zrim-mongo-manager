/**
 * Generic MongoDB Manager
 */

const ConnectableObject = require('js-zrim-core').ConnectableObject,
  _ = require('lodash'),
  util = require('util'),
  mongodb = require('mongodb'),
  MongoClient = mongodb.MongoClient,
  exceptions = require("js-zrim-core").exceptions,
  async = require('async'),
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
  this.properties.mongoDbDriver = MongoClient; // Set the driver
  this.properties.connectionTimeoutMs = DEFAULT_TIMEOUT_CONNECTION_MS; // The timeout in ms
  this.properties.virtualCollections = {};
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
    return this.properties.virtualCollections;
  }
});

MongoDbManager._defineProperty(MongoDbManager, 'nativeInstance', {
  get: function () {
    return this.properties.mongoDataBase;
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

  const collections = this.properties.mongoDbCollections;
  if (collections) {
    return collections[collectionName] || undefined;
  } else {
    return undefined;
  }
};

const _handleInitializationOptionsSchema = Joi.object().keys({
  connectionTimeoutMs: Joi.number().min(1000),
  connectionString: Joi.string().trim().min(1).required(),
  collections: Joi.array().items(
    Joi.object().keys({
      name: Joi.string().trim().min(1).required(),
      index: Joi.array().items(
        Joi.object().keys({
          native: Joi.object().keys({
            keys: Joi.object().pattern(/^[a-z_][a-z0-9_.-]*$/i, Joi.number().required()).required(),
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
 * @see https://docs.mongodb.com/manual/reference/connection-string/
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
          if (options.connectionTimeoutMs) {
            this.properties.connectionTimeoutMs = options.connectionTimeoutMs;
          }

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
  const __pretty_name__ = '_handleConnection';

  return new Promise((resolve, reject) => {

    /**
     * Handle the mondodb error event after connexion succeed
     * @param {Error} error The error
     * @private
     */
    const _handleMongoDbConnectionClosed = error => {
      if (this.currentState === MongoDbManager.States.Disconnecting) {
        this.logger.debug("[%s][%s] rule=ignore -> States = Disconnecting", __pretty_name__, '_handleMongoDbConnectionClosed');
        return;
      }
      this.logger.error("[%s][%s] Received error: %s\n%s", __pretty_name__, '_handleMongoDbConnectionClosed', error.toString(), error.stack, error);
      this._onConnectionLost()
        .then(() => {
          this.logger.debug("[%s][%s] Connection lost handled", __pretty_name__, '_handleMongoDbConnectionClosed');
        }, error => {
          this.logger.error("[%s][%s] Connection lost handled with error: %s\n%s",
            __pretty_name__, '_handleMongoDbConnectionClosed', error.message, error.stack);
        });
    };

    /**
     * Handle a reconnection
     * @private
     */
    const _handleReconnection = () => {
      if (this.currentState === MongoDbManager.States.Disconnecting) {
        this.logger.debug("[%s][%s] rule=ignore -> States = Disconnecting", __pretty_name__, '_handleMongoDbConnectionClosed');
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
        context.mongoDataBase.removeListener('close', context.handleErrorFunction);
        context.mongoDataBase.removeListener('connect', context.handleReconnection);
        context.mongoDataBase.removeListener('reconnect', context.handleReconnection);
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
      logger: this.logger,
      collections: {}
    };

    // Create the tasks
    let tasks = [
      this._handleConnection.Steps.connection,
      this._handleConnection.Steps.fetchCollections,
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
      this.properties.mongoClientInstance = context.mongoClientInstance;
      this.properties.mongoDataBase = context.mongoDataBase;

      // This will keep necessary information in case of connection last and manually connection
      const currentConnexionContext = {
        handleErrorFunction: _handleMongoDbConnectionClosed,
        handleReconnection: _handleReconnection,
        freeContext: undefined,
        mongoClientInstance: this.properties.mongoClientInstance,
        mongoDataBase: this.properties.mongoDataBase
      };
      currentConnexionContext.freeContext = _freePreviousContext.bind(this, currentConnexionContext);

      this.logger.debug("[%s][%s] Save the current connection context", __pretty_name__, '_handleTasksDone');
      this.properties.currentConnexionContext = currentConnexionContext;

      // Handle the disconnect error
      this.logger.debug("[%s][%s] Connect signal '%s'", __pretty_name__, '_handleTasksDone', 'close');
      this.properties.mongoDataBase.on('error', _handleMongoDbConnectionClosed);
      // Handle reconnection
      this.logger.debug("[%s][%s] Connect signal '%s'", __pretty_name__, '_handleTasksDone', 'connect');
      this.properties.mongoDataBase.on('connect', _handleReconnection);
      this.logger.debug("[%s][%s] Connect signal '%s'", __pretty_name__, '_handleTasksDone', 'reconnect');
      this.properties.mongoDataBase.on('reconnect', _handleReconnection);

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
 * @property {mongodb.MongoClient|undefined} mongoClientInstance The mongoDB client instance (when connection succeed)
 * @property {mongodb.Db|undefined} mongoDataBase The current database used
 * @property {Object} collections The collections by names
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

    let connectionTimeoutId,
      mongoClientInstance,
      connectionTimedOut = false;

    /**
     * Remove the listener in the instance
     * @private
     */
    function _removeListeners() {
      mongoClientInstance.removeListener('timeout', _mongoConnectionSuccess);
    }

    /**
     * Handle the connection success
     * @param {Object} db The database
     * @private
     */
    function _mongoConnectionSuccess(db) {
      context.logger.debug("[%s][Step:%s][%s][Connected]", __pretty_name__, __step_name__, '_mongoConnectionSuccess');
      _removeListeners();

      if (connectionTimedOut) {
        // Ignore because the timeout already append
        context.logger.debug("[%s][Step:%s][%s][Connected] Timeout already reached. Rule=ignore connected", __pretty_name__, __step_name__, '_mongoConnectionSuccess');
        return;
      }

      context.logger.debug("[%s][Step:%s][%s][Connected] Clear the timeout", __pretty_name__, __step_name__, '_mongoConnectionSuccess');
      clearTimeout(connectionTimeoutId);
      connectionTimeoutId = undefined;

      /** @type {MongoDbManager._handlePostConnection~Context} */
      const postConnectionContext = {
        mongoClientInstance: mongoClientInstance,
        mongoDataBase: db
      };

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
            error = new exceptions.BaseError(util.format("Post connection failed"), userError);
            context.logger.debug("[%s][Step:%s][%s] User error was not instance of Error. New error created.", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
          } else {
            error = userError;
          }

          // Close connection
          db.close(true)
            .then(() => {
              context.logger.debug("[%s][Step:%s][%s] Connection closed with success",
                __pretty_name__, __step_name__, '_mongoConnectionSuccess');
              context.logger.debug("[%s][Step:%s][%s][Exit]", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
              setImmediate(stepDone, error);
            })
            .catch(error => {
              context.logger.debug("[%s][Step:%s][%s] Failed to close the connection: %s\n%s",
                __pretty_name__, __step_name__, '_mongoConnectionSuccess', error.message, error.stack);
              context.logger.debug("[%s][Step:%s][%s][Exit]", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
              setImmediate(stepDone, error);
            });
        } else {
          // Accept connection
          context.logger.debug("[%s][Step:%s][%s] Save the mongodb instance into the context", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
          context.mongoClientInstance = mongoClientInstance;
          context.mongoDataBase = db;
          context.logger.debug("[%s][Step:%s][%s][Exit]", __pretty_name__, __step_name__, '_handlePostConnectionCallback');
          setImmediate(stepDone);
        }
      }

      // Call post connection
      context.logger.debug("[%s][Step:%s][%s] Call the handle post connection", __pretty_name__, __step_name__, '_mongoConnectionSuccess');
      context.manager._handlePostConnection(postConnectionContext)
        .then(_handlePostConnectionCallback)
        .catch(error => {
          context.logger.debug("[%s][Step:%s][%s] Received error from _handlePostConnection: %s\n%s",
            __pretty_name__, __step_name__, '_mongoConnectionSuccess', error.message, error.stack);

          // Should close the database
          db.close(true)
            .then(() => {
              context.logger.debug("[%s][Step:%s][%s] Connection closed with success",
                __pretty_name__, __step_name__, '_mongoConnectionSuccess');
              setImmediate(stepDone, error);
            })
            .catch(error => {
              context.logger.debug("[%s][Step:%s][%s] Failed to close the connection: %s\n%s",
                __pretty_name__, __step_name__, '_mongoConnectionSuccess', error.message, error.stack);
              setImmediate(stepDone, error);
            });
        });
    }

    /**
     * Handle the error returned by the connection
     * @param {Error} error The error
     * @private
     */
    function _mongoConnectionError(error) {
      context.logger.debug("[%s][Step:%s][%s][Connection][Error] %s\n%s", __pretty_name__, __step_name__, '_mongoConnectionError', error.toString(), error.stack);
      _removeListeners();

      if (connectionTimedOut) {
        // Ignore because the timeout already append
        context.logger.debug("[%s][Step:%s][%s][Connection][Error] Timeout already reached Rule=ignore", '_mongoConnectionError', __pretty_name__, __step_name__);
        return;
      }

      context.logger.error("[%s][Step:%s][%s][Connection][Error] %s\n%s", __pretty_name__, __step_name__, '_mongoConnectionError', error.toString(), error.stack);
      context.logger.debug("[%s][Step:%s][%s][Connection][Error] Clear the timeout", __pretty_name__, __step_name__, '_mongoConnectionError');
      clearTimeout(connectionTimeoutId);
      connectionTimeoutId = undefined;
      const errorToReturn = new Error(util.format("Error while connecting : %s", error.toString()));
      errorToReturn.cause = error;

      /** @type {MongoDbManager._handlePostConnection~Context} */
      const postConnectionContext = {
        mongoClientInstance: mongoClientInstance
      };

      // Call post connection
      context.logger.debug("[%s][Step:%s][%s] Call the post connection", __pretty_name__, __step_name__, '_mongoConnectionError');
      context.manager._handlePostConnection(postConnectionContext, errorToReturn)
        .then(() => {
          context.logger.debug("[%s][Step:%s][%s][%s] Done -> Next step", __pretty_name__, __step_name__, '_mongoConnectionError', '_handlePostConnection');
          setImmediate(stepDone, errorToReturn);
        })
        .catch(error => {
          context.logger.debug("[%s][Step:%s][%s][%s] Done -> Next step", __pretty_name__, __step_name__, '_mongoConnectionError', '_handlePostConnection');
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
      connectionTimedOut = true;
      return stepDone(new exceptions.TimedOutException(util.format("Connection timeout. Reached after %d ms", context.connectionTimeoutMs)));
    }

    // Start the connection
    connectionTimeoutId = setTimeout(_connectionTimeout, context.connectionTimeoutMs);
    const connectionOptions = {};
    mongoClientInstance = new context.mongoDbDriver();
    mongoClientInstance.connect(context.mongoDbOptions.connectionString, connectionOptions)
      .then(_mongoConnectionSuccess, _mongoConnectionError);
  },
  fetchCollections: function (context, stepDone) {
    const __pretty_name__ = '_handleConnection', __step_name__ = 'fetchCollections';

    const collectionNames = _.map(context.mongoDbOptions.collections, function (collection) {
      return collection.name;
    });

    // Create promises
    const fetchPromises = _.map(collectionNames, collectionName => {
      return new Promise((resolve, reject) => {
        context.mongoDataBase.collection(collectionName, undefined, (error, collection) => {
          if (error) {
            context.logger.error("[%s][Step:%s] Failed to fetch the collection '%s': %s\n%s",
              __pretty_name__, __step_name__, collectionName, error.message, error.stack);
            setImmediate(reject, error);
          } else {
            context.logger.debug("[%s][Step:%s] Fetch the collection '%s' with success",
              __pretty_name__, __step_name__, collectionName);
            setImmediate(resolve, {
              collectionName: collectionName,
              collection: collection
            });
          }
        });
      });
    });

    Promise.all(fetchPromises)
      .then(result => {
        _.each(result, el => {
          context.collections[el.collectionName] = el.collection;
        });

        setImmediate(stepDone);
      })
      .catch(error => {
        setImmediate(stepDone, error);
      });
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
        name: collection.name,
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
        if (!context.collections[collectionName]) {
          const error = new Error(util.format("Cannot find the collection '%s'", collectionName));
          context.logger.error("[%s][Step:%s] Cannot find the collection '%s'",
            __pretty_name__, __step_name__, collectionName);
          return taskDone(error);
        } else if (!_.isFunction(context.collections[collectionName].createIndex)) {
          const error = new Error(util.format("The collection '%s' does not have the function 'createIndex'", collectionName));
          context.logger.error("[%s][Step:%s][Collection:%s] The collection does not have the function 'createIndex'",
            __pretty_name__, __step_name__, collectionName);
          return taskDone(error);
        }

        context.logger.debug("[%s][Step:%s][Collection:%s] Create the index '%s'",
          __pretty_name__, __step_name__, collectionName, JSON.stringify(index));
        context.collections[collectionName].createIndex(index.keys, index.options)
          .then(() => {
            context.logger.debug("[%s][Step:%s][Collection:%s] Create the index succeed : '%s'",
              __pretty_name__, __step_name__, collectionName, JSON.stringify(index));
            return taskDone();
          })
          .catch(error => {
            context.logger.error("[%s][Step:%s][Collection:%s] createIndex failed : %s\n%s",
              __pretty_name__, __step_name__, collectionName, error.message, error.stack);
            return taskDone(error);
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

    function _setCollection() {
    }

    const objProperties = {};
    _.each(context.collections, function (collection, collectionName) {
      context.logger.debug("[%s][Step:%s] Export collection '%s'",
        __pretty_name__, __step_name__, collection);

      objProperties[collectionName] = {
        configurable: false,
        enumerable: true,
        set: _setCollection,
        get: _createGetCollection(context.manager, collectionName) // By using a function we are able to override the getCollectionByName
      };
    });

    context.logger.debug("[%s][Step:%s] Create the collection container", __pretty_name__, __step_name__);
    const virtualCollections = Object.create(Object.prototype, objProperties);
    context.logger.debug("[%s][Step:%s] Apply the container by setting the manager property", __pretty_name__, __step_name__);
    context.manager.properties.virtualCollections = virtualCollections;
    context.manager.properties.mongoDbCollections = context.collections;

    context.logger.debug("[%s][Step:%s][Exit]", __pretty_name__, __step_name__);
    return stepDone();
  }
};


/**
 * @typedef {Object} MongoDbManager._handlePostConnection~Context
 * @description The context for this post connection
 * @property {Object} mongoClientInstance The instance used for the connection
 * @property {Object} mongoDataBase The instance of the current selected database
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

    this.properties.mongoDataBase.close()
      .then(() => {
        this.logger.warn("[%s][%s] Disconnected", __pretty_name__, '_handleClosed');

        if (this.properties.currentConnexionContext) {
          this.logger.debug("[%s] Free the connection context", __pretty_name__);
          this.properties.currentConnexionContext.freeContext();
          this.logger.debug("[%s] Destroy the connection context", __pretty_name__);
          this.properties.currentConnexionContext = undefined;
          delete this.properties.currentConnexionContext;
        }

        this.properties.mongoDataBase = undefined;
        this.properties.mongoClientInstance = undefined;

        setImmediate(resolve);
      })
      .catch(error => {
        this.logger.warn("[%s][%s][Error] %s\n%s", __pretty_name__, '_handleClosed', error.message, error.stack);

        setImmediate(reject, error);
      });
  });
};


exports = module.exports = MongoDbManager;
