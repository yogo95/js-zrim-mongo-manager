describe("Unit Test - MongoDbManager", function () {
  const MongoDbManager = require('./../../index'),
    LoggerMock = require('js-zrim-core').mocks.LoggerMock,
    _ = require('lodash');

  /**
   * Returns a new instance for test
   * @return {MongoDbManager} The instance to test
   */
  function createInstance() {
    return new MongoDbManager({
      loggerTarget: new LoggerMock()
    });
  }

  /**
   * The default value for the property connectionTimeoutMs
   * @type {number}
   */
  const EXPECTED_DEFAULT__connectionTimeoutMs = 2000;

  describe("#construct", function () {
    it("without operator new Then must return new instance", function () {
      const a = MongoDbManager(), b = MongoDbManager();

      expect(a).toEqual(jasmine.any(MongoDbManager));
      expect(b).toEqual(jasmine.any(MongoDbManager));
      expect(a).not.toBe(b);
    });

    it("with operator new Then must return new instance", function () {
      const a = new MongoDbManager(), b = new MongoDbManager();

      expect(a).toEqual(jasmine.any(MongoDbManager));
      expect(b).toEqual(jasmine.any(MongoDbManager));
      expect(a).not.toBe(b);
    });

    it("Then must default value", function () {
      const instance = createInstance();

      expect(instance.properties.mongoDbDriver).toBe(require('mongodb').MongoClient);
      expect(instance.properties.connectionTimeoutMs).toEqual(2000);
      expect(instance.properties.virtualCollections).toEqual({});
    });
  }); // #construct

  describe("##properties", function () {
    describe("#connectionTimeoutMs", function () {
      describe("#get", function () {
        it("Then must return expected value", function () {
          const instance = createInstance();

          instance.properties.connectionTimeoutMs = 12345678;
          expect(instance.connectionTimeoutMs).toEqual(12345678);
        });
      }); // #get

      describe("#set", function () {
        it("Given same value Then must do nothing", function () {
          const instance = createInstance();

          instance.properties.connectionTimeoutMs = 12345678;
          instance.connectionTimeoutMs = 12345678;
          expect(instance.properties.connectionTimeoutMs).toEqual(12345678);
        });

        it("Given null Then must set default value", function () {
          const instance = createInstance();

          instance.properties.connectionTimeoutMs = 12345678;
          instance.connectionTimeoutMs = null;
          expect(instance.properties.connectionTimeoutMs).toEqual(EXPECTED_DEFAULT__connectionTimeoutMs);
        });

        it("Given positive number Then must set new value", function () {
          const instance = createInstance();

          instance.properties.connectionTimeoutMs = 12345678;
          instance.connectionTimeoutMs = 12;
          expect(instance.properties.connectionTimeoutMs).toEqual(12);
        });

        it("Given negative number or 0 Then must do nothing", function () {
          const instance = createInstance();

          instance.properties.connectionTimeoutMs = 12345678;
          instance.connectionTimeoutMs = -1;
          expect(instance.properties.connectionTimeoutMs).toEqual(12345678);

          instance.connectionTimeoutMs = 0;
          expect(instance.properties.connectionTimeoutMs).toEqual(12345678);
        });
      }); // #set
    }); // #connectionTimeoutMs

    describe("#collections", function () {
      describe("#get", function () {
        it("Then must return internal property mongoDbCollections", function () {
          const instance = createInstance();

          instance.properties.virtualCollections = {
            a: 2
          };
          expect(instance.collections).toBe(instance.properties.virtualCollections);
        });
      }); // #get
    }); // #collections

    describe("#nativeInstance", function () {
      describe("#get", function () {
        it("Then must return internal property mongoDbInstance", function () {
          const instance = createInstance();

          instance.properties.mongoDataBase = {
            a: 2
          };
          expect(instance.nativeInstance).toBe(instance.properties.mongoDataBase);
        });
      }); // #get
    }); // #nativeInstance
  }); // ##properties

  describe("#getCollectionByName", function () {
    it("Given no string Then must return undefined", function () {
      const instance = createInstance();

      expect(instance.getCollectionByName(undefined)).toBeUndefined();
      expect(instance.getCollectionByName(null)).toBeUndefined();
      expect(instance.getCollectionByName({})).toBeUndefined();
    });

    it("Given valid name but not found Then must return undefined", function () {
      const instance = createInstance();

      instance.properties.mongoDbCollections = undefined;
      expect(instance.getCollectionByName("a")).toBeUndefined();

      instance.properties.mongoDbCollections = {};
      expect(instance.getCollectionByName("a")).toBeUndefined();
    });

    it("Given valid name and exists Then must return expected collection", function () {
      const instance = createInstance();

      instance.properties.mongoDbCollections = {
        a: {
          a: 1
        }
      };
      expect(instance.getCollectionByName("a")).toBe(instance.properties.mongoDbCollections.a);
    });
  }); // #getCollectionByName

  describe("#_handleInitialization", function () {
    const Joi = require('joi');

    it("Given invalid connection string Then must return error", function (testDone) {
      const instance = createInstance();

      spyOn(Joi, 'validate').and.callThrough();
      const options = {
        connectionString: ""
      };
      instance._handleInitialization(options)
        .then(() => {
          expect("Must not be called").toBeUndefined();
          testDone();
        })
        .catch(error => {
          expect(error).toEqual(jasmine.any(TypeError));
          expect(Joi.validate).toHaveBeenCalledWith(options, jasmine.any(Object), jasmine.any(Function));
          testDone();
        });
    });

    it("Given invalid CollectionOption.name Then must return error", function (testDone) {
      const instance = createInstance();

      spyOn(Joi, 'validate').and.callThrough();
      const options = {
        connectionString: "12",
        collections: [{
          name: ""
        }]
      };
      instance._handleInitialization(options)
        .then(() => {
          expect("Must not be called").toBeUndefined();
          testDone();
        })
        .catch(error => {
          expect(error).toEqual(jasmine.any(TypeError));
          expect(Joi.validate).toHaveBeenCalledWith(options, jasmine.any(Object), jasmine.any(Function));
          testDone();
        });
    });

    it("Given invalid connectionTimeoutMs Then must return error", function (testDone) {
      const instance = createInstance();

      spyOn(Joi, 'validate').and.callThrough();
      const options = {
        connectionTimeoutMs: 1,
        connectionString: "12",
        collections: [{
          name: "aa",
          index: []
        }]
      };
      instance._handleInitialization(options)
        .then(() => {
          expect("Must not be called").toBeUndefined();
          testDone();
        })
        .catch(error => {
          expect(error).toEqual(jasmine.any(TypeError));
          expect(Joi.validate).toHaveBeenCalledWith(options, jasmine.any(Object), jasmine.any(Function));
          testDone();
        });
    });

    it("Given valid options Then must return success", function (testDone) {
      const instance = createInstance();

      spyOn(Joi, 'validate').and.callThrough();
      const options = {
        connectionTimeoutMs: 999999,
        connectionString: "12",
        collections: [{
          name: "aa",
          index: [
            {
              native: {
                keys: {
                  a: 1,
                  b: 2
                },
                options: {
                  a: 3
                }
              }
            }
          ]
        }]
      };
      instance._handleInitialization(options)
        .then(() => {
          expect(Joi.validate).toHaveBeenCalledWith(options, jasmine.any(Object), jasmine.any(Function));
          expect(instance.properties.mongoDbOptions.connectionString).toEqual(options.connectionString);
          expect(instance.properties.mongoDbOptions.collections).toEqual(options.collections);
          expect(instance.properties.mongoDbOptions.collections).not.toBe(options.collections);
          expect(instance.properties.connectionTimeoutMs).toEqual(999999);
          testDone();
        })
        .catch(error => {
          console.log(error.message);
          expect("Must not be called").toBeUndefined();
          expect(error).toBeUndefined();
          testDone();
        });
    });
  }); // #_handleInitialization

  describe("#_handlePostConnection", function () {
    it("Given error Then must return error", function (testDone) {
      const instance = createInstance();

      const expectedError = new Error("Unit Test - Fake error");
      instance._handlePostConnection({}, expectedError)
        .then(() => {
          expect("Must not be called").toBeUndefined();
          testDone();
        })
        .catch(error => {
          expect(error).toBe(expectedError);
          testDone();
        });
    });

    it("Given not error Then must return success", function (testDone) {
      const instance = createInstance();

      instance._handlePostConnection({}, undefined)
        .then(() => {
          expect(true).toBeTruthy();
          testDone();
        })
        .catch(error => {
          expect("Must not be called").toBeUndefined();
          expect(error).toBeUndefined();
          testDone();
        });
    });
  }); // #_handlePostConnection

  describe("#_handleDisconnection", function () {
    it("Given mongoDataBase.close to fail Then return error", function (testDone) {
      const instance = createInstance();

      const expectedError = new Error("Unit Test - Fake Error");
      instance.properties.mongoDataBase = {
        close: jasmine.createSpy("close").and.callFake(() => new Promise((resolve, reject) => setImmediate(reject, expectedError)))
      };

      instance._handleDisconnection()
        .then(() => {
          expect("Must not be called").toBeUndefined();
          testDone();
        })
        .catch(error => {
          expect(error).toBe(expectedError);
          expect(instance.properties.mongoDataBase.close).toHaveBeenCalled();
          testDone();
        });
    });

    it("Given close succeed and currentConnexionContext undefined Then must success", function (testDone) {
      const instance = createInstance();

      const closeMock = jasmine.createSpy("close").and.callFake(() => new Promise(resolve => setImmediate(resolve)));
      instance.properties.mongoDataBase = {
        close: closeMock
      };
      instance.properties.mongoClientInstance = {};

      instance._handleDisconnection()
        .then(() => {
          expect(closeMock).toHaveBeenCalled();
          expect(instance.properties.mongoDataBase).toBeUndefined();
          expect(instance.properties.mongoClientInstance).toBeUndefined();
          testDone();
        })
        .catch(error => {
          expect("Must not be called").toBeUndefined();
          expect(error).toBeUndefined();
          testDone();
        });
    });

    it("Given close succeed and currentConnexionContext defined Then must success and release resources", function (testDone) {
      const instance = createInstance();

      const closeMock = jasmine.createSpy("close").and.callFake(() => new Promise(resolve => setImmediate(resolve)));
      instance.properties.mongoDataBase = {
        close: closeMock
      };
      instance.properties.mongoClientInstance = {};
      const currentConnexionContext = {
        freeContext: jasmine.createSpy("freeContext")
      };
      instance.properties.currentConnexionContext = currentConnexionContext;

      instance._handleDisconnection()
        .then(() => {
          expect(closeMock).toHaveBeenCalled();
          expect(instance.properties.mongoDataBase).toBeUndefined();
          expect(instance.properties.mongoClientInstance).toBeUndefined();
          expect(currentConnexionContext.freeContext).toHaveBeenCalledTimes(1);
          testDone();
        })
        .catch(error => {
          expect("Must not be called").toBeUndefined();
          expect(error).toBeUndefined();
          testDone();
        });
    });
  }); // #_handleDisconnection
});
