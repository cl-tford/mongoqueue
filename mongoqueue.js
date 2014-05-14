var mongodb  = require('mongodb');
var classify = require('classify-js');
var _        = require('underscore');

var db = null;
var connectionString = "mongodb://localhost:27017/test";

var defaultCollectionName = 'queues';

var queueField = "q";

var queueFields = {};
queueFields[queueField] = 1;

var enqueueUpdateTemplate = {
  "$push" : {}
};

var dequeueUpdateTemplate = {
  "$pop" : {}
};

dequeueUpdateTemplate.$pop[queueField] = -1; // Pop from front, aka "shift"

var exists = {};

var MongoQueue = classify({
  name : "MongoQueue",
  initialize : function(options) {
    this._collectionName = options.collectionName || defaultCollectionName;
    this._criteria = options.criteria || {};
    this._collection = null;
  },
  classMethods : {
    getDb : function(callback) {
      if (db) {
        return callback(null, db);        
      }      
      mongodb.connect(connectionString, function(err, connection) {
        if (err) {
          return callback(err);
        }
        db = connection;
        callback(null, db);
      });
    },
    getQueueField : function() {
      return queueField;
    },
    getQueueFields : function() {
      return queueFields;
    },
    getEnqueueUpdate : function(document) {
      var enqueueUpdate = _.extend({}, enqueueUpdateTemplate);
      
      enqueueUpdate.$push[this.getQueueField()] = document;
      return enqueueUpdate;
    },
    getDequeueUpdate : function() {
      return dequeueUpdateTemplate;
    }
  },
  instanceMethods : {
    getCriteria : function() {
      return this._criteria;
    },
    getCollection : function(callback) {
      var self = this;
      
      if (self._collection) {
        return callback(null, self._collection);
      }
      MongoQueue.getDb(function(err, db) {
        if (err) {
          return callback(err);
        }
        db.collection(self._collectionName, function(err, collection) {
          if (err) {
            return callback(err);
          }
          self._collection = collection;
          callback(null, self._collection);
        });
      });
    },
    checkQueue : function(callback) {
      var self = this;

      self.getCollection(function(err, collection) {
        if (err) {
          return callback(err);
        }
        self._ensureExistence(function(err) {
          collection.findOne(
            self.getCriteria(),
            MongoQueue.getQueueFields(),
            function(err, queueDocument) {
              var queueArray = null;
              var queueTop = null;
              
              if (err) {
                return callback(err);
              }
              queueArray = queueDocument[MongoQueue.getQueueField()];
              queueTop = queueArray[queueArray.length - 1];
              callback(null, queueTop);
            }
          );          
        });  
      });
    },
    enqueue : function(document, callback) {
      var self = this;
      var enqueueUpdate = MongoQueue.getEnqueueUpdate(document);
      
      self.getCollection(function(err, collection) {
        if (err) {
          return callback(err);
        }
        self._ensureExistence(function(err) {
          if (err) {
            return callback(err);
          }
console.log("Inside /Users/terranceford/vagrant/src/mongoqueue/mongoqueue.js.enqueue, about to collection.findAndModify, with criteria:\n", self.getCriteria());
          collection.findAndModify(
//            self.getCriteria,   // query
            self.getCriteria(), // query
            [[ "_id", "asc" ]], // sort
            enqueueUpdate,      // update "doc"
            {                   // options
              "new" : true      // callback with the newest version.
            },
            function(err, newDocument) {
              var newLength = null;
              
              if (err) {
                return callback(err);
              }
              newLength = newDocument[MongoQueue.getQueueField()].length;
              callback(null,  newLength);
            }
          );
        });
      });
    },
    dequeue : function(callback) {
      var self = this;
      var dequeueUpdate = MongoQueue.getDequeueUpdate();
      
      self.getCollection(function(err, collection) {
        if (err) {
          return callback(err);
        }
        collection.findAndModify(
          self._criteria,     // query
          [[ '_id', 'asc' ]], // sort
          dequeueUpdate,      // update "doc"
          { new : false },    // options : return old version.
          function(err, oldDocument) {
            var result = null;

            if (err) {
              return callback(err);
            }
            result = oldDocument[MongoQueue.getQueueField()][0];
            callback(null, result);
          }
        );
      });
    },
    _ensureExistence : function(callback) {
      var self = this;
      var existenceKey = self._makeExistenceKey();
      
      if (exists[existenceKey]) {
        return callback(null);
      }
      self._findOrCreate(function(err, queueDocument) {
        var queueDocumentId = null;
        
        if (err) {
          return callback(err);
        }
        queueDocumentId = queueDocument._id;
        exists[existenceKey] = queueDocumentId;
        callback(null, queueDocumentId);
      });
    },
    _makeExistenceKey : function() {
      var keyObject = {
        collectionName : this._collectionName,
        criteria       : this._criteria
      };

      return JSON.stringify(keyObject);
    },
    _findOrCreate : function(callback) {
      var self = this;

      self.getCollection(function(err, collection) {
        if (err) {
          return callback(err);
        }
        collection.findAndModify(
          { test : "enqueue" }, // query
          [[ "_id", "asc" ]],   // sort
          {                     // update "doc:"
            "$setOnInsert" : self._newQueueDocument()
          },                 
          {                     // options
            upsert : true,
            "new"    : true // Return newly-inserted doc.
          },
          callback
        );
      });
    },
    _newQueueDocument : function() {
      var newQueueDocument = _.extend({}, this.getCriteria());
      
      newQueueDocument[MongoQueue.getQueueField()] = [];
      return newQueueDocument;
    }
  }
});

module.exports = MongoQueue;
                          
