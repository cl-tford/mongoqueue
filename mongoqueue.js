var mongodb  = require('mongodb');
var classify = require('classify-js');
var _        = require('underscore');

var db = null;
var connectionString = "mongodb://localhost:27017/test";

var MongoQueue = classify({
  name : "MongoQueue",
  initialize : function(options) {
    this._collectionName = options.collectionName;
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
    enqueue : function(document, callback) {
      var self = this;

      self.getCollection(function(err, collection) {
        if (err) {
          return callback(err);
        }
        _.extend(document, self._criteria);
        collection.insert(document, callback);
      });
    },
    dequeue : function(callback) {
      var self = this;

      self.getCollection(function(err, collection) {
        if (err) {
          return callback(err);
        }
        collection.findAndModify(
          self._criteria,
          [[ '_id', 'asc' ]],
          {},
          { remove : true },
          function(err, modified) {
            callback(err, modified);
          }
        );
      });
    }    
  }
});

module.exports = MongoQueue;
