var assert     = require('chai').assert;
var MongoQueue = require('../index');
var _          = require('underscore');

function toJSON(mongodoc) {
  return JSON.parse(JSON.stringify(mongodoc));
}

var testCollectionName = 'testqueue';

describe('MongoQueue', function() {
  after(function(done) {
    MongoQueue.getDb(function(err, db) {
      assert.equal(err, null, "Got no error retrieving the db for cleanup");
      db.collection(testCollectionName, function(err, collection) {
        assert.equal(err, null, "Got no error retrieving the collection for cleanup");
        collection.remove(function(err) {
          assert.equal(err, null, "Got no error clearing out the collection");
          done();
        });
      });
    });
  });

  describe('enqueue', function() {
    var mongoQueue = new MongoQueue({
      collectionName : testCollectionName,
      criteria : {
        test : 'enqueue'
      }
    });

    after(function(done) {
      mongoQueue.getCollection(function(err, collection) {
        assert.equal(err, null, "Got no error retrieving the collection");
        collection.remove(mongoQueue.getCriteria(), function(err) {
          assert.equal(err, null, "Got no error clearing out the collection");
          done();
        });
      });
    });

    beforeEach(function(done) {
      mongoQueue.getCollection(function(err, collection) {
        assert.equal(err, null, "Got no error retrieving the collection");
        collection.remove(mongoQueue.getCriteria(), function(err) {
          assert.equal(err, null, "Got no error clearing out the collection");
          done();
        });
      });
    });

    it('should add a new element to the queue, when all goes well', function(done) {
      var testDoc = { a : 1 };

      mongoQueue.enqueue(testDoc, function(err, newLength) {
        assert.equal(newLength, 1, "Got the right new length");
        mongoQueue.getCollection(function(err, collection) {
          collection.find(mongoQueue.getCriteria()).toArray(function(err, docs) {
            var queueArray = null;

            assert.equal(docs.length, 1);
            queueArray = docs[0][MongoQueue.getQueueField()];
            assert.deepEqual(toJSON(queueArray), toJSON([testDoc]));
            
            done();
          });
        });
      });
    }); // End enqueue when all goes well.

  }); // End enqueue block.

  describe('dequeue', function() {
    var mongoQueue = new MongoQueue({
      collectionName : testCollectionName,
      criteria : {
        "test" : "dequeue"
      }
    });
    var testDoc = {a:1};

    beforeEach(function(done) {
      mongoQueue.getCollection(function(err, collection) {
        var rawDoc = mongoQueue._newQueueDocument();

        rawDoc[MongoQueue.getQueueField()].push(testDoc);
        assert.equal(err, null, "No error getting the collection");
        collection.insert(rawDoc, function(err, insertedDocs) {
          assert.equal(err, null, "No error inserting the doc.");
          done();
        });
      });
    });
    
    it('should remove a doc from the queue, when all goes well.', function(done) {
      mongoQueue.dequeue(function(err, doc) {
        assert.equal(err, null, "Got no error dequeing the document");
        assert.deepEqual(toJSON(doc), toJSON(testDoc), "Dequeued the right document");
        mongoQueue.getCollection(function(err, collection) {
          assert.equal(err, null, "No error getting the collection afterwards");
          collection.find(mongoQueue.getCriteria()).toArray(function(err, docs) {
            var queueArray = null;

            assert.equal(err, null, "No error getting the docs afterwards");
            assert.equal(docs.length, 1, "Got only one doc");
            queueArray = docs[0][MongoQueue.getQueueField()];
            assert.deepEqual(queueArray, [], "No more elements in the queue");
            done();
          });
        });
      });
    });

  }); // End dequeue block.
});
