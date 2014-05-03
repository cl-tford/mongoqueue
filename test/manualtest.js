var MongoQueue = require('../index');

var mongoQueue = new MongoQueue({
  criteria : {
    test : 'enqueue'
  }
});

mongoQueue.enqueue({ a : 1 }, function(err, newLength) {

  if (newLength === 1) {
    console.log("That was the first element on the queue\n");
  } else {
    console.log("That was not the first element on the queue\n");
  }
  mongoQueue.enqueue({ a : 2 }, function(err, newLength) {
    if (newLength === 1) {
      console.log("That was the first element on the queue\n");
    } else {
      console.log("That was not the first element on the queue\n");
    }
    mongoQueue.dequeue(function(err, doc) {
      console.log("dequeue'ed doc:\n", doc);
      mongoQueue.dequeue(function(err, doc) {
        console.log("dequeue'ed doc:\n", doc);
        mongoQueue.dequeue(function(err, doc) {
          console.log("Finally dequeue'ed doc:\n", doc);
          process.exit();          
        });
      });
    });
  });
});
