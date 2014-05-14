var MongoQueue = require('../index');

var mongoQueue = new MongoQueue({
  criteria : {
    test : 'enqueue'
  }
});
console.log("About to enqueue a : 1\n");
mongoQueue.enqueue({ a : 1 }, function(err, newLength) {
  console.log("a : 1 enqueued\n");
  if (newLength === 1) {
    console.log("That was the first element on the queue\n");
  } else {
    console.log("That was not the first element on the queue\n");
  }
  mongoQueue.checkQueue(function(err, queueTop) {
    console.log("The top of the queue is:\n", queueTop);
    console.log("About to enqueue a: 2\n");
    mongoQueue.enqueue({ a : 2 }, function(err, newLength) {
      console.log("a:2 enqueued\n");
      if (newLength === 1) {
        console.log("That was the first element on the queue\n");
      } else {
        console.log("That was not the first element on the queue\n");
      }
      mongoQueue.checkQueue(function(err, queueTop) {
        console.log("the top of the queue is:\n", queueTop);
        mongoQueue.dequeue(function(err, doc) {
          console.log("dequeue'ed doc:\n", doc);
          mongoQueue.dequeue(function(err, doc) {
            console.log("dequeue'ed doc:\n", doc);
            mongoQueue.checkQueue(function(err, queueTop) {
              console.log("The top of the queue is:\n", queueTop);
              mongoQueue.dequeue(function(err, doc) {
                console.log("Finally dequeue'ed doc:\n", doc);
                process.exit();          
              });
            });
          });
        });
      });      
    });
  });
});
