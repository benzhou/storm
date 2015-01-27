var storm       = require('./storm');
var Spout       = storm.Spout;
var MongoDb = require('mongodb');


function viewerSpout() {
    Spout.call(this);
    this.runningTupleId = 0;
    this.pending = {};
    this.visitorQueue = [];

    var self = this;

    var mongoClient = MongoDb.MongoClient;

    mongoClient.connect("mongodb://192.168.169.126:27017/influenceDb", function(err, db){
       var filter = {};

       // set MongoDB cursor options
       var cursorOptions = {
         tailable: true,
         await_data: true,
         numberOfRetries: -1
       };

       var coll = db.collection("ConsumerPostLog");

       // create stream and listen
       var stream = coll.find(filter, cursorOptions).sort({$natural: -1}).stream();

       // call the callback
       stream.on('data', function(document) {
         self.log("======================================================================================");
         self.log("Got data from MongoDB");
         self.visitorQueue.push(document);
       });

    });
};

viewerSpout.prototype = Object.create(Spout.prototype);
viewerSpout.prototype.constructor = viewerSpout;


viewerSpout.prototype.nextTuple = function(done) {
    var self = this;

    var entry = this.visitorQueue.shift();
    this.log("nextTuple called.");
    if(entry){
        self.log("======================>");
        self.log(entry.userAgentString);
        var tup = [entry.userAgentString];
        var id = this.createNextTupleId();
        this.pending[id] = tup;

        //This timeout can be removed if TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS is configured to 100
        setTimeout(function() {
            self.emit({tuple: tup, id: id}, function(taskIds) {
                self.log(tup + ' sent to task ids - ' + taskIds);
            });
            done();
        },100);
    }else{
        self.log("===> Nothing");
        done();
    }

}

viewerSpout.prototype.createNextTupleId = function() {
    var id = this.runningTupleId;
    this.runningTupleId++;
    return id;
}

viewerSpout.prototype.ack = function(id, done) {
    this.log('Received ack for - ' + id);
    delete this.pending[id];
    done();
}

viewerSpout.prototype.fail = function(id, done) {
    var self = this;
    this.log('Received fail for - ' + id + '. Retrying.');
    this.emit({tuple: this.pending[id], id:id}, function(taskIds) {
        self.log(self.pending[id] + ' sent to task ids - ' + taskIds);
    });
    done();
}

var v = new viewerSpout();
v.run();
//v.nextTuple(function(){console.log("=== done")});