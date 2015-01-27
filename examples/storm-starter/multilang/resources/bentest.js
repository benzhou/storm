var storm = require('./storm');
var MongoDb = require('mongodb');
var Spout = storm.Spout;

function Postlog() {
    Spout.call(this);
    this.runningTupleId = 0;
    this.pending = {};

    this.sentenceQueue = [];
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
         self.sentenceQueue.push(document);
       });

    });
};

Postlog.prototype = Object.create(Spout.prototype);
Postlog.prototype.constructor = Postlog;


Postlog.prototype.nextTuple = function(done) {
    var self = this;
    //var sentence = this.getRandomSentence();
    var doc = this.sentenceQueue.shift();

    if(doc){
        self.log("Got the next!");
        self.log(doc.sentence);
        var tup = [doc.sentence];
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
        self.log("Nothing for the next!");
        done();
    }
}

Postlog.prototype.createNextTupleId = function() {
    var id = this.runningTupleId;
    this.runningTupleId++;
    return id;
}

Postlog.prototype.ack = function(id, done) {
    this.log('Received ack for - ' + id);
    delete this.pending[id];
    done();
}

Postlog.prototype.fail = function(id, done) {
    var self = this;
    this.log('Received fail for - ' + id + '. Retrying.');
    this.emit({tuple: this.pending[id], id:id}, function(taskIds) {
        self.log(self.pending[id] + ' sent to task ids - ' + taskIds);
    });
    done();
}

new Postlog().run();
