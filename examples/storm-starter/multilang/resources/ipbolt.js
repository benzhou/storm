var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function IPBolt() {
    BasicBolt.call(this);
    //this.countObj = {};

    this.log("====> bolt initialized");
};

IPBolt.prototype = Object.create(BasicBolt.prototype);
IPBolt.prototype.constructor = IPBolt;

IPBolt.prototype.process = function(tup, done) {
        this.log("====> bolt process called");
        var self = this;
        var ip = tup.values[0];

        var count = this.countObj[ip] || 0;
        count++;
        this.countObj[ip] = count;

        self.emit({tuple: [ip, count], anchorTupleId: tup.id}, function(taskIds) {
            self.log([ip, count] + ' sent to task ids - ' + taskIds);
        });

        done();
}

new IPBolt().run();