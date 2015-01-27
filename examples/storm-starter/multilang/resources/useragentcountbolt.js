var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function UseragentCountBolt() {
    BasicBolt.call(this);
    this.countObj = {};
};

UseragentCountBolt.prototype = Object.create(BasicBolt.prototype);
UseragentCountBolt.prototype.constructor = UseragentCountBolt;

UseragentCountBolt.prototype.process = function(tup, done) {
        var self = this;
        var userAgent = tup.values[0];

        var count = this.countObj[userAgent] || 0;
        count++;
        this.countObj[userAgent] = count;

        self.emit({tuple: [userAgent, count], anchorTupleId: tup.id}, function(taskIds) {
            self.log(self.countObj + ' sent to task ids - ' + taskIds);
        });

        done();
}

new UseragentCountBolt().run();