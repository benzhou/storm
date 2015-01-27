package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by benzhou on 10/15/14.
 */
public class IpCount {
    public static class IPSpout extends ShellSpout implements IRichSpout {

        public IPSpout() {
            //super("node", "bentest.js");
            super("node", "ipspout.js");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("ip"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class IPBolt extends ShellBolt implements IRichBolt {

        public IPBolt() {
            super("node", "ipbolt.js");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("ip", "count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class IPBolt2 extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String ip = tuple.getString(0);
            Integer count = counts.get(ip);
            if (count == null)
                count = 0;
            count++;
            counts.put(ip, count);
            collector.emit(new Values(ip, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("ip", "count"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("visitorlog", new IPSpout(), 1);

        builder.setBolt("ipcount", new IPBolt2(), 2).fieldsGrouping("visitorlog", new Fields("ip"));
        //builder.setBolt("bolt", new IPBolt2(), 2).shuffleGrouping("visitorlog");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ip-count", conf, builder.createTopology());

            Thread.sleep(120000);

            cluster.shutdown();
        }
    }
}
