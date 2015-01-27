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
 * Created by benzhou on 10/9/14.
 */
public class visitorCountTopology {
    public static class VisitorSpout extends ShellSpout implements IRichSpout {

        public VisitorSpout() {
            //super("node", "bentest.js");
            super("node", "viewerSpout.js");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("visitor"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class VisitorCount2 extends ShellBolt implements IRichBolt {

        public VisitorCount2() {
            super("node", "useragentcountbolt.js");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class VisitorCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String jsonStr = tuple.getString(0);
            Integer count = counts.get(jsonStr);
            if (count == null)
                count = 0;
            count++;
            counts.put(jsonStr, count);
            collector.emit(new Values(jsonStr, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("jsonStr", "count"));
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new VisitorSpout(), 1);

        builder.setBolt("count", new VisitorCount2(), 12).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("visitor-count", conf, builder.createTopology());

            Thread.sleep(120000);

            cluster.shutdown();
        }
    }

}
