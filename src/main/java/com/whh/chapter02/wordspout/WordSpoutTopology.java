package com.whh.chapter02.wordspout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * WordSpoutTopology
 * Created by xuzhuo on 2017/7/30.
 */
public class WordSpoutTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", new WordSpout(), 2);
        topologyBuilder.setBolt("split", new SplitSentenceBolt(), 2).shuffleGrouping("spout");
        topologyBuilder.setBolt("word", new WordCountBolt(), 3).fieldsGrouping("split", new Fields("word"));
        Config conf = new Config();
//        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            try {
                cluster.submitTopology("word-count-demo", conf, topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}
