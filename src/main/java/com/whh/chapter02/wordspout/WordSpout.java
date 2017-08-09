package com.whh.chapter02.wordspout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * WordSpout
 * Created by xuzhuo on 2017/7/30.
 */
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static final String[] msgs = new String[]{
            "First, you need java and git installed and in your user's PATH.",
            "Also, two of the examples in storm-starter require Python and Ruby.",
            "Next, make sure you have the storm-starter code available on your machine.",
            "Git/GitHub beginners may want to use the following command to download the latest storm-starter code and change to the new directory that contains the downloaded code.",
            "storm-starter contains a variety of examples of using Storm.",
            "If this is your first time working with Storm, check out these topologies first:"
    };
    private static final Random random = new Random();

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        String sentence = msgs[random.nextInt(msgs.length)];
        collector.emit(new Values(sentence));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
