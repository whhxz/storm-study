package com.whh.chapter02.wordspout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * SplitSentenceBolt
 * Created by xuzhuo on 2017/7/30.
 */
public class SplitSentenceBolt implements IBasicBolt {
    private static final long serialVersionUID = -7430292574480924019L;

    public void prepare(Map map, TopologyContext topologyContext) {
        System.out.println("prepare~~~~~~~~~~~~~~~~~~~~~");
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String sentence = tuple.getString(0);
        for (String word : sentence.split(" ")) {
            basicOutputCollector.emit(new Values(word));
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
