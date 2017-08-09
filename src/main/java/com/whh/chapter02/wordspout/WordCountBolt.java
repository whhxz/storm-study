package com.whh.chapter02.wordspout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * WordCountBolt
 * Created by xuzhuo on 2017/7/30.
 */
public class WordCountBolt implements IBasicBolt {
    private Map<String, Integer> counts = new HashMap<>();

    public void prepare(Map map, TopologyContext topologyContext) {

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null){
            count = 0;
        }
        count++;
        counts.put(word, count);
        basicOutputCollector.emit(new Values(word, count));
    }

    public void cleanup() {
        counts.forEach((key, value) -> System.out.println(key + "~~~~~~~~~~~~~" + value));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
