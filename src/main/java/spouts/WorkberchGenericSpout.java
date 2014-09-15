package main.java.spouts;

import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WorkberchGenericSpout extends BaseRichSpout {

    private List<String> spoutFields;
    private SpoutOutputCollector collector;
    private Random rand;
    private boolean oneTime = true;

    public WorkberchGenericSpout(final List<String> fields) {
	spoutFields = fields;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	this.collector = collector;
	this.rand = new Random();
    }

    @Override
    public void nextTuple() {
	//if (oneTime) {
	    Values values = new Values("2000");
	    collector.emit(values);
	//}
	//oneTime = false;
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(spoutFields));
    }

}
