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
    private String streamId;
    private SpoutOutputCollector collector;
    private Random rand;

    public WorkberchGenericSpout(final String streamId, final List<String> fields) {
	spoutFields = fields;
	this.streamId = streamId;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	this.collector = collector;
	this.rand = new Random();
    }

    @Override
    public void nextTuple() {
	Utils.sleep(5000);
	Values values = new Values(streamId, rand.nextInt(10));
	System.out.print("Emitiendo tupla: ");
	for (Object value : values) {
	    System.out.print(value.toString());
	    System.out.print("-");
	}
	System.out.println();
	collector.emit(values);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(spoutFields));
    }

}
