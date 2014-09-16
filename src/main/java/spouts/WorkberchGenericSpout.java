package main.java.spouts;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WorkberchGenericSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    
    private List<String> spoutFields;
    private SpoutOutputCollector collector;
    private boolean oneTime = true;
    private long index = 0L;

    public WorkberchGenericSpout(final List<String> fields) {
	fields.add(INDEX_FIELD);
	spoutFields = fields;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	this.collector = collector;
    }

    @Override
    public void nextTuple() {
	if (oneTime) {
	    Values values = new Values("2000", index++);
	    collector.emit(values);
	}
	oneTime = false;
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(spoutFields));
    }

}
