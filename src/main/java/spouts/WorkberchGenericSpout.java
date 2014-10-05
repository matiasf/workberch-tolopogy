package main.java.spouts;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.TavernaProcessor;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public abstract class WorkberchGenericSpout extends BaseRichSpout implements TavernaProcessor {

    private static final long serialVersionUID = 1L;
    
    private List<String> spoutFields;
    protected SpoutOutputCollector collector;
    private boolean oneTime = true;
    private long index = 0L;

    public WorkberchGenericSpout(final List<String> fields) {
	fields.add(INDEX_FIELD);
	this.spoutFields = fields;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	this.collector = collector;
    }


    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(spoutFields));
    }
    
    @Override
    public List<String> getInputPorts() {
    	return new ArrayList<String>();
    }
	
    @Override
	public List<String> getOutputPorts() {
    	return spoutFields;
    }

}
