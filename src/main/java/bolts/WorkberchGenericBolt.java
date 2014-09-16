package main.java.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

abstract public class WorkberchGenericBolt extends BaseBasicBolt {
    
    private List<String> outputFields;
    
    protected List<String> getOutputFields() {
	return outputFields;
    }

    public WorkberchGenericBolt(final List<String> inputFields, final List<String> outputFields) {
	this.outputFields = outputFields;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
	WorkberchTuple baseTuple = new WorkberchTuple(input);	
	executeLogic(baseTuple, collector);
    };

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(outputFields));
    }

    public abstract void executeLogic(WorkberchTuple input, BasicOutputCollector collector);

}
