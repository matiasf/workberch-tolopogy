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

public class WorkberchCartesianBolt extends BaseBasicBolt {
    
    private Map<String, List<Object>> executedInputs = new HashMap<String, List<Object>>();
    
    private List<String> fields;
    
    private boolean uncompleteTuples = true;
    
    private void createTuples(List<String> remainingFields, WorkberchTuple baseTuple, BasicOutputCollector collector) {
	if (remainingFields.isEmpty()) {
	    final List<Object> tupleToEmit = new ArrayList<Object>();
	    for (String field : fields) {
		tupleToEmit.add(baseTuple.getValues().get(field));
	    }
	    collector.emit(tupleToEmit);
	} else {
	    String nextField = remainingFields.get(0);
	    remainingFields.remove(0);
	    for (Object value : executedInputs.get(nextField)) {
		baseTuple.getValues().put(nextField, value);
		this.createTuples(remainingFields, baseTuple, collector);
	    }
	}
    }

    private void addExecutedValues(WorkberchTuple tuple, List<String> remainingFields) {
	for (String string : tuple.getValues().keySet()) {
	    if (!remainingFields.contains(string)) {
		List<Object> values = this.executedInputs.get(string);
		if (values != null) {
		    values.add(tuple.getValues().get(string));
		}
	    }
	}
    }
    
    public WorkberchCartesianBolt(final List<String> fields) {
	this.fields = fields;
	for (String inputField : fields) {
	    this.executedInputs.put(inputField, new ArrayList<Object>());
	}
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
	List<String> inputFields = input.getFields().toList();

	WorkberchTuple baseTuple = new WorkberchTuple(input);

	List<String> remainingFields = new ArrayList<String>();
	remainingFields.addAll(this.executedInputs.keySet());
	remainingFields.removeAll(inputFields);

	addExecutedValues(baseTuple, remainingFields);
	
	if (uncompleteTuples) {
	    boolean notAllValues = false;
	    for (String key : executedInputs.keySet()) {
		notAllValues |= executedInputs.get(key).isEmpty();
	    }
	    uncompleteTuples &= notAllValues;
	}

	if (!uncompleteTuples) {
	    createTuples(remainingFields, baseTuple, collector);
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(fields));
    }

}
