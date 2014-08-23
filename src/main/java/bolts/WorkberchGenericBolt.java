package main.java.bolts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

abstract public class WorkberchGenericBolt extends BaseBasicBolt {

    private Map<String, List<Object>> streamsValues = new HashMap<String, List<Object>>();

    private List<String> streamsKeysOrdered = new ArrayList<String>();

    private boolean uncompleteTuples = true;

    private void createTuples(String streamId, Tuple input, List<String> streamsKeys, BasicOutputCollector collector,
	    List<Object> currentTuple) {
	if (streamsKeys.isEmpty()) {
	    WorkberchTuple tuple = new WorkberchTuple();
	    List<Values> values = new ArrayList<Values>();
	    for (Object value : currentTuple) {
		values.addAll((List<Values>) value);
	    }
	    tuple.setValues(values);
	    executeLogic(tuple, collector);
	} else {
	    String firstStream = streamsKeys.get(0);
	    if (firstStream.equals(streamId)) {
		currentTuple.add(input.getValues().subList(1, input.getValues().size()));
		List<String> subKeys = new ArrayList<String>(streamsKeys.size());
		for (String key : streamsKeys) {
		    subKeys.add(key);
		}
		subKeys = subKeys.subList(1, subKeys.size());
		createTuples(streamId, input, subKeys, collector, currentTuple);
		currentTuple.remove(currentTuple.size() - 1);
	    } else {
		for (Object streamValue : streamsValues.get(streamsKeys.get(0))) {
		    Tuple tuple = (Tuple) streamValue;
		    currentTuple.add(tuple.getValues().subList(1, tuple.getValues().size()));
		    List<String> subKeys = new ArrayList<String>(streamsKeys.size());
		    for (String key : streamsKeys) {
			subKeys.add(key);
		    }
		    subKeys = subKeys.subList(1, subKeys.size());
		    createTuples(streamId, input, subKeys, collector, currentTuple);
		    currentTuple.remove(currentTuple.size() - 1);
		}
	    }
	}
    }

    public WorkberchGenericBolt(List<String> streams) {
	for (Iterator<String> iterator = streams.iterator(); iterator.hasNext();) {
	    final String stream = iterator.next();
	    streamsValues.put(stream, new ArrayList<Object>());
	    streamsKeysOrdered.add(stream);
	}
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
	String streamId = (String) input.getValueByField("stream");
	
	List<Object> values = streamsValues.get(streamId);
	values.add(input);

	if (uncompleteTuples) {
	    boolean notAllValues = false;
	    for (String key : streamsValues.keySet()) {
		notAllValues |= streamsValues.get(key).isEmpty();
	    }
	    uncompleteTuples &= notAllValues;
	}

	if (!uncompleteTuples) {
	    createTuples(streamId, input, streamsKeysOrdered, collector, new ArrayList<Object>());
	}	
    };

    public abstract void executeLogic(WorkberchTuple input, BasicOutputCollector collector);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// TODO Auto-generated method stub
    }

}
