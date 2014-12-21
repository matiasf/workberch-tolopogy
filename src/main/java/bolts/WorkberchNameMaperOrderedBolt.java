package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class WorkberchNameMaperOrderedBolt extends WorkberchOrderBolt {

	private static final long serialVersionUID = -755957444551948715L;
	private final Map<String, String> mapedInputs = new HashMap<String, String>();
	private  Long newIndex = (long) 0;
	
	public WorkberchNameMaperOrderedBolt(final List<String> outputFields) {
		super(outputFields, Boolean.TRUE);
	}
	
	public void addLink(final String sourceField, final String toName) {
		mapedInputs.put(toName, sourceField);
	}


	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(getOutputFields()));
	}
	
	@Override
	public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValue) {
		
		
		Object outputValue = null;
		for (final String output : getOutputFields()) {
			if (!output.equals(INDEX_FIELD)) {
				final String inputField = mapedInputs.get(output);
				if (inputField != null) {
					outputValue = input.getValues().get(inputField);
					break;
				}
			}
		}
		
		final Collection<Object> values = (Collection<Object>) outputValue;
		
		for (final Object object : values) {
			final List<Object> emitTuple = new ArrayList<Object>();
			emitTuple.add(object);
			emitTuple.add(newIndex++);
			emitTuple(emitTuple, collector, lastValue);
		}
		
		
		
		
		
	}

}
