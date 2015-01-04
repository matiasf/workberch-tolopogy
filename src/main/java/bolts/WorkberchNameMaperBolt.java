package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class WorkberchNameMaperBolt extends WorkberchProvenanceBolt {

	private static final long serialVersionUID = -755957444551948715L;
	
	private final Map<String, String> mapedInputs = new HashMap<String, String>();
	
	public WorkberchNameMaperBolt(final String guid, final List<String> outputFields) {
		super(guid, outputFields);
	}
	
	public void addLink(final String sourceField, final String toName) {
		mapedInputs.put(toName, sourceField);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(getOutputFields()));
	}

	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {
		final List<Object> emitTuple = new ArrayList<Object>();
		
		for (final String output : getOutputFields()) {
			if (!output.equals(INDEX_FIELD)) {
				Object outputValue = null;
				final String inputField = mapedInputs.get(output);

				if (inputField != null) {
					outputValue = input.getValues().get(inputField);
				}
				
				emitTuple.add(outputValue);
			}
		}
		
		emitTuple.add(input.getValues().get(INDEX_FIELD));
		emitTuple(emitTuple, collector, lastValues);
	}

}
