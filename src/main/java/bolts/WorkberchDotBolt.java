package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class WorkberchDotBolt extends WorkberchProvenanceBolt {

	private static final long serialVersionUID = 1L;

	private final Map<Long, Map<String, Object>> vectorsMap = new HashMap<Long, Map<String, Object>>();
	
	public WorkberchDotBolt(final String guid, final List<String> outputFields) {
		super(guid, new ArrayList<String>(outputFields));
	}
	
	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValue, final String uuid) {
		final Long index = Long.valueOf(input.getValues().get(INDEX_FIELD).toString());
		final Map<String, Object> value = vectorsMap.containsKey(index) ? vectorsMap.get(index) : new HashMap<String, Object>();

		final List<String> notPresentFields = new ArrayList<String>();
		notPresentFields.addAll(getOutputFields());
		notPresentFields.removeAll(input.getFields());

		final List<String> presentFields = new ArrayList<String>();
		presentFields.addAll(getOutputFields());
		presentFields.removeAll(notPresentFields);

		for (final String field : presentFields) {
			value.put(field, input.getValues().get(field));
		}

		if (value.keySet().size() == getOutputFields().size()) {
			final List<Object> tupleToEmit = new ArrayList<Object>();
			for (final String field : getOutputFields()) {
				tupleToEmit.add(value.get(field));
			}
			emitTuple(tupleToEmit, collector, lastValue, uuid);
			vectorsMap.remove(index);
		} else {
			vectorsMap.put(index, value);
		}
	}

}
