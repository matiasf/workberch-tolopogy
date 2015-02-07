package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class WorkberchCartesianDummyBolt extends WorkberchProvenanceBolt {

	private static final long serialVersionUID = 1L;

	private final String flowField;
	private final int singleValuesSize;
	private final Map<String, Object> singleValues = new HashMap<String, Object>();
	private final List<WorkberchTuple> waitingTuples = new ArrayList<WorkberchTuple>();
	
	private void emitTuple(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues, final String uuid) {
		final List<Object> emitTuple = new ArrayList<Object>();
		for (final String field : getOutputFields()) {
			if (!field.equals(flowField)) {
				emitTuple.add(singleValues.get(field));
			}
			else {
				emitTuple.add(input.getValues().get(flowField));
			}
		}
		emitTuple.add(input.getValues().get(INDEX_FIELD));
		emitTuple(emitTuple, collector, lastValues, uuid);
	}

	public WorkberchCartesianDummyBolt(final String guid, final List<String> outputFields, final String flowField) {
		super(guid, outputFields);
		singleValuesSize = outputFields.size() - 1;
		this.flowField = flowField;
	}

	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues, final String uuid) {
		if (!input.getValues().containsKey(flowField)) {
			for (final String field: input.getFields()) {
				if (!field.equals(INDEX_FIELD)) {
					singleValues.put(field, input.getValues().get(field));
				}
			}
			if (singleValues.size() >= singleValuesSize) {
				final Iterator<WorkberchTuple> iterWaiting = waitingTuples.iterator();
				while (iterWaiting.hasNext()) {
					emitTuple(iterWaiting.next(), collector, lastValues && !iterWaiting.hasNext(), uuid);
				}
			}
			else {
				waitingTuples.add(input);
			}
		} else if (input.getValues().containsKey(flowField) && singleValues.size() >= singleValuesSize) {
			emitTuple(input, collector, lastValues, uuid);
		}

	}

}
