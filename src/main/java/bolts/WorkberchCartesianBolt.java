package main.java.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class WorkberchCartesianBolt extends WorkberchGenericBolt {

	private static final long serialVersionUID = 1L;
	private final Map<String, List<Object>> executedInputs = new HashMap<String, List<Object>>();
	private boolean uncompleteTuples = true;

	private void createTuples(final List<String> remainingFields, final WorkberchTuple baseTuple, final BasicOutputCollector collector) {
		if (remainingFields.isEmpty()) {
			final List<Object> tupleToEmit = new ArrayList<Object>();
			for (final String field : getOutputFields()) {
				tupleToEmit.add(baseTuple.getValues().get(field));
			}
			emitTuple(tupleToEmit, collector);
		} else {
			final String nextField = remainingFields.get(0);
			remainingFields.remove(0);
			for (final Object value : executedInputs.get(nextField)) {
				baseTuple.getValues().put(nextField, value);
				createTuples(remainingFields, baseTuple, collector);
			}
		}
	}

	private void addExecutedValues(final WorkberchTuple tuple, final List<String> remainingFields) {
		for (final String string : tuple.getValues().keySet()) {
			if (!remainingFields.contains(string)) {
				final List<Object> values = executedInputs.get(string);
				if (values != null) {
					values.add(tuple.getValues().get(string));
				}
			}
		}
	}

	public WorkberchCartesianBolt(final List<String> outputFields) {
		super(outputFields);
		for (final String inputField : outputFields) {
			executedInputs.put(inputField, new ArrayList<Object>());
		}
	}

	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
		final List<String> remainingFields = new ArrayList<String>();
		remainingFields.addAll(executedInputs.keySet());
		remainingFields.removeAll(input.getFields());

		addExecutedValues(input, remainingFields);

		if (uncompleteTuples) {
			boolean notAllValues = false;
			for (final String key : executedInputs.keySet()) {
				notAllValues |= executedInputs.get(key).isEmpty();
			}
			uncompleteTuples &= notAllValues;
		}

		if (!uncompleteTuples) {
			createTuples(remainingFields, input, collector);
		}
	}

}
