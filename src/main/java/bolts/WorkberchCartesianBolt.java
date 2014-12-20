package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.java.utils.ExecutedValue;
import main.java.utils.WorkberchTuple;
import main.java.utils.cartesianindex.CartesianIndex;
import main.java.utils.cartesianindex.CartesianLeaf;
import main.java.utils.cartesianindex.CartesianNode;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;

public class WorkberchCartesianBolt extends WorkberchProvenanceBolt {

	private static final long serialVersionUID = 1L;
	private final Map<String, List<ExecutedValue>> executedInputs = new HashMap<String, List<ExecutedValue>>();
	private boolean uncompleteTuples = true;

	private List<Object> uniteIndexOnCartesianIndex(final List<Object> valuesToEmit) {
		final List<Object> realValues = new ArrayList<Object>();
		final List<CartesianIndex> indexValues = new ArrayList<CartesianIndex>();
		final Iterator<Object> iterValEmit = valuesToEmit.iterator();
		final Iterator<String> iterField = getOutputFields().iterator();
		String field = StringUtils.EMPTY;
		while (iterValEmit.hasNext()) {
			final Object value = iterValEmit.next();
			field = iterField.hasNext() ? (String) iterField.next() : field;
			if (field.startsWith(INDEX_FIELD)) {
				indexValues.add(value instanceof CartesianIndex ? (CartesianIndex) value : new CartesianLeaf((Long) value));
			} else {
				realValues.add(value);
			}
		}
		final CartesianIndex cartesianNode = new CartesianNode(indexValues);
		realValues.add(cartesianNode);
		return realValues;
	}

	private void createTuples(final List<String> remainingFields, final WorkberchTuple baseTuple, final List<List<Object>> valuesToEmit) {
		if (remainingFields.isEmpty()) {
			final List<Object> tupleToEmit = new ArrayList<Object>();
			for (final String field : getOutputFields()) {
				if (field.startsWith(INDEX_FIELD)) {
					for (final String sourceIndex : getRunningNodes()) {
						if (baseTuple.getSource().equals(sourceIndex)) {
							tupleToEmit.add(baseTuple.getValues().get(INDEX_FIELD));
						}
						else {
							tupleToEmit.add(baseTuple.getValues().get(INDEX_FIELD + sourceIndex));
						}						
					}
				} else {
					tupleToEmit.add(baseTuple.getValues().get(field));
				}
			}
			valuesToEmit.add(uniteIndexOnCartesianIndex(tupleToEmit));
		} else {
			final String nextField = remainingFields.get(0);
			remainingFields.remove(0);
			for (final ExecutedValue value : executedInputs.get(nextField)) {
				final Map<String, Object> tupleValues = baseTuple.getValues();
				tupleValues.put(nextField, value.getValue());
				tupleValues.put(value.getIndexCode(), value.getIndex());
				createTuples(remainingFields, baseTuple, valuesToEmit);
			}
		}
	}

	private void addExecutedValues(final WorkberchTuple tuple, final List<String> remainingFields) {
		for (final String field : tuple.getFields()) {
			if (!remainingFields.contains(field) && !field.equals(INDEX_FIELD)) {
				final List<ExecutedValue> values = executedInputs.get(field);
				if (values != null) {
					final ExecutedValue valueExec = new ExecutedValue(tuple.getValues().get(field), INDEX_FIELD + tuple.getSource(), tuple
							.getValues().get(INDEX_FIELD));
					values.add(valueExec);
				}
			}
		}
	}

	public WorkberchCartesianBolt(final List<String> outputFields) {
		super(outputFields);
		for (final String inputField : outputFields) {
			if (!inputField.equals(INDEX_FIELD)) {
				executedInputs.put(inputField, new ArrayList<ExecutedValue>());
			}
		}
	}

	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {
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
			final List<List<Object>> valuesToEmit = new ArrayList<List<Object>>();
			createTuples(remainingFields, input, valuesToEmit);
			final Iterator<List<Object>> iterValue = valuesToEmit.iterator();
			while (iterValue.hasNext()) {
				emitTuple(iterValue.next(), collector, lastValues && !iterValue.hasNext());
			}
		}
	}

}
