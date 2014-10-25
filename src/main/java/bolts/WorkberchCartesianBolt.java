package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.cartesianindex.CartesianIndex;
import main.java.utils.cartesianindex.CartesianLeaf;
import main.java.utils.cartesianindex.CartesianNode;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;

public class WorkberchCartesianBolt extends WorkberchGenericBolt {

	private static final long serialVersionUID = 1L;
	private final Map<String, List<Object>> executedInputs = new HashMap<String, List<Object>>();
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
			if (field.equals(INDEX_FIELD)) {
				indexValues.add(value instanceof CartesianIndex ? (CartesianIndex) value : new CartesianLeaf((Long) value));
			} else {
				realValues.add(value);
			}
		}
		final CartesianIndex cartesianNode = new CartesianNode(indexValues);
		realValues.add(cartesianNode);
		return realValues;
	}

	private void createTuples(final List<String> remainingFields, final WorkberchTuple baseTuple, final BasicOutputCollector collector) {
		if (remainingFields.isEmpty()) {
			final List<Object> tupleToEmit = new ArrayList<Object>();
			for (final String field : getOutputFields()) {
				tupleToEmit.add(baseTuple.getValues().get(field));
			}
			emitTuple(uniteIndexOnCartesianIndex(tupleToEmit), collector);
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
