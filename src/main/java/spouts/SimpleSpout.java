package main.java.spouts;

import java.util.List;

import backtype.storm.tuple.Values;

public class SimpleSpout extends WorkberchGenericSpout {

	private static final long serialVersionUID = 5528236980790637197L;

	private final List<Values> valuesToEmit;

	public SimpleSpout(final List<String> fields, final List<Values> valuesToEmit) {
		super(fields);
		this.valuesToEmit = valuesToEmit;
	}

	@Override
	public void emitNextTuple(final Values values) {
		collector.emit(values);
	}

	@Override
	public List<Values> getValues() {
		return valuesToEmit;
	}

}
