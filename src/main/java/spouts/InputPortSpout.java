package main.java.spouts;

import java.util.List;

import backtype.storm.tuple.Values;

public class InputPortSpout extends WorkberchGenericSpout {

	private static final long serialVersionUID = 1L;

	private final String defaultValue;
	private final boolean runDefault;
	private boolean executed;

	public InputPortSpout(final List<String> fields, final String defaultValue, final boolean runDefault) {
		super(fields);
		this.defaultValue = defaultValue;
		this.runDefault = runDefault;

	}

	private String getValue() {
		// TODO hay que implementar sacar los datos del archivo si no es
		// runDefault
		return defaultValue;
	}

	@Override
	public void emitNextTuple(final Values values) {
		if (!runDefault || runDefault && !executed) {
			executed = true;
			collector.emit(new Values(getValue()));
		}
	}

}
