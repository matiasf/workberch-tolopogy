package main.java.spouts;

import java.util.List;

import backtype.storm.tuple.Values;

public class InputPortSpout extends WorkberchGenericSpout {
	
	private String defaultValue;
	
	private boolean runDefault;
	private boolean executed;
	

	public InputPortSpout(List<String> fields, String defaultValue, boolean runDefault) {
		super(fields);
		this.defaultValue = defaultValue;
		this.runDefault = runDefault;

	}
	
	private String getValue() {
		//TODO hay que implementar sacar los datos del archivo si no es runDefault
		return defaultValue;
	}

	@Override
	public void nextTuple() {

		if (!runDefault || (runDefault && !executed)) {
			executed = true;
			this.collector.emit(new Values(this.getValue()));
		}

	}

}
