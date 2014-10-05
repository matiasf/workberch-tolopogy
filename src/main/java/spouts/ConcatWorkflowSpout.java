package main.java.spouts;

import java.util.List;

import backtype.storm.tuple.Values;

public class ConcatWorkflowSpout extends WorkberchGenericSpout {

	private static final long serialVersionUID = 860660651400110573L;
	
	public ConcatWorkflowSpout(final List<String> fields) {
		super(fields);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void emitNextTuple(final Values values) {
		collector.emit(values);
	}

}
