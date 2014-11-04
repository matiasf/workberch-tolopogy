package main.java.bolts;

import java.util.ArrayList;
import java.util.List;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class OutputBoltTruecho extends WorkberchGenericBolt {

	public OutputBoltTruecho(final List<String> outputFields) {
		super(new ArrayList<String>());
	}

	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValue) {
		for (final String param : input.getValues().keySet()) {
			System.out.println("Llego la salida " + param + " - Con valor: " + input.getValues().get(param));
		}

	}

}
