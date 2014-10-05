package main.java.bolts;

import java.util.ArrayList;
import java.util.List;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class OutputBoltTruecho extends WorkberchGenericBolt {

	public OutputBoltTruecho(List<String> inputFields, List<String> outputFields) {
		super(inputFields, new ArrayList<String>());
	}

	@Override
	public void executeLogic(WorkberchTuple input,
			BasicOutputCollector collector) {
		for (String param : input.getValues().keySet()) {
			System.out.println("Llego la salida " + param + " - Con valor: " + input.getValues().get(param));
		}

	}

}
