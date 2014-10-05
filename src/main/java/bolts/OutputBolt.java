package main.java.bolts;

import java.util.ArrayList;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class OutputBolt extends WorkberchOrderBolt {

	private static final long serialVersionUID = 1L;
	
	public OutputBolt(final Boolean ordered) {
		super(new ArrayList<String>(), ordered);
	}

	@Override
	public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector) {
		System.out.println("Valor" + input.getValues().get("out"));
	}

}
