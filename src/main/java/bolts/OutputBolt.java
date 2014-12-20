package main.java.bolts;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class OutputBolt extends WorkberchOrderBolt {

	private static final long serialVersionUID = 1L;
	
	public OutputBolt(final Boolean ordered) {
		super(new ArrayList<String>(), ordered);
	}

	@Override
	public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {
		try {
			PrintWriter output = new PrintWriter("/home/proyecto/Code/workberch-server/Files/gid/out/out.xml");
			output.write("Valor" + input.getValues().get("out"));
			output.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
