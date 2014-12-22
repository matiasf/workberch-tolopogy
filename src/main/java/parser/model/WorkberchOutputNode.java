package main.java.parser.model;

import main.java.bolts.OutputBolt;
import main.java.bolts.WorkberchGenericBolt;

public class WorkberchOutputNode implements WorkberchBoltBuilder {

	private String name;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	@Override
	public WorkberchGenericBolt buildBolt() {
		final OutputBolt ret = new OutputBolt(true);
		return ret;
	}

}
