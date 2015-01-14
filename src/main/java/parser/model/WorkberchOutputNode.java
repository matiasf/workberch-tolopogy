package main.java.parser.model;

import main.java.bolts.OutputBolt;
import main.java.bolts.WorkberchGenericBolt;

public class WorkberchOutputNode implements WorkberchBoltBuilder {

	private String name;
	private String outputPath;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}
	
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(final String outputPath) {
		this.outputPath = outputPath;
	}

	@Override
	public WorkberchGenericBolt buildBolt(final String guid) {
		final OutputBolt ret = new OutputBolt(guid, true, outputPath);
		return ret;
	}

}
