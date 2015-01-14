package main.java.parser.model;

import java.util.List;

import main.java.bolts.WorkberchGenericBolt;
import main.java.spouts.WorkberchGenericSpout;

public abstract class WorkberchProcessorNode implements WorkberchNode , WorkberchBoltBuilder {

	private final String name;
	private final List<String> outputs;
	private final List<String> inputs;

	public WorkberchProcessorNode(final String name, final List<String> outputs, final List<String> inputs) {
		this.name = name;
		this.outputs = outputs;
		this.inputs = inputs;
	}
	
	@Override
	public String getName() {
		return name;
	}


	public List<String> getOutputs() {
		return outputs;
	}

	public List<String> getInputs() {
		return inputs;
	}

	@Override
	public WorkberchGenericSpout buildSpout() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	abstract public WorkberchGenericBolt buildBolt(final String guid);

}
