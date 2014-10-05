package main.java.bolts;

import java.util.List;

import main.java.utils.TavernaProcessor;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class WorkberchTavernaProcessorBolt extends WorkberchGenericBolt implements TavernaProcessor{
	
	public WorkberchTavernaProcessorBolt(List<String> outputFields) {
		super(outputFields);
	}
		
	
	public WorkberchTavernaProcessorBolt(List<String> inputFields,
			List<String> outputFields,
			JsonNode node) {
		
		super(outputFields);
		this.initFromJsonNode(node);
		
	}
	
	@Override
	public List<String> getInputPorts() {
		return this.getInputPorts();
	}
	
	@Override
	public List<String> getOutputPorts() {
		return this.getOutputPorts();
	}

	abstract protected void initFromJsonNode(JsonNode jsonNode);

}
