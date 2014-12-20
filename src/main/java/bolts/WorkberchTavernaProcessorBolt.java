package main.java.bolts;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class WorkberchTavernaProcessorBolt extends WorkberchProvenanceBolt {
	
	public WorkberchTavernaProcessorBolt(final List<String> outputFields) {
		super(outputFields);
	}
		
	
	public WorkberchTavernaProcessorBolt(final List<String> inputFields,
			final List<String> outputFields,
			final JsonNode node) {
		
		super(outputFields);
		initFromJsonNode(node);
		
	}
	
	
	public List<String> getInputPorts() {
		return getInputPorts();
	}
	
	
	public List<String> getOutputPorts() {
		return getOutputPorts();
	}

	abstract protected void initFromJsonNode(JsonNode jsonNode);

}
