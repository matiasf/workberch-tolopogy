package main.java.bolts;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class WorkberchTavernaProcessorBolt extends WorkberchProvenanceBolt {

	private static final long serialVersionUID = 1L;

	public WorkberchTavernaProcessorBolt(final String guid, final List<String> outputFields) {
		super(guid, outputFields);
	}

	public WorkberchTavernaProcessorBolt(final String guid, final List<String> inputFields, final List<String> outputFields, final JsonNode node) {
		super(guid, outputFields);
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
