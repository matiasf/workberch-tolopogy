package main.java.bolts;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class WorkberchTavernaProcessor extends WorkberchGenericBolt {

	public WorkberchTavernaProcessor(final List<String> inputFields, final List<String> outputFields) {
		super(outputFields);
	}

	public WorkberchTavernaProcessor(final List<String> inputFields, final List<String> outputFields, final JsonNode node) {

		super(outputFields);
		initFromJsonNode(node);

	}

	abstract protected void initFromJsonNode(JsonNode jsonNode);

}
