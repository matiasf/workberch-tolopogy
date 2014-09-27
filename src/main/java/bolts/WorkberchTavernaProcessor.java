package main.java.bolts;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class WorkberchTavernaProcessor extends WorkberchGenericBolt {
	
	public WorkberchTavernaProcessor(List<String> inputFields,
			List<String> outputFields) {
		super(inputFields, outputFields);
	}
		
	
	public WorkberchTavernaProcessor(List<String> inputFields,
			List<String> outputFields,
			JsonNode node) {
		
		super(inputFields, outputFields);
		this.initFromJsonNode(node);
		
	}

	abstract protected void initFromJsonNode(JsonNode jsonNode);

}
