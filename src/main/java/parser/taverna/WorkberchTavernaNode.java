package main.java.parser.taverna;

import java.lang.invoke.WrongMethodTypeException;
import java.util.List;

import main.java.bolts.BeanshellBolt;
import main.java.bolts.RestBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.bolts.XPathBolt;
import main.java.parser.model.WorkberchProcessorNode;
import main.java.utils.constants.TavernaNodeType;

import com.fasterxml.jackson.databind.JsonNode;

public class WorkberchTavernaNode extends WorkberchProcessorNode {

	private TavernaNodeType nodeType;
	private JsonNode config;

	public WorkberchTavernaNode(final String name, final List<String> outputs, final List<String> inputs) {
		super(name, outputs, inputs);
	}
	
	public TavernaNodeType getNodeType() {
		return nodeType;
	}

	public void setNodeType(final TavernaNodeType nodeType) {
		this.nodeType = nodeType;
	}

	public JsonNode getConfig() {
		return config;
	}

	public void setConfig(final JsonNode config) {
		this.config = config;
	}

	@Override
	public WorkberchGenericBolt buildBolt(final String guid) {
		
		WorkberchGenericBolt ret = null;
    	switch (nodeType) {
    		case XPATH:
    			ret = new XPathBolt(guid, getInputs(), getOutputs(), config);
    			break;
    		case REST:
    			ret = new RestBolt(guid, getInputs(), getOutputs(), config);
    			break;
    		case BEANSHELL:
    			ret = new BeanshellBolt(guid, getInputs(), getOutputs(), config);
    			break;
    		default:
    			throw new WrongMethodTypeException("No se ha implementado el tipo de processor de taverna: " + nodeType);
    	}
    	
    	return ret;
	}

}
