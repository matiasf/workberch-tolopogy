package main.java.parser;

import java.lang.invoke.WrongMethodTypeException;
import java.util.ArrayList;
import java.util.List;

import main.java.spouts.InputPortSpout;
import main.java.spouts.TextConstantSpout;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.constants.TavernaNodeType;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;

public class WorkberchSpoutBuilder {
	
	static public WorkberchGenericSpout buildProcessor(final Processor processor, final Configuration config) {
		WorkberchGenericSpout ret;
		
		final String processorType = config.getType().toString();
		
		final List<String> outputFields = new ArrayList<String>();
		
		for (final OutputProcessorPort outputPort : processor.getOutputPorts()) {
			outputFields.add(outputPort.getName());
		}
		
		switch (TavernaNodeType.valueOf(processorType)) {
			case TEXT_CONSTANT :
				final List<String> outputFieldsTrucho = new ArrayList<String>();
				outputFieldsTrucho.add(processor.getName());
				ret = new TextConstantSpout(outputFieldsTrucho, config.getJson().get("string").asText());
				break;
			default:
				throw new WrongMethodTypeException("No se ha implementado el tipo de processor de taverna: " + processorType);
		}
		
		return ret;
	}
	
	static public WorkberchGenericSpout buildInputPort(final InputPort inputPort) {
		final List<String> outputFields = new ArrayList<String>();
		outputFields.add(inputPort.getName());
		
		final InputPortSpout ret = new InputPortSpout(outputFields, "stem cells", true);
		return ret;
	}

}
