package main.java.parser;

import java.lang.invoke.WrongMethodTypeException;
import java.util.ArrayList;
import java.util.List;

import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;
import main.java.bolts.OutputBoltTruecho;
import main.java.bolts.RestBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.bolts.WorkberchTavernaProcessorBolt;
import main.java.bolts.XPathBolt;

public class WorkberchBoltBuilder {
	

	
	static public WorkberchTavernaProcessorBolt buildProcessor(Processor processor, Configuration config) {
		WorkberchTavernaProcessorBolt ret;
		
		String processorType = config.getType().toString();
		
		List<String> inputFields = new ArrayList<String>();
		List<String> outputFields = new ArrayList<String>();
		
		for (InputProcessorPort inputPort : processor.getInputPorts()) {
			inputFields.add(inputPort.getName());
		}
		
		for (OutputProcessorPort outputPort : processor.getOutputPorts()) {
			outputFields.add(outputPort.getName());
		}
		
		switch (processorType) {
			case WorkberchTaverna.TAVERNA_XPATH_TYPE:
				ret = new XPathBolt(inputFields, outputFields, config.getJson());
				break;
			case WorkberchTaverna.TAVERNA_REST_TYPE:
				ret = new RestBolt(inputFields, outputFields, config.getJson());
				break;
	
			default:
				//TODO Esta bien esta excepcion? Capas un assert?
				throw new WrongMethodTypeException("No se ha implementado el tipo de processor de taverna: " + processorType);
		}
		
		return ret;
	}
	
	
	static public WorkberchGenericBolt buildOutputPort(OutputPort outputPort) {
		
		WorkberchGenericBolt ret = new OutputBoltTruecho(new ArrayList<String>());
		return ret;
	}
	
}
