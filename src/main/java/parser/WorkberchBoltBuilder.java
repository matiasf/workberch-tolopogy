package main.java.parser;

import java.lang.invoke.WrongMethodTypeException;
import java.util.ArrayList;
import java.util.List;

import main.java.bolts.OutputBoltTruecho;
import main.java.bolts.RestBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.bolts.WorkberchTavernaProcessorBolt;
import main.java.bolts.XPathBolt;
import main.java.utils.constants.TavernaNodeType;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;

public class WorkberchBoltBuilder {
	
	static public WorkberchTavernaProcessorBolt buildProcessor(final Processor processor, final Configuration config) {
		WorkberchTavernaProcessorBolt ret;
		
		final String processorType = config.getType().toString();
		
		final List<String> inputFields = new ArrayList<String>();
		List<String> outputFields = new ArrayList<String>();
		
		for (final InputProcessorPort inputPort : processor.getInputPorts()) {
			inputFields.add(inputPort.getName());
		}
		
		for (final OutputProcessorPort outputPort : processor.getOutputPorts()) {
			outputFields.add(outputPort.getName());
		}
		
		outputFields = new ArrayList<String>();
		outputFields.add(processor.getName());
		switch (TavernaNodeType.valueOf(processorType)) {
			case XPATH:
				ret = new XPathBolt(inputFields, outputFields, config.getJson());
				break;
			case REST:
				ret = new RestBolt(inputFields, outputFields, config.getJson());
				break;
			default:
				//TODO Esta bien esta excepcion? Capas un assert?
				throw new WrongMethodTypeException("No se ha implementado el tipo de processor de taverna: " + processorType);
		}
		
		return ret;
	}
	
	
	static public WorkberchGenericBolt buildOutputPort(final OutputPort outputPort) {
		
		final WorkberchGenericBolt ret = new OutputBoltTruecho(new ArrayList<String>());
		return ret;
	}
	
}
