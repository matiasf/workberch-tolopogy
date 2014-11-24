package main.java.parser.taverna;

import java.lang.invoke.WrongMethodTypeException;
import java.util.ArrayList;
import java.util.List;

import main.java.parser.model.DataGenerator;
import main.java.parser.model.TextDataGenerator;
import main.java.parser.model.WorkberchLink;
import main.java.parser.model.WorkberchNodeInput;
import main.java.parser.model.WorkberchProcessorNode;
import main.java.utils.constants.TavernaNodeType;
import main.java.utils.constants.WorkberchConstants;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;
import uk.org.taverna.scufl2.api.port.ReceiverPort;
import uk.org.taverna.scufl2.api.port.SenderPort;

public class WorkberchTavernaFactory {

	static public WorkberchNodeInput inputPort2NodeInput(final InputWorkflowPort inputWorkflowPort, final DataGenerator dataGenerator ) {
		final List<String> output = new ArrayList<String>();
		output.add(inputWorkflowPort.getName() + WorkberchConstants.NAME_DELIMITER + inputWorkflowPort.getName());
		
		final WorkberchNodeInput ret = new WorkberchNodeInput(inputWorkflowPort.getName(), dataGenerator, output);
		
		return ret;
	}
	
	static public WorkberchNodeInput processor2NodeInput(final Processor processor, final Configuration config) {

		
		final String processorType = config.getType().toString();
		DataGenerator dg = null;
		
		switch (TavernaNodeType.fromString(processorType)) {
			case TEXT_CONSTANT :
				dg = new TextDataGenerator(config.getJson().get("string").asText());

				break;
			default:
				throw new WrongMethodTypeException("No se ha implementado el tipo de processor de taverna: " + processorType);
		}
		
		final List<String> outputFields = new ArrayList<String>();
		
		for (final OutputProcessorPort outputPort : processor.getOutputPorts()) {
			outputFields.add(processor.getName() + WorkberchConstants.NAME_DELIMITER + outputPort.getName());
		}
		
		return new WorkberchNodeInput(processor.getName(), dg, outputFields);
		
	}

	static public WorkberchProcessorNode processeor2ProcessorNode(final Processor processor, final Configuration config) {
		
		
		config.getType().toString();
		
		final List<String> inputFields = new ArrayList<String>();
		final List<String> outputFields = new ArrayList<String>();
		
		for (final InputProcessorPort inputPort : processor.getInputPorts()) {
			inputFields.add(processor.getName() + WorkberchConstants.NAME_DELIMITER + inputPort.getName());
		}
		
		for (final OutputProcessorPort outputPort : processor.getOutputPorts()) {
			outputFields.add(processor.getName() + WorkberchConstants.NAME_DELIMITER + outputPort.getName());
		}
		
		final WorkberchTavernaNode ret = new WorkberchTavernaNode(processor.getName(), outputFields, inputFields);
		final String processorType = config.getType().toString();
		ret.setNodeType(TavernaNodeType.fromString(processorType));
		ret.setConfig(config.getJson());
		
		return ret;
	}
	
	static public WorkberchLink dataLink2Link(final DataLink dataLink) {
		final WorkberchLink link = new WorkberchLink();
		
		link.setSourceNode(getSourceNameFromDataLink(dataLink));
		link.setSourceOutput(dataLink.getReceivesFrom().getName());
		
		link.setDestNode(getDestNameFromDataLink(dataLink));
		link.setDestOutput(dataLink.getSendsTo().getName());
		
		return link;
	}
	
	
	
	
	static private String getDestNameFromDataLink(final DataLink dataLink) {
		
		final ReceiverPort receiverPort = dataLink.getSendsTo();
		String ret;
		
		if (receiverPort instanceof InputProcessorPort) {
			ret = ((InputProcessorPort) receiverPort).getParent().getName();
		}
		else {
			ret = receiverPort.getName();
		}
		
		return ret;
	}
	
	static private String getSourceNameFromDataLink(final DataLink dataLink) {
		
		final SenderPort senderPort = dataLink.getReceivesFrom();
		String ret;
		
		if (senderPort instanceof OutputProcessorPort) {
			ret = ((OutputProcessorPort) senderPort).getParent().getName();
		}
		else {
			ret = senderPort.getName();
		}
		
		return ret;
	}
}
