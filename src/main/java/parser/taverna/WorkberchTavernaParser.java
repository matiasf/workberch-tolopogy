package main.java.parser.taverna;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import main.java.parser.WorkberchTopologyBuilder;
import main.java.parser.model.FileDataGenerator;
import main.java.parser.model.WorkberchLink;
import main.java.parser.model.WorkberchNodeInput;
import main.java.parser.model.WorkberchOutputNode;
import main.java.parser.model.WorkberchProcessorNode;
import main.java.utils.constants.WorkberchConstants;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import backtype.storm.generated.StormTopology;

import com.google.common.base.Throwables;

public class WorkberchTavernaParser {
	
	private static String APP_TYPE_TAVERNA_WORKFLOW = "application/vnd.taverna.t2flow+xml";
	
	public StormTopology parse() {
		
		final WorkflowBundleIO io = new WorkflowBundleIO();
		final File t2File = new File(WorkberchConstants.WORKFLOW_PATH);
		
		final WorkberchTopologyBuilder builder = new WorkberchTopologyBuilder();
		try {
			
			final WorkflowBundle wfBundle = io.readBundle(t2File, APP_TYPE_TAVERNA_WORKFLOW);
			
			final Workflow workflow = wfBundle.getMainWorkflow();
			final Profile profile = wfBundle.getMainProfile();
			
			//Agrego puertos de entrada
			final Set<InputWorkflowPort> wfInputPorts = workflow.getInputPorts();
			for (final InputWorkflowPort inputWorkflowPort : wfInputPorts) {
				final FileDataGenerator dg = new FileDataGenerator();
				dg.setFilePath(WorkberchConstants.INPUT_PATH + inputWorkflowPort.getName() + ".xml");
				final WorkberchNodeInput inputNode = WorkberchTavernaFactory.inputPort2NodeInput(inputWorkflowPort, dg);
				builder.addInputNode(inputNode);
				
			}
			
			//Agrego procesors
			for (final Processor processor : workflow.getProcessors()) {
				final Configuration config = profile.getConfigurations().getByName(processor.getName());
				//Si el processor es no recibe de nadie entonces es un input
				if (WorkberchSCUFL2Utils.isProcessorInput(processor)) {
					final WorkberchNodeInput inputNode = WorkberchTavernaFactory.processor2NodeInput(processor, config);
					builder.addInputNode(inputNode);
				}
				else {
					final WorkberchProcessorNode processorNode = WorkberchTavernaFactory.processeor2ProcessorNode(processor, config);
					final Set<DataLink> allDataLinks = workflow.getDataLinks();
					final Set<DataLink> incomingDataLinks = WorkberchSCUFL2Utils.getIncomingDataLinksFromProcessor(processor, allDataLinks);
					final List<WorkberchLink> incomingLinks = new ArrayList<WorkberchLink>();
					for (final DataLink dataLink : incomingDataLinks) {
						incomingLinks.add(WorkberchTavernaFactory.dataLink2Link(dataLink));
					}
					builder.addNode(processorNode, incomingLinks);					
				}
			}
			
			for (final OutputPort outputPort: workflow.getOutputPorts()) {
				final WorkberchOutputNode outputNode = new WorkberchOutputNode();
				outputNode.setName(outputPort.getName());
				
				
				final Set<DataLink> allDataLinks = workflow.getDataLinks();
				final DataLink dataLink = WorkberchSCUFL2Utils.getIncomingDataLinksFromOutputPort(outputPort, allDataLinks);
				/*final List<WorkberchLink> incomingLinks = new ArrayList<WorkberchLink>();
				for (final DataLink dataLink : incomingDataLinks) {
					incomingLinks.add(WorkberchTavernaFactory.dataLink2Link(dataLink));
				}*/
				builder.addOutput(outputNode, WorkberchTavernaFactory.dataLink2Link(dataLink));
			}
		} catch (final ReaderException e) {
			Throwables.propagate(e);
		} catch (final IOException e) {
			Throwables.propagate(e);
		}
		
		return builder.buildTopology();
	}
	
	
	
	
}
