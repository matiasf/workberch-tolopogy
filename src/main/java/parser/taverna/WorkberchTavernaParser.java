package main.java.parser.taverna;

import static main.java.utils.constants.WorkberchConstants.GUID_REPLACE;
import static uk.org.taverna.scufl2.translator.t2flow.T2FlowReader.APPLICATION_VND_TAVERNA_T2FLOW_XML;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import main.java.parser.WorkberchTopologyBuilder;
import main.java.parser.model.FileDataGenerator;
import main.java.parser.model.WorkberchIterStgy;
import main.java.parser.model.WorkberchNodeInput;
import main.java.parser.model.WorkberchOutputNode;
import main.java.parser.model.WorkberchProcessorNode;

import org.apache.commons.lang.StringUtils;

import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import uk.org.taverna.scufl2.translator.t2flow.T2FlowReader;
import backtype.storm.generated.StormTopology;

import com.google.common.base.Throwables;

public class WorkberchTavernaParser {
	
	private String guid;
	private String workflowPath;
	private String inputPath;
	private String outputPath;
	
	public String getGuid() {
		return guid;
	}

	public void setGuid(final String guid) {
		this.guid = guid;
	}

	public String getWorkflowPath() {
		return workflowPath;
	}

	public void setWorkflowPath(final String workflowPath) {
		this.workflowPath = StringUtils.replace(workflowPath, GUID_REPLACE, guid);
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(final String inputPath) {
		this.inputPath = StringUtils.replace(inputPath, GUID_REPLACE, guid);
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(final String outputPath) {
		this.outputPath = StringUtils.replace(outputPath, GUID_REPLACE, guid);
	}
	
	public StormTopology parse() {
		
		final T2FlowReader io = new T2FlowReader();
		final File t2File = new File(workflowPath);
		
		final WorkberchTopologyBuilder builder = new WorkberchTopologyBuilder();
		builder.setGuid(guid);
		try {
			final WorkflowBundle wfBundle = io.readBundle(t2File, APPLICATION_VND_TAVERNA_T2FLOW_XML);
			
			final Workflow workflow = wfBundle.getMainWorkflow();
			final Profile profile = wfBundle.getMainProfile();
			
			//Agrego puertos de entrada
			final Set<InputWorkflowPort> wfInputPorts = workflow.getInputPorts();
			for (final InputWorkflowPort inputWorkflowPort : wfInputPorts) {
				final FileDataGenerator dg = new FileDataGenerator();
				dg.setFilePath(getInputPath() + inputWorkflowPort.getName() + ".xml");
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
					final WorkberchProcessorNode processorNode = WorkberchTavernaFactory.processeor2ProcessorNode(guid, processor, config);
					final Set<DataLink> allDataLinks = workflow.getDataLinks();
					final Set<DataLink> incomingDataLinks = WorkberchSCUFL2Utils.getIncomingDataLinksFromProcessor(processor, allDataLinks);
					
					final Map<String , DataLink> linksMap = new HashMap<String, DataLink>();
					
					for (final DataLink dataLink : incomingDataLinks) {
						linksMap.put(dataLink.getSendsTo().getName(), dataLink);
					}
					
					final WorkberchIterStgy strategy = WorkberchTavernaFactory.iterationStrategyStack2WorkberchIterStgyNode(processor.getIterationStrategyStack(), linksMap);
					builder.addNode(processorNode, strategy);					
				}
			}
			
			for (final OutputPort outputPort: workflow.getOutputPorts()) {
				final WorkberchOutputNode outputNode = new WorkberchOutputNode();
				outputNode.setName(outputPort.getName());
				outputNode.setOutputPath(outputPath);
				
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
