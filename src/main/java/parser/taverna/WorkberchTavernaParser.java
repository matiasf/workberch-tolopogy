package main.java.parser.taverna;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import main.java.parser.WorkberchTopologyBuilder;
import main.java.parser.model.DataGenerator;
import main.java.parser.model.TextDataGenerator;
import main.java.parser.model.WorkberchLink;
import main.java.parser.model.WorkberchNodeInput;
import main.java.parser.model.WorkberchProcessorNode;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import backtype.storm.generated.StormTopology;

public class WorkberchTavernaParser {

	private String filePath;
	
	private String inputPath;
	
	private String outputPath;
	
	private static String APP_TYPE_TAVERNA_WORKFLOW = "application/vnd.taverna.t2flow+xml"; 
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(final String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(final String outputPath) {
		this.outputPath = outputPath;
	}	

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(final String filePath) {
		this.filePath = filePath;
	}
	
	public StormTopology parse() {
		
		final WorkflowBundleIO io = new WorkflowBundleIO();
		final File t2File = new File(filePath);
		
		final WorkberchTopologyBuilder builder = new WorkberchTopologyBuilder();
		try {
			
			
			final WorkflowBundle wfBundle = io.readBundle(t2File, APP_TYPE_TAVERNA_WORKFLOW);
			
			final Workflow workflow = wfBundle.getMainWorkflow();
			final Profile profile = wfBundle.getMainProfile();
			
			
			//Agrego puertos de entrada
			final Set<InputWorkflowPort> wfInputPorts = workflow.getInputPorts();
			for (final InputWorkflowPort inputWorkflowPort : wfInputPorts) {
				final DataGenerator dg = new TextDataGenerator("4");
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
			
			
			
			
			
		} catch (final ReaderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (final IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return builder.buildTopology();
	}
	
	
	
	
}
