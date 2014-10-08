package main.java.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import main.java.bolts.OutputBolt;
import main.java.bolts.WorkberchDotBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.constants.WorkberchConstants;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputPort;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputWorkflowPort;
import uk.org.taverna.scufl2.api.port.SenderPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WorkberchTavernaTopologyBuilder {
	
	
	private final Map<String, BoltDeclarer> boltsDeclarers = new HashMap<String, BoltDeclarer>();
	private final Map<String, Processor> addedProcessors = new HashMap<String, Processor>();
	private File t2File;
	private final TopologyBuilder tBuilder = new TopologyBuilder();
	
	private final static String TAVERNA_APP_FORMAT = "application/vnd.taverna.t2flow+xml";
	
	
	static private boolean isProcessorInput(final Processor processor) {
		return processor.getInputPorts().isEmpty();
	}
	
	
	private void addInputPorts(final Workflow workflow) {
		
		for (final InputPort inputPort : workflow.getInputPorts()) {
			final WorkberchGenericSpout spout = WorkberchSpoutBuilder.buildInputPort(inputPort);
			tBuilder.setSpout(inputPort.getName(), spout);
		}
	}
	
	private void addOutputPorts(final Workflow workflow) {
		for (final OutputPort outputPort : workflow.getOutputPorts()) {
			final WorkberchGenericBolt bolt = WorkberchBoltBuilder.buildOutputPort(outputPort);
			final BoltDeclarer declarer = tBuilder.setBolt(outputPort.getName(), bolt);
			boltsDeclarers.put(outputPort.getName(), declarer);
		}
	}
			
	private void addProcessors(final Workflow workflow, final Profile profile) {
		
		for (final Processor processor : workflow.getProcessors()) {
			final Configuration config = profile.getConfigurations().getByName(processor.getName());
			if (WorkberchTavernaTopologyBuilder.isProcessorInput(processor)) {
				final WorkberchGenericSpout spout = WorkberchSpoutBuilder.buildProcessor(processor, config);
				tBuilder.setSpout(processor.getName(), spout);
			}
			else {
				final WorkberchGenericBolt bolt = WorkberchBoltBuilder.buildProcessor(processor, config);
				final BoltDeclarer declarer = tBuilder.setBolt(processor.getName(), bolt);
				boltsDeclarers.put(processor.getName(), declarer);
			}
			addedProcessors.put(processor.getName(), processor);
		}
	}
	
	private String getSourceNameFromDataLink(final DataLink dataLink) {
		
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
	
	private Set<DataLink> getIncomingDataLinksFromProcessor(final Processor processor, final Set<DataLink> set) {
		final Set<DataLink> ret = new HashSet<DataLink>();
		
		for (final DataLink dataLink : set) {
			
			if (dataLink.getSendsTo() instanceof InputProcessorPort) {
				final InputProcessorPort port = (InputProcessorPort)dataLink.getSendsTo();
				if (processor == port.getParent()) {
					ret.add(dataLink);
				}
			}
		}
		
		return ret;
	}
	
	private void addProcessorDeclarerDataLinks(final BoltDeclarer destBolt, final Processor destProcessor, final Set<DataLink> incomnigLinks) {
		final List<String> incomingNodes = new ArrayList<String>();
		for (final DataLink dataLink : incomnigLinks) {
			final String name = getSourceNameFromDataLink(dataLink);
			incomingNodes.add(name);
		}
		final WorkberchDotBolt dotBolt = new WorkberchDotBolt(incomingNodes);
		final String bdName =  "DOT_" + destProcessor.getName();
		final BoltDeclarer bd = tBuilder.setBolt(bdName, dotBolt);
		for (final String string : incomingNodes) {
			bd.fieldsGrouping(string, new Fields(WorkberchConstants.INDEX_FIELD));
		}
		destBolt.shuffleGrouping(bdName);
	}
	
	private void addOutputDataLink(final BoltDeclarer bd, final String output, final Set<DataLink> dataLinks) {
		String parentName = "";
		for (final DataLink dataLink : dataLinks) {
			if (dataLink.getSendsTo() instanceof OutputWorkflowPort) {
				final OutputWorkflowPort port = (OutputWorkflowPort) dataLink.getSendsTo();
				if (output == port.getName()) {
					if (dataLink.getReceivesFrom() instanceof OutputProcessorPort) {
						final OutputProcessorPort processorPort = (OutputProcessorPort) dataLink.getReceivesFrom();
						parentName = processorPort.getParent().getName();
					}
					else {
						parentName = dataLink.getReceivesFrom().getName();
						
					}
					break;
						
				}
			}
			
		}
		
		final OutputBolt ob = new OutputBolt(true);
		final String boltName = "OB_" + output; 
		final BoltDeclarer boltDeclarer = tBuilder.setBolt(boltName, ob);
		boltDeclarer.shuffleGrouping(parentName);
		bd.shuffleGrouping(boltName);
	}
		
	private void processTavernaGraph(final Workflow wf, final Profile profile) {
		
		addInputPorts(wf);
		addProcessors(wf, profile);
		addOutputPorts(wf);
		
		
		for (final String name : boltsDeclarers.keySet()) {
			final BoltDeclarer bd = boltsDeclarers.get(name);
			final Processor pr = addedProcessors.get(name);
			if (pr !=  null) {
				addProcessorDeclarerDataLinks(bd, pr, getIncomingDataLinksFromProcessor(pr, wf.getDataLinks()));
			} else {
				addOutputDataLink(bd, name, wf.getDataLinks());
			}
		}
		
//		for (DataLink dataLink : wf.getDataLinks()) {
//			String destName = getDestNameDataLink(dataLink);			
//			String sourceName = getSourceNameFromDataLink(dataLink);
//			this.addStormLink(sourceName, destName);
//		}
		
	}
	
	public StormTopology buildTavernaTopology() throws ReaderException, IOException {
		
		
		final WorkflowBundleIO io = new WorkflowBundleIO();
		
		final WorkflowBundle wfBundle = io.readBundle(t2File, TAVERNA_APP_FORMAT);
		
		final Workflow wf = wfBundle.getMainWorkflow();
		final Profile profile = wfBundle.getMainProfile();
		
		processTavernaGraph(wf, profile);
		
		return tBuilder.createTopology();
	}
	
	public File getT2File() {
		return t2File;
	}

	public void setT2File(final File t2File) {
		this.t2File = t2File;
	}

	
	 

}
