package main.java.parser;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import main.java.bolts.OutputBolt;
import main.java.bolts.WorkberchCartesianBolt;
import main.java.bolts.WorkberchDotBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.TavernaProcessor;
import uk.org.taverna.scufl2.api.common.NamedSet;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.iterationstrategy.IterationStrategyStack;
import uk.org.taverna.scufl2.api.port.InputPort;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.port.OutputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputWorkflowPort;
import uk.org.taverna.scufl2.api.port.ProcessorPort;
import uk.org.taverna.scufl2.api.port.ReceiverPort;
import uk.org.taverna.scufl2.api.port.SenderPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;

public class WorkberchTavernaTopologyBuilder {
	
	
	private Map<String, BoltDeclarer> boltsDeclarers = new HashMap<String, BoltDeclarer>();
	private Map<String, Processor> addedProcessors = new HashMap<String, Processor>();
	private File t2File;
	private TopologyBuilder tBuilder = new TopologyBuilder();
	
	private final static String TAVERNA_APP_FORMAT = "application/vnd.taverna.t2flow+xml";
	
	
	static private boolean isProcessorInput(Processor processor) {
		return processor.getInputPorts().isEmpty();
	}
	
	
	private void addInputPorts(Workflow workflow) {
		
		for (InputPort inputPort : workflow.getInputPorts()) {
			WorkberchGenericSpout spout = WorkberchSpoutBuilder.buildInputPort(inputPort);
			tBuilder.setSpout(inputPort.getName(), spout);
		}
	}
	
	private void addOutputPorts(Workflow workflow) {
		for (OutputPort outputPort : workflow.getOutputPorts()) {
			WorkberchGenericBolt bolt = WorkberchBoltBuilder.buildOutputPort(outputPort);
			BoltDeclarer declarer = tBuilder.setBolt(outputPort.getName(), bolt);
			boltsDeclarers.put(outputPort.getName(), declarer);
		}
	}
			
	private void addProcessors(Workflow workflow, Profile profile) {
		
		for (Processor processor : workflow.getProcessors()) {
			Configuration config = profile.getConfigurations().getByName(processor.getName());
			if (WorkberchTavernaTopologyBuilder.isProcessorInput(processor)) {
				WorkberchGenericSpout spout = WorkberchSpoutBuilder.buildProcessor(processor, config);
				tBuilder.setSpout(processor.getName(), spout);
			}
			else {
				WorkberchGenericBolt bolt = WorkberchBoltBuilder.buildProcessor(processor, config);
				BoltDeclarer declarer = tBuilder.setBolt(processor.getName(), bolt);
				boltsDeclarers.put(processor.getName(), declarer);
			}
			addedProcessors.put(processor.getName(), processor);
		}
	}
	
	private String getSourceNameFromDataLink(DataLink dataLink) {
		
		SenderPort senderPort = dataLink.getReceivesFrom();
		String ret;
		
		if (senderPort instanceof OutputProcessorPort) {
			ret = ((OutputProcessorPort) senderPort).getParent().getName();
		}
		else {
			ret = senderPort.getName();
		}
		
		return ret;
	}
	
	private String getDestNameDataLink(DataLink dataLink) {
		ReceiverPort port = dataLink.getSendsTo();
		String nodeName;
		if (port instanceof InputProcessorPort) {
			InputProcessorPort inputPort =  (InputProcessorPort) port ;
			nodeName = inputPort.getParent().getName();
		}
		else {
			nodeName = port.getName();
		}
		
		return nodeName;
	}
	
	private void addStormLink(String sourceName, String destName) {
		BoltDeclarer boltDeclarer = boltsDeclarers.get(destName);
		
		Processor destProcessor = addedProcessors.get(destName);
		//Si no hay processor entonces es un puerto de salida. Hay que ponerle uno que solo ordene
		if (destProcessor != null) {
			//TODO aca hay que hacer algo con el iteration strategy y encajar los bolts previos			
			IterationStrategyStack iterationStrategy = destProcessor.getIterationStrategyStack();
			
		}
		
		
		//TODO este allGrouping en determinado caso dependiendo del iter strategy puede ser un shuffleGrouping
		boltDeclarer.allGrouping(sourceName);
		
	}
	
	private Set<DataLink> getIncomingDataLinksFromProcessor(Processor processor, Set<DataLink> set) {
		Set<DataLink> ret = new HashSet<DataLink>();
		
		for (DataLink dataLink : set) {
			
			if (dataLink.getSendsTo() instanceof InputProcessorPort) {
				InputProcessorPort port = (InputProcessorPort)dataLink.getSendsTo();
				if (processor == port.getParent()) {
					ret.add(dataLink);
				}
			}
		}
		
		return ret;
	}
	
	private void addProcessorDeclarerDataLinks(BoltDeclarer destBolt, Processor destProcessor, Set<DataLink> incomnigLinks) {
		List<String> incomingNodes = new ArrayList<String>();
		for (DataLink dataLink : incomnigLinks) {
			String name = getSourceNameFromDataLink(dataLink);
			incomingNodes.add(name);
		}
		WorkberchDotBolt dotBolt = new WorkberchDotBolt(incomingNodes);
		String bdName =  "DOT_" + destProcessor.getName();
		BoltDeclarer bd = tBuilder.setBolt(bdName, dotBolt);
		for (String string : incomingNodes) {
			bd.fieldsGrouping(string, new Fields(INDEX_FIELD));
		}
		destBolt.shuffleGrouping(bdName);
	}
	
	private void addOutputDataLink(BoltDeclarer bd, String output, Set<DataLink> dataLinks) {
		String parentName = "";
		for (DataLink dataLink : dataLinks) {
			if (dataLink.getSendsTo() instanceof OutputWorkflowPort) {
				OutputWorkflowPort port = (OutputWorkflowPort) dataLink.getSendsTo();
				if (output == port.getName()) {
					if (dataLink.getReceivesFrom() instanceof OutputProcessorPort) {
						OutputProcessorPort processorPort = (OutputProcessorPort) dataLink.getReceivesFrom();
						parentName = processorPort.getParent().getName();
					}
					else {
						parentName = dataLink.getReceivesFrom().getName();
						
					}
					break;
						
				}
			}
			
		}
		
		OutputBolt ob = new OutputBolt(true);
		String boltName = "OB_" + output; 
		BoltDeclarer boltDeclarer = tBuilder.setBolt(boltName, ob);
		boltDeclarer.shuffleGrouping(parentName);
		bd.shuffleGrouping(boltName);
	}
		
	private void processTavernaGraph(Workflow wf, Profile profile) {
		
		this.addInputPorts(wf);
		this.addProcessors(wf, profile);
		this.addOutputPorts(wf);
		
		
		for (String name : boltsDeclarers.keySet()) {
			BoltDeclarer bd = boltsDeclarers.get(name);
			Processor pr = addedProcessors.get(name);
			if (pr !=  null)
				this.addProcessorDeclarerDataLinks(bd, pr, getIncomingDataLinksFromProcessor(pr, wf.getDataLinks()));
			else
				addOutputDataLink(bd, name, wf.getDataLinks());
		}
		
//		for (DataLink dataLink : wf.getDataLinks()) {
//			String destName = getDestNameDataLink(dataLink);			
//			String sourceName = getSourceNameFromDataLink(dataLink);
//			this.addStormLink(sourceName, destName);
//		}
		
	}
	
	public StormTopology buildTavernaTopology() throws ReaderException, IOException {
		
		
		WorkflowBundleIO io = new WorkflowBundleIO();
		
		WorkflowBundle wfBundle = io.readBundle(t2File, TAVERNA_APP_FORMAT);
		
		Workflow wf = wfBundle.getMainWorkflow();
		Profile profile = wfBundle.getMainProfile();
		
		this.processTavernaGraph(wf, profile);
		
		return tBuilder.createTopology();
	}
	
	public File getT2File() {
		return t2File;
	}

	public void setT2File(File t2File) {
		this.t2File = t2File;
	}

	
	 

}
