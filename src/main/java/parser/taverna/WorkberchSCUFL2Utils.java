package main.java.parser.taverna;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class WorkberchSCUFL2Utils {
	
	
	private final Map<String, BoltDeclarer> boltsDeclarers = new HashMap<String, BoltDeclarer>();
	private final Map<String, Processor> addedProcessors = new HashMap<String, Processor>();
	private final TopologyBuilder tBuilder = new TopologyBuilder();
	
	static public boolean isProcessorInput(final Processor processor) {
		return processor.getInputPorts().isEmpty();
	}
	
	static public Set<DataLink> getIncomingDataLinksFromProcessor(final Processor processor, final Set<DataLink> set) {
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
	
		
	 

}
