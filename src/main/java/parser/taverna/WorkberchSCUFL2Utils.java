package main.java.parser.taverna;

import java.util.HashSet;
import java.util.Set;

import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;
import uk.org.taverna.scufl2.api.port.OutputPort;
import uk.org.taverna.scufl2.api.port.OutputWorkflowPort;

public class WorkberchSCUFL2Utils {
	
	
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
	
	static public DataLink getIncomingDataLinksFromOutputPort(final OutputPort outputPort, final Set<DataLink> set) {
		
		
		for (final DataLink dataLink : set) {
			
			if (dataLink.getSendsTo() instanceof OutputWorkflowPort) {
				final OutputWorkflowPort port = (OutputWorkflowPort)dataLink.getSendsTo();
				if (outputPort == port) {
					return dataLink;
				}
			}
		}
		
		return null;
	}
	
		
	 

}
