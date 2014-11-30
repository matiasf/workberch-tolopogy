package main.java.parser.taverna;

import java.util.HashSet;
import java.util.Set;

import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.port.InputProcessorPort;

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
	
		
	 

}
