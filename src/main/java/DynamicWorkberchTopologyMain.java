package main.java;

import java.io.File;
import java.io.IOException;

import uk.org.taverna.scufl2.api.common.NamedSet;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;

public class DynamicWorkberchTopologyMain {
	
	public static void main(String[] args) throws ReaderException, IOException {
		WorkflowBundleIO io = new WorkflowBundleIO();
		File t2File = new File("/home/proyecto/Downloads/pasteur_sra.t2flow");
		WorkflowBundle wfBundle = io.readBundle(t2File, "application/vnd.taverna.t2flow+xml");
		NamedSet<InputWorkflowPort> inputPorts = wfBundle.getWorkflows().first().getInputPorts();
		
		for (InputWorkflowPort inputWorkflowPort : inputPorts) {
			
			System.out.println(inputWorkflowPort.getName());
			
		}
	}

}
