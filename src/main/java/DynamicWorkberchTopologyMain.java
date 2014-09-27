package main.java;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;

import uk.org.taverna.scufl2.api.common.NamedSet;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import uk.org.taverna.scufl2.api.activity.Activity;

public class DynamicWorkberchTopologyMain {
	
	public static void main(String[] args) throws ReaderException, IOException {
		WorkflowBundleIO io = new WorkflowBundleIO();
		File t2File = new File("/home/proyecto/Downloads/rest_xpath_example.t2flow");
		WorkflowBundle wfBundle = io.readBundle(t2File, "application/vnd.taverna.t2flow+xml");
		NamedSet<Processor> processors = wfBundle.getWorkflows().first().getProcessors();
		
		Profile profile  = wfBundle.getMainProfile();
		
		//for (Profile profile : wfBundle.getProfiles())
		Configuration config =  profile.getConfigurations().getByName("XPath_Service");
		/*for (Configuration config : profile.getConfigurations()) {
			System.out.println(config.getName());
			System.out.println(config.getType());
			System.out.println(config.getJson());
			
			
		}*/
		
		System.out.println(config.getJson());
		
		JsonNode node = config.getJson();
		
		String expression = node.get("xpathExpression").asText();
		System.out.println(expression);
		
			
		/*for (Activity activity : profile.getActivities()) {
			System.out.println(activity.getName());
			System.out.println(activity.getType());
			
		}*/
		
		/*for (Processor processor: processors) {
			System.out.println(processor.getName());
		}*/
	}

}
