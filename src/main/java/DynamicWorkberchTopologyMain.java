package main.java;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import main.java.bolts.WorkberchTavernaProcessorBolt;
import main.java.parser.WorkberchBoltBuilder;
import main.java.parser.WorkberchTavernaTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.fasterxml.jackson.databind.JsonNode;

import uk.org.taverna.scufl2.api.common.NamedSet;
import uk.org.taverna.scufl2.api.configurations.Configuration;
import uk.org.taverna.scufl2.api.container.WorkflowBundle;
import uk.org.taverna.scufl2.api.core.DataLink;
import uk.org.taverna.scufl2.api.core.Processor;
import uk.org.taverna.scufl2.api.core.Workflow;
import uk.org.taverna.scufl2.api.io.ReaderException;
import uk.org.taverna.scufl2.api.io.WorkflowBundleIO;
import uk.org.taverna.scufl2.api.port.InputPort;
import uk.org.taverna.scufl2.api.port.InputWorkflowPort;
import uk.org.taverna.scufl2.api.profiles.Profile;
import uk.org.taverna.scufl2.api.activity.Activity;

public class DynamicWorkberchTopologyMain {
	
	public static void main(String[] args) throws ReaderException, IOException {
		WorkflowBundleIO io = new WorkflowBundleIO();
		File t2File = new File("C:\\Martin\\Proyecto\\rest_xpath_example.t2flow");
		WorkflowBundle wfBundle = io.readBundle(t2File, "application/vnd.taverna.t2flow+xml");
		
		WorkberchTavernaTopologyBuilder builder = new WorkberchTavernaTopologyBuilder();
		builder.setT2File(t2File);
		
		
		//builder.buildTavernaTopology();
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("workberch", conf, builder.buildTavernaTopology());
		
		System.out.println("Termino");
			
		/*for (Activity activity : profile.getActivities()) {
			System.out.println(activity.getName());
			System.out.println(activity.getType());
			
		}*/
		
		/*for (Processor processor: processors) {
			System.out.println(processor.getName());
		}*/
	}

}
