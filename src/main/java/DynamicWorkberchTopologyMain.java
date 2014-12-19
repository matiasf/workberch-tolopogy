package main.java;

import java.io.IOException;

import main.java.parser.taverna.WorkberchTavernaParser;
import uk.org.taverna.scufl2.api.io.ReaderException;
import backtype.storm.Config;
import backtype.storm.LocalCluster;

public class DynamicWorkberchTopologyMain {
	
	public static void main(final String[] args) throws ReaderException, IOException {
		
		
		final WorkberchTavernaParser parser = new WorkberchTavernaParser();
		parser.setFilePath("ejemlo_base.t2flow");
		
		
		final Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(1);

		final LocalCluster cluster = new LocalCluster();
		
		
		
		
		parser.setInputPath("C:\\Martin\\Proyecto\\");
		cluster.submitTopology("workberch", conf, parser.parse());
		
		System.out.println("Termino");
	}

}
