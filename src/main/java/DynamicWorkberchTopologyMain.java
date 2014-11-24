package main.java;

import java.io.IOException;

import main.java.parser.taverna.WorkberchTavernaParser;
import uk.org.taverna.scufl2.api.io.ReaderException;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class DynamicWorkberchTopologyMain {
	
	public static void main(final String[] args) throws ReaderException, IOException {
		
		
		final WorkberchTavernaParser parser = new WorkberchTavernaParser();
		parser.setFilePath("C:\\Martin\\Proyecto\\rest_xpath_example - Copy.t2flow");
		
		
		final Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(1);

		final LocalCluster cluster = new LocalCluster();
		
		final StormTopology topo = parser.parse();
		
		for(final String sput:topo.get_spouts().keySet()) {
			System.out.println(sput);
		}
		
		for(final String sput:topo.get_bolts().keySet()) {
			System.out.println(sput);
		}
		
		cluster.submitTopology("workberch", conf, parser.parse());
		
		System.out.println("Termino");
	}

}
