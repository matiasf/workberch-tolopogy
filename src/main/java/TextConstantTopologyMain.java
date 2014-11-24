package main.java;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.OutputBolt;
import main.java.parser.model.DataGenerator;
import main.java.parser.model.TextDataGenerator;
import main.java.parser.model.WorkberchNodeInput;
import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TextConstantTopologyMain {

	public static void main(final String[] args) {
		
		final TopologyBuilder builder = new TopologyBuilder();
		
		final DataGenerator dg = new TextDataGenerator("Esto es una orueba");
		final List<String> nodeInputOutputs = new ArrayList<String>();
		nodeInputOutputs.add("prueba");
		final WorkberchNodeInput nodeInput = new WorkberchNodeInput("prueba", dg, nodeInputOutputs);
		
		builder.setSpout(nodeInput.getName(), nodeInput.buildSpout());
		
		final OutputBolt bolt = new OutputBolt(false);
		
		builder.setBolt("salida", bolt).shuffleGrouping("prueba");
		
		final Config conf = new Config();
		conf.setDebug(true);
		
		final Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis.close();

		// StormSubmitter.submitTopology("workberch", conf,
		// builder.createTopology());
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("workberch", conf, builder.createTopology());

		
	}
}
