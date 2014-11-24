package main.java.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.bolts.WorkberchDotBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.bolts.WorkberchNameMaperBolt;
import main.java.parser.model.WorkberchLink;
import main.java.parser.model.WorkberchNode;
import main.java.parser.model.WorkberchProcessorNode;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.constants.WorkberchConstants;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WorkberchTopologyBuilder {

	private static String MAPPER_PREFIX = "NAME_MAPPER_";
	private static String DOT_PREFIX = "DOT_";
	
	private final TopologyBuilder tBuilder = new TopologyBuilder();
	
	Map<String, WorkberchNode> nodes = new HashMap<String, WorkberchNode>();
	
	public void addInputNode(final WorkberchNode inputNode) {
		final WorkberchGenericSpout spout = inputNode.buildSpout();
		
		tBuilder.setSpout(inputNode.getName(), spout);
		nodes.put(inputNode.getName(), inputNode);
	}
	
	public void addNode(final WorkberchProcessorNode node, final List<WorkberchLink> incomingLinks) {
		
		final List<String> inputAsOutputs = new ArrayList<String>();
		
		for (final WorkberchLink incomingLink : incomingLinks) {
			inputAsOutputs.add(incomingLink.getSourceNode() + WorkberchConstants.NAME_DELIMITER + incomingLink.getSourceOutput());
		}
		
		final WorkberchDotBolt dotBolt = new WorkberchDotBolt(inputAsOutputs);
		
		final String dotBoltName = DOT_PREFIX + node.getName();
		
		BoltDeclarer boltDeclarer = tBuilder.setBolt(dotBoltName, dotBolt);
		
		for (final WorkberchLink incomingLink : incomingLinks) {
			boltDeclarer = boltDeclarer.fieldsGrouping(incomingLink.getSourceNode(), new Fields(WorkberchConstants.INDEX_FIELD));
		}
		
		
		final WorkberchNameMaperBolt mapper = new WorkberchNameMaperBolt(node.getInputs());
		
		final String mapperName = MAPPER_PREFIX + node.getName();
		
		
		for (final WorkberchLink incomingLink : incomingLinks) {
			mapper.addLink( incomingLink.getStormSourceField(), incomingLink.getStormDestField());
		}
		
		boltDeclarer = tBuilder.setBolt(mapperName, mapper);
		
		boltDeclarer.shuffleGrouping(dotBoltName);
		
		final WorkberchGenericBolt bolt = node.buildBolt();
		tBuilder.setBolt(node.getName(), bolt).shuffleGrouping(mapperName);
		
		nodes.put(node.getName(), node);
	}
	
	public StormTopology buildTopology() {
		return tBuilder.createTopology();
	}
	
}
