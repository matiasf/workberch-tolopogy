package main.java.parser;

import java.util.HashMap;
import java.util.Map;

import main.java.bolts.WorkberchGenericBolt;
import main.java.parser.model.WorkberchIterStgy;
import main.java.parser.model.WorkberchLink;
import main.java.parser.model.WorkberchNode;
import main.java.parser.model.WorkberchOutputNode;
import main.java.parser.model.WorkberchProcessorNode;
import main.java.spouts.WorkberchGenericSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class WorkberchTopologyBuilder {

	private final TopologyBuilder tBuilder = new TopologyBuilder();
	
	private String outputPath;
	
	private String inputPath;
	
	private final Map<String, WorkberchNode> nodes = new HashMap<String, WorkberchNode>();
	
	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(final String outputPath) {
		this.outputPath = outputPath;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(final String inputPath) {
		this.inputPath = inputPath;
	}

	public void addInputNode(final WorkberchNode inputNode) {
		final WorkberchGenericSpout spout = inputNode.buildSpout();
		
		tBuilder.setSpout(inputNode.getName(), spout);
		nodes.put(inputNode.getName(), inputNode);
	}
	
	public void addNode(final WorkberchProcessorNode node, final WorkberchIterStgy strategy) {
	
		strategy.addStrategy2Topology(tBuilder);
		final WorkberchGenericBolt bolt = node.buildBolt();
		tBuilder.setBolt(node.getName(), bolt).shuffleGrouping(strategy.getBoltName());
		
		nodes.put(node.getName(), node);
	}
	
	public void addOutput(final WorkberchOutputNode node, final WorkberchLink incomingLink) {
		final WorkberchGenericBolt bolt = node.buildBolt();
		tBuilder.setBolt(node.getName(), bolt).shuffleGrouping(incomingLink.getSourceNode());
	}
	
	public StormTopology buildTopology() {
		return tBuilder.createTopology();
	}
	
}
