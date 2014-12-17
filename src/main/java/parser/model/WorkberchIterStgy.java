package main.java.parser.model;

import java.util.List;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public interface WorkberchIterStgy {

	public String getProcessorName();
	
	public String getBoltName();
	
	public BoltDeclarer addStrategy2Topology(TopologyBuilder tBuilder);
	
	List<String> getOutputFields();
}
