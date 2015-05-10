package main.java.parser.model;

import java.io.Serializable;
import java.util.List;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public interface WorkberchIterStgy extends Serializable {

	public String getProcessorName();
	
	public String getBoltName();
	
	public BoltDeclarer addStrategy2Topology(String guid, TopologyBuilder tBuilder, int parallelism);
	
	public List<String> getOutputFields();
	
	public void setProcessorName(String processorName);
}
