package main.java.parser.model;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.WorkberchDotBolt;
import main.java.utils.constants.WorkberchConstants;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WorkberchIterStgyNode implements WorkberchIterStgy {

	private boolean cross;

	private String processorName;
	
	private static String DOT_PREFIX = "DOT_";
	private static String CROSS_PREFIX = "CROSS_";
	
	private List<WorkberchIterStgy> childStrategies;
	
	
	@Override
	public String getProcessorName() {
		return processorName;
	}

	public void setProcessorName(final String processorName) {
		this.processorName = processorName;
	}

	public boolean isCross() {
		return cross;
	}

	public void setCross(final boolean cross) {
		this.cross = cross;
	}

	public List<WorkberchIterStgy> getChildStrategies() {
		return childStrategies;
	}

	public void setChildStrategies(final List<WorkberchIterStgy> childStrategies) {
		this.childStrategies = childStrategies;
	}

	@Override
	public String getBoltName() {
		final String prefix = cross ? CROSS_PREFIX  : DOT_PREFIX;
		return prefix + processorName;
	}

	@Override
	public BoltDeclarer addStrategy2Topology(final String guid, final TopologyBuilder tBuilder) {

		final List<String> strategiesNames = new ArrayList<String>(); 
		
		for (final WorkberchIterStgy workberchIterStgy : childStrategies) {
			strategiesNames.add(workberchIterStgy.getBoltName());
			workberchIterStgy.addStrategy2Topology(guid, tBuilder);
		}
		
		final WorkberchDotBolt dotBolt = new WorkberchDotBolt(guid, getOutputFields());
		
		final String dotBoltName = getBoltName();
		
		BoltDeclarer boltDeclarer = tBuilder.setBolt(dotBoltName, dotBolt);
		
		for (final String strategyName: strategiesNames) {
			boltDeclarer = boltDeclarer.fieldsGrouping(strategyName, new Fields(WorkberchConstants.INDEX_FIELD));
		}
		
		
		return boltDeclarer;
	}

	@Override
	public List<String> getOutputFields() {
		final List<String> ret = new ArrayList<String>();
		
		for (final WorkberchIterStgy workberchIterStgy : childStrategies) {
			ret.addAll(workberchIterStgy.getOutputFields());
		}
		
		return ret;
	}
	
	
}
