package main.java.parser.model;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.WorkberchCartesianBolt;
import main.java.bolts.WorkberchDotBolt;
import main.java.bolts.WorkberchOrderBolt;
import main.java.utils.WorkberchTuple;
import main.java.utils.constants.WorkberchConstants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WorkberchIterStgyNode implements WorkberchIterStgy {

	private boolean cross;

	private String processorName;
	
	private static String DOT_PREFIX = "DOT_";
	private static String CROSS_PREFIX = "CROSS_";
	private static String ORDER_PREFIX = "ORDER_";
	
	private List<WorkberchIterStgy> childStrategies;
	
	
	@Override
	public String getProcessorName() {
		return processorName;
	}

	@Override
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
	public BoltDeclarer addStrategy2Topology(final TopologyBuilder tBuilder) {

		final List<String> strategiesNames = new ArrayList<String>(); 
		
		for (final WorkberchIterStgy workberchIterStgy : childStrategies) {
			strategiesNames.add(workberchIterStgy.getBoltName());
			workberchIterStgy.addStrategy2Topology(tBuilder);
		}
		
		
		BoltDeclarer boltDeclarer = null;
		String startBolt = "";
		
		if (cross) {
			
			final WorkberchCartesianBolt bolt = new WorkberchCartesianBolt(getOutputFields());
			
			boltDeclarer = tBuilder.setBolt(CROSS_PREFIX + processorName, bolt);
			
			final WorkberchOrderBolt orderBolt = new WorkberchOrderBolt(getOutputFields(), false) {
				
				
				private static final long serialVersionUID = -1687335238822989302L;

				@Override
				public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {
					emitTuple(new ArrayList<Object>(input.getValues().values()), collector, lastValues);
					
				}
			};
			boltDeclarer = tBuilder.setBolt(ORDER_PREFIX + processorName, orderBolt);			
		}
		else {
			final WorkberchDotBolt bolt = new WorkberchDotBolt(getOutputFields());
			startBolt = DOT_PREFIX + processorName;
			boltDeclarer = tBuilder.setBolt(startBolt, bolt);			
		}
		
		
		
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
