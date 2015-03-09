package main.java.parser.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import main.java.bolts.WorkberchCartesianBolt;
import main.java.bolts.WorkberchCartesianDummyBolt;
import main.java.bolts.WorkberchDotBolt;
import main.java.bolts.WorkberchOrderBolt;
import main.java.utils.WorkberchTuple;
import main.java.utils.constants.WorkberchConstants;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WorkberchIterStgyNode implements WorkberchIterStgy {

	private static final long serialVersionUID = 5340528828113698457L;

	private boolean cross;
	private boolean optimized;
	private String flowField;
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
	
	public boolean isOptimized() {
		return optimized;
	}

	public void setOptimized(final boolean optimized) {
		this.optimized = optimized;
	}

	public String getFlowField() {
		return flowField;
	}

	public void setFlowField(final String flowField) {
		this.flowField = flowField;
	}

	@Override
	public String getBoltName() {
		final String prefix = cross ? CROSS_PREFIX : DOT_PREFIX;
		return prefix + processorName;
	}

	@Override
	public BoltDeclarer addStrategy2Topology(final String guid, final TopologyBuilder tBuilder) {
		final List<String> strategiesNames = new ArrayList<String>();

		for (final WorkberchIterStgy workberchIterStgy : childStrategies) {
			strategiesNames.add(workberchIterStgy.getBoltName());
			workberchIterStgy.addStrategy2Topology(guid, tBuilder);
		}

		final BoltDeclarer boltReturned;
		if (cross) {
			if (optimized) {
				final WorkberchCartesianDummyBolt bolt = new WorkberchCartesianDummyBolt(guid, getOutputFields(), getFlowField());
				BoltDeclarer boltDeclarer = tBuilder.setBolt(CROSS_PREFIX + processorName, bolt);
				boltReturned = boltDeclarer;
				
				final Iterator<String> iterStrategies = strategiesNames.iterator();
				while (iterStrategies.hasNext()) {
					final String strategyName = iterStrategies.next();
					boltDeclarer = StringUtils.endsWith(strategyName, getFlowField()) ? boltDeclarer.shuffleGrouping(strategyName) : boltDeclarer.allGrouping(strategyName);
				}
			} else {
				final WorkberchCartesianBolt bolt = new WorkberchCartesianBolt(guid, getOutputFields());
				BoltDeclarer boltDeclarer = tBuilder.setBolt(CROSS_PREFIX + processorName, bolt);

				final Iterator<String> iterStrategies = strategiesNames.iterator();
				while (iterStrategies.hasNext()) {
					final String strategyName = iterStrategies.next();
					boltDeclarer = iterStrategies.hasNext() ? boltDeclarer.allGrouping(strategyName) : boltDeclarer.shuffleGrouping(strategyName);
				}
				
				final WorkberchOrderBolt orderBolt = new WorkberchOrderBolt(guid, getOutputFields(), optimized) {
					private static final long serialVersionUID = -1687335238822989302L;

					@Override
					public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues,
							final String uuid) {
						emitTuple(new ArrayList<Object>(input.getValues().values()), collector, lastValues, uuid);
					}
				};			
				boltReturned = tBuilder.setBolt(ORDER_PREFIX + processorName, orderBolt, 1).shuffleGrouping(CROSS_PREFIX + processorName);
			}
		} else {
			final WorkberchDotBolt bolt = new WorkberchDotBolt(guid, getOutputFields());
			final String startBolt = DOT_PREFIX + processorName;
			BoltDeclarer boltDeclarer = tBuilder.setBolt(startBolt, bolt);

			for (final String strategyName : strategiesNames) {
				boltDeclarer = boltDeclarer.fieldsGrouping(strategyName, new Fields(WorkberchConstants.INDEX_FIELD));
			}
			boltReturned = boltDeclarer;
		}

		return boltReturned;
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
