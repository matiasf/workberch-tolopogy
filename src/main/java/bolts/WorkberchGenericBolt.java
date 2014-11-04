package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.redis.RedisHandeler;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

abstract public class WorkberchGenericBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1656859471059135768L;

	private final List<String> runningNodes = new ArrayList<String>();
	private final List<String> outputFields;
	private String boltId;

	protected List<String> getOutputFields() {
		return outputFields;
	}

	protected void emitTuple(final List<Object> tuple, final BasicOutputCollector collector, final boolean lastValue) {
		RedisHandeler.increseEmitedState(boltId);
		if (lastValue) {
			System.out.println("Finishing: " + boltId);
			RedisHandeler.setStateFinished(boltId);
		}
		collector.emit(tuple);
	}

	protected String getBoltId() {
		return boltId;
	}

	public WorkberchGenericBolt(final List<String> outputFields) {
		this.outputFields = new ArrayList<String>(outputFields);
		this.outputFields.add(INDEX_FIELD);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(final Map stormConf, final TopologyContext context) {
		boltId = context.getThisComponentId();
		for (final GlobalStreamId globalStreamId : context.getThisSources().keySet()) {
			runningNodes.add(globalStreamId.get_componentId());
		}
	}

	@Override
	public void execute(final Tuple input, final BasicOutputCollector collector) {
		int runningNodesCount = 0;
		final long incState = RedisHandeler.increseRecivedState(boltId + "-" + input.getSourceComponent());
		for (final String node : runningNodes) {
			if (!(RedisHandeler.getFinishedState(node) && incState == RedisHandeler.getEmitedState(node))) {
				runningNodesCount++;
				if (RedisHandeler.getFinishedState(node)) {
					System.out.println("Node finished: " + node);
				}
			}
			System.out.println("Inc value on " + getBoltId() + ": " + incState);
			System.out.println("Emited state on " + getBoltId() + " for node " + node + ": " + RedisHandeler.getEmitedState(node));
		}

		System.out.println("Running nodes on " + getBoltId() + " " + runningNodes.size());
		System.out.println("Running nodes counted on " + getBoltId() + " " + runningNodesCount);
		final WorkberchTuple baseTuple = new WorkberchTuple(input);
		if (runningNodesCount == 0) {
			System.out.println("Last value for node " + getBoltId());
		}
		executeLogic(baseTuple, collector, runningNodesCount == 0);
	};

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFields));
	}

	abstract public void executeLogic(WorkberchTuple input, BasicOutputCollector collector, boolean lastValues);

}
