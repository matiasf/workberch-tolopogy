package main.java.bolts;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.RedisHandeler;
import main.java.utils.WorkberchTuple;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

abstract public class WorkberchGenericBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private final List<String> runningNodes = new ArrayList<String>();
	private final List<String> outputFields;
	private String boltId;

	protected List<String> getOutputFields() {
		return outputFields;
	}

	protected void emitTuple(final List<Object> tuple, final BasicOutputCollector collector) {
		RedisHandeler.increseEmitedState(boltId);
		collector.emit(tuple);
	}
	
	protected String getBoltId() {
		return boltId;
	}

	public WorkberchGenericBolt(final List<String> outputFields) {
		this.outputFields = outputFields;
		this.outputFields.add(INDEX_FIELD);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(final Map stormConf, final TopologyContext context) {
		boltId = context.getThisComponentId();
	}

	@Override
	public void execute(final Tuple input, final BasicOutputCollector collector) {
		int runningNodesCount = 0;
		input.getSourceComponent();
		for (final String node : runningNodes) {
			if (!(RedisHandeler.getStateFinished(node) && RedisHandeler.increseRecivedState(boltId + "-" + node) == RedisHandeler
					.getEmitedState(node))) {
				runningNodesCount++;
			}
		}

		if (runningNodesCount == 0) {
			RedisHandeler.setStateFinished(boltId);
		}

		final WorkberchTuple baseTuple = new WorkberchTuple(input);
		executeLogic(baseTuple, collector);
	};

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFields));
	}

	public abstract void executeLogic(WorkberchTuple input, BasicOutputCollector collector);

}
