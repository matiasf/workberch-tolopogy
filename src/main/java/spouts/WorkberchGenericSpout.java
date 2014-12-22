package main.java.spouts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import main.java.utils.redis.RedisHandeler;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

abstract public class WorkberchGenericSpout extends BaseRichSpout  {

	private static final long serialVersionUID = 1L;

	private final List<String> spoutFields;
	private String boltId;
	private boolean init = true;
	private long indexSpout = 0L;

	protected SpoutOutputCollector collector;

	public WorkberchGenericSpout(final List<String> fields) {
		fields.add(INDEX_FIELD);
		spoutFields = fields;
	}

	
	public List<String> getInputPorts() {
		return new ArrayList<String>();
	}

	
	public List<String> getOutputPorts() {
		return spoutFields;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		this.collector = collector;
		boltId = context.getThisComponentId();
	}

	@Override
	public void nextTuple() {
		if (init) {
			final Iterator<Values> iterValues = getValues().iterator();
			while (iterValues.hasNext()) {
				RedisHandeler.increseEmitedState(boltId);
				final Values value = iterValues.next();
				value.add(indexSpout++);
				if (!iterValues.hasNext()) {
					RedisHandeler.setStateFinished(boltId);
				}
				emitNextTuple(value);
			}
		}
		init = false;
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(spoutFields));
	}

	abstract public List<Values> getValues();

	abstract public void emitNextTuple(final Values values);

}
