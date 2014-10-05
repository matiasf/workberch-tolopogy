package main.java.spouts;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.List;
import java.util.Map;

import main.java.utils.RedisHandeler;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WorkberchGenericSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private final List<String> spoutFields;
	private SpoutOutputCollector collector;
	private long index = 0L;
	private String boltId;
	private boolean init = true;

	public WorkberchGenericSpout(final List<String> fields) {
		fields.add(INDEX_FIELD);
		spoutFields = fields;
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
			for (int i = 0; i < 10; i++) {
				RedisHandeler.increseEmitedState(boltId);
				final Values values = new Values(String.valueOf(i+1), index++);
				if (i == 10) {
					RedisHandeler.setStateFinished(boltId);
				}
				collector.emit(values);
			}
		}
		init = false;
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(spoutFields));
	}

}
