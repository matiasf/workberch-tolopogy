package main.java.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.redis.RedisException;
import main.java.utils.redis.RedisHandeler;
import backtype.storm.topology.BasicOutputCollector;

import com.google.common.base.Throwables;

public abstract class WorkberchProvenanceBolt extends WorkberchGenericBolt {
	
	private static final long serialVersionUID = 1L;

	@Override
	protected void emitTuple(final List<Object> tuple, final BasicOutputCollector collector, final boolean lastValue) {
		try {
			RedisHandeler.setProvenanceEmitedInfo(getBoltId(), getOutputFields(), tuple);
			super.emitTuple(tuple, collector, lastValue);
		} catch (final RedisException e) {
			Throwables.propagate(e);
		}	
	}
	
	public WorkberchProvenanceBolt(final List<String> outputFields) {
		super(outputFields);
	}
	
	@Override
	public void executeProvenance(final WorkberchTuple input, final BasicOutputCollector collector, final boolean lastValues) {
		final Map<String, Object> valuesMap = input.getValues();
		final List<String> fields = new ArrayList<String>();
		final List<Object> values = new ArrayList<Object>();
		for (final String key : valuesMap.keySet()) {
			fields.add(key);
			values.add(valuesMap.get(key));
		}
		
		try {
			RedisHandeler.setProvenanceReceivedInfo(getBoltId(), fields, values);
		} catch (final RedisException e) {
			Throwables.propagate(e);
		}
		executeLogic(input, collector, lastValues);
	}
	
	abstract public void executeLogic(WorkberchTuple input, BasicOutputCollector collector, boolean lastValues);

}
