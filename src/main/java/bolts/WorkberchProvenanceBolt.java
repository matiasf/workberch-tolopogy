package main.java.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.utils.WorkberchTuple;
import main.java.utils.redis.RedisException;
import main.java.utils.redis.RedisHandeler;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;

import com.google.common.base.Throwables;

public abstract class WorkberchProvenanceBolt extends WorkberchGenericBolt {
	
	private static final long serialVersionUID = 1L;
	
	private final String guid;

	@Override
	protected void emitTuple(final List<Object> tuple, final BasicOutputCollector collector, final boolean lastValue, final String uuid) {
		try {
			RedisHandeler.setProvenanceEmitedInfo(guid, getBoltId(), getOutputFields(), tuple, uuid);
			super.emitTuple(tuple, collector, lastValue, uuid);
		} catch (final RedisException e) {
			Throwables.propagate(e);
		}	
	}
	
	protected String getGuid() {
		return guid;
	}
	
	public WorkberchProvenanceBolt(final String guid, final List<String> outputFields) {
		super(outputFields);
		this.guid = guid;
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
		
		String uuid = StringUtils.EMPTY;
		try {
			uuid = RedisHandeler.setProvenanceReceivedInfo(guid, getBoltId(), fields, values);
		} catch (final RedisException e) {
			Throwables.propagate(e);
		}
		executeLogic(input, collector, lastValues, uuid);
	}
	
	abstract public void executeLogic(WorkberchTuple input, BasicOutputCollector collector, boolean lastValues, String uuid);

}
