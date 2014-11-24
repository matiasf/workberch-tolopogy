package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WorkberchNameMaperBolt extends BaseBasicBolt{

	private static final long serialVersionUID = -755957444551948715L;
	
	private final List<String> outputFields;
	private final Map<String, String> mapedInputs = new HashMap<String, String>();
	
	
	public WorkberchNameMaperBolt(final List<String> outputFields) {
		this.outputFields = outputFields;
		this.outputFields.add(INDEX_FIELD);
	}
	
	public void addLink(final String sourceField, final String toName) {
		mapedInputs.put(toName, sourceField);
	}
	
	@Override
	public void execute(final Tuple input, final BasicOutputCollector collector) {
		new ArrayList<Object>();

		final List<Object> emitTuple = new ArrayList<Object>();
		

		for (final String output : outputFields) {
			if (!output.equals(INDEX_FIELD)) {
				Object outputValue = null;
				final String inputField = mapedInputs.get(output);

				if (inputField != null) {
					outputValue = input.getValueByField(inputField);
					
				}
				
				emitTuple.add(outputValue);
			}
		}
		
		emitTuple.add(input.getValueByField(INDEX_FIELD));
		collector.emit(emitTuple);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFields));
	}

}
