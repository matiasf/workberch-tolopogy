package main.java.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import main.java.utils.BaseTuple;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class BaseBolt extends BaseBasicBolt {
	
	Map<String, List<Object>> executedInputs = new HashMap<String, List<Object>>();
	List<String> outputFields = new ArrayList<String>();
	
	public BaseBolt(List<String> inputFields, List<String> outputFields) {
		this.outputFields = outputFields;
		for (String inputField: inputFields) {
			this.executedInputs.put(inputField, new ArrayList<Object>());
		}
	}
	
	protected boolean canExecuteLogic(List<String> inputFields) {
		boolean ret = true;
		for (String string : executedInputs.keySet()) {
			if (!inputFields.contains(string))	{
				ret &= !executedInputs.get(string).isEmpty();
			}
		}
		
		return ret;
	}
	
	public abstract void executeLogic(BasicOutputCollector collector, BaseTuple tuple);
	
	protected void createTuples(List<String> remainingFields, BasicOutputCollector collector, BaseTuple baseTuple) {
		
		if (remainingFields.isEmpty()) {
			this.executeLogic(collector, baseTuple);
		}
		else {
			String nextField = remainingFields.get(0);
			remainingFields.remove(0);
			for (Object value : executedInputs.get(nextField)) {
				baseTuple.getValues().put(nextField, value);
				this.createTuples(remainingFields, collector, baseTuple);				
				
			}
		}
		
	}
	
	protected void addExecutedValues(BaseTuple tuple, Set<String> remainingFields ) {
		for (String string : tuple.getValues().keySet()) {
			if (!remainingFields.contains(string)) {
				List<Object> values = this.executedInputs.get(string);
				if (values != null) {
					values.add(tuple.getValues().get(string));
				}
			}
			
		}
	}
	
	@Override
    public void execute(Tuple input, BasicOutputCollector collector) {
		
		List<String> inputFields = input.getFields().toList(); 
		
		System.out.println("Llego " + inputFields.toString());
		System.out.println("Hay " + executedInputs.toString());
		
		BaseTuple baseTuple = new BaseTuple();
		for (String inputField : inputFields) {
			baseTuple.getValues().put(inputField, input.getValueByField(inputField));
		}
		Set<String> remainingFields = new HashSet<String>();
		remainingFields.addAll(this.executedInputs.keySet());
		remainingFields.removeAll(inputFields);
		if (this.canExecuteLogic(inputFields)) {

			this.createTuples(new ArrayList<String>(remainingFields), collector, baseTuple);
			
		}
		this.addExecutedValues(baseTuple, remainingFields);
	
	}
	
	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFields));
    }

}
