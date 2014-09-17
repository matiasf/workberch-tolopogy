package main.java.bolts;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WorkberchCrossBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;

    private Map<Long, Map<String, Object>> vectorsMap = new HashMap<Long, Map<String, Object>>();

    private List<String> fields;

    public WorkberchCrossBolt(final List<String> fields) {
	fields.add(INDEX_FIELD);
	this.fields = fields;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
	Long index = (Long) input.getValueByField(INDEX_FIELD);
	Map<String, Object> value = vectorsMap.containsKey(index) ? vectorsMap.get(index)
		: new HashMap<String, Object>();
	
	List<String> notPresentFields = new ArrayList<String>();
	notPresentFields.addAll(fields);
	notPresentFields.removeAll(input.getFields().toList());
	
	List<String> presentFields = new ArrayList<String>();
	presentFields.addAll(fields);
	presentFields.removeAll(notPresentFields);

	for (String field : presentFields) {
	    value.put(field, input.getValueByField(field));
	}

	if (value.keySet().size() == fields.size()) {
	    final List<Object> tupleToEmit = new ArrayList<Object>();
	    for (String field : fields) {
		tupleToEmit.add(value.get(field));
	    }
	    System.out.println();
	    collector.emit(tupleToEmit);
	    vectorsMap.remove(index);
	}
	else {
	    vectorsMap.put(index, value);
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields(this.fields));
    }

}
