package main.java.bolts;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.utils.RedisHandeler;
import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

abstract public class WorkberchOrderBolt extends WorkberchGenericBolt {

	private static final long serialVersionUID = 1L;

	private final Map<Long, WorkberchTuple> indexMap = new HashMap<Long, WorkberchTuple>();
	private long lastIndex = 0;
	private final boolean ordered;

	public WorkberchOrderBolt(final List<String> outputFields, final Boolean ordered) {
		super(outputFields);
		this.ordered = ordered;
	}

	@Override
	public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
		if (ordered) {
			final Long currentLong = (Long) input.getValues().get(INDEX_FIELD);
			final long currentIndex = currentLong.longValue();
			if (currentIndex > lastIndex) {
				indexMap.put(currentLong, input);
			} else if (currentIndex == lastIndex) {
				indexMap.put(currentLong, input);
				WorkberchTuple tuple;
				do {
					tuple = indexMap.get(lastIndex);
					executeOrdered(tuple, collector);
				} while (indexMap.containsKey(++lastIndex));
			}
		} else {
			// FIXME: Arbol
		}
		if (RedisHandeler.getStateFinished(getBoltId())) {
			System.out.print("Workflow terminado");
		}
	}

	abstract public void executeOrdered(final WorkberchTuple input, final BasicOutputCollector collector);

}
