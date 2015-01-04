package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import main.java.parser.model.BeanshellLogicExecutor;
import main.java.utils.WorkberchTuple;
import main.java.utils.constants.WorkberchConstants;
import backtype.storm.topology.BasicOutputCollector;

import com.fasterxml.jackson.databind.JsonNode;

public class BeanshellBolt extends WorkberchTavernaProcessorBolt {

	private static final long serialVersionUID = -4520749632325572455L;

	private BeanshellLogicExecutor logicExecuter;

	public BeanshellBolt(final String guid, final List<String> inputFields, final List<String> outputFields, final JsonNode node) {
		super(guid, inputFields, outputFields, node);
		final List<String> scriptOutputs = new ArrayList<String>();

		for (final String string : getOutputFields()) {
			if (!string.equals(WorkberchConstants.INDEX_FIELD)) {
				scriptOutputs.add(string.split("\\" + WorkberchConstants.NAME_DELIMITER)[1]);
			}
		}

		logicExecuter.setOutputNames(scriptOutputs);
	}

	@Override
	public void executeLogic(final WorkberchTuple tuple, final BasicOutputCollector collector, final boolean lastValues) {

		for (final String param : tuple.getValues().keySet()) {
			if (!param.equals(WorkberchConstants.INDEX_FIELD)) {
				final String localParam = param.split("\\" + WorkberchConstants.NAME_DELIMITER)[1];
				logicExecuter.setParam(localParam, tuple.getValues().get(param));
			}
		}

		final Map<String, Object> outputs = logicExecuter.executeLogic();

		final ArrayList<Object> values = new ArrayList<Object>();

		for (final String outuput : outputs.keySet()) {
			values.add(outputs.get(outuput));
		}

		values.add(tuple.getValues().get(INDEX_FIELD));
		emitTuple(values, collector, lastValues);

	}

	public BeanshellLogicExecutor getLogicExecuter() {
		return logicExecuter;
	}

	public void setLogicExecuter(final BeanshellLogicExecutor logicExecuter) {
		this.logicExecuter = logicExecuter;
	}

	@Override
	protected void initFromJsonNode(final JsonNode jsonNode) {
		logicExecuter = new BeanshellLogicExecutor();
		logicExecuter.setScript(jsonNode.get("script").asText());
	}

}
