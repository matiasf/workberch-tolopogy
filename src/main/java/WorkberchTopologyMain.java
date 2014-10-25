package main.java;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.OutputBolt;
import main.java.bolts.WorkberchDotBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.utils.WorkberchTuple;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.TopologyBuilder;

public class WorkberchTopologyMain {

	public static void main(final String[] args) throws AlreadyAliveException, InvalidTopologyException {
		final TopologyBuilder builder = new TopologyBuilder();

		final List<String> outputFieldInput = new ArrayList<String>();
		outputFieldInput.add("count");

		final List<String> outputFieldBoo = new ArrayList<String>();
		outputFieldBoo.add("string2");

		final List<String> outputField2XXX = new ArrayList<String>();
		outputField2XXX.add("string2");

		final List<String> outputField2YYY = new ArrayList<String>();
		outputField2YYY.add("string2");

		//FIXME
		//builder.setSpout("input", new ConcatWorkflowSpout(outputFieldInput), 1);
		//builder.setSpout("boo", new ConcatWorkflowSpout(outputFieldBoo), 1);
		//builder.setSpout("xxx", new ConcatWorkflowSpout(outputField2XXX), 1);
		//builder.setSpout("yyy", new ConcatWorkflowSpout(outputField2YYY), 1);

		final List<String> outputFieldsListEmitter = new ArrayList<String>();
		outputFieldsListEmitter.add("input");

		builder.setBolt("List_Emitter", new WorkberchGenericBolt(outputFieldsListEmitter) {

			private static final long serialVersionUID = 1L;

			@Override
			public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
				final String count = (String) input.getValues().get("count");;
				final int icount = Integer.parseInt(count);
				System.out.println("ListEmitter is going to emit" + icount);

				final List<Object> values = new ArrayList<Object>();
				final List<String> listValue = new ArrayList<String>();
				for (int i = 0; i < icount; i++) {
					listValue.add(String.valueOf(icount));
				}
				values.add(listValue);
				values.add(input.getValues().get(INDEX_FIELD));
				emitTuple(values, collector);
			}

		}, 2).shuffleGrouping("input");

		final List<String> inputFieldsConcat = new ArrayList<String>();
		inputFieldsConcat.add("input");

		final List<String> outputFieldsConcat = new ArrayList<String>();
		outputFieldsConcat.add("string1");

		builder.setBolt("Concat", new WorkberchGenericBolt(outputFieldsConcat) {

			private static final long serialVersionUID = 1L;

			@Override
			public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
				final List<String> input2 = (List<String>) input.getValues().get("input");
				String concatValue = StringUtils.EMPTY;
				for (final String unitValue : input2) {
					concatValue += unitValue;
				}
				final String output = concatValue + "XXX";
				System.out.println("Concate recive tuple with value " + concatValue);
				System.out.println("Concate emit tuple with value " + output);
				final List<Object> outputValues = new ArrayList<Object>();
				outputValues.add(output);
				outputValues.add(input.getValues().get(INDEX_FIELD));
				emitTuple(outputValues, collector);
			}

		}, 2).shuffleGrouping("List_Emitter");

		final List<String> inputFieldsConcatTS = new ArrayList<String>();
		inputFieldsConcatTS.add("string1");
		inputFieldsConcatTS.add("string2");

		final List<String> outputFieldsConcatTS = new ArrayList<String>();
		outputFieldsConcatTS.add("string1");

		builder.setBolt("Concatenate_two_strings_dot", new WorkberchDotBolt(inputFieldsConcatTS), 2).allGrouping("boo").shuffleGrouping("Concat");

		builder.setBolt("Concatenate_two_strings", new WorkberchGenericBolt(outputFieldsConcatTS) {

			private static final long serialVersionUID = 1L;

			@Override
			public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
				final String string1 = (String) input.getValues().get("string1");
				final String string2 = (String) input.getValues().get("string2");
				final String output = string1 + string2;
				System.out.println("Concatenate_two_strings recive tuple with value " + string1 + " and " + string2);
				System.out.println("Concatenate_two_strings emit tuple with value " + output);
				final List<Object> outputValues = new ArrayList<Object>();
				outputValues.add(output);
				outputValues.add(input.getValues().get(INDEX_FIELD));
				emitTuple(outputValues, collector);
			}

		}, 2).shuffleGrouping("Concatenate_two_strings_dot");

		final List<String> inputFieldsConcatTS2 = new ArrayList<String>();
		inputFieldsConcatTS2.add("string1");
		inputFieldsConcatTS2.add("string2");

		final List<String> outputFieldsConcatTS2 = new ArrayList<String>();
		outputFieldsConcatTS2.add("string1");

		builder.setBolt("Concatenate_two_strings_2_dot", new WorkberchDotBolt(inputFieldsConcatTS2), 2).allGrouping("xxx")
				.shuffleGrouping("Concatenate_two_strings");

		builder.setBolt("Concatenate_two_strings_2", new WorkberchGenericBolt(outputFieldsConcatTS2) {

			private static final long serialVersionUID = 1L;

			@Override
			public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
				final String string1 = (String) input.getValues().get("string1");
				final String string2 = (String) input.getValues().get("string2");
				final String output = string1 + string2;
				System.out.println("Concatenate_two_strings_2 recive tuple with value " + string1 + " and " + string2);
				System.out.println("Concatenate_two_strings_2 emit tuple with value " + output);
				final List<Object> outputValues = new ArrayList<Object>();
				outputValues.add(output);
				outputValues.add(input.getValues().get(INDEX_FIELD));
				emitTuple(outputValues, collector);
			}

		}, 2).shuffleGrouping("Concatenate_two_strings_2_dot");

		final List<String> inputFieldsConcatTS3 = new ArrayList<String>();
		inputFieldsConcatTS3.add("string1");
		inputFieldsConcatTS3.add("string2");

		final List<String> outputFieldsConcatTS3 = new ArrayList<String>();
		outputFieldsConcatTS3.add("out");

		builder.setBolt("Concatenate_two_strings_3_dot", new WorkberchDotBolt(inputFieldsConcatTS3), 2).allGrouping("yyy")
				.shuffleGrouping("Concatenate_two_strings_2");

		builder.setBolt("Concatenate_two_strings_3", new WorkberchGenericBolt(outputFieldsConcatTS3) {

			private static final long serialVersionUID = 1L;

			@Override
			public void executeLogic(final WorkberchTuple input, final BasicOutputCollector collector) {
				final String string1 = (String) input.getValues().get("string1");
				final String string2 = (String) input.getValues().get("string2");
				final String output = string1 + string2;
				System.out.println("Concatenate_two_strings_3 recive tuple with value " + string1 + " and " + string2);
				System.out.println("Concatenate_two_strings_3 emit tuple with value " + output);
				final List<Object> outputValues = new ArrayList<Object>();
				outputValues.add(output);
				outputValues.add(input.getValues().get(INDEX_FIELD));
				emitTuple(outputValues, collector);
			}

		}, 2).shuffleGrouping("Concatenate_two_strings_3_dot");

		final List<String> inputFieldsConcatOutput = new ArrayList<String>();
		inputFieldsConcatOutput.add("out");

		builder.setBolt("output", new OutputBolt(Boolean.TRUE), 1).shuffleGrouping("Concatenate_two_strings_3");

		final Config conf = new Config();
		conf.setDebug(true);
		
		final Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis.close();

		// StormSubmitter.submitTopology("workberch", conf,
		// builder.createTopology());
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("workberch", conf, builder.createTopology());

	}

}