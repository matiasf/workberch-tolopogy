package main.java;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import main.java.bolts.WorkberchGenericBolt;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.WorkberchTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.TopologyBuilder;

public class WorkberchTopologyMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
	TopologyBuilder builder = new TopologyBuilder();

	List<String> outputFieldInput = new ArrayList<String>();
	outputFieldInput.add("count");

	List<String> outputFieldBoo = new ArrayList<String>();
	outputFieldBoo.add("string2");

	List<String> outputField2XXX = new ArrayList<String>();
	outputField2XXX.add("string2");

	List<String> outputField2YYY = new ArrayList<String>();
	outputField2YYY.add("string2");

	builder.setSpout("input", new WorkberchGenericSpout(outputFieldInput), 1);
	builder.setSpout("boo", new WorkberchGenericSpout(outputFieldBoo), 1);
	builder.setSpout("xxx", new WorkberchGenericSpout(outputField2XXX), 1);
	builder.setSpout("yyy", new WorkberchGenericSpout(outputField2YYY), 1);

	List<String> inputFieldsListEmitter = new ArrayList<String>();
	inputFieldsListEmitter.add("count");

	List<String> outputFieldsListEmitter = new ArrayList<String>();
	outputFieldsListEmitter.add("input");

	builder.setBolt("List_Emitter", new WorkberchGenericBolt(inputFieldsListEmitter, outputFieldsListEmitter) {

	    private static final long serialVersionUID = 1L;

	    @Override
	    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
		String count = (String) input.getValues().get("count");
		System.out.println("ListEmitter recive tuple with value " + count);
		List list = new ArrayList();
		int icount = Integer.parseInt(count);

		for (int i = 0; i < icount; i++) {
		    List<Object> values = new ArrayList<Object>();
		    values.add(String.valueOf(icount));
		    collector.emit(values);
		}
	    }

	}).shuffleGrouping("input");

	List<String> inputFieldsConcat = new ArrayList<String>();
	inputFieldsConcat.add("input");

	List<String> outputFieldsConcat = new ArrayList<String>();
	outputFieldsConcat.add("string1");

	builder.setBolt("Concat", new WorkberchGenericBolt(inputFieldsConcat, outputFieldsConcat) {

	    private static final long serialVersionUID = 1L;

	    @Override
	    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
		String input2 = (String) input.getValues().get("input");
		String output = input2 + "XXX";
		System.out.println("Concate recive tuple with value " + input2);
		System.out.println("Concate emit tuple with value " + output);
		List<Object> outputValues = new ArrayList<Object>();
		outputValues.add(output);
		collector.emit(outputValues);
	    }

	}, 2).shuffleGrouping("List_Emitter");

	List<String> inputFieldsConcatTS = new ArrayList<String>();
	inputFieldsConcatTS.add("string1");
	inputFieldsConcatTS.add("string2");

	List<String> outputFieldsConcatTS = new ArrayList<String>();
	outputFieldsConcatTS.add("string1");

	builder.setBolt("Concatenate_two_strings", new WorkberchGenericBolt(inputFieldsConcatTS, outputFieldsConcatTS) {

	    private static final long serialVersionUID = 1L;

	    @Override
	    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
		String string1 = (String) input.getValues().get("string1");
		String string2 = (String) input.getValues().get("string2");
		String output = string1 + string2;
		System.out.println("Concatenate_two_strings recive tuple with value " + string1 + " and " + string2);
		System.out.println("Concatenate_two_strings emit tuple with value " + output);
		List<Object> outputValues = new ArrayList<Object>();
		outputValues.add(output);
		collector.emit(outputValues);
	    }

	}, 2).allGrouping("boo").shuffleGrouping("Concat");

	List<String> inputFieldsConcatTS2 = new ArrayList<String>();
	inputFieldsConcatTS2.add("string1");
	inputFieldsConcatTS2.add("string2");

	List<String> outputFieldsConcatTS2 = new ArrayList<String>();
	outputFieldsConcatTS2.add("string1");

	builder.setBolt("Concatenate_two_strings_2",
		new WorkberchGenericBolt(inputFieldsConcatTS2, outputFieldsConcatTS2) {

		    private static final long serialVersionUID = 1L;

		    @Override
		    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
			String string1 = (String) input.getValues().get("string1");
			String string2 = (String) input.getValues().get("string2");
			String output = string1 + string2;
			System.out.println("Concatenate_two_strings_2 recive tuple with value " + string1 + " and "
				+ string2);
			System.out.println("Concatenate_two_strings_2 emit tuple with value " + output);
			List<Object> outputValues = new ArrayList<Object>();
			outputValues.add(output);
			collector.emit(outputValues);
		    }

		}, 2).allGrouping("xxx").shuffleGrouping("Concatenate_two_strings");

	List<String> inputFieldsConcatTS3 = new ArrayList<String>();
	inputFieldsConcatTS3.add("string1");
	inputFieldsConcatTS3.add("string2");

	List<String> outputFieldsConcatTS3 = new ArrayList<String>();
	outputFieldsConcatTS3.add("out");

	builder.setBolt("Concatenate_two_strings_3",
		new WorkberchGenericBolt(inputFieldsConcatTS3, outputFieldsConcatTS3) {

		    private static final long serialVersionUID = 1L;

		    @Override
		    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
			String string1 = (String) input.getValues().get("string1");
			String string2 = (String) input.getValues().get("string2");
			String output = string1 + string2;
			System.out.println("Concatenate_two_strings_3 recive tuple with value " + string1 + " and "
				+ string2);
			System.out.println("Concatenate_two_strings_3 emit tuple with value " + output);
			List<Object> outputValues = new ArrayList<Object>();
			outputValues.add(output);
			collector.emit(outputValues);
		    }

		}, 2).allGrouping("yyy").shuffleGrouping("Concatenate_two_strings_2");

	List<String> inputFieldsConcatOutput = new ArrayList<String>();
	inputFieldsConcatOutput.add("out");

	builder.setBolt("output", new WorkberchGenericBolt(inputFieldsConcatOutput, new ArrayList<String>()) {

	    private static final long serialVersionUID = 1L;

	    @Override
	    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
		try {
		    File tempOutput = new File("/tmp/peteco");
		    if (tempOutput.canWrite()) {
			BufferedWriter output;
			output = new BufferedWriter(new FileWriter(tempOutput));
			output.write("Valor" + input.getValues().get("out") + "\n");
			output.close();
		    }
		} catch (IOException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }
	}

	, 1).shuffleGrouping("Concatenate_two_strings_3");

	Config conf = new Config();
	conf.setDebug(true);

	StormSubmitter.submitTopology("workberch", conf, builder.createTopology());
	// LocalCluster cluster = new LocalCluster();
	// cluster.submitTopology("workberch", conf, builder.createTopology());
    }

}