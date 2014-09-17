package main.java;

import static main.java.utils.WorkberchConstants.INDEX_FIELD;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.WorkberchCrossBolt;
import main.java.bolts.WorkberchGenericBolt;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.WorkberchTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class CrossProductMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
	TopologyBuilder builder = new TopologyBuilder();

	List<String> outputFieldInput1 = new ArrayList<String>();
	outputFieldInput1.add("input1");

	List<String> outputFieldInput2 = new ArrayList<String>();
	outputFieldInput2.add("input2");

	builder.setSpout("input1", new WorkberchGenericSpout(outputFieldInput1), 1);
	builder.setSpout("input2", new WorkberchGenericSpout(outputFieldInput2), 1);

	List<String> inputFieldsCrossProduct = new ArrayList<String>();
	inputFieldsCrossProduct.add("input1");
	inputFieldsCrossProduct.add("input2");

	builder.setBolt("CrossProduct", new WorkberchCrossBolt(inputFieldsCrossProduct), 2)
		.fieldsGrouping("input1", new Fields(INDEX_FIELD)).fieldsGrouping("input2", new Fields(INDEX_FIELD));

	builder.setBolt("GenericNode", new WorkberchGenericBolt(inputFieldsCrossProduct, new ArrayList<String>()) {

	    private static final long serialVersionUID = 1L;

	    @Override
	    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
		System.out.println(input.getValues().get("input1") + "---" + input.getValues().get("input2"));
	    }

	}, 2).shuffleGrouping("CrossProduct");
	
	Config conf = new Config();
	conf.setDebug(true);

	LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("workberch", conf, builder.createTopology());
    }

}
