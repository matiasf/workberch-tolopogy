package main.java;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import main.java.bolts.WorkberchGenericBolt;
import main.java.spouts.WorkberchGenericSpout;
import main.java.utils.WorkberchTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WorkberchTopologyMain {

    public static void main(String[] args) throws InterruptedException {
	TopologyBuilder builder = new TopologyBuilder();

	// Fields
	List<String> fields1 = new ArrayList<String>();
	List<String> fields2 = new ArrayList<String>();
	fields1.add("stream");
	fields1.add("field1");
	fields2.add("stream");
	fields2.add("field2");

	// Streams
	List<String> streams = new ArrayList<String>();
	streams.add("1");
	streams.add("2");

	// Spouts: La idea es que dos spouts emiten a un bolt que genera tuplas
	// cartesianas
	builder.setSpout("1", new WorkberchGenericSpout("1", fields1));
	builder.setSpout("2", new WorkberchGenericSpout("2", fields2));

	// Ensamble stream distribution
	BoltDeclarer bolt = builder.setBolt("3", new WorkberchGenericBolt(streams) {

	    @Override
	    public void executeLogic(WorkberchTuple input, BasicOutputCollector collector) {
		System.out.println("Tupla actual:");
		for (Object value : input.getValues()) {
		    System.out.print(value.toString());
		    System.out.print("-");
		}
		System.out.println();
	    }

	}, 1);
	
	for (final Iterator<String> iterator = streams.iterator(); iterator.hasNext();) {
	    String stream = (String) iterator.next();
	    if (iterator.hasNext()) {
		bolt.allGrouping("1");
	    } else {
		bolt.shuffleGrouping("2");
	    }
	}
	
	Config conf = new Config();
	conf.setDebug(true);
	conf.setMaxTaskParallelism(1);
	
	LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("workberch", conf, builder.createTopology());
	//Thread.sleep(10000);	
	//cluster.shutdown();
    }

}