package main.java;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.BaseBolt;
import main.java.bolts.RestBolt;
import main.java.bolts.XPathBolt;
import main.java.spouts.SpoutTrucho;
import main.java.utils.BaseTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class OtroMain {

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		
		
		List<String> field1 = new ArrayList<String>();
		field1.add("db");
		field1.add("term");
		field1.add("retmax");
		
		builder.setSpout("1", new SpoutTrucho(field1));
		
		List<String> field2 = new ArrayList<String>();
		field2.add("B");
		//builder.setSpout("2", new SpoutTrucho(field2));
		
		/*
		List<String> field3 = new ArrayList<String>();
		field3.add("A");
		field3.add("B");*/
		
		String url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db={db}&term={term}&retmax={retmax}&usehistory=y";
		BaseBolt searchStudies = new RestBolt(field1, field2, url, "GET", "text/xml");
		
		//BoltDeclarer bolt = builder.setBolt("3", basebolt, 1).allGrouping("1").shuffleGrouping("2");
		BoltDeclarer bolt = builder.setBolt("searchStudies", searchStudies, 1).shuffleGrouping("1");
		
		List<String> getStudiesOut = new ArrayList<String>();
		getStudiesOut.add("id");
		BaseBolt getStudiesId = new XPathBolt(field2, getStudiesOut, "/eSearchResult/IdList/Id");
		builder.setBolt("getStudiesId", getStudiesId, 1).shuffleGrouping("searchStudies");
		
		List<String> downloadExperimentsFields = new ArrayList<String>();
		downloadExperimentsFields.add("db");
		downloadExperimentsFields.add("id");
		
		
		url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={db}&id={id}";
		BaseBolt downloadExperiments = new RestBolt(downloadExperimentsFields, field2, url, "GET", "text/xml");
		BoltDeclarer bd = builder.setBolt("downloadExperiments", downloadExperiments, 1).allGrouping("getStudiesId");
		bd.allGrouping("1");
		
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMaxTaskParallelism(1);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("workberch", conf, builder.createTopology());
	}
}
