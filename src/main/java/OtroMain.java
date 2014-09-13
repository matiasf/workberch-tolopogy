package main.java;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.BaseBolt;
import main.java.bolts.CreateFTPURLsBolt;
import main.java.bolts.DownloadSRA;
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
		field1.add("ftpURLInput");
		
		builder.setSpout("1", new SpoutTrucho(field1));
		
		List<String> field2 = new ArrayList<String>();
		field2.add("B");
		//builder.setSpout("2", new SpoutTrucho(field2));
		
		List<String> searchStudiesInput = new ArrayList<String>();
		searchStudiesInput.add("db");
		searchStudiesInput.add("term");
		searchStudiesInput.add("retmax");
		String url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db={db}&term={term}&retmax={retmax}&usehistory=y";
		BaseBolt searchStudies = new RestBolt(searchStudiesInput, field2, url, "GET", "text/xml");
		
		//BoltDeclarer bolt = builder.setBolt("3", basebolt, 1).allGrouping("1").shuffleGrouping("2");
		BoltDeclarer bolt = builder.setBolt("searchStudies", searchStudies, 1).shuffleGrouping("1");
		
		List<String> getStudiesOut = new ArrayList<String>();
		getStudiesOut.add("id");
		BaseBolt getStudiesId = new XPathBolt(field2, getStudiesOut, "/eSearchResult/IdList/Id");
		builder.setBolt("getStudiesId", getStudiesId, 3).shuffleGrouping("searchStudies");
		
		List<String> downloadExperimentsFields = new ArrayList<String>();
		downloadExperimentsFields.add("db");
		downloadExperimentsFields.add("id");
		
		
		url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={db}&id={id}";
		BaseBolt downloadExperiments = new RestBolt(downloadExperimentsFields, field2, url, "GET", "text/xml");
		BoltDeclarer bd = builder.setBolt("downloadExperiments", downloadExperiments, 3).shuffleGrouping("getStudiesId");
		bd.allGrouping("1");
		
		
		List<String> getRunAccessionsIdsOut = new ArrayList<String>();
		getRunAccessionsIdsOut.add("runAccessionID");
		BaseBolt getRunAccessions = new XPathBolt(field2, getRunAccessionsIdsOut, "/EXPERIMENT_PACKAGE_SET/EXPERIMENT_PACKAGE/RUN_SET/RUN/@accession");
		builder.setBolt("getRunAccessions", getRunAccessions, 3).shuffleGrouping("downloadExperiments");
		
		/*List<String> createFtpUrlsInput = new ArrayList<>();
		createFtpUrlsInput.add("runAccessionID");
		createFtpUrlsInput.add("ftpURLInput");
		
		List<String> createFtpUrlsOutput = new ArrayList<>();
		createFtpUrlsOutput.add("runAccessionID");
		createFtpUrlsOutput.add("ftpURLInput");
		
		BaseBolt createFTPUrls = new DownloadSRA(createFtpUrlsInput, createFtpUrlsOutput);
		builder.setBolt("createFTPUrls", createFTPUrls).globalGrouping("1").shuffleGrouping("getRunAccessions");*/
		
		
		/*
		
		List<String> getExperimentAccessionOut = new ArrayList<String>();
		getExperimentAccessionOut.add("experimentAccessionID");
		BaseBolt getExpAccessions = new XPathBolt(field2, getExperimentAccessionOut, "/EXPERIMENT_PACKAGE_SET/EXPERIMENT_PACKAGE/EXPERIMENT/@accession");
		builder.setBolt("getExpAccessions", getExpAccessions, 1).shuffleGrouping("downloadExperiments");
		
		List<String> getStudyRefIdOut = new ArrayList<String>();
		getStudyRefIdOut.add("studyRedId");
		BaseBolt getStudyRefId = new XPathBolt(field2, getStudyRefIdOut, "/EXPERIMENT_PACKAGE_SET/EXPERIMENT_PACKAGE/STUDY/@accession");
		builder.setBolt("getStudyRefId", getStudyRefId, 1).shuffleGrouping("downloadExperiments");
		*/
		
		
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("workberch", conf, builder.createTopology());
	}
}
