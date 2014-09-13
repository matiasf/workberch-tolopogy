package main.java.spouts;

import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SpoutTrucho extends BaseRichSpout{

	private List<String> spoutFields;
    private SpoutOutputCollector collector;
    private Random rand;
    
    boolean send=false;

    public SpoutTrucho(final List<String> fields) {
    	spoutFields = fields;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.rand = new Random();
    }

    @Override
    public void nextTuple() {
		if (!send) {
			send = true;
			
			Values values = new Values("sra", "stem%20cell", 1000, "ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/");
			
			//Values values = new Values("<?xml version=\"1.0\" encoding=\"UTF-8\"?><bookstore><book category=\"COOKING\">  <title lang=\"en\">Everyday Italian</title>  <author>Giada De Laurentiis</author>  <year>2005</year>  <price>30.00</price></book><book category=\"CHILDREN\">  <title lang=\"en\">Harry Potter</title>  <author>J K. Rowling</author>  <year>2005</year>  <price>29.99</price></book><book category=\"WEB\">  <title lang=\"en\">XQuery Kick Start</title>  <author>James McGovern</author>  <author>Per Bothner</author>  <author>Kurt Cagle</author>  <author>James Linn</author>  <author>Vaidyanathan Nagarajan</author>  <year>2003</year>  <price>49.99</price></book><book category=\"WEB\">  <title lang=\"en\">Learning XML</title>  <author>Erik T.Ray</author>  <year>2003</year>  <price>39.95</price></book></bookstore>");
			
			System.out.print("Emitiendo tupla: ");
			for (Object value : values) {
			    System.out.print(value.toString());
			    System.out.print("-");
			}
			System.out.println();
			collector.emit(values);
		}
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields(spoutFields));
    	//System.out.println(spoutFields.toString());
    }
}
