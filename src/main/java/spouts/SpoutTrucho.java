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
			
			Values values = new Values("sra", "stem%20cell", 1000);
			
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
