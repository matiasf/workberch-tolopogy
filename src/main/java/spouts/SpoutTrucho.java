package main.java.spouts;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SpoutTrucho extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	
	private final List<String> spoutFields;
    private SpoutOutputCollector collector;
    
    boolean send=false;

    public SpoutTrucho(final List<String> fields) {
    	spoutFields = fields;
    }

    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		this.collector = collector;
    }

    @Override
    public void nextTuple() {
		if (!send) {
			send = true;
			
			final Values values = new Values("sra", "stem%20cell", 1000, "ftp://ftp-trace.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/");
			collector.emit(values);
		}
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields(spoutFields));
    }
}
