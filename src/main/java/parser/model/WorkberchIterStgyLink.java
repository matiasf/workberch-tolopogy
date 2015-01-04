package main.java.parser.model;

import java.util.ArrayList;
import java.util.List;

import main.java.bolts.WorkberchNameMaperBolt;
import main.java.bolts.WorkberchNameMaperOrderedBolt;
import main.java.utils.constants.WorkberchConstants;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class WorkberchIterStgyLink implements WorkberchIterStgy {

	private WorkberchLink link;
	
	private static String MAPPER_PREFIX = "NAME_MAPPER_";
	private String processorName;

	public WorkberchLink getLink() {
		return link;
	}

	public void setLink(final WorkberchLink link) {
		this.link = link;
	}
	
	public void setProcessorName(final String processorName) {
		this.processorName = processorName;
	}

	@Override
	public String getProcessorName() {
		return processorName;
	}
	
	@Override
	public String getBoltName() {
		final String mapperName = MAPPER_PREFIX + processorName + WorkberchConstants.NAME_DELIMITER + link.getStormSourceField() ;
		
		return mapperName;
	}

	@Override
	public BoltDeclarer addStrategy2Topology(final String guid, final TopologyBuilder tBuilder) {
		BoltDeclarer ret = null;
		final List<String> inputs = new ArrayList<String>();
		inputs.add(link.getStormDestField());
		if (link.getSourceDepth() > link.getDestDepth()) {
			final WorkberchNameMaperOrderedBolt mapper = new WorkberchNameMaperOrderedBolt(guid, inputs);
			mapper.addLink(link.getStormSourceField(), link.getStormDestField());
			ret = tBuilder.setBolt(getBoltName(), mapper).shuffleGrouping(link.getSourceNode());
			
		}
		else {
			final WorkberchNameMaperBolt mapper = new WorkberchNameMaperBolt(guid, inputs);
			mapper.addLink(link.getStormSourceField(), link.getStormDestField());
			ret = tBuilder.setBolt(getBoltName(), mapper).shuffleGrouping(link.getSourceNode());
			
		}
		
		return ret;
	}

	@Override
	public List<String> getOutputFields() {
		final List<String> ret = new ArrayList<String>();
		ret.add(link.getStormDestField());
		return ret;
	}

}
