package main.java.parser.model;

import main.java.bolts.WorkberchGenericBolt;

public interface WorkberchBoltBuilder {

	public WorkberchGenericBolt buildBolt(final String guid);
	
}
