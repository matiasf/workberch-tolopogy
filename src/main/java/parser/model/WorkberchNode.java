package main.java.parser.model;

import main.java.spouts.WorkberchGenericSpout;


public interface WorkberchNode {

	String getName();

	WorkberchGenericSpout buildSpout();
}
