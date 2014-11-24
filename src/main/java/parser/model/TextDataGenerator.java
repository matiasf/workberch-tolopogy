package main.java.parser.model;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;

public class TextDataGenerator implements DataGenerator{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5043536220878487954L;
	String textConstant;
	
	

	public TextDataGenerator(final String textConstant) {
		super();
		this.textConstant = textConstant;
	}

	public String getTextConstant() {
		return textConstant;
	}

	public void setTextConstant(final String textConstant) {
		this.textConstant = textConstant;
	}

	@Override
	public List<Values> getValues() {
		final List<Values> ret = new ArrayList<Values>(); 

		final Values values = new Values(textConstant);
		
		ret.add(values);
		
		return ret;
	}
	
	
}
