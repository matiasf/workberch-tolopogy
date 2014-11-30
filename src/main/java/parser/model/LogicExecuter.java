package main.java.parser.model;

import java.util.List;
import java.util.Map;

public interface LogicExecuter {
	
	public void setParam(String inputName, Object value);
	
	public void setOutputNames(List<String> outputNames);
	
	public Map<String, Object> executeLogic();

}
