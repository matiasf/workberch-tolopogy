package main.java.parser.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bsh.EvalError;
import bsh.Interpreter;

public class BeanshellLogicExecutor implements LogicExecuter, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4388909980140292783L;
	
	
	String script;
	Map<String, Object> inputValues = new HashMap<String, Object>();
	List<String> outputNames = new ArrayList<String>();
	
	public String getScript() {
		return script;
	}

	public void setScript(final String script) {
		this.script = script;
	}

	
	public Map<String, Object> getInputValues() {
		return inputValues;
	}

	public void setInputValues(final Map<String, Object> inputValues) {
		this.inputValues = inputValues;
	}

	public List<String> getOutputNames() {
		return outputNames;
	}
	
	@Override
	public void setOutputNames(final List<String> outputNames) {
		this.outputNames = outputNames;
	}

	
	@Override
	public void setParam(final String inputName, final Object value) {
		inputValues.put(inputName, value);
	}

	@Override
	public Map<String, Object> executeLogic() {
		final Interpreter interpreter = new Interpreter();
		final Map<String, Object> ret = new HashMap<String, Object>();
		try {
    		for (final String input : inputValues.keySet()) {
    			interpreter.set(input, inputValues.get(input));
			}
    		
    		interpreter.eval(script);
    		
    		for (final String output : outputNames) {
    			ret.put(output, interpreter.get(output));
    		}
    		
		}
		
		catch (final EvalError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return ret;
	}
	

}
