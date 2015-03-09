package main.java.parser.model;

import java.io.Serializable;

import main.java.utils.constants.WorkberchConstants;

public class WorkberchLink implements Serializable {

	private static final long serialVersionUID = -7867960519903176328L;
	
	private String sourceNode;
	private String sourceOutput;
	private String destNode;
	private String destOutput;
	private int sourceDepth;
	private int destDepth;
	
	public int getSourceDepth() {
		return sourceDepth;
	}

	public void setSourceDepth(final int sourceDepth) {
		this.sourceDepth = sourceDepth;
	}
	
	public int getDestDepth() {
		return destDepth;
	}
	
	public void setDestDepth(final int destDepth) {
		this.destDepth = destDepth;
	}
	
	public String getSourceNode() {
		return sourceNode;
	}
	
	public void setSourceNode(final String sourceNode) {
		this.sourceNode = sourceNode;
	}
	
	public String getSourceOutput() {
		return sourceOutput;
	}

	public void setSourceOutput(final String sourceOutput) {
		this.sourceOutput = sourceOutput;
	}
	
	public String getDestNode() {
		return destNode;
	}
	
	public void setDestNode(final String destNode) {
		this.destNode = destNode;
	}
	
	public String getDestOutput() {
		return destOutput;
	}
	
	public void setDestOutput(final String destOutput) {
		this.destOutput = destOutput;
	}
	
	public String getStormSourceField() {
		return getSourceNode() + WorkberchConstants.NAME_DELIMITER + getSourceOutput();
	}
	
	public String getStormDestField() {
		return getDestNode() + WorkberchConstants.NAME_DELIMITER + getDestOutput();
	}
	
}
