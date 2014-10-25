package main.java.utils.cartesianindex;

import java.util.List;

public class CartesianLeaf implements CartesianIndex {
	
	private static final long serialVersionUID = 596686272185221264L;
	
	private Long currentValue;
	
	public CartesianLeaf(final Long value) {
		currentValue = value;
	}

	@Override
	public boolean isLeafValue() {
		return true;		
	}

	@Override
	public Long getValue() {
		return currentValue;
	}
	
	@Override
	public void setValue(final long value) {
		currentValue = value;
	}

	@Override
	public List<CartesianIndex> getNodes() {
		throw new UnsupportedOperationException("This is a Cartesian value, not nodes in here.");
	}
	
	@Override
	public void setNewNodes(final List<CartesianIndex> newNodes) {
		throw new UnsupportedOperationException("This is a Cartesian value, not nodes in here.");
	}
	
	@Override
    public int hashCode() {
		return currentValue.intValue();
    } 
	
	@Override
	public boolean equals(final Object node) {
		if (node instanceof CartesianLeaf) {
			return ((CartesianLeaf) node).getValue().equals(currentValue);
		}
		return false;
	}

}
