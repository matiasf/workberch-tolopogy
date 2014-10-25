package main.java.utils.cartesianindex;

import java.util.Iterator;
import java.util.List;

public class CartesianNode implements CartesianIndex {
	
	private static final long serialVersionUID = 3021723666996102390L;
	
	private List<CartesianIndex> childNodes;
	
	public CartesianNode(final List<CartesianIndex> childNodes) {
		this.childNodes = childNodes;
	}

	@Override
	public boolean isLeafValue() {
		return false;
	}

	@Override
	public Long getValue() {
		throw new UnsupportedOperationException("This is a Cartesian node, not value in here.");
	}
	
	@Override
	public void setValue(final long value) {
		throw new UnsupportedOperationException("This is a Cartesian node, not value in here.");
	}

	@Override
	public List<CartesianIndex> getNodes() {
		return childNodes;
	}
	
	@Override
	public void setNewNodes(final List<CartesianIndex> newNodes) {
		childNodes = newNodes;
	}
	
	@Override
    public int hashCode() {
		int byMult = 1;
		int hashCode = 0;
        for (final CartesianIndex node : childNodes) {
        	hashCode += node.hashCode()*byMult;
        	byMult *= 10;
		}
        return hashCode;
    } 
	
	@Override
	public boolean equals(final Object node) {
		if (node instanceof CartesianNode) {
			boolean allEquals = true;
			final Iterator<CartesianIndex> itOther = ((CartesianNode)node).getNodes().iterator();
			final Iterator<CartesianIndex> itCurrent = childNodes.iterator();

			while (itCurrent.hasNext() && itOther.hasNext()) {
				allEquals &= itCurrent.next().equals(itOther.next());			
			}
			return allEquals && !itOther.hasNext() && !itCurrent.hasNext();
		}
		return false;
	}

}
