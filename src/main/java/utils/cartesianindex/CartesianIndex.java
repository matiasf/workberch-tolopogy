package main.java.utils.cartesianindex;

import java.io.Serializable;
import java.util.List;

public interface CartesianIndex extends Serializable {
	
	boolean isLeafValue();
	
	Long getValue();
	
	List<CartesianIndex> getNodes();

	void setNewNodes(List<CartesianIndex> newNodes);

	void setValue(long value);

}
