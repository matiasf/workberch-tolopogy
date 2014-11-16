package main.java.utils;

public class ExecutedValue {

	private Object value;
	private String indexCode;
	private Object index;
	
	public ExecutedValue(final Object value, final String indexCode, final Object index) {
		this.value = value;
		this.indexCode = indexCode;
		this.index = index;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(final Object value) {
		this.value = value;
	}

	public String getIndexCode() {
		return indexCode;
	}

	public void setIndexCode(final String indexCode) {
		this.indexCode = indexCode;
	}

	public Object getIndex() {
		return index;
	}

	public void setIndex(final Object index) {
		this.index = index;
	}

}
