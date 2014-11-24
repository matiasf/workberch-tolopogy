package main.java.utils.constants;

public enum TavernaNodeType {
	
	XPATH("http://ns.taverna.org.uk/2010/activity/xpath#Config"),
	REST("http://ns.taverna.org.uk/2010/activity/rest#Config"),
	BEANSHELL("http://ns.taverna.org.uk/2010/activity/beanshell#Config"),
	TEXT_CONSTANT("http://ns.taverna.org.uk/2010/activity/constant#Config");
	
	private final String url;
	
	TavernaNodeType(final String typeUrl) {
		url = typeUrl;
	}
	
	public String getTypeUrl() {
		return url;
	}	
	
	public static TavernaNodeType fromString(final String value) {
		if (value != null) {
			for (final TavernaNodeType type : TavernaNodeType.values()) {
				if (value.equalsIgnoreCase(type.getTypeUrl())) {
					return type;
				}
			}
		}
		
		return null;
	}

}
