package main.java.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

<<<<<<< HEAD
public class RestBolt extends WorkberchTavernaProcessorBolt {
=======
import com.fasterxml.jackson.databind.JsonNode;

public class RestBolt extends WorkberchTavernaProcessor {
>>>>>>> c6badcd24c15517eb0d844be52c904a885b4a610

    String address;
    String requestMethod;
    String accetpHeader;

    final String ACCEPT_PROP = "Accept";    
    
    public RestBolt(final List<String> inputFields, final List<String> outputFields, final String address, final String requestMethod,
	    final String accetpHeader) {

		super(inputFields, outputFields);
	
		this.address = address;
		this.requestMethod = requestMethod;
		this.accetpHeader = accetpHeader;

    }
    
    public RestBolt(final List<String> inputFields, final List<String> outputFields, final JsonNode node) {
    	super(inputFields, outputFields, node);
    }

    @Override
    public void executeLogic(final WorkberchTuple tuple, final BasicOutputCollector collector) {

	String localAddress = address;

	for (final String param : tuple.getValues().keySet()) {
	    localAddress = localAddress.replace("{" + param + "}", tuple.getValues().get(param).toString());
	}

	try {

	    final URL url = new URL(localAddress);

	    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    conn.setRequestMethod(requestMethod);
	    conn.setRequestProperty(ACCEPT_PROP, accetpHeader);

	    if (conn.getResponseCode() != 200) {
		System.out.println("Se fallo en la url " + localAddress);
		throw new RuntimeException("Falló el la conexion en REST Bolt : HTTP error code : "
			+ conn.getResponseCode());
	    }

	    final BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

	    String output;
	    System.out.println("Output from Server .... \n");

	    final StringBuilder sb = new StringBuilder();
	    while ((output = br.readLine()) != null) {
		sb.append(output);
		// System.out.println(output);
	    }

	    conn.disconnect();

	    final String outputValue = sb.toString();

	    final List<Object> outputValues = new ArrayList<Object>();

	    for (final String string : getOutputFields()) {
		outputValues.add(outputValue);
	    }

	    collector.emit(outputValues);

	} catch (final MalformedURLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();

	} catch (final ProtocolException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();

	} catch (final IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

	@Override
	protected void initFromJsonNode(final JsonNode jsonNode) {
		
		final JsonNode requestNode = jsonNode.get("request");
		
		requestMethod = requestNode.get("httpMethod").asText();
		address = requestNode.get("absoluteURITemplate").asText();
		
		//TODO Hay que ver que se con los demas headers
		final JsonNode header = requestNode.get("headers").get(0);
		
		accetpHeader = header.get("value").asText();
		
	}

}
