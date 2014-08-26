package main.java.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import main.java.utils.BaseTuple;
import backtype.storm.topology.BasicOutputCollector;

public class RestBolt extends BaseBolt{

	String address;
	String requestMethod;
	String accetpHeader;
	
	final String ACCEPT_PROP = "Accept";
	
	public RestBolt(List<String> inputFields, 
			List<String> outputFields, 
			String address, 
			String requestMethod, 
			String accetpHeader) {
		
		super(inputFields, outputFields);
			
		this.address = address;
		this.requestMethod = requestMethod;
		this.accetpHeader = accetpHeader;
			
		

	}

	@Override
	public void executeLogic(BasicOutputCollector collector, BaseTuple tuple) {
		
		String localAddress = this.address;
		
		for (String param : tuple.getValues().keySet()) {
			localAddress = localAddress.replace("{" + param + "}", tuple.getValues().get(param).toString());
		}
		
		try {
			
			URL url = new URL(localAddress);

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(requestMethod);
			conn.setRequestProperty(ACCEPT_PROP, accetpHeader);

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException(
						"Falló el la conexion en REST Bolt : HTTP error code : "
								+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
					(conn.getInputStream())));

			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}

			conn.disconnect();

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		catch (ProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	

}