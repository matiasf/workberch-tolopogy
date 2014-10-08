package main.java.bolts;

import static main.java.utils.constants.WorkberchConstants.INDEX_FIELD;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.databind.JsonNode;

import main.java.utils.WorkberchTuple;
import backtype.storm.topology.BasicOutputCollector;

public class XPathBolt extends WorkberchTavernaProcessorBolt {

    String xPathExpression;

    public XPathBolt(List<String> outputFields, String xPathExpression) {
		super(outputFields);
		this.xPathExpression = xPathExpression;
    }
    
    public XPathBolt(List<String> inputFields, List<String> outputFields, JsonNode node) {
    	super(inputFields, outputFields, node);
    	
	}

	@Override
	public void executeLogic(WorkberchTuple tuple,
			BasicOutputCollector collector) {

		try {

			XPathFactory xPathfactory = XPathFactory.newInstance();
			XPath xpath = xPathfactory.newXPath();
			XPathExpression expr = xpath.compile(xPathExpression);

			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder;
			
			Object value = tuple.getValues().values().iterator().next();
			String xmlString = value.toString();

			builder = factory.newDocumentBuilder();
			Document document = builder.parse(new InputSource(
					new StringReader(xmlString)));
			NodeList nl = (NodeList) expr.evaluate(document,
					XPathConstants.NODESET);
			
			List<Object> emitTuple = new ArrayList<Object>();
			
			for (int i = 0; i < nl.getLength(); i++) {
				System.out.println(nl.item(i).toString()
						+ nl.item(i).getTextContent());
				String nodeContent = nl.item(i).getTextContent();
				emitTuple.add(nodeContent);
			    	
			}
			List<Object> salida = new ArrayList<Object>();
			salida.add(emitTuple);
			salida.add(tuple.getValues().get(INDEX_FIELD));
		    emitTuple(salida, collector);
			

		} catch (XPathExpressionException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (ParserConfigurationException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (SAXException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

	@Override
	protected void initFromJsonNode(JsonNode jsonNode) {
		this.xPathExpression = jsonNode.get("xpathExpression").asText();
		
	}

}
