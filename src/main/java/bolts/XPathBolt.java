package main.java.bolts;

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

import main.java.utils.WorkberchTuple;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import backtype.storm.topology.BasicOutputCollector;

import com.fasterxml.jackson.databind.JsonNode;

public class XPathBolt extends WorkberchTavernaProcessor {

    String xPathExpression;

    public XPathBolt(final List<String> inputFields, final List<String> outputFields, final String xPathExpression) {
		super(inputFields, outputFields);
		this.xPathExpression = xPathExpression;
    }
    
    public XPathBolt(final List<String> inputFields, final List<String> outputFields, final JsonNode node) {
    	super(inputFields, outputFields, node);
    	
    }

    @Override
    public void executeLogic(final WorkberchTuple tuple, final BasicOutputCollector collector) {

	try {

	    final XPathFactory xPathfactory = XPathFactory.newInstance();
	    final XPath xpath = xPathfactory.newXPath();
	    final XPathExpression expr = xpath.compile(xPathExpression);

	    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder builder;
	    for (final Object value : tuple.getValues().values()) {
		final String xmlString = value.toString();

		builder = factory.newDocumentBuilder();
		final Document document = builder.parse(new InputSource(new StringReader(xmlString)));
		final NodeList nl = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
		for (int i = 0; i < nl.getLength(); i++) {
		    System.out.println(nl.item(i).toString() + nl.item(i).getTextContent());
		    final String nodeContent = nl.item(i).getTextContent();
		    final List<Object> emitTuple = new ArrayList<Object>();
		    for (final String string : getOutputFields()) {
			emitTuple.add(nodeContent);
		    }
		    collector.emit(emitTuple);
		}
	    }

	} catch (final XPathExpressionException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (final ParserConfigurationException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (final SAXException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (final IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

	@Override
	protected void initFromJsonNode(final JsonNode jsonNode) {
		xPathExpression = jsonNode.get("xpathExpression").asText();
		
	}

}
