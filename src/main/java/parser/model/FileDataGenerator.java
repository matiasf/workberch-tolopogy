package main.java.parser.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import backtype.storm.tuple.Values;

public class FileDataGenerator implements DataGenerator {

	static private String VALUE_TAG_NAME = "t2sr:value";
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8530606377443305806L;
	private String filePath;
	

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(final String filePath) {
		this.filePath = filePath;
	}

	@Override
	public List<Values> getValues() {
		
		final File fXmlFile = new File(filePath);
		final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		final List<Values> ret = new ArrayList<Values>();
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			final Document doc = dBuilder.parse(fXmlFile);
			
			final NodeList nList = doc.getElementsByTagName(VALUE_TAG_NAME);
			
			for (int temp = 0; temp < nList.getLength(); temp++) {
				final Node nNode = nList.item(temp);
				final Values values = new Values(nNode.getTextContent());
				ret.add(values);
			
			}
		
			
			
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
		
		
		
		return ret;
	}

}
