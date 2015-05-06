package Utils;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLUtils {

	/**
	 * get response body in xml format, and parse it to generate a DOM object.
	 * The request url must be valid and will return a xml document, otherwise
	 * null is returned and print stacktrace, but it won't throw any exception
	 * @param requestUrl
	 * @return
	 */
	public static Document fetchDOM(String requestUrl) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(requestUrl);
			return doc;
		} catch (Exception e) {
			System.err.println("fetchDOM error, return null...");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * parse a DOM object to generate string representation of the xml file
	 * @param doc
	 * @return
	 */
	public static String toXMLString(Document doc) {
		DOMSource domSource = new DOMSource(doc);
		StringWriter writer = new StringWriter();
		StreamResult result = new StreamResult(writer);
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer;
		try {
			transformer = tf.newTransformer();
			transformer.transform(domSource, result);
		} catch (TransformerException e) {
			e.printStackTrace();
			return null;
		}
		return writer.toString();
	}

	/**
	 * convert a nodelist to an arraylist of Node objects
	 * null input will throw NullpointerException
	 * @param nodes
	 * @return
	 */
	public static ArrayList<Node> convertNodeList(NodeList nodes) {
		ArrayList<Node> list = new ArrayList<Node>();
		for(int i = 0; i < nodes.getLength(); i++) {
			list.add(nodes.item(i));
		}
		return list;
	}

	/**
	 * get the first element of the given name, throws Exception if no element of 
	 * such name exists
	 * @param elem
	 * @param name
	 * @return
	 */
	public static Element getFirstElementByTagName(Element elem, String name) {
		return (Element) elem.getElementsByTagName(name).item(0);
	}
	

	/**
	 * get text content of the first element by tag name, returns null if not found
	 * @param elem
	 * @param name
	 * @return
	 */
	public static String getFirstElementTextByTagName(Element elem, String name) {
		try {
			Element found = getFirstElementByTagName(elem, name);
			return found.getTextContent();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * get a list of nodes with the given tagName, returns an empty list if no match,
	 * throw NullPointerException if node is null
	 * @param node
	 * @param tagName
	 * @return
	 */
	public static ArrayList<Node> getChildrenByTagName(Node node, String tagName) {
		ArrayList<Node> list = new ArrayList<Node>();
		NodeList nodes = node.getChildNodes();
		for(int i = 0; i < nodes.getLength(); i++) {
			if(nodes.item(i).getNodeName().equals(tagName)) {
				list.add(nodes.item(i));
			}
		}
		return list;
	}
}
