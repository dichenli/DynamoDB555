/**********************************************************************************************
 * Copyright 2009 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file 
 * except in compliance with the License. A copy of the License is located at
 *
 *       http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License. 
 *
 * ********************************************************************************************
 *
 *  Amazon Product Advertising API
 *  Signed Requests Sample Code
 *
 *  API Version: 2009-03-31
 *
 */

package SearchDynamo;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.*;

import Utils.XMLUtils;
import DynamoDB.DynamoTable;

/**
 * search Amazon products
 * Sample code about Amazon Products API:
 * https://aws.amazon.com/code/Product-Advertising-API/2478
 * search results XML schema: 
 * http://webservices.amazon.com/AWSECommerceService/AWSECommerceService.xsd
 * @author dichenli
 *
 */
public class AmazonProductAPI {

	private static final String AWS_ACCESS_KEY_ID = DynamoTable.credentials.getAWSAccessKeyId();
	private static final String AWS_SECRET_KEY = DynamoTable.credentials.getAWSSecretKey();
	/*
	 * Use one of the following end-points, according to the region you are
	 * interested in:
	 *      US: ecs.amazonaws.com 
	 *      CA: ecs.amazonaws.ca 
	 *      UK: ecs.amazonaws.co.uk 
	 *      DE: ecs.amazonaws.de 
	 *      FR: ecs.amazonaws.fr 
	 *      JP: ecs.amazonaws.jp
	 */
	private static final String ENDPOINT = "ecs.amazonaws.com";
	private static final String assciateTag = "accio07-20";
	private static SignedRequestsHelper helper;
	static {
		try {
			helper = SignedRequestsHelper.getInstance(ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY);
		} catch (Exception e) {
			e.printStackTrace();
			helper = null;
		}
	}


	/**
	 * get a DOM object by fetching search results from Amazon using search phrase
	 * The first 10 results of the millions of hits will be returned
	 * @param phrase: the query phrase
	 * @return
	 */
	public static Document getDocumentByQuery(String phrase) {
		/*
		 * http://webservices.amazon.com/onca/xml
		 * ?Service=AWSECommerceService
		 * &Operation=ItemSearch
		 * &ResponseGroup=Small&SearchIndex=All
		 * &Keywords=harry_potter
		 * &AWSAccessKeyId=[My_AWSAccessKeyID]
		 * &AssociateTag=[My_AssociateTag]
		 * &Timestamp=[YYYY-MM-DDThh:mm:ssZ]
		 * &Signature=[Request_Signature]
		 */
		Map<String, String> params = new HashMap<String, String>();
		params.put("Service", "AWSECommerceService");
		params.put("Operation", "ItemSearch");
		params.put("Keywords", AmazonProductAPI.convertPhrase(phrase));
		params.put("ResponseGroup", "Small"); //return 10 results
		params.put("SearchIndex", "All");
		params.put("AssociateTag", assciateTag);

		String requestUrl = helper.sign(params);
		Document doc = XMLUtils.fetchDOM(requestUrl);
		return doc;
	}

	/**
	 * return a list of nodes that has Tag name "Item" from Amazon search results
	 * The Item has schema available on:
	 * http://webservices.amazon.com/AWSECommerceService/AWSECommerceService.xsd
	 * @param phrase
	 * @return search results in Node list, or an empty list if no 
	 * results returned or internal error happened (in which error stacktrace will be
	 * printed but not thrown)
	 */
	public static List<Node> searchAmazonProducts(String phrase) {
		try {
			Document doc = getDocumentByQuery(phrase);
			System.out.println(XMLUtils.toXMLString(doc));
			NodeList items = doc.getElementsByTagName("Item");
			if(items == null || items.getLength() == 0) {
				return new ArrayList<Node>();
			}
			return XMLUtils.convertNodeList(items); 
		} catch (Exception e) {
			System.err.println("Error happened in searchAmazonProducts...");
			return new ArrayList<Node>();
		}
	}

	/**
	 * the search query phrase needs to be removed of empty spaces before they can
	 * be sent as query parameters
	 * @param raw
	 * @return
	 */
	static String convertPhrase(String raw) {
		return raw.replaceAll(" |\t|\n|\r", "_");
	}

//	static String getTitle(Node item) {
//		try {
//			if(item.getNodeName().equals("Item")) {
//				throw new IllegalArgumentException();
//			}
//
//			return XMLUtils.getFirstElementByTagName((Element) item, "Title").getTextContent();
//		} catch (Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//	}

	/**
	 * get the title or url of the node as an Item element
	 * @param item
	 * @return
	 */
	static String getTitleOrUrl(Node item) {
		if(!item.getNodeName().equals("Item")) {
			throw new IllegalArgumentException();
		}
		String title = XMLUtils.getFirstElementTextByTagName((Element) item, "Title");
		if(title == null) {
			return XMLUtils.getFirstElementTextByTagName((Element) item, "DetailPageURL");
		}
		return title;
	}
	
	static String getUrl(Node item) {
		if(!item.getNodeName().equals("Item")) {
			throw new IllegalArgumentException();
		}
		return XMLUtils.getFirstElementTextByTagName((Element) item, "DetailPageURL");
	}

}
