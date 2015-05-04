package SearchDynamo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import spellchecker.Dictionary;
import spellchecker.FileCorrector;
import spellchecker.SpellChecker;
import spellchecker.SwapCorrector;
import BerkeleyDB.DBWrapper;
import DynamoDB.QueryRecord;
import SearchUtils.SearchResult;
import Utils.ProcessUtils;

/**
 * Servlet implementation class Accio
 */

public class Accio extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final String PARSER = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";

       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Accio() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {		
		
		String path = request.getRequestURI().substring(request.getContextPath().length());

		if(path.equals("/Accio")){
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.write("<!DOCTYPE html><html>"
					+ "<head>"
					+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
					+ "<link rel=\"stylesheet\" href=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css\">"
					+ "<style>"
					+ "body {"
						+ "background: url('/DynamoDB555/hp.png');"
						+ "background-size: 1280px 800px;"
						+ "background-repeat:no-repeat;"
						+ "padding-top: 150px;"
					+ "}"
					+ "@media (max-width: 980px) {"
					+ "body {"
					+ "padding-top: 0;"
					+ "}"
					+ "}"
					+ "</style>"
					+ "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>"
					+ "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js\"></script>"
					
					+ "<title>Accio Search Engine</title>"
					+ "</head>"
						+ "<body>"	
						
						+ "<div class = \"row\"></div>"
						+ "<div class=\"container\">"
							+ "<h1 class = \"text-center\">Accio</h1>"
							+ "<div class=\"row\">"
								+ "<form role=\"form\" action=\"/DynamoDB555/Accio\" method=\"post\">"
									+ "<div class=\"col-md-3\">"
									+ "</div>"
									+ "<div class=\"col-md-6\">"
										+ "<input type=\"text\" id=\"inputdefault\" class=\"form-control\" name=\"phrase\" placeholder=\"What you are looking for?\">"
									+ "</div>"
									+ "<div class=\"col-md-3\">"
										+ "<button type=\"submit\" class=\"btn btn-info\">"
											+ "<span class=\"glyphicon glyphicon-flash\"></span> search"
										+ "</button>"
									+ "</div>"
								+ "</form>"
							+ "</div>"
		
						+ "</div>"
						
						+ "</body> "
					+ "</html>");
			out.flush();
		}
		else if(path.equals("/insertquery")){
			String url = request.getParameter("url");
			String query = request.getParameter("query");
			String docID = ProcessUtils.toBigInteger(url);
			QueryRecord.increment(query, docID);
		} else if (path.equals("/match_highlight")) {
			System.out.println("match_highlight");
			String decimalID = request.getParameter("decimalID");
			String query = request.getParameter("query");
			System.out.println(decimalID + query);
			//the query here is actually the wordList in QueryInfo class, which means it is stemmed, selected, 
			//then it is marshalled by SearchResult getWordListMarshall(), and send to client side
			//then it is returned to server by the parameter "query" here
			String highlight = HighlightGenerator.generate(decimalID, query);
			response.setHeader("Content-Type", "text/plain");
		    response.setHeader("success", "yes");
		    response.setHeader("Content-Type", "" + highlight.length());
		    PrintWriter writer = response.getWriter();
		    writer.write(highlight); //send plain text that is the highlight text
		    writer.close();
		} 
		/**
		 * insist on searching the original page
		 * */
		else if(path.equals("/insist")){
			doPost(request, response);
		}
		else {
			System.out.println("no match");
			response.sendError(400);
		}
		
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String path = request.getRequestURI().substring(request.getContextPath().length());
		String phrase = request.getParameter("phrase");
		String wiki_html = "";
		String webapp = request.getContextPath();
		List<SearchResult> results = new ArrayList<SearchResult>();
		String word;
		ArrayList<String> words = new ArrayList<String>();
		StringBuilder newPhrase = new StringBuilder("");
		int i = 0;
		boolean correct = true;
		System.out.println("the phrase is "+phrase);
		StringTokenizer tokenizer = new StringTokenizer(phrase,PARSER);
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if (word.equals("")) continue;
//			System.out.println(word);
			words.add(word);
			
			
		}
		
//		for(int i = 0; i < words.length; i++){
//			words[i].trim().toLowerCase();
//			if(words[i].length()!=0){
//				
//			}
//			
//		}
		if(path.equals("/Accio")){
			SpellChecker sc = new SpellChecker();
			
			/**
			 * spell check part
			 * */
			
			for(i = 0; i < words.size(); i++){
				word = words.get(i);
				System.out.println("the word is "+word);
				if(sc.isWord(word)){
					System.out.println("in the dictionary");
					correct = true;
					continue;
				}
				else{
					System.out.println("not in the dictionary");
					correct = false;
					if(sc.isCommonMisspell(word)){
						System.out.println("is common misspelling");
						String right = sc.getRightMisspell(word);
						words.set(i, right);
					}
					else{
						String right = sc.getRightSwap(word);
						words.set(i, right);
					}
				}
			}
			
			for(i = 0 ; i < words.size(); i++){
				newPhrase.append(words.get(i)+" ");
			}
		}
		else{
			newPhrase.append(phrase);
		}
		
		/**
		 * wiki part
		 * */
		try {
			wiki_html = WikiSearch.wiki(words);
		} catch (Exception e) {
			e.printStackTrace();
		}
		/**
		 * our own part
		 * */
		try {
			results = AnalQuery.search(newPhrase.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.write("<!DOCTYPE html>"
				+ "<html lang=\"en\">"
					+ "<head>"
						+ "<title>Accio</title>"
						+ "<meta charset=\"utf-8\">"
						+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
						+ "<link rel=\"stylesheet\" href=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css\">"
						+ "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>"
						+ "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js\"></script>"
						
						// Youtube
						+ "<script src=\"/DynamoDB555/youtube/js/jquery-1.11.2.min.js\"></script>"
						+ "<script src=\"/DynamoDB555/youtube/js/script.js\"></script>"
//						+ "<script src=\"http://localhost:8080/DynamoDB555/youtube/js/Togglable_tab.js\"></script>"

						+ "<style>"
							+ "body {"
								+ "padding-top: 15px;"
							+ "}"
						+ "</style>"
					+ ""
					+ "</head>"
					+ "<body>"
					
					
					
					
					
					+ "<h3 hidden id=\"query\">" + newPhrase +"</h3>"
						+ "<div class=\"container\">"
							+ "<div class=\"row\">"
								+ "<form role=\"form\" action=\""+webapp+"/Accio\" method=\"post\">"
									+ "<div class=\"col-md-1\">"
										+ "<a href=\"/DynamoDB555/Accio\">"
										+ "<h4>Accio</h4>"
										+ "</a>"
									+ "</div>"
									+ "<div class=\"col-md-6\">"
										+ "<input type=\"text\" id=\"inputdefault\" class=\"form-control\" name=\"phrase\" placeholder=\""
										+ phrase
										+ "\">"
									+ "</div>"
									+ "<div class=\"col-md-3\">"
										+ "<button type=\"submit\" class=\"btn btn-info\">"
											+ "<span class=\"glyphicon glyphicon-flash\"></span> search"
										+ "</button>"
									+ "</div>"
								+ "</form>"
							+ "</div>");
		if(!correct){
			out.write("<div class=\"row\">"
						
						+ "<h4><i> Showing results for "+newPhrase.toString()+"</i></h4>"
					+ "</div>");
			out.write("<div class=\"row\">"
						+ "<h5>Insist on searching "
						+ "<a href=\""+webapp+"/insist?phrase="+phrase+"\">"
								+ phrase
						+ "</a>"
					+ "</div>");
		}
		
//							+ "<h2>"
//							+ "important things need to be said three times!"
//							+ "</h2>"
		out.write("<div class=\"col-md-8\">"
					+ "<ul class=\"list-group\">");
		
		
		for(int j = 0; j < results.size(); j++){
			out.write("<li class=\"list-group-item\">");
			out.write("<a size=\"30\" href="+results.get(j).getUrl()+" onclick=\"sendRequest()\">"+results.get(j).getTitle()+"</a>");
			out.write("<p id=\"match_highlight" + j + "\" style=\"color:grey\">loading...</p>");
			out.write("</li>");
		}
									
						out.write("</ul>"
							+ "</div>"
						    + "<div class=\"col-md-4\">"
							
							// Togglable Tab
								+"<div role=\"tabpanel\">"

							  
									+" <ul class=\"nav nav-tabs\" role=\"tablist\">"
									+"    <li role=\"presentation\" class=\"active\"><a href=\"#wiki\" aria-controls=\"wiki\" role=\"tab\" data-toggle=\"tab\">Wikipedia</a></li>"
									+"    <li role=\"presentation\"><a href=\"#youtube\" aria-controls=\"youtube\" role=\"tab\" data-toggle=\"tab\">Youtube</a></li>"
									+" </ul>"

							 
								+"<div class=\"tab-content\">"
								+"    <div role=\"tabpanel\" class=\"well tab-pane active\" id=\"wiki\">"+ wiki_html +"</div>"
								+"    <div role=\"tabpanel\" class=\"tab-pane\" id=\"youtube\">"
								
										// Youtube
										+ "<div id=\"container_youtube\">"
										+"	<h1>Youtube Videos</h1>"
										+"	<ul id=\"results_youtube\"></ul>"
										+"</div>"
								
									+"</div>"
								+"</div>"

							  + "</div>"
								
							+ "</div>"
						+ "</div>"
					+ "\n<script type=\"text/javascript\">");
					out.write("window.onload = function() {");
					for(int j = 0; j < results.size(); j++) {
						out.write("match_highlight("
									+ j + ",'" 
									+ results.get(j).getID() + "','" 
									+ phrase //send the stemmed and processed word list to highlight generator
								+ "');\n");
					}
					out.write("};\n");
					//generate match highlight text
					out.write("function match_highlight(i, decimalID, query) { "
					+	"console.log(decimalID);"
					+ 	"var xmlhttp; "
					+ 	"if (window.XMLHttpRequest){ "
					+ 		"xmlhttp = new XMLHttpRequest(); "
					+ 	"} else { "
					+ 		"xmlhttp = new ActiveXObject(\"Microsoft.XMLHTTP\"); "
					+ 	"} "
					+ 	"var path = \"/DynamoDB555/match_highlight?\" + \"decimalID=\" + decimalID + \"&query=\" + query;"
					+	"xmlhttp.onreadystatechange=function() { "
					+		"if (xmlhttp.readyState==4 && xmlhttp.status==200) {"
					+ 			"document.getElementById(\"match_highlight\" + i).innerHTML "
					+ 			"= xmlhttp.responseText; "
					+ 		"} else {document.getElementById(\"match_highlight\" + i).innerHTML = \"Error\"}"
					+ 	"};"
					+ 	"xmlhttp.open(\"GET\", path, true); "//false: synchronous
					+ 	"xmlhttp.send(); "
					+ "};\n"
					//click to send to QueryRecord
					+ "function sendRequest() {"
					+ "console.log(\"receive request\");"
					+ "var target = event.target;"
					+ "var url = target.innerHTML;"
					+ "var query = document.getElementById(\"query\").innerHTML;"
					+ "console.log(query);"
					+ "var xmlhttp;"
					+ "if (window.XMLHttpRequest){"
					+ "xmlhttp = new XMLHttpRequest();"
					+ "}"
					+ "else{ xmlhttp = new ActiveXObject(\"Microsoft.XMLHTTP\");}"
					+ "var getquery = \"url=\" + url + \"&query=\" + query;"
					+ "var path = \"/DynamoDB555/insertquery?\" + getquery;"
					+ "console.log(path);"
					+ "xmlhttp.open(\"GET\", path, true);"
					+ "xmlhttp.send();"
					+ "}"
					+ "</script>"	

					
					+ "</body>"
				+ "</html>");

		out.flush();
	}
	
	

}
