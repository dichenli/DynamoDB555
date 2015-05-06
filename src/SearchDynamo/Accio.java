package SearchDynamo;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import spellchecker.DBdir;
import spellchecker.SpellChecker;
import BerkeleyDB.DBWrapper;
import DynamoDB.*;
import SearchUtils.DocResult;
import Utils.BinaryUtils;
import Utils.ProcessUtils;
import Utils.TimeUtils;

// TODO: Auto-generated Javadoc
/**
 * Servlet implementation class Accio.
 */

public class Accio extends HttpServlet {
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;
	
	/** The Constant PARSER. */
	private static final String PARSER = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	
	private DBWrapper db;
	
    /**
     * Instantiates a new accio.
     *
     * @see HttpServlet#HttpServlet()
     */
    public Accio() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    /* (non-Javadoc)
     * @see javax.servlet.GenericServlet#init(javax.servlet.ServletConfig)
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
    	super.init(config);
//    	File lock = new File("/Users/peach/Documents/upenn/dBDictionary/*.lck");
    	String dbDir = DBdir.dir;
    	Path lockPath = Paths.get(dbDir+"je.lck");
    	Path lock2 = Paths.get(dbDir+"je.info.0.lck");
    	try {
			Files.delete(lockPath);
			Files.delete(lock2);
		} catch (IOException e1) {
//			e1.printStackTrace();
		}
    	db = new DBWrapper(DBdir.dir);
    	System.out.println("===================init");
    	try {
			DocURLTitle.init();
			IDF.init();
			InvertedIndex.init();
			QueryRecord.init();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    @Override
    public void destroy(){
    	db.closeEnv();
    	String dbDir = DBdir.dir;
    	Path lockPath = Paths.get(dbDir+"je.lck");
    	Path lock2 = Paths.get(dbDir+"je.info.0.lck");
    	try {
			Files.delete(lockPath);
			Files.delete(lock2);
		} catch (IOException e1) {
//			e1.printStackTrace();
		}
    	System.out.println("in the method of destroy");

    }

	/**
	 * Do get.
	 *
	 * @param request the request
	 * @param response the response
	 * @throws ServletException the servlet exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {		
		
		String path = request.getRequestURI().substring(request.getContextPath().length());
		String webapp = request.getContextPath();
		if(path.equals("/Accio")){
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.write("<!DOCTYPE html><html>"
					+ "<head>"
					+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, maximum-scale=10, user-scalable=no\">"
					+ "<link rel=\"stylesheet\" href=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css\">"
					+ "<link href='http://fonts.googleapis.com/css?family=Old+Standard+TT' rel='stylesheet'  type='text/css'>"
					+ "<style>"
					+ "body {"

						+ "background: url('"
						+ webapp
						+ "/hp1.jpg');"
						+ "background-size: cover;"
						
						+ "padding-top: 150px;"
					+ "}"
					+ "@media (max-width: 980px) {"
					+ "body {"
					+ "padding-top: 0;"
					+ "}"
					+ "}"
					+ "div.aboutus{"
					+	"padding-top: 180px;"
					+ "}"
					+ "h1.bigtitle{"
					+	"padding-top: 50px;"
					+ "}"
					+ "</style>"
					+ "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>"
					+ "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js\"></script>"
					
					+ "<title>Accio Search Engine</title>"
					+ "</head>"
						+ "<body>"	
						
						+ "<div class = \"row\"></div>"
						+ "<div class=\"container\">"
							+ "<h1 style=\"font-family: 'Old Standard TT', sans-serif\" class = \"text-center\"> <font size=\"10\">Accio</font></h1>"
							+ "<div class=\"row\">"
								+ "<form role=\"form\" action=\""+webapp+"/Accio\" method=\"post\">"
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
						
						+ "<div class=\"aboutus\" align=\"center\">"
						+ "<h3><a style=\"font-family: 'Old Standard TT', sans-serif\" href=\"/DynamoDB555/AboutUs.html\">"
							+ "<font style=\"color:black\">"
								+ "About us"
							+ "</font>"
						+ "</a></h3>"
						+ "</div>"
						
						+ "</body> "
					+ "</html>");
			out.flush();
		}
		else if(path.equals("/insertquery")){
			System.out.println("get insert query");
			String url = request.getParameter("url");
			String query = request.getParameter("query");
			System.out.println(url+" "+query);
			String docID = ProcessUtils.toBigInteger(url);
			QueryRecord.increment(query, docID);
			response.sendError(400);
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
	 * Do post.
	 *
	 * @param request the request
	 * @param response the response
	 * @throws ServletException the servlet exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String path = request.getRequestURI().substring(request.getContextPath().length());
		String phrase = request.getParameter("phrase");
		String wiki_html = "";
		String webapp = request.getContextPath();
		List<DocResult> results = new ArrayList<DocResult>();
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
			System.out.println(word);
			words.add(word);
			
			
		}
//		newPhrase.append(phrase);
//		
//		for(int i = 0; i < words.length; i++){
//			words[i].trim().toLowerCase();
//			if(words[i].length()!=0){
//				
//			}
//			
//		}
		if(path.equals("/Accio")){
			SpellChecker sc = new SpellChecker(db);
			
			/**
			 * spell check part
			 * */
			
			for(i = 0; i < words.size(); i++){
				word = words.get(i);
				System.out.println("the word is "+word);
				if(sc.isWord(word.toLowerCase())){
					System.out.println("in the dictionary");
					continue;
				}
				else{
					System.out.println("not in the dictionary");
					
					if(sc.isCommonMisspell(word.toLowerCase())){
						correct = false;
						System.out.println("is common misspelling");
						String right = sc.getRightMisspell(word.toLowerCase());
						words.set(i, right);
					}
					else{
						String right = sc.getRightSwap(word.toLowerCase());
//						System.out.println("the swap suggestion is "+right);
						if(!words.get(i).equalsIgnoreCase(right)){
//							System.out.println("found one");
							correct = false;
						}
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
		System.out.println(newPhrase.toString());
		words = new ArrayList<String>();
		tokenizer = new StringTokenizer(newPhrase.toString(),PARSER);
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if (word.equals("")) continue;
			System.out.println(word);
			words.add(word);
			
			
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
		if(newPhrase.toString().length()!=0){
			try {
				results = AnalQuery.search(newPhrase.toString());
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Servlet doPost: no matching result");
				results = new ArrayList<DocResult>(); //no match, return empty result
			}
		}
		response.setContentType("text/html");

//		HttpSession session = request.getSession(true);
//		if(session.isNew()) {
//			session.setAttribute("searchID", TimeUtils.timeStamp().toString());
//		}
//		String searchID = (String) session.getAttribute("searchID");
		
		PrintWriter out = response.getWriter();
		out.write("<!DOCTYPE html>"
				+ "<html lang=\"en\">"
					+ "<head>"
						+ "<title>Accio</title>"
						+ "<meta charset=\"utf-8\">"

						+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1,  maximum-scale=1, user-scalable=no\">"
						+ "<link href=\"font-awesome/css/font-awesome.min.css\" rel=\"stylesheet\" type=\"text/css\">"
						+ "<link href=\"https://fonts.googleapis.com/css?family=Montserrat:400,700\" rel=\"stylesheet\" type=\"text/css\">"
						+ "<link href='https://fonts.googleapis.com/css?family=Kaushan+Script' rel='stylesheet' type='text/css'>"
						+ "<link href='https://fonts.googleapis.com/css?family=Droid+Serif:400,700,400italic,700italic' rel='stylesheet' type='text/css'>"
						+ "<link href='https://fonts.googleapis.com/css?family=Roboto+Slab:400,100,300,700' rel='stylesheet' type='text/css'>"

						+ "<link rel=\"stylesheet\" href=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css\">"
						+ "<link rel=\"stylesheet\" type=\"text/css\" href=\"http://localhost:8080/DynamoDB555/youtube/mystyle.css\">"
						+ "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>"
						+ "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js\"></script>"
						
						+ "<script src=\"http://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js\"></script>"
						
						// Youtube
						+ "<script src=\"/DynamoDB555/youtube/js/jquery-1.11.2.min.js\"></script>"
						+ "<script src=\"/DynamoDB555/youtube/js/script.js\"></script>"

						+ "<style>"
							+ "body {"
								+ "padding-top: 15px;"
							+ "}"
//							+ "div.align{"
//								+ "display:flex;"		
//								+ "align-items:center;"
//							+ "}"
							+ "div.pad{"
								+ "padding-top: 40px;"
							+ "}"
						+ "</style>"
					+ ""
					+ "</head>"
					+ "<body>"
					
					+ "<h3 hidden id=\"query\">" + newPhrase +"</h3>"
						+ "<div class=\"container\">"
							
							+ "<div class=\"row\">"

								+ "<div class=\"col-md-1 align\">"
									+ "<a class=\"navbar-brand page-scroll\" href=\"/DynamoDB555/Accio\">"
									+ "Accio"
									+ "</a>"
								+ "</div>"
								+"<div class=\"col-md-9 align\">"
									+ "<form role=\"form\" action=\""+webapp+"/Accio\" method=\"post\">"
	
										+ "<div class=\"col-md-10\">"
											+ "<input type=\"text\" id=\"inputdefault\" class=\"form-control\" name=\"phrase\" placeholder=\""
											+ phrase
											+ "\">"
										+ "</div>"
										+ "<div class=\"col-md-2\">"
											+ "<button type=\"submit\" class=\"btn btn-info\">"
												+ "<span class=\"glyphicon glyphicon-flash\"></span> search"
											+ "</button>"
										+ "</div>"

									+ "</form>"
								+ "</div>"
//								+ "<div class=\"col-md-2 align\">"
//									+ "<a href=\""+webapp+"/AboutUs.html\">About us</a>"
//								+ "</div>"

//								+ "<form role=\"form\" action=\""+webapp+"/Accio\" method=\"post\">"
//									+ "<div class=\"col-md-1\">"
//										+ "<a href=\""+webapp+"/Accio\">"
//										+ "<h4>Accio</h4>"
//										+ "</a>"
//									+ "</div>"
//									+ "<div class=\"col-md-6\">"
//										+ "<input type=\"text\" id=\"inputdefault\" class=\"form-control\" name=\"phrase\" placeholder=\""
//										+ phrase
//										+ "\">"
//									+ "</div>"
//									+ "<div class=\"col-md-3\">"
//										+ "<button type=\"submit\" class=\"btn btn-info\">"
//											+ "<span class=\"glyphicon glyphicon-flash\"></span> search"
//										+ "</button>"
//									+ "</div>"
//								+ "</form>"

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
		else{
			out.write("<div class=\"row\">"
					+ "<h3 hidden>Important things need to be said three times</h3>"
					+ "</div>"
					+ "<div class=\"row\"></div>");
			
		}
		
//							+ "<h2>"
//							+ "important things need to be said three times!"
//							+ "</h2>"
		out.write("<div class=\"col-md-8 pad\">"
					+ "<ul class=\"list-group\">");
		
		if(results == null || results.size() == 0) {
			out.write("<li style=\"red\" class=\"list-group-item\">");
			out.write("No matching result!");
			out.write("</li>");
		} else {
			out.write("<h3 hidden id=\"resultsize\">"+results.size()+"</h3>");
			for(int j = 0; j < results.size(); j++){
				out.write("<li id=\""+j+"\" class=\"list-group-item\">");
					out.write("<a size=\"30\" href="+results.get(j).getUrl()+" onclick=\"sendRequest()\"><font size=\"4\">"+results.get(j).getTitle()+"</font></a>");
					out.write("<p>"
							+ "<font style=\"color:DarkSlateGray\" size=\"2\">"+results.get(j).getUrl()+"</font><br>");
					out.write("<font id=\"match_highlight" + j + "\" style=\"color:grey\" size=\"2.5\">loading...</font></p>");
				out.write("</li>");
			}
			
			
//			out.write("<nav>"
//					+"  <ul class=\"pagination\">"
//					+"    <li>"
//					+"      <a href=\"#\" aria-label=\"Previous\">"
//					+"        <span aria-hidden=\"true\">&laquo;</span>"
//					+"      </a>"
//					+"    </li>"
//					+"    <li><a href=\"#\">1</a></li>"
//					+"    <li><a href=\"#\">2</a></li>"
//					+"    <li><a href=\"#\">3</a></li>"
//					+"    <li><a href=\"#\">4</a></li>"
//					+"    <li><a href=\"#\">5</a></li>"
//					+"    <li>"
//					+"      <a href=\"#\" aria-label=\"Next\">"
//					+"        <span aria-hidden=\"true\">&raquo;</span>"
//					+"      </a>"
//					+"    </li>"
//					+"  </ul>"
//				+ "</nav>");

			
			
			out.write("<nav>"
					+ "<ul class=\"pagination\">");
			int pages = results.size()/10;
			pages = results.size()%10 == 0? pages:(pages+1);
			for(int p=0;p<pages;p++){
				int pageNo = p+1;
				out.write("<li><a class = \"page\" id=\"page"+pageNo+"\">"+pageNo+"</a></li>");
			}
			out.write("<li><a id=\"next\">Next</a></li>");
			out.write("</ul>");
			out.write("</nav>");
		}
									
						out.write("</ul>"
							+ "</div>"
						    + "<div class=\"col-md-4 pad\">"
							
							// Togglable Tab
								+"<div role=\"tabpanel\">"

							  
									+" <ul class=\"nav nav-tabs\" role=\"tablist\">"
									+"    <li role=\"presentation\" class=\"active\"><a href=\"#wiki\" aria-controls=\"wiki\" role=\"tab\" data-toggle=\"tab\">Wikipedia</a></li>"
									+"    <li role=\"presentation\"><a href=\"#youtube\" aria-controls=\"youtube\" role=\"tab\" data-toggle=\"tab\">Youtube</a></li>"
									+" </ul>"

							 
								+"<div class=\"tab-content\">"
								+"    <div style=\"overflow:scroll;height:400px\" role=\"tabpanel\" class=\"well tab-pane active\" id=\"wiki\" align=\"justify\" style=\"width:350px;\">"+ wiki_html +"</div>"
								+"    <div style=\"overflow:scroll;height:400px\" role=\"tabpanel\" class=\"tab-pane\" id=\"youtube\">"
								
										// Youtube
										+ "<div id=\"container_youtube\">"
										+"	<h2>Youtube Videos</h2>"
										+"	<ul style=\"list-style-type:none; margin-left:0px;padding-left:0px;\" id=\"results_youtube\"></ul>"
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
									+ BinaryUtils.byteArrayToDecimalString(results.get(j).getDocID().array()) + "','" 
									+ newPhrase //send the stemmed and processed word list to highlight generator
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
					+ 		"} else {document.getElementById(\"match_highlight\" + i).innerHTML = \"\"}"
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
					+ "var path = \""+webapp+"/insertquery?\" + getquery;"
					+ "console.log(path);"
					+ "xmlhttp.open(\"GET\", path, true);"
					+ "xmlhttp.send();"
					+ "}"
					+ "</script>"
					+ "<script>"
					+ "var resultsize = document.getElementById(\"resultsize\").innerHTML;"
					+ "$(document).ready(function(){"
					+ "		$(\".list-group-item\").hide();"
					+ "		for(var i=0;i<resultsize;i++){"
					+ "			var id = \"#\"+i.toString();"
					+ "			if(i<10) $(id).show();"
					+ "			else $(id).hide();"
					+ "		}"
					+ "		var count = 0;"
					+ "		$(\"#next\").click(function(){"
					+ "			count++;"
					+ "			var linkstart = count*10;"
					+ "			var linkend = Math.min(resultsize, linkstart+10);"
					+ "			for(var i=0;i<resultsize;i++){"
					+ "				var id = \"#\"+i.toString();"
					+ "				if(i>=linkstart && i<linkend){"
					+ "					$(id).show();"
    				+ "				}"
    				+ "				else{"
    				+ "					$(id).hide();"
    				+ "				}"
    				+ "			}"
    				+ "			$('html,body').scrollTop(0);"
					+ "		});"
					+ "		$(\".page\").click(function(event){"
					+ "			$(\".page\").css(\"color\", \"SteelBlue\");"
					+ "			var id = event.target.innerHTML;"
					+ "			var getid = \"#page\" + id;"
					+ "			$(getid).css(\"color\", \"IndianRed\");"
					+ "			count = parseInt(id)-1;"
					+ "			var linkstart = count*10;"
					+ "			var linkend = Math.min(resultsize, linkstart+10);"
					+ "			for(var i=0;i<resultsize;i++){"
					+ "				var id = \"#\"+i.toString();"
					+ "				if(i>=linkstart && i<linkend){"
					+ "					$(id).show();"
    				+ "				}"
    				+ "				else{"
    				+ "					$(id).hide();"
    				+ "				}"
    				+ "			}"
    				+ "			$('html,body').scrollTop(0);"
    				+ "		});"
					+ "});"
					+ "</script>"

					
					+ "</body>"
				+ "</html>");

		out.flush();
	}
	
	

}
