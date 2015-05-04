package SearchDynamo;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import DynamoDB.QueryRecord;
import SearchUtils.SearchResult;
import Utils.URLtoDocID;

/**
 * Servlet implementation class Accio
 */

public class Accio extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
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
		String webapp = request.getContextPath();
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
			String docID = URLtoDocID.toBigInteger(url);
			QueryRecord.increment(query, docID);
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String phrase = request.getParameter("phrase");
		String html = "";
		String webapp = request.getContextPath();
		List<SearchResult> results = new ArrayList<SearchResult>();
		/**
		 * wiki part
		 * */
		try {
			html = WikiSearch.wiki(phrase);
		} catch (Exception e) {
			e.printStackTrace();
		}
		/**
		 * our own part
		 * */
		try {
			results = AnalQuery.search(phrase);
		} catch (Exception e) {
			e.printStackTrace();
		}
		PrintWriter out = response.getWriter();
		response.setContentType("text/html");
		out.write("<!DOCTYPE html>"
				+ "<html lang=\"en\">"
					+ "<head>"
						+ "<title>Accio</title>"
						+ "<meta charset=\"utf-8\">"
						+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
						+ "<link rel=\"stylesheet\" href=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css\">"
						+ "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js\"></script>"
						+ "<script src=\"http://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/js/bootstrap.min.js\"></script>"
						+ "<style>"
							+ "body {"
								+ "padding-top: 15px;"
							+ "}"
						+ "</style>"
					+ ""
					+ "</head>"
					+ "<body>"
					+ "<h3 hidden id=\"query\">" + phrase +"</h3>"
						+ "<div class=\"container\">"
							+ "<div class=\"row\">"
								+ "<form role=\"form\" action=\""+webapp+"/Accio\" method=\"post\">"
									+ "<div class=\"col-md-1\">"
										+ "<a href=\"/DynamoDB555/Accio\">"
										+ "<h3>Accio</h3>"
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
							+ "</div>"
							+ "<h2>"
							+ "important things need to be said three times!"
							+ "</h2>"
							+ "<div class=\"col-md-8\">"
								+ "<ul class=\"list-group\">");
		for(int i = 0; i < results.size(); i++){
			out.write("<li class=\"list-group-item\">");
			out.write("<a href="+results.get(i).getUrl()+" onclick=\"sendRequest()\">"+results.get(i).getUrl()+"</a>");
			out.write("</li>");
			
		}
									
								out.write("</ul>"
							+"</div>"
							+ "<div class=\"well col-md-4\">"
								+ html
							+ "</div>"
						+ "</div>"
					+ "<script>"
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
