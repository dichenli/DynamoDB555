package SearchDynamo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;

public class WikiSearch {
	
	public static String wiki(String phrase) throws Exception{
		String USER_AGENT = "cis455crawler";
	
		String[] words = phrase.split(" ");
		String fin = "";
		String url = "http://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=";
		for(int i = 0; i < words.length; i++){
			if(words[i].length()!=0){
				if(i!=words.length-1){
					fin += words[i].trim()+"_";
				}
				else{
					fin += words[i].trim();
				}
			}
		}
		System.out.println("wiki:"+fin);
		url += fin
				+ "&rvprop=content&format=json&rvsection=0&rvparse=1";
		String html = "";
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
 
		//add request header
		con.setRequestProperty("User-Agent", USER_AGENT);
 
		int responseCode = con.getResponseCode();
//		System.out.println("\nSending 'GET' request to URL : " + url);
//		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
			response.append("\n");
		}
		in.close();
 
		//print result
//		System.out.println(response.toString());
		
		
		String jsonresult = response.toString();
		JSONObject jsonObj = new JSONObject(jsonresult);
		boolean exist = false;
		try{
			JSONObject result = jsonObj.getJSONObject("query").getJSONObject("pages").getJSONObject("-1");
		}
		catch(Exception e){
			exist = true;
		}
		if(exist){
			JSONObject page = jsonObj.getJSONObject("query").getJSONObject("pages");
			String content = page.toString();
			int i = 2;
			while(content.charAt(i) != '"'){
				i++;
			}
			String pageid = content.substring(2, i);
			JSONArray revisions = jsonObj.getJSONObject("query").getJSONObject("pages").getJSONObject(pageid).getJSONArray("revisions");
			html = revisions.getJSONObject(0).getString("*");
			html = html.replaceAll("<a href=\"/wiki/", "<a href=\"http://en.wikipedia.org/wiki/");
			html = html.replaceAll("src=\"//upload", "src=\"http://upload");
			
		}
		return html;
	}
}