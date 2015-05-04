function wordHeat(var i, var decimalID, var query) {
	var xmlhttp;
	if (window.XMLHttpRequest){
		xmlhttp = new XMLHttpRequest();
	}
	else{ 
		xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
	}
	var path = "/DynamoDB555/wordHeat?" + "decimalID=" + decimalID + "query=" + query;
	xmlhttp.open("GET", path, false); //synchronous
	xmlhttp.send();
	wordHeatText = xmlhttp.responseText;
	document.getElementById("wordHeat" + i).innerHTML = wordHeatText;
}

function sendRequest() {
	console.log("receive request");
	var target = event.target;
	var url = target.innerHTML;
	var query = document.getElementById("query").innerHTML;
	console.log(query);
	var xmlhttp;
	if (window.XMLHttpRequest){
		xmlhttp = new XMLHttpRequest();
	}
	else{ 
		xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
	}
	var getquery = "url=" + url + "&query=" + query;
	var path = "/DynamoDB555/insertquery?" + getquery;
	console.log(path);
	xmlhttp.open("GET", path, true);
	xmlhttp.send();
}