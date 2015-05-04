var query;
var vidWidth = 250;
var vidHeight = 200;
var vidResults = 5;

$(document).ready(function() {
	query = document.getElementById("query").innerHTML;
	$.get(
		"https://www.googleapis.com/youtube/v3/search", {
			part: 'snippet',
			maxResults: vidResults,
			q: query,
			key: 'AIzaSyC0yUeYeWo24P3PPrkeaDBdXatGLRDURHI'},
			
			function(data) {
				var output;
				$.each(data.items, function(i, item){
					console.log(item);
					videTitle = item.snippet.title;
					videoId = item.id.videoId;
					
					output = '<li><iframe height="'+vidHeight+'" width="'+vidWidth+'" src=\"//www.youtube.com/embed/' + videoId + '\"></iframe</li>';
					
					//Append to results listStyleType
					$('#results_youtube').append(output);
				})
			}
	);
	
});