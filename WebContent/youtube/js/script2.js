var channelName = 'TechGuyWeb';
var query = 'youtube';
var vidWidth = 500;
var vidHeight = 400;
var vidResults = 10;

$(document).ready(function() {
	$.get(
		"https://www.googleapis.com/youtube/v3/channels", {
			part: 'contentDetails',
			forUsername: channelName,
			key: 'AIzaSyC0yUeYeWo24P3PPrkeaDBdXatGLRDURHI'},
			
			function(data) {
				$.each(data.items, function(i, item){
					console.log(item);
					pid = item.contentDetails.relatedPlaylists.uploads;
					getVids(pid);
				})
			}
	);
	
	function getVids(pid) {
		$.get(
			"https://www.googleapis.com/youtube/v3/playlistItems", {
				part: 'snippet',
				maxResults: vidResults,
				playlistId: pid,
				key: 'AIzaSyC0yUeYeWo24P3PPrkeaDBdXatGLRDURHI'},
			
				function(data) {
					var output;
					$.each(data.items, function(i, item){
						console.log(item);
						videTitle = item.snippet.title;
						videoId = item.snippet.resourceId.videoId;
						
						output = '<li><iframe height="'+vidHeight+'" width="'+vidWidth+'" src=\"//www.youtube.com/embed/' + videoId + '\"></iframe</li>';
						
						//Append to results listStyleType
						$('#results').append(output);
					})
				}
		);
	}
});