$(function () {
	$(".container").mapael({
        map: {
            name: "world_countries",
            defaultArea: {
                attrs: {
                    fill: "#f4f4e8"
                    , stroke: "#00a1fe"
                }
                , attrsHover: {
                    fill: "#a4e100"
                }
            },
            defaultPlot: {
                attrs: {
                    fill: "#24EF00",
                    opacity: 0.6
                },
                attrsHover: {
                    opacity: 1
                }
            }
        }
	});

    var socket = io();
    socket.on('tweets', function (data) {
        var tweet = JSON.parse(JSON.parse(data).value);
        console.log(tweet);
        var username = tweet.user.name
        var newPlots = {}
        var sentimentValue = ({
            "POSITIVE"  : "#24EF00",
            "NEGATIVE" : "#dc0000",
            "NEUTRAL" : "#A44BDB"
        })[tweet.sentiment]
        newPlots['Username: '+username] = {
            latitude: tweet.location.lat,
            longitude: tweet.location.lon,
            tooltip: {content: tweet.text},
            attrs: {
                fill: sentimentValue,
                opacity: 0.6
            },
            attrsHover: {
                opacity: 1
            }
        }
        console.log(newPlots)
        $(".container").trigger('update', [{
            newPlots: newPlots,
            animDuration: 1000
        }]);
    });
});