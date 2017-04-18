'use strict';

var express = require('express');
var	app = express();
var path = require('path');
var http = require('http').Server(app);
var io = require('socket.io')(http);
var kafka  = require('kafka-node');

app.set("view engine", "pug");
app.set("views", path.join(__dirname, "views"));
app.use(express.static(path.join(__dirname, 'public')));

// Constants
const PORT = 4000;

// Start server
http.listen(PORT, function(){
  	console.log('listening on port ' + PORT);
});

// App
app.get('/', function (req, res) {
  	res.render("index");
});

console.log('Running on http://localhost:' + PORT);

// Configure Kafka
var reset_kafka;
reset_kafka = function () {
	console.log("Starting kafka client....");
	var Consumer = kafka.Consumer;
	var client = new kafka.Client("zookeeper:2181")

	var on_error = function (error) {
	  	console.log("Got kafka error:", error);
	};

	var close_client = function() {
		console.log("Closing client");
		client.close(reset_kafka());
	}

	client.on('error', function (err) {
		console.log("Client connection error:", err);
		setTimeout(close_client, 5000);
	});

	var topics = [
	    {topic: "TwitterStream", partition: 1},
	    {topic: "TwitterStream", partition: 0}
	];
	var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

	var consumer = new Consumer(client, topics, options);

	consumer.on('message', function (message) {
		io.sockets.emit('tweets', JSON.stringify(message));
	  	console.log(message);
	  	message = null
	});

	consumer.on('error', function (err) {
	  console.log("Got kafka error:", err);
	  setTimeout(function () {
		  consumer.close(close_client);
		}, 5000);
	});

	consumer.on('offsetOutOfRange', function (topic) {
		topic.maxNum = 2;
		offset.fetch([topic], function (err, offsets) {
			if (err) {
			  	return console.error(err);
			}
			var min = Math.min(offsets[topic.topic][topic.partition]);
			consumer.setOffset(topic.topic, topic.partition, min);
		});
	});
}

reset_kafka();
