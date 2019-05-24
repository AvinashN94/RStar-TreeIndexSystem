var express = require('express');
var http = require('http');
var url = require('url');
var rbush = require('rbush');
var fs = require('fs');
var querystring = require('querystring');
var driver  = require('cassandra-driver');

var server = http.createServer(function(req, res) {

    res.writeHead(200, {"Content-Type": "text/plain"});
    res.write('Well Hello');
    res.end();
    var tree = rbush(9, ['[0]', '[1]', '[0]', '[1]']); // accept [x, y] points
    tree.insert([20, 50]);
    var result = tree.search({
        minX: 10,
        minY: 20,
        maxX: 80,
        maxY: 70
    });
    console.log("Testing");
    console.log(result);
});
server.listen(8082);



