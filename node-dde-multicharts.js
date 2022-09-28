// Require express and body-parser
const express = require("express")
const fs = require('fs')
const https = require('https')
const http = require('http');
const dde = require('node-dde');
const { Console } = require("console");


// Initialize express and define a port
const app = express()
const PORT = 4444
const dde_server = dde.createServer('VNI');

// Tell express to use body-parser's JSON parsing
app.use(express.json())

var price = 0
var volume = 0
app.post("", (req, res) => {
    console.log(req.body)
    try {
        symbol = req.body.symbol
        price = req.body.price
        volume = req.body.volume
        console.log(price, " ", volume)
        dde_server.advise('LAST', symbol)
        dde_server.advise('VOLUME', symbol)
        throw "error parsing body";        
      }
      catch (e) {
        console.log(e);
      }
      
})

var i = 0;
dde_server.on('disconnect', function(service, topic) {
  console.log('OnDisconnect: '
    + 'Service: ' + service
    + ', Topic: ' + topic);
});
dde_server.on('advise', function(topic, item, format) {
  console.log('OnAdvise: '
    + 'Topic: ' + topic
    + ', Item: ' + item
    + ', Format: ' + format);
    // return 'advise_' + i++;
    // let utf8Encode = new TextEncoder();
    // return utf8Encode.encode('advise_' + i++);
    //     var result = 'advise_' + i++;
    // var bytes = [];
    // var charCode;

    // for (var i = 0; i < result.length; ++i)
    // {
    //     charCode = result.charCodeAt(i);
    //     bytes.push((charCode & 0xFF00) >> 8);
    //     bytes.push(charCode & 0xFF);
    // }
    // return  bytes;
});
// dde_server.on('request', function(service, topic, item, format) {
//     console.log('OnRequest: '
//     + 'Topic: ' + topic
//     + ', Item: ' + item
//     + ', Format: ' + format);
//     return "advise_" + i++;
//     // return i++;
// });
dde_server.onBeforeConnect = function(topic) { return true; };
dde_server.onAfterConnect = function(service, topic) {};
dde_server.onDisconnect = function(service, topic) {};
dde_server.onStartAdvise = function(service, topic, item, format) { return true; };
dde_server.onStopAdvise = function(service, topic, item) {};
dde_server.onExecute = function(service, topic, command) {};
dde_server.onPoke = function(service, topic, item, data, format) {};
dde_server.onRequest = function(service, topic, item, format) { return 'advise_' + i++; }
// dde_server.onAdvise = function(topic, item, format) { return 'advise_' + str(i++);};

// var i = 0;
// dde_server.onAdvise = function(topic, item, format) {
//     // let utf8Encode = new TextEncoder();
//     var result = 'advise_' + i++;
//     var bytes = [];
//     var charCode;

//     for (var i = 0; i < str.length; ++i)
//     {
//         charCode = result.charCodeAt(i);
//         bytes.push((charCode & 0xFF00) >> 8);
//         bytes.push(charCode & 0xFF);
//     }
//     return  bytes;
// };
// dde_server.onRequest = function(service, topic, item, format) { return i++; };

setInterval(function() { dde_server.advise('TEST', 'C'); }, 1000);

// var i = 0;
// dde_server.onAdvise = function() {
//  return i++;
// };

// dde_server.onAdvise = function (topic, item, format) {
//     return i++;
// };
// setInterval(function() { dde_server.advise('TEST', 'A'); dde_server.onAdvise(); console.log(i) }, 10);

dde_server.register();


// Start express on the defined port
// http.createServer(app).listen(PORT, () => console.log(`🚀HTTP Server running on port ${PORT}`))