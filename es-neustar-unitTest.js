var express = require('express');
var app = express();
const httpPort = 8081;
const uploadScript = 'es-neustar-create-load.js';
const esHost = 'localhost';
const esPort = httpPort.toString();
const metadataIndex = 'neustar.metadata';
const scratchSpace = 'neustar.scratch.space';
const neustarIpRegExp = 'neustar.ipinfo.*';
const metadataTypeName = '1';

// This responds with "Hello World" on the homepage
app.get('/', function (req, res) {
    console.log("Got a GET request for the homepage");
    res.send('Hello GET');
});

// This responds a POST request for the homepage
app.post('/', function (req, res) {
    console.log("Got a POST request for the homepage");
    res.send('Hello POST');
});

app.put('/' + metadataIndex + '?pretty', function (req, res) {
    console.log("Got a PUT request for metadata");
    res.status(200).send('Hello PUT');
});

app.head('/' + metadataIndex, function (req, res) {
    console.log("Got a HEAD request for neustar.metadata");
    res.status(404).send("Can't find metadata");
})

app.head('/' + scratchSpace, function (req, res) {
    console.log("Got a HEAD request for neustar.scratch.space");
    res.status(404).send("Can't find scratch space");
})

app.put('/' + scratchSpace + '?pretty', function (req, res) {
    console.log("Got a PUT request for scratch space");
    res.send('Hello PUT');
});

//path: '/' + indexName + '?pretty',
app.put('/' + neustarIpRegExp + '?pretty', function (req, res) {
    console.log("Got a PUT request for neustar doc");
    res.send('Neustar doc uploaded');
});

app.get('/' + scratchSpace + '/' + metadataTypeName + '/0/_source', function(req, res) {
    console.log("Got a GET request for scratch space 0th doc");
    res.status(404).send("No docs on scratch space");
});


var server = app.listen(httpPort, function () {
    var host = server.address().address
    var port = server.address().port

    console.log("Example app listening at http://%s:%s", host, port)
})

var childProcess = require('child_process');
var exec = require('child_process').exec;
var output;

function runScript(scriptPath, command, callback) {
    // keep track of whether callback has been invoked to prevent multiple invocations
    if (command === '--upload') {
        const child1 = childProcess.spawn('node', ['./es-neustar-create-load.js', '--host', esHost, '--port', esPort, command]);

        exec('sh std_input.sh 10', function(error, stdout, stderr) {
            console.log(stdout);
        }).stdout.pipe(child1.stdin);

        child1.stdout.on('data', (data) => {
            console.log(`${data}`);
        });
    }
}

//Test upload
// Now we can run a script and invoke a callback when complete, e.g.
runScript(uploadScript, '--upload', function (err) {
    if (err) {
        throw err;
    }
    console.log('finished running ' + uploadScript);
});