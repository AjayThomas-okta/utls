var express = require('express');
var app = express();
const httpPort = 8081;
const uploadScript = 'es-neustar-create-load.js';
const esHost = 'localhost';
const esPort = httpPort.toString();
const metadataIndex = 'neustar.metadata';
const scratchSpace = 'neustar.scratch.space';
const neustarIpRegExp = 'neustar.ipinfo.*';
var indexName;
//upload doesnt work when I go beyond 3500
var maxLimit = 3500;
console.log(maxLimit);
const metadataTypeName = '1';
const metadataPutData = {
    "settings" : {
        "index" : {
            "number_of_shards" : 2,//compare times with more shards
            "number_of_replicas" : 1
        }
    },
    "mappings" : {
        "1" : {
            "properties": {
                "currentIndex": {
                    "ignore_above": 10922,
                    "type": "string" //prefix + timestamp (measured from epoch)
                },
                "maxBlockSize": {
                    "type": "integer"
                },
                "version": {//use this property for debugging - more data about the csv file itself
                    "ignore_above": 10922,
                    "type": "string"
                }
            }
        }
    }
};

const scratchSpacePutData = {
    "settings" : {
        "index" : {
            "number_of_shards" : 2,//compare times with more shards
            "number_of_replicas" : 1
        }
    },
    "mappings" : {
        "1" : {
            "properties": {
                "loadedIndex": {
                    "ignore_above": 10922,
                    "type": "string" //measured from epoch
                },
                "loadedMaxBlockSize": {
                    "type": "integer"
                },
                "pausedIndex": {
                    "ignore_above": 10922,
                    "type": "string" //measured from epoch
                }
            }
        }
    }
};

const NeustarIndexPutData = {
    "settings" : {
        "index" : {
            "number_of_shards" : 2,
            "number_of_replicas" : 1
        }
    },
    "mappings" : {
        "1" : {
            "properties": {
                "endIP": {
                    "type": "long"
                },
                "carrier": {
                    "ignore_above": 10922,
                    "type": "string"
                },
                "startIP": {
                    "type": "long"
                },
                "anonymizerStatus": {
                    "ignore_above": 10922,
                    "type": "string"
                },
                "organization": {
                    "ignore_above": 10922,
                    "type": "string"
                },
                "proxyType": {
                    "ignore_above": 10922,
                    "type": "string"
                },
                "sld": {
                    "ignore_above": 10922,
                    "type": "string"
                },
                "asn": {
                    "type": "integer"
                },
                "tld": {
                    "ignore_above": 10922,
                    "type": "string"
                }
            }
        }
    }
};

//this is to concat req body whenever we want to read it
app.use (function(req, res, next) {
    var data='';
    req.setEncoding('utf8');
    req.on('data', function(chunk) {
        data += chunk;
    });

    req.on('end', function() {
        req.body = data;
        next();
    });
});

app.put('/' + metadataIndex + '/', function (req, res) {
    console.log("Got a PUT request for metadata");
    if (req.body === JSON.stringify(metadataPutData)) {
        res.send('Hello PUT for metadata');
    } else {
        res.status(404).send('PUT for metadata has wrong request body');
    }
});

//always respond that metadata doesn't exist
app.head('/' + metadataIndex + '/', function (req, res) {
    console.log("Got a HEAD request for neustar.metadata");
    res.status(404).send("Can't find metadata");
});

//always respond that scratch.space doesn't exist
app.head('/' + scratchSpace, function (req, res) {
    console.log("Got a HEAD request for neustar.scratch.space");
    res.status(404).send("Can't find scratch space");
})

app.put('/' + scratchSpace + '/', function (req, res) {
    console.log("Got a PUT request for scratch space");
    if (req.body === JSON.stringify(scratchSpacePutData)) {
        res.send('Hello PUT for scratch space');
    } else {
        res.status(404).send('PUT for scratch space create index has wrong request body');
    }
});

app.put('/' + scratchSpace + '/' + metadataTypeName + '/0/', function (req, res) {
    console.log("Got a PUT request for creating doc on scratch space");
    const putData = {
        "loadedIndex": null,
        "loadedMaxBlockSize": null,
        "pausedIndex": indexName
    }

    if (req.body === JSON.stringify(putData)) {
        res.status(201).send('Hello PUT for scratch space/1/0/');
    } else {
        res.status(404).send('PUT for scratch space/1/0/ has wrong request body');
    }
});

app.put('/' + neustarIpRegExp + '/', function (req, res) {
    console.log("Got a PUT request for neustar ip reputation index creation");
    //get string after / and before ?
    indexName = req.url.substr(req.url.indexOf('/') + 1, req.url.indexOf('?') - 1);

    if (req.body === JSON.stringify(NeustarIndexPutData)) {
        res.send('Neustar ip reputation index ' + indexName + ' created');
    } else {
        res.status(404).send('PUT for scratch space create index has wrong request body');
    }
});

app.get('/' + scratchSpace + '/' + metadataTypeName + '/0/_source', function(req, res) {
    console.log("Got a GET request for scratch space 0th doc");
    res.status(404).send("No docs on scratch space");
});

app.post('/' + neustarIpRegExp + '/' + metadataTypeName + '/_bulk', function(req, res) {
    console.log("Got a POST request to upload docs to neustar ip index");
    //print out the request body
    //console.log(req.body);
    //todo: check request body
    const someResponse = {
        "ajay" : 12
    }
    res.setHeader('Content-Type', 'application/json');
    res.status(200).send(JSON.stringify(someResponse));
});

app.post('/' + scratchSpace + '/' + metadataTypeName + '/0/_update', function(req, res) {
    const postData = {
        "doc" : {
            "loadedIndex": indexName,
            "loadedMaxBlockSize": maxLimit,
            "pausedIndex": null
        }
    }
    if (req.body === JSON.stringify(postData)) {
        res.send('LoadedIndex on Scratch space updated with ' + indexName + ' and maxBlockSize with ' + maxLimit);
    } else {
        res.status(404).send('POST for scratch space update has wrong request body');
    }
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
    const child1 = childProcess.spawn('node', [uploadScript, '--host', esHost, '--port', esPort, command]);
    if (command === '--upload') {
        exec('sh std_input.sh ' + maxLimit, function(error, stdout, stderr) {
        }).stdout.pipe(child1.stdin);
    }

    child1.stdout.on('data', (data) => {
        console.log(`${data}`);
    });

    child1.stderr.on('data', (data) => {
        console.log("Errors on running child process " + data.toString());
    });

    child1.on('exit', (data) => {
        callback();
    });
}

function testUpload() {
    //Test upload
    // Now we can run a script and invoke a callback when complete, e.g.
    runScript(uploadScript, '--upload', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with upload command');
        //testDeleteAll();
    });
}

//TODO:
function testDeleteAll() {
    //Test upload
    // Now we can run a script and invoke a callback when complete, e.g.
    runScript(uploadScript, '--deleteAll', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with deleteAll command');
        testDeleteOld();
    });
}

//TODO:
function testDeleteOld() {
    //Test upload
    // Now we can run a script and invoke a callback when complete, e.g.
    runScript(uploadScript, '--deleteOld', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with deleteOld command');
        testSwitch();
    });
}

//TODO:
function testSwitch() {
    //Test upload
    // Now we can run a script and invoke a callback when complete, e.g.
    runScript(uploadScript, '--switch', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with switch command');
    });
}

testUpload();