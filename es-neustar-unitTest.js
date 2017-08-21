var express = require('express');
var app = express();
const httpPort = 8081;
const uploadScript = 'es-neustar-create-load.js';
const esHost = 'localhost';
const esPort = httpPort.toString();
const metadataIndex = 'neustar.metadata';
const scratchSpace = 'neustar.scratch.space';
const neustarIpPrefix = 'neustar.ipinfo.';
const neustarIpRegExp = neustarIpPrefix + '*';
var uploadTest = false;
var resumeTest = false;
var uploadedIndexName;
const metadataCurrentIndex = neustarIpPrefix+'7789';
var scratchSpaceLoadedIndex = neustarIpPrefix+'3589';
const scratchSpacePausedIndex = neustarIpPrefix+'9189';
const scratchSpaceLoadedIndexMaxBlockSize = 128;

//upload doesnt work when I go beyond 3500
var maxLimit = 3500;
const metadataTypeName = '1';

//this is to concat req body whenever we want to read it
app.use(function (req, res, next) {
    var data = '';
    req.setEncoding('utf8');
    req.on('data', function (chunk) {
        data += chunk;
    });

    req.on('end', function () {
        req.body = data;
        next();
    });
});

app.put('/' + metadataIndex + '/', function (req, res) {
    console.log("Got a PUT request for metadata");
    const metadataPutData = {
        "settings": {
            "index": {
                "number_of_shards": 2,//compare times with more shards
                "number_of_replicas": 1
            }
        },
        "mappings": {
            "1": {
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

    if (req.body === JSON.stringify(metadataPutData)) {
        res.send('Hello PUT for metadata');
    } else {
        res.status(404).send('PUT for metadata has wrong request body');
    }
});

//updateMetadataIndex()
app.put('/' + metadataIndex + '/' + metadataTypeName + '/0', function (req, res) {
    console.log("Got a PUT request for metadata 0th doc");
    const metadataDocPutData = {
        "maxBlockSize" : scratchSpaceLoadedIndexMaxBlockSize,
        "currentIndex" : scratchSpaceLoadedIndex,
        "version" : "something" //do we need this??
    }
    if (req.body === JSON.stringify(metadataDocPutData)) {
        res.send('Hello PUT for metadata 0th doc');
    } else {
        res.status(404).send('PUT for metadata doc has wrong request body');
    }
});

//updateMetadataIndex()
app.post('/' + metadataIndex + '/' + metadataTypeName + '/0/_update', function (req, res) {
    const metadataDocPostData = {
        "doc" : {
            "maxBlockSize" : scratchSpaceLoadedIndexMaxBlockSize,
            "currentIndex" : scratchSpaceLoadedIndex
        }
    }
    console.log("Got a POST request to update metadata 0th doc");
    if (req.body === JSON.stringify(metadataDocPostData)) {
        res.send('Hello POST for metadata 0th doc update');
    } else {
        res.status(404).send('POST for metadata doc has wrong request body');
    }
});

//always respond that metadata doesn't exist
app.head('/' + metadataIndex + '/', function (req, res) {
    console.log("Got a HEAD request for neustar.metadata");
    res.status(404).send("Can't find metadata");
});

//doesScratchSpaceIndexExist()
//always respond that scratch.space doesn't exist
app.head('/' + scratchSpace, function (req, res) {
    console.log("Got a HEAD request for neustar.scratch.space");
    res.status(404).send("Can't find scratch space");
});

//doesNeustarIpReputationIndexExist
app.head('/' + neustarIpRegExp, function (req, res) {
    console.log("Got a HEAD request for a neustar ip reputation index");
    res.send("IP Reputation index exists");
});

app.put('/' + scratchSpace + '/', function (req, res) {
    console.log("Got a PUT request for scratch space");
    const scratchSpacePutData = {
        "settings": {
            "index": {
                "number_of_shards": 2,//compare times with more shards
                "number_of_replicas": 1
            }
        },
        "mappings": {
            "1": {
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

    if (req.body === JSON.stringify(scratchSpacePutData)) {
        res.send('Hello PUT for scratch space');
    } else {
        res.status(404).send('PUT for scratch space create index has wrong request body');
    }
});

//updateScratchSpaceIndexWithPausedIndex
//it's a PUT because we're creating a new doc on scratch space since there was no docs earlier
app.put('/' + scratchSpace + '/' + metadataTypeName + '/0/', function (req, res) {
    console.log("Got a PUT request for creating doc on scratch space");
    const putData = {
        "loadedIndex": null,
        "loadedMaxBlockSize": null,
        "pausedIndex": uploadedIndexName
    }

    if (req.body === JSON.stringify(putData)) {
        res.status(201).send('Hello PUT for scratch space/1/0/');
    } else {
        res.status(404).send('PUT for scratch space/1/0/ has wrong request body');
    }
});

//updateScratchSpaceWithLoadedIndex() or
//removeInfoFromScratchSpace
app.post('/' + scratchSpace + '/' + metadataTypeName + '/0/_update', function (req, res) {
    var postData, removePostData;
    var loadedIndex;
    if (uploadTest || resumeTest) {
        if (uploadTest) {
            loadedIndex = uploadedIndexName;
        } else {
            loadedIndex = scratchSpacePausedIndex;
        }
        postData = {
            "doc": {
                "loadedIndex": loadedIndex,
                "loadedMaxBlockSize": maxLimit,
                "pausedIndex": null
            }
        }
    } else {
        //removeInfoFromScratchSpace
        postData = {
            "doc": {
                "pausedIndex": null
            }
        }

        removePostData = {
            "doc": {
                "loadedIndex": null,
                "loadedMaxBlockSize": null
            }
        }
    }
    if ((uploadTest || resumeTest) && req.body === JSON.stringify(postData)) {
        res.send('LoadedIndex on Scratch space updated with ' + loadedIndex + ' and maxBlockSize with ' + maxLimit);
    } else if (uploadTest || resumeTest) {
        res.status(404).send('POST for scratch space update has wrong request body');
    } else {
        if (req.body === JSON.stringify(postData) || req.body === JSON.stringify(removePostData)) {
            res.send('Scratch space loaded or paused index nullified');
        } else {
            res.status(404).send('POST for scratch space update nullifying data has wrong request body');
        }
    }
});

app.put('/' + neustarIpRegExp + '/', function (req, res) {
    console.log("Got a PUT request for neustar ip reputation index creation");
    //get string after / and before ?
    uploadedIndexName = req.url.substr(req.url.indexOf('/') + 1, req.url.indexOf('?') - 1);

    const NeustarIndexPutData = {
        "settings": {
            "index": {
                "number_of_shards": 2,
                "number_of_replicas": 1
            }
        },
        "mappings": {
            "1": {
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

    if (req.body === JSON.stringify(NeustarIndexPutData)) {
        res.send('Neustar ip reputation index ' + uploadedIndexName + ' created');
    } else {
        res.status(404).send('PUT for scratch space create index has wrong request body');
    }
});

//getScratchSpaceInfo()
app.get('/' + scratchSpace + '/' + metadataTypeName + '/0/_source', function (req, res) {
    console.log("Got a GET request for scratch space 0th doc");
    if (!uploadTest) {
        var scratchSpaceData = {
            "loadedIndex": scratchSpaceLoadedIndex,
            "loadedMaxBlockSize": scratchSpaceLoadedIndexMaxBlockSize,
            "pausedIndex": scratchSpacePausedIndex
        }
        res.send(scratchSpaceData);
    } else {
        res.status(404).send("No docs on scratch space");
    }
});

//getCurrentIndexFromMetadata()
app.get('/' + metadataIndex + '/' + metadataTypeName + '/0/_source', function (req, res) {
    console.log("Got a GET request for metadata index 0th doc");
    const metadataDoc = {
        "currentIndex": metadataCurrentIndex,
        "maxBlockSize": 133,
        "version": 'bca'
    };
    res.send(metadataDoc);
});

//getAllNeustarIndexes()
app.get('/_cat/indices/' + neustarIpRegExp, function (req, res) {
    console.log("Got a GET request for all neustar ip indexes on ES");
    const neustarIndexesList = 'index\n' + neustarIpPrefix + '789\n' + neustarIpPrefix + '456\n' +
        neustarIpPrefix + '123\n' + metadataCurrentIndex + '\n' + scratchSpacePausedIndex + '\n' +
        scratchSpaceLoadedIndex + '\n';
    res.send(neustarIndexesList);
});

//deleteIndex()
app.delete('/' + neustarIpRegExp + '/', function (req, res) {
    console.log("Got a DELETE request for a neustar ip index on ES. Req URL=" + req.url);
    //var deletedIndexName = req.url.substr(req.url.indexOf('/') + 1, req.url.indexOf('?') - 1);
    res.status(200).send("Neustar index deleted");
});

//updateSkipLinesIfNeededAndParse()
app.get('/_cat/count/' + neustarIpRegExp, function (req, res) {
    console.log("Got a GET request for counting docs on a neustar ip index on ES. Req URL=" + req.url);
    //skip the first n lines
    var getCountData;
    if (maxLimit > 1002) {
        getCountData = "count\n1001\n";
    } else {
        getCountData = "count\n1\n";
    }
    res.send(getCountData);
});

app.post('/' + neustarIpRegExp + '/' + metadataTypeName + '/_bulk', function (req, res) {
    console.log("Got a POST request to upload docs to neustar ip index");
    //todo: check request body
    const someResponse = {
        "ajay": 12
    }
    res.setHeader('Content-Type', 'application/json');
    res.status(200).send(JSON.stringify(someResponse));
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
        exec('sh std_input.sh ' + maxLimit, function (error, stdout, stderr) {
        }).stdout.pipe(child1.stdin);
    }

    child1.stdout.on('data', (data) => {
        console.log(`${data}`);
})
    ;

    child1.stderr.on('data', (data) => {
        console.log("Errors on running child process " + data.toString());
})
    ;

    child1.on('exit', (data) => {
        callback();
})
    ;
}

function testUpload() {
    //is there a better way?
    uploadTest = true;
    // Now we can run a script and invoke a callback when complete, e.g.
    console.log('Begin running ' + uploadScript + ' with upload command');
    runScript(uploadScript, '--upload', function (err) {
        if (err) {
            throw err;
        }
        uploadTest = false;
        console.log('finished running ' + uploadScript + ' with upload command');
        testResume();
    });
}

function testResume() {
    // Now we can run a script and invoke a callback when complete, e.g.
    resumeTest = true;
    console.log('Begin running ' + uploadScript + ' with upload command - resuming uploading');
    runScript(uploadScript, '--upload', function (err) {
        if (err) {
            throw err;
        }
        resumeTest = false;
        console.log('finished running ' + uploadScript + ' with upload command - resuming uploading');
        testDeleteAll();
    });

}

function testDeleteAll() {
    console.log('Begin running ' + uploadScript + ' with deleteAll command');
    runScript(uploadScript, '--deleteAll', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with deleteAll command');
        testDeleteOld();
    });
}

function testDeleteOld() {
    console.log('Begin running ' + uploadScript + ' with deleteOld command');
    runScript(uploadScript, '--deleteOld', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with deleteOld command');
        testNoSwitch();
    });
}

function testNoSwitch() {
    //this tests a switch to an older index and doesnt do it
    console.log('Begin running ' + uploadScript + ' with switch command and no switching');
    runScript(uploadScript, '--switch', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with switch command and no switching');
        testSwitch();
    });
}

function testSwitch() {
    //this tests a switch to an older index and doesnt do it
    var tempIndex = scratchSpaceLoadedIndex;
    scratchSpaceLoadedIndex = neustarIpPrefix + '9999';
    console.log('Begin running ' + uploadScript + ' with switch command and switching');
    runScript(uploadScript, '--switch', function (err) {
        if (err) {
            throw err;
        }
        console.log('finished running ' + uploadScript + ' with switch command and switching');
        scratchSpaceLoadedIndex = tempIndex;
    });
}

testUpload();