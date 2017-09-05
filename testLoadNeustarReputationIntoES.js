/*This script tests es-neustar-create-load with commands
--upload (both regular upload and resume)
--deleteAll
--deleteOld
--switch (both when a switch should happen and not happen)

Log levels - run with command --logLevel
If you wish to see just summaries of tests, no need of this parameter, default is info
If you wish to see verbose logs from tests, run with --logLevel verbose
If you wish to see all logs from the other script being tested as well, run with --logLevel debug
*/

var express = require('express');
var app = express();
var winston = require('winston');
var program = require('commander');
const httpPort = 8081;
const uploadScript = 'loadNeustarReputationIntoES.js';
const esHost = 'localhost';
const esPort = httpPort.toString();
const metadataIndex = 'neustar.metadata';
const scratchSpace = 'neustar.scratch.space';
const neustarIpPrefix = 'neustar.ipinfo.';
const neustarIpRegExp = neustarIpPrefix + '*';
var uploadTest = false;
var resumeTest = false;
var noSwitchTest = false;
var switchTest = false;
var switchToLastUsedIndexTest = false;
var uploadedIndexName;
const metadataCurrentIndex = neustarIpPrefix+'7789';
var scratchSpaceLoadedIndex = neustarIpPrefix+'3589';
const scratchSpacePausedIndex = neustarIpPrefix+'9189';
const scratchSpaceLoadedIndexMaxBlockSize = 128;
const metadataCurrentMaxBlockSize = 133;
const lastUsedMaxBlockSize = 414;
const lastUsedIndex = neustarIpPrefix+'9200';

var uploadHttpSwitches, resumeHttpSwitches, deleteOldHttpSwitches, deleteAllHttpSwitches;
var deleteSingleMetadataHttpSwitches, deleteSingleLastUsedHttpSwitches, deleteSingleUnusedHttpSwitches;
var switchHttpSwitches, noSwitchHttpSwitches, switchToLastUsedIndexHttpSwitches;
//maxLimit should be under 1001 since one batch size is capped at 1000
//if you increase this, then fix Bulk upload test since that breaks
//this is not a stress test, so 800 is alright
var maxLimit = 800;
var resumeFrom = 156;

const metadataTypeName = '1';

program
    .version('0.0.1')
    .option('--logLevel [winstonLogLevel]', 'log level for console logging')
    .parse(process.argv);

setLogLevel();

function setLogLevel() {
    if (program.logLevel == null) {
        winston.level = 'info';
    } else {
        switch(program.logLevel.toString()) {
            case 'debug':
                winston.level = 'debug';
                break;
            case 'verbose':
                winston.level = 'verbose';
                break;
            default:
                winston.level = 'info';
                break;
        }
    }
    console.log("Logging level set to " + winston.level);
}

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

//"createMetadataIndex"
app.put('/' + metadataIndex + '/', function (req, res) {

    uploadHttpSwitches["createMetadataIndex"] = true;
    resumeHttpSwitches["createMetadataIndex"] = true;
    deleteOldHttpSwitches["createMetadataIndex"] = true;
    deleteAllHttpSwitches["createMetadataIndex"] = true;
    deleteSingleUnusedHttpSwitches["createMetadataIndex"] = true;
    deleteSingleLastUsedHttpSwitches["createMetadataIndex"] = true;
    deleteSingleMetadataHttpSwitches["createMetadataIndex"] = true;
    switchHttpSwitches["createMetadataIndex"] = true;
    noSwitchHttpSwitches["createMetadataIndex"] = true;
    switchToLastUsedIndexHttpSwitches["createMetadataIndex"] = true;

    winston.log("verbose", "Got a PUT request for metadata");
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
        winston.log("error", 'PUT for metadata has wrong request body');
        res.status(404).send('PUT for metadata has wrong request body');
    }
});

//updateMetadataIndex()
/*app.put('/' + metadataIndex + '/' + metadataTypeName + '/0', function (req, res) {
    winston.log("verbose", "Got a PUT request for metadata 0th doc");
    const metadataDocPutData = {
        "maxBlockSize" : scratchSpaceLoadedIndexMaxBlockSize,
        "currentIndex" : scratchSpaceLoadedIndex,
        "version" : "something" //do we need this??
    }
    if (req.body === JSON.stringify(metadataDocPutData)) {
        res.send('Hello PUT for metadata 0th doc');
    } else {
        winston.log("error", 'PUT for metadata doc has wrong request body');
        res.status(404).send('PUT for metadata doc has wrong request body');
    }
});*/

//updateMetadataIndex()
app.post('/' + metadataIndex + '/' + metadataTypeName + '/0/_update', function (req, res) {
    var metadataDocPostData;

    if (switchTest) {
        metadataDocPostData = {
            "doc" : {
                "maxBlockSize" : scratchSpaceLoadedIndexMaxBlockSize,
                "currentIndex" : scratchSpaceLoadedIndex
            }
        }
    } else if (switchToLastUsedIndexTest) {
        metadataDocPostData = {
            "doc" : {
                "maxBlockSize" : lastUsedMaxBlockSize,
                "currentIndex" : lastUsedIndex
            }
        }
    } else {
        throw new Error("Test should not have made a call to this HTTP endpoint.");
    }

    winston.log("verbose", "Got a POST request to update metadata 0th doc");
    if (req.body === JSON.stringify(metadataDocPostData)) {
        switchHttpSwitches['updateMetadataIndex'] = true;
        switchToLastUsedIndexHttpSwitches['updateMetadataIndex'] = true;
        res.send('Hello POST for metadata 0th doc update');
    } else {
        winston.log("error", 'POST for metadata has wrong request body');
        res.status(404).send('POST for metadata doc has wrong request body');
    }
});

//always respond that metadata doesn't exist
//doesMetadataIndexExist()
app.head('/' + metadataIndex + '/', function (req, res) {
    //switching on all classes that hit this endpoint
    uploadHttpSwitches['metadataExist'] = true;
    resumeHttpSwitches['metadataExist'] = true;
    deleteOldHttpSwitches['metadataExist'] = true;
    deleteAllHttpSwitches['metadataExist'] = true;
    deleteSingleUnusedHttpSwitches["metadataExist"] = true;
    deleteSingleLastUsedHttpSwitches["metadataExist"] = true;
    deleteSingleMetadataHttpSwitches["metadataExist"] = true;
    switchHttpSwitches['metadataExist'] = true;
    noSwitchHttpSwitches['metadataExist'] = true;
    switchToLastUsedIndexHttpSwitches['metadataExist'] = true;

    winston.log("verbose", "Got a HEAD request for neustar.metadata");
    res.status(404).send("Can't find metadata");
});

//doesScratchSpaceIndexExist()
//always respond that scratch.space doesn't exist
app.head('/' + scratchSpace, function (req, res) {

    uploadHttpSwitches["scratchSpaceExist"] = true;
    resumeHttpSwitches["scratchSpaceExist"] = true;
    deleteSingleUnusedHttpSwitches["scratchSpaceExist"] = true;
    deleteSingleLastUsedHttpSwitches["scratchSpaceExist"] = true;
    deleteSingleMetadataHttpSwitches["scratchSpaceExist"] = true;
    deleteAllHttpSwitches["scratchSpaceExist"] = true;
    deleteOldHttpSwitches["scratchSpaceExist"] = true;
    switchHttpSwitches["scratchSpaceExist"] = true;
    noSwitchHttpSwitches["scratchSpaceExist"] = true;
    switchToLastUsedIndexHttpSwitches['scratchSpaceExist'] = true;

    winston.log("verbose", "Got a HEAD request for neustar.scratch.space");
    res.status(404).send("Can't find scratch space");
});

//doesNeustarIpReputationIndexExist
app.head('/' + neustarIpRegExp, function (req, res) {
    switchHttpSwitches['doesNeustarIpReputationIndexExist'] = true;
    noSwitchHttpSwitches['doesNeustarIpReputationIndexExist'] = true;
    switchToLastUsedIndexHttpSwitches['doesNeustarIpReputationIndexExist'] = true;

    winston.log("verbose", "Got a HEAD request for a neustar ip reputation index");
    res.send("IP Reputation index exists");
});

//createScratchSpaceIndex()
app.put('/' + scratchSpace + '/', function (req, res) {
    winston.log("verbose", "Got a PUT request for scratch space");

    uploadHttpSwitches["createScratchSpaceIndex"] = true;
    resumeHttpSwitches["createScratchSpaceIndex"] = true;
    deleteSingleUnusedHttpSwitches["createScratchSpaceIndex"] = true;
    deleteSingleLastUsedHttpSwitches["createScratchSpaceIndex"] = true;
    deleteSingleMetadataHttpSwitches["createScratchSpaceIndex"] = true;
    deleteAllHttpSwitches["createScratchSpaceIndex"] = true;
    deleteOldHttpSwitches["createScratchSpaceIndex"] = true;
    switchHttpSwitches["createScratchSpaceIndex"] = true;
    noSwitchHttpSwitches["createScratchSpaceIndex"] = true;
    switchToLastUsedIndexHttpSwitches["createScratchSpaceIndex"] = true;

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
                    },
                    "lastUsedIndex": {
                        "ignore_above": 10922,
                        "type": "string" //measured from epoch
                    },
                    "lastUsedMaxBlockSize": {
                        "type": "integer"
                    }
                }
            }
        }
    };

    if (req.body === JSON.stringify(scratchSpacePutData)) {
        res.send('Hello PUT for scratch space');
    } else {
        winston.log("error", 'PUT for scratch space create index has wrong request body');
        res.status(404).send('PUT for scratch space create index has wrong request body');
    }
});

//updateScratchSpaceIndexWithPausedIndex
//it's a PUT because we're creating a new doc on scratch space since there was no docs earlier
app.put('/' + scratchSpace + '/' + metadataTypeName + '/0/', function (req, res) {

    uploadHttpSwitches['updateScratchSpaceIndexWithPausedIndex'] = true;

    winston.log("verbose", "Got a PUT request for creating doc on scratch space");
    const putData = {
        "loadedIndex": null,
        "loadedMaxBlockSize": null,
        "pausedIndex": uploadedIndexName,
        "lastUsedIndex": null,
        "lastUsedMaxBlockSize" : null
    }

    if (req.body === JSON.stringify(putData)) {
        res.status(201).send('Hello PUT for scratch space/1/0/');
    } else {
        winston.log("error", 'PUT for scratch space/1/0/ has wrong request body');
        res.status(404).send('PUT for scratch space/1/0/ has wrong request body');
    }
});

//createIndex()
app.put('/' + neustarIpRegExp + '/', function (req, res) {
    winston.log("verbose", "Got a PUT request for neustar ip reputation index creation");

    uploadHttpSwitches["createReputationIndex"] = true;

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
        winston.log("error", 'PUT for ip reputation create index has wrong request body');
        res.status(404).send('PUT for ip reputation create index has wrong request body');
    }
});

//updateScratchSpaceWithLastUsedIndex
//removeInfoFromScratchSpace
//updateScratchSpaceIndexWithPausedIndex - not used in any tests
//updateScratchSpaceWithLoadedIndex
app.post('/' + scratchSpace + '/' + metadataTypeName + '/0/_update', function (req, res) {
    var removePausedIndex, removeLoadedIndex;
    var loadedIndex;
    if (uploadTest || resumeTest) {
        if (uploadTest) {
            uploadHttpSwitches['updateScratchSpaceWithLoadedIndex'] = true;
            loadedIndex = uploadedIndexName;
        } else {
            resumeHttpSwitches['updateScratchSpaceWithLoadedIndex'] = true;
            loadedIndex = scratchSpacePausedIndex;
        }
        postData = {
            "doc": {
                "loadedIndex": loadedIndex,
                "loadedMaxBlockSize": maxLimit,
                "pausedIndex": null
            }
        }
    } else if (switchTest) {
        //updateScratchSpaceWithLastUsedIndex
        postData = {
            "doc" : {
                "lastUsedIndex": metadataCurrentIndex,
                "lastUsedMaxBlockSize": metadataCurrentMaxBlockSize
            }
        }
    } else {
        //removeInfoFromScratchSpace
        removePausedIndex = {
            "doc": {
                "pausedIndex": null
            }
        }

        removeLoadedIndex = {
            "doc": {
                "loadedIndex": null,
                "loadedMaxBlockSize": null
            }
        }
    }

    if ((uploadTest || resumeTest) && req.body === JSON.stringify(postData)) {
        res.send('LoadedIndex on Scratch space updated with ' + loadedIndex + ' and maxBlockSize with ' + maxLimit);
    } else if (uploadTest || resumeTest) {
        winston.log("error", 'POST for scratch space update has wrong request body');
        res.status(404).send('POST for scratch space update has wrong request body');
    } else if (switchTest && req.body === JSON.stringify(postData)) {
        switchHttpSwitches['updateScratchSpaceWithLastUsedIndex'] = true;
        res.send('LastUsedIndex on Scratch space updated with ' + metadataCurrentIndex + ' and lastUsedMaxBlockSize with ' + metadataCurrentMaxBlockSize);
    } else if (req.body === JSON.stringify(removePausedIndex)) {
        //deleteOld does not delete Paused Index since it's newer than what's the current index in metadata
        //deleteAll should reach here
        deleteAllHttpSwitches['removePausedIndexFromScratchSpace'] = true;
        deleteSingleUnusedHttpSwitches["removePausedIndexFromScratchSpace"] = true;
        res.send('Paused index nullified');
    } else if (req.body === JSON.stringify(removeLoadedIndex)) {
        deleteOldHttpSwitches['removeLoadedIndexFromScratchSpace'] = true;
        deleteAllHttpSwitches['removeLoadedIndexFromScratchSpace'] = true;
        res.send('Loaded index nullified');
    } else {
        winston.log("error", 'POST for scratch space update nullifying data has wrong request body');
        res.status(404).send('POST for scratch space update nullifying data has wrong request body');
    }
});

//getScratchSpaceInfo()
app.get('/' + scratchSpace + '/' + metadataTypeName + '/0/_source', function (req, res) {

    uploadHttpSwitches['getScratchSpaceDoc'] = true;
    resumeHttpSwitches['getScratchSpaceDoc'] = true;
    deleteSingleUnusedHttpSwitches["getScratchSpaceDoc"] = true;
    deleteSingleLastUsedHttpSwitches["getScratchSpaceDoc"] = true;
    deleteSingleMetadataHttpSwitches["getScratchSpaceDoc"] = true;
    deleteAllHttpSwitches['getScratchSpaceDoc'] = true;
    deleteOldHttpSwitches['getScratchSpaceDoc'] = true;
    switchHttpSwitches['getScratchSpaceDoc'] = true;
    noSwitchHttpSwitches['getScratchSpaceDoc'] = true;
    switchToLastUsedIndexHttpSwitches['getScratchSpaceDoc'] = true;

    winston.log("verbose", "Got a GET request for scratch space 0th doc");
    if (!uploadTest) {
        var scratchSpaceData = {
            "loadedIndex": scratchSpaceLoadedIndex,
            "loadedMaxBlockSize": scratchSpaceLoadedIndexMaxBlockSize,
            "pausedIndex": scratchSpacePausedIndex,
            "lastUsedIndex" : lastUsedIndex,
            "lastUsedMaxBlockSize" : lastUsedMaxBlockSize
        }
        res.send(scratchSpaceData);
    } else {
        res.status(404).send("No docs on scratch space");
    }
});

//getCurrentIndexAndBlockSizeFromMetadata()
app.get('/' + metadataIndex + '/' + metadataTypeName + '/0/_source', function (req, res) {
    winston.log("verbose", "Got a GET request for metadata index 0th doc");

    deleteSingleUnusedHttpSwitches["getCurrentIndexAndBlockSizeFromMetadata"] = true;
    deleteSingleLastUsedHttpSwitches["getCurrentIndexAndBlockSizeFromMetadata"] = true;
    deleteSingleMetadataHttpSwitches["getCurrentIndexAndBlockSizeFromMetadata"] = true;
    deleteOldHttpSwitches['getCurrentIndexAndBlockSizeFromMetadata'] = true;
    deleteAllHttpSwitches['getCurrentIndexAndBlockSizeFromMetadata'] = true;
    switchHttpSwitches['getCurrentIndexAndBlockSizeFromMetadata'] = true;
    noSwitchHttpSwitches['getCurrentIndexAndBlockSizeFromMetadata'] = true;
    switchToLastUsedIndexHttpSwitches['getCurrentIndexAndBlockSizeFromMetadata'] = true;

    const metadataDoc = {
        "currentIndex": metadataCurrentIndex,
        "maxBlockSize": metadataCurrentMaxBlockSize,
        "version": 'bca'
    };
    res.send(metadataDoc);
});

//getAllNeustarIndexes()
app.get('/_cat/indices/' + neustarIpRegExp, function (req, res) {
    winston.log("verbose", "Got a GET request for all neustar ip indexes on ES");

    deleteOldHttpSwitches['getAllNeustarIndexes'] = true;
    deleteAllHttpSwitches['getAllNeustarIndexes'] = true;

    const neustarIndexesList = 'index\n' + neustarIpPrefix + '789\n' + neustarIpPrefix + '456\n' +
        neustarIpPrefix + '123\n' + metadataCurrentIndex + '\n' + lastUsedIndex + '\n' +
        scratchSpacePausedIndex + '\n' + scratchSpaceLoadedIndex + '\n';
    res.send(neustarIndexesList);
});

//deleteIndex()
app.delete('/' + neustarIpRegExp + '/', function (req, res) {
    deleteOldHttpSwitches['deleteIndex'] = true;
    deleteAllHttpSwitches['deleteIndex'] = true;
    deleteSingleUnusedHttpSwitches["deleteIndex"] = true;

    //assert that deleteIndex is NOT called on the index in metadata
    //or the last used index
    var firstSlash = req.url.indexOf('/') + 1;
    var deletedIndexName = req.url.substr(firstSlash, req.url.indexOf('/', firstSlash) - 1);
    //there should have been no request to DELETE the current index in metadata
    if (deletedIndexName == metadataCurrentIndex) {
        throw new Error("A DELETE request for metadata current index came. This is not supposed to happen.");
        res.status(404).send("Cannot DELETE metadata current index");
    } else if (deletedIndexName == lastUsedIndex) {
        throw new Error("A DELETE request for monolith last used index came. This is not supposed to happen.");
        res.status(404).send("Cannot DELETE last used index");
    } else {
        winston.log("verbose", "Got a DELETE request for a neustar ip index on ES. Deleted Index=" + deletedIndexName);
        res.status(200).send("Neustar index deleted");
    }
});

//updateSkipLinesIfNeededAndParse()
app.get('/_cat/count/' + neustarIpRegExp, function (req, res) {
    winston.log("verbose", "Got a GET request for counting docs on a neustar ip index on ES. Req URL=" + req.url);

    resumeHttpSwitches['updateSkipLinesIfNeededAndParse'] = true;

    //skip the first n lines
    var getCountData = "count\n" + resumeFrom + "\n";
    res.send(getCountData);
});

//httpWrite()
app.post('/' + neustarIpRegExp + '/' + metadataTypeName + '/_bulk', function (req, res) {
    winston.log("verbose", "Got a POST request to upload docs to neustar ip index");

    uploadHttpSwitches['httpWrite'] = true;
    resumeHttpSwitches['httpWrite'] = true;

    var reqBody = req.body.split(/\r\n|\r|\n/);
    if (resumeTest) {
        if (JSON.parse(reqBody[1]).startIP == (((resumeFrom * (resumeFrom+1))/2) + resumeFrom + 1)) {
            winston.log("verbose", "Resume test start IP of first row is " + JSON.parse(reqBody[1]).startIP);
        } else {
            throw new Error("Resume test start IP of first row is not " + (((resumeFrom * (resumeFrom+1))/2) + resumeFrom + 1));
        }
    } else if (uploadTest) {
        //console.log(reqBody);
        if (JSON.parse(reqBody[1]).startIP == 1) {
            winston.log("verbose", "Upload test start IP of first row is 1");
        } else {
            throw new Error("Upload test start IP of first row is not 1");
        }
        //this works since the std_input script that is used as csv input
        //prints maxLimit number of csv rows with the start IP starting with 1 for the first row and increasing to 3 and then 6 and so on..
        if ((JSON.parse(reqBody[(maxLimit*2) - 1]).startIP) == (maxLimit * (maxLimit+1))/2) {
            winston.log("verbose", "Upload test start IP of last row is " + (maxLimit * (maxLimit+1))/2);
        } else {
            throw new Error("Upload test start IP of last row is not " + (maxLimit * (maxLimit+1))/2);
        }
    } else {
        throw new Error("Bulk upload HTTP endpoint should not have been hit for this test");
    }

    const someResponse = {
        "ajay": 12
    };
    res.setHeader('Content-Type', 'application/json');
    res.status(200).send(JSON.stringify(someResponse));
});

function resetSwitches() {
    uploadHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getScratchSpaceDoc" : false,
        "createReputationIndex" : false,
        "updateScratchSpaceIndexWithPausedIndex" : false,
        "httpWrite" : false,
        "updateScratchSpaceWithLoadedIndex" : false
    };

    resumeHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "updateSkipLinesIfNeededAndParse" : false,
        "getScratchSpaceDoc" : false,
        "httpWrite" : false,
        "updateScratchSpaceWithLoadedIndex" : false
    };

    deleteOldHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getAllNeustarIndexes" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "getScratchSpaceDoc" : false,
        "removeLoadedIndexFromScratchSpace" : false,
        "deleteIndex" : false
    };

    deleteAllHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getAllNeustarIndexes" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "getScratchSpaceDoc" : false,
        "removeLoadedIndexFromScratchSpace" : false,
        "removePausedIndexFromScratchSpace" : false,
        "deleteIndex" : false
    };

    deleteSingleUnusedHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "getScratchSpaceDoc" : false,
        //since the unused index being deleted in this test is the paused index
        "removePausedIndexFromScratchSpace" : false,
        "deleteIndex" : false
    };

    deleteSingleMetadataHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "getScratchSpaceDoc" : false
    };

    deleteSingleLastUsedHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "getScratchSpaceDoc" : false
    };

    switchHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getScratchSpaceDoc" : false,
        "doesNeustarIpReputationIndexExist" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "updateMetadataIndex" : false,
        "updateScratchSpaceWithLastUsedIndex" : false
    };

    noSwitchHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getScratchSpaceDoc" : false,
        "doesNeustarIpReputationIndexExist" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false
    };

    switchToLastUsedIndexHttpSwitches = {
        "metadataExist" : false,
        "createMetadataIndex" : false,
        "scratchSpaceExist" : false,
        "createScratchSpaceIndex" : false,
        "getScratchSpaceDoc" : false,
        "doesNeustarIpReputationIndexExist" : false,
        "getCurrentIndexAndBlockSizeFromMetadata" : false,
        "updateMetadataIndex" : false
    };
}

var server = app.listen(httpPort, function () {
    var host = server.address().address
    var port = server.address().port

    winston.log("verbose", "Example app listening at http://%s:%s", host, port)
});

var childProcess = require('child_process');
var exec = require('child_process').exec;
var output;

function runScript(scriptPath, command, commandArg, callback) {
    var uploadScriptLogLevel = 'info';
    //give debug level if you want to see verbose logs from the upload script as well
    //give verbose level if you want to see verbose logs only from the test
    if (winston.level === 'verbose') {
        uploadScriptLogLevel = 'verbose';
    } else if (winston.level === 'debug') {
        uploadScriptLogLevel = 'debug';
    }

    if (commandArg == null) {
        commandArg = "";
    }

    const spawnedProcess = childProcess.spawn('node', [uploadScript, '--host', esHost, '--port', esPort,
        command, commandArg, '--logLevel', uploadScriptLogLevel]);
    if (command === '--upload') {
        exec('sh std_input.sh ' + maxLimit, function (error, stdout, stderr) {
        }).stdout.pipe(spawnedProcess.stdin);
    }

    spawnedProcess.stdout.on('data', (data) => {
        winston.log("debug", `${data}`);
    });

    spawnedProcess.stderr.on('data', (data) => {
        winston.log("error", "Errors on running child process " + data.toString());
    });

    spawnedProcess.on('exit', (data) => {
        callback();
    });
}

function testUpload() {
    uploadTest = true;
    // Now we can run a script and invoke a callback when complete, e.g.
    winston.log("info", 'Begin running ' + uploadScript + ' with upload command');
    resetSwitches();
    runScript(uploadScript, '--upload', null, function (err) {
        if (err) {
            throw err;
        }

        checkAllCallsMade(uploadHttpSwitches);
        uploadTest = false;
        winston.log("info", 'finished running ' + uploadScript + ' with upload command');
        testResume();
    });
}

function testResume() {
    // Now we can run a script and invoke a callback when complete, e.g.
    resumeTest = true;
    winston.log("info", 'Begin running ' + uploadScript + ' with upload command - resuming uploading');
    resetSwitches();
    runScript(uploadScript, '--upload', null, function (err) {
        if (err) {
            throw err;
        }

        checkAllCallsMade(resumeHttpSwitches);
        resumeTest = false;
        winston.log("info", 'finished running ' + uploadScript + ' with upload command - resuming uploading');
        testDeleteSingleLastUsedIndex();
    });

}

function testDeleteSingleLastUsedIndex() {
    //no deletion should occur
    winston.log("info", 'Begin running ' + uploadScript + ' with deleteSingle command and lastUsedIndex as paramter');
    resetSwitches();
    runScript(uploadScript, '--deleteSingle', lastUsedIndex, function (err) {
        if (err) {
            throw err;
        }

        checkAllCallsMade(deleteSingleLastUsedHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with deleteSingle command and lastUsedIndex as paramter');
        testDeleteSingleMetadataIndex();
    });
}

function testDeleteSingleMetadataIndex() {
    //no deletion should occur
    winston.log("info", 'Begin running ' + uploadScript + ' with deleteSingle command and metadataIndex as paramter');
    resetSwitches();
    runScript(uploadScript, '--deleteSingle', metadataCurrentIndex, function (err) {
        if (err) {
            throw err;
        }

        checkAllCallsMade(deleteSingleMetadataHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with deleteSingle command and metadataIndex as paramter');
        testDeleteSingleUnusedIndex();
    });
}

function testDeleteSingleUnusedIndex() {
    //unused index should be deleted
    winston.log("info", 'Begin running ' + uploadScript + ' with deleteSingle command and an unused index as paramter');
    resetSwitches();
    runScript(uploadScript, '--deleteSingle', scratchSpacePausedIndex, function (err) {
        if (err) {
            throw err;
        }

        checkAllCallsMade(deleteSingleUnusedHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with deleteSingle command and an unused index as paramter');
        testDeleteAll();
    });
}

function testDeleteAll() {
    winston.log("info", 'Begin running ' + uploadScript + ' with deleteAll command');
    resetSwitches();
    runScript(uploadScript, '--deleteAll', null, function (err) {
        if (err) {
            throw err;
        }

        checkAllCallsMade(deleteAllHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with deleteAll command');
        testDeleteOld();
    });
}

function testDeleteOld() {
    winston.log("info", 'Begin running ' + uploadScript + ' with deleteOld command');
    resetSwitches();
    runScript(uploadScript, '--deleteOld', null, function (err) {
        if (err) {
            throw err;
        }
        checkAllCallsMade(deleteOldHttpSwitches);
        winston.log("info", 'Finished running ' + uploadScript + ' with deleteOld command');
        testNoSwitch();
    });
}

function testNoSwitch() {
    //this tests a switch to an older index and doesnt do it
    winston.log("info", 'Begin running ' + uploadScript + ' with switch command and no switching');
    resetSwitches();
    noSwitchTest = true;
    runScript(uploadScript, '--switch', null, function (err) {
        if (err) {
            throw err;
        }
        checkAllCallsMade(noSwitchHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with switch command and no switching');
        noSwitchTest = false;
        testSwitch();
    });
}

function testSwitch() {
    //this tests a switch to an older index and doesnt do it
    var tempIndex = scratchSpaceLoadedIndex;
    //doing this to make the LoadedIndex in Scratch space newer than the current Index in monolith
    //so switching can occur
    scratchSpaceLoadedIndex = neustarIpPrefix + '9999';
    winston.log("info", 'Begin running ' + uploadScript + ' with switch command and switching');
    resetSwitches();
    switchTest = true;

    runScript(uploadScript, '--switch', null, function (err) {
        if (err) {
            throw err;
        }
        checkAllCallsMade(switchHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with switch command and switching');
        scratchSpaceLoadedIndex = tempIndex;
        switchTest = false;
        testSwitchToLastUsedIndex();
    });
}

function testSwitchToLastUsedIndex() {
    //this tests a switchToLastUsedIndex
    winston.log("info", 'Begin running ' + uploadScript + ' with switchToLastUsedIndex command');
    resetSwitches();
    switchToLastUsedIndexTest = true;

    runScript(uploadScript, '--switchToLastUsedIndex', null, function (err) {
        if (err) {
            throw err;
        }
        checkAllCallsMade(switchToLastUsedIndexHttpSwitches);
        winston.log("info", 'finished running ' + uploadScript + ' with switchToLastUsedIndex command');
        switchToLastUsedIndexTest = false;
        //close server after this last test
        server.listen(httpPort, function () {
            server.close();
            winston.log("info", "Shutting down HTTP server.");
        })
    });
}

function checkAllCallsMade(httpSwitches) {
    for (var property in httpSwitches) {
        if (httpSwitches[property.toString()] == false) {
            winston.log("error", property + " not called.");
            throw new Error("Not all HTTP calls were made for this test");
        }
    }
}

testUpload();