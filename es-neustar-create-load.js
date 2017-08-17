//Neustar ipinfo index creation and loading of docs
//Neustar metadata index creation script (one time script) should be run first before this
//This script is idempotent
//The csv file that is passed as input will have a header, that is skipped (even if you dont ask it to skip)
//every line after that is processed unless you skip them
//It first checks to see if there are any neustar indexes already present (max 2 indexes only at a time)
//checks the neustar metadata index to know what the current index in use is
//deletes the other (older) index and creates a new one and loads docs into it
//This script also allows for pausing and resuming of docs loading into Neustar index
//Once all the docs are loaded, it updates the metadata index with the new index
//Monolith code then clears cache for metadata so new requests come to elasticSearch to know what the new ipinfo index is
//Neustar index is of the form neustar.ipinfo.* (where * is the timestamp of when the index was created)
//it is of type int (time form epoch)

var parse = require('csv-parse');
var http = require('http');
var program = require('commander');
var makeSource = require("stream-json");
program
    .version('0.0.1')
    .option('-s, --skiplines <n>', 'skip first n lines', parseInt)
    .option('-b, --batchsize <n>', 'upload in bulk size', parseInt)
    .option('--host [esHost]', 'elasticSearch Host')
    .option('--port [esPort]', 'elasticSearch port')
    .option('--deleteAll', 'delete all indexes except monolith one')
    .option('--deleteOld', 'delete indexes older that whats used by monolith')
    .option('--upload', 'upload csv file')
    .option('--switch', 'start using new index for ip reputation')
    .parse(process.argv);

var indexRegExp = "neustar.ipinfo.*";
var metadataIndexName = "neustar.metadata";
var scratchSpaceIndexName = "neustar.scratch.space";
var metadataTypeName = "1";
var objectType = "1";//name of mapping for Neustar index

const neustarIndexPrefix = "neustar.ipinfo."
var recordCount = 0;
var total = 0;

optionCheck();

function optionCheck() {
    if (program.host == null || program.port == null) {
        console.log("Elasticsearch host and port not set");
        return;
    } else {
        console.log("ElasticSearch host= " + program.host + " and port= " + program.port);
    }
    if (program.deleteAll == null && program.deleteOld == null && program.upload == null && program.switch == null) {
        console.log("No option selected. Use --delete to clear old indexes, --upload to upload csv file " +
            "or --switch to enable the new index");
        return;
    } else if (program.deleteOld != null && program.deleteAll == null && program.upload == null && program.switch == null) {
        console.log("Delete old indexes option selected. Program will now delete old unused elasticsearch indexes for Neustar IP reputation.");
    } else if (program.deleteOld == null && program.deleteAll != null && program.upload == null && program.switch == null) {
        console.log("Delete all indexes option selected. Program will now delete all unused elasticsearch indexes for Neustar IP reputation.");
    } else if (program.deleteOld == null && program.deleteAll == null && program.upload != null && program.switch == null) {
        console.log("Upload option selected. Program will upload Neustar IP reputation to a new index.");
    } else if (program.deleteOld == null && program.deleteAll == null && program.upload == null && program.switch != null) {
        console.log("Switch option selected. Program will switch current index in metadata index to use latest Neustar IP reputation index.");
    } else {
        console.log("More than one option among delete, upload, switch not allowed.");
        return;
    }
    //first check if metadata index exist
    doesMetadataIndexExist();

}

function doesMetadataIndexExist() {
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + metadataIndexName,
        method: 'HEAD'
    };

    var callback = function(response) {
        var httpResponseCode = '404';
        httpResponseCode = response.statusCode;
        console.log("Metadata index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            console.log("This means metadata index exists.");
            doOperationBasedOnOption();
        } else {
            console.log("This means metadata index does not exist. Creating it...");
            createMetadataIndex();
        }
    };

    http.request(options, callback).end();
}

function createMetadataIndex() {
    const putData = {
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
                        "type": "string" //measured from epoch
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

    var options = {
        host: program.host,
        port: program.port,
        path: '/' + metadataIndexName + '?pretty',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'PUT'
    };

    var callback = function(response) {
        console.log("Metadata index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                console.log("Metadata index creation failed with error:");
                console.log(str);
                console.log("Aborting...");
            } else {
                console.log(str);
                doOperationBasedOnOption();
            }
        });
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    console.log("Creating metadata index " + metadataIndexName);
}

function doOperationBasedOnOption() {
    if (program.deleteOld != null) {
        //clear old indexes
        deleteUnusedIndexes(false);
    } else if (program.deleteAll != null) {
        //clear all indexes
        deleteUnusedIndexes(true);
    } else if (program.upload != null) {
        uploadCsv();
    } else if (program.switch != null) {
        switchIndex();
    } else {
        console.log("doOperationBasedOnOption aborting...");
    }
}

function deleteIndex(indexToBeDeleted) {
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + indexToBeDeleted + '/',
        method: 'DELETE'
    };

    var callback = function(response) {
        var indexDeleted = response.statusCode;
        var str = '';
        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (indexDeleted != '200') {
                console.log(indexToBeDeleted + "failed to be deleted with response code = " + indexDeleted);
                console.log("Index deletion error is " + str);
            } else {
                console.log("Old index " + indexToBeDeleted + " deleted");
            }
        });
    }

    http.request(options, callback).end();
}

function deleteUnusedIndexes(deleteAll) {
    doesScratchSpaceIndexExist(function (success) {
        if (success === true) {
            getAllNeustarIndexes(function(arrayOfIndexes) {
                if (arrayOfIndexes === null) {
                    console.log("No indexes to delete.");
                    return;
                }
                var i;
                getCurrentIndexFromMetadata(function(currentMonolithIndex) {
                    getScratchSpaceInfo(function (scratchSpaceInfo) {
                        var loadedIndex = null;
                        var pausedIndex = null;
                        if (scratchSpaceInfo !== null) {
                            loadedIndex = scratchSpaceInfo.loadedIndex;
                            pausedIndex = scratchSpaceInfo.pausedIndex;
                            if (loadedIndex !== null) {
                                loadedIndex = loadedIndex.trim();
                            }
                            if (pausedIndex !== null) {
                                pausedIndex = pausedIndex.trim();
                            }
                        }
                        for (i = 0; i < arrayOfIndexes.length; i++) {
                            if (arrayOfIndexes[i] != currentMonolithIndex) {
                                deleteIndex(arrayOfIndexes[i]);
                                if (arrayOfIndexes[i] === loadedIndex) {
                                    removeLoadedIndexFromScratchSpace();
                                }
                                if (arrayOfIndexes[i] === pausedIndex) {
                                    removePausedIndexFromScratchSpace();
                                }
                            } else {
                                if (deleteAll) {
                                    if (i == 0) {
                                        console.log("Nothing to be deleted");
                                    }
                                    break;
                                } else {
                                    continue;
                                }
                            }
                        }
                    });
                })
            });
        } else {
            console.log("Scratch space creation failed");
        }
    });
}

function getAllNeustarIndexes(cb) {
    console.log("Getting all Neustar indexes");
    //get all indexes that match regexp 'indexName'
    var options = {
        host: program.host,
        port: program.port,
        path: '/_cat/indices/' + indexRegExp + '?pri&v&h=index&pretty',
        method: 'GET'
    };

    var callback = function(response) {
        var str = '';
        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            console.log("Neustar indexes on ES are: " + str);

            var arrayOfIndexes = str.split(/\r\n|\r|\n/);
            var indexToDelete = null;
            var indexCount = arrayOfIndexes.length - 2;
            console.log("Number of indexes = " + indexCount);
            if (indexCount == 0) {
                cb(null);
            } else {
                var i;
                //remove the first element since that just says "index" (header of the table)
                arrayOfIndexes.shift();
                //remove last line which is a blank line
                arrayOfIndexes.pop();
                for (i = 0; i < indexCount; i++) {
                    arrayOfIndexes[i] = arrayOfIndexes[i].toString().trim();
                }
                arrayOfIndexes.sort();
                cb(arrayOfIndexes);
            }
        });
    }

    http.request(options, callback).end();
}

function uploadCsv() {
    doesScratchSpaceIndexExist(function (success) {
        if (success === true) {
            getScratchSpaceInfo(function(scratchSpaceInfo) {
                var ipReputationIndexName;
                if (scratchSpaceInfo == null || scratchSpaceInfo.pausedIndex == null) {
                    ipReputationIndexName = neustarIndexPrefix + Math.round(new Date().getTime() / 1000);
                    console.log("Creating new index for csv upload = " + ipReputationIndexName);
                    createIndex(ipReputationIndexName, function (success) {
                        if (success === true) {
                            var createDoc = (scratchSpaceInfo == null) ? true : false;
                            updateScratchSpaceIndexWithPausedIndex(ipReputationIndexName, createDoc, function(result) {
                                if (result === true) {
                                    parseCsv(ipReputationIndexName, program.skiplines);
                                }
                            });
                        }
                    });
                } else {
                    pausedIndex = scratchSpaceInfo.pausedIndex.trim();
                    ipReputationIndexName = pausedIndex;
                    console.log("Reusing Paused index in scratch space for csv upload = " + ipReputationIndexName);
                    //TODO: Gotta be careful, we're not uploading paused index with new csv file
                    updateSkipLinesIfNeededAndParse(ipReputationIndexName);
                }
            });
        }
    });
}

function updateScratchSpaceIndexWithPausedIndex(indexName, create, cb) {
    if (create === true) {
        //there were no docs on the scratch space
        //create a new one with new index name
        var options = {
            host: program.host,
            port: program.port,
            path: '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0',
            headers: {
                'Content-Type': 'application/json'
            },
            method: 'PUT'
        };

        var callback = function(response) {
            var uploadSucceeded = response.statusCode;
            var str = '';
            //another chunk of data has been recieved, so append it to `str`
            response.on('data', function (chunk) {
                str += chunk;
            });

            //the whole response has been recieved, so we just print it out here
            response.on('end', function () {
                if (uploadSucceeded != '201') {
                    console.log("Creating new doc on scratch space with new index name " + indexName + " failed with response code = " + uploadSucceeded);
                    console.log("Aborting...");
                    cb(false);
                } else {
                    console.log("Creating new doc on scratch space with new index name " + indexName + " succeeded.");
                    console.log(str);
                    cb(true);
                }
            });
        }

        const putData = {
            "loadedIndex": null,
            "loadedMaxBlockSize": null,
            "pausedIndex": indexName
        }

        var req = http.request(options, callback);
        //This is the data we are posting, it needs to be a string or a buffer
        req.write(JSON.stringify(putData));
        req.end();

    } else {
        //update the doc on metadata index with new index name and new maxBlockSize
        var options = {
            host: program.host,
            port: program.port,
            path: '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0/_update',
            headers: {
                'Content-Type': 'application/json'
            },
            method: 'POST'
        };

        var callback = function(response) {
            var updateSucceeded = response.statusCode;
            var str = '';
            //another chunk of data has been recieved, so append it to `str`
            response.on('data', function (chunk) {
                str += chunk;
            });

            //the whole response has been recieved, so we just print it out here
            response.on('end', function () {
                if (updateSucceeded != '200') {
                    console.log("Updating scratch space paused index with " + indexName + " failed with response code = " + updateSucceeded);
                    console.log(str);
                    console.log("Aborting...");
                    cb(false);
                } else {
                    console.log("Updating scratch space paused index with " + indexName + " succeeded.");
                    console.log(str);
                    cb(true);
                }
            });
        }

        const postData = {
            "doc" : {
                "pausedIndex": indexName
            }
        }

        var req = http.request(options, callback);
        //This is the data we are posting, it needs to be a string or a buffer
        req.write(JSON.stringify(postData));
        req.end();

    }


}

function switchIndex() {
    doesScratchSpaceIndexExist(function (success) {
        if (success === true) {
            updateMetadataIndexWithLoadedIndex();
        }
    });
}

function updateMetadataIndexWithLoadedIndex() {
    //get the only doc on the metadata index
    //its contents is in a JSON format
    //one of the keys (currentIndex) is the name of the current neustar ip info
    getScratchSpaceInfo(function (scratchSpaceInfo) {
        if (scratchSpaceInfo == null) {
            console.log("No documents on scratch space. Cannot update metadata index.")
            return;
        }
        var loadedIndex = scratchSpaceInfo.loadedIndex;
        if (loadedIndex == null) {
            console.log("Nothing fully loaded so far into ES. Loaded index is null from scratch space.")
            console.log("Paused index = " + scratchSpaceInfo.pausedIndex);
            return;
        }
        loadedIndex = loadedIndex.trim();
        //check if loaded index is present before modifying neustar.metadata
        doesNeustarIpReputationIndexExist(loadedIndex, function (success) {
            if (success) {
                var newMaxBlockSize = scratchSpaceInfo.loadedMaxBlockSize;
                getCurrentIndexFromMetadata(function (currentMonolithIndex) {
                    if (currentMonolithIndex == null) {
                        console.log("Metadata has no index in use currently. Setting it as the " +
                            "loaded index " + loadedIndex + " from scratch space");
                        //create a doc
                        updateMetadataIndex(true, loadedIndex, newMaxBlockSize);
                    } else {
                        if (loadedIndex > currentMonolithIndex) {
                            console.log("Metadata has index " + currentMonolithIndex + " in use currently. Setting it as the " +
                                "loaded index " + loadedIndex + " from scratch space");
                            //update doc
                            updateMetadataIndex(false, loadedIndex, newMaxBlockSize);
                        } else {
                            console.log("Metadata has index " + currentMonolithIndex + " in use currently. Not switching since " +
                                "loaded index " + loadedIndex + " from scratch space is older or the same.");
                            return;
                        }
                    }
                })
            }
        })
    });
}

function getScratchSpaceInfo(cb) {
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0/_source',
        method: 'GET'
    };

    var callback = function(response) {
        var httpResponseCode = '404';
        httpResponseCode = response.statusCode;
        console.log("Scratch Space get document response code=" + httpResponseCode);
        if (httpResponseCode == '404') {
            //Metadata Index does not exist
            console.log("Metadata scratch space index has no documents.");
            cb(null);
            return;
        }
        var str = '';
        //another chunk of data has been received, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been received, so we just print it out here
        response.on('end', function () {
            console.log(str);
            if (str == null) {
                console.log("Empty metadata");
                cb(null);
            } else {
                //pass the indexName to the callBack
                var jsonMetadataString = JSON.parse(str);
                cb(jsonMetadataString);
            }
        });
    }

    http.request(options, callback).end();

}

function doesNeustarIpReputationIndexExist(neustarIpInfoIndexName, cb) {
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + neustarIpInfoIndexName,
        method: 'HEAD'
    };

    var callback = function(response) {
        var httpResponseCode = '404';
        httpResponseCode = response.statusCode;
        console.log(neustarIpInfoIndexName + " index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            console.log("This means " + neustarIpInfoIndexName + " index exists.");
            cb(true);
        } else {
            console.log("This means " + neustarIpInfoIndexName + " index does not exist. Aborting switching...");
            cb(false);
        }
    };

    http.request(options, callback).end();
}

function doesScratchSpaceIndexExist(cb) {
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + scratchSpaceIndexName,
        method: 'HEAD'
    };

    var callback = function(response) {
        var httpResponseCode = '404';
        httpResponseCode = response.statusCode;
        console.log("Metadata scratch space index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            console.log("This means metadata scratch space index exists.");
            cb(true);
        } else {
            console.log("This means metadata scratch space index does not exist. Creating it...");
            createScratchSpaceIndex(cb);
        }
    };

    http.request(options, callback).end();
}

function createScratchSpaceIndex(cb) {
    const putData = {
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

    var options = {
        host: program.host,
        port: program.port,
        path: '/' + scratchSpaceIndexName + '?pretty',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'PUT'
    };

    var callback = function(response) {
        console.log("Metadata scratch space index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                console.log("Metadata scratch space index creation failed with error:");
                console.log(str);
                console.log("Aborting...");
                cb(false);
            } else {
                console.log(str);
                cb(true);
            }
        });
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    console.log("Creating metadata index " + scratchSpaceIndexName);
}


function getCurrentIndexFromMetadata(cb) {
    //get the only doc on the metadata index
    //its contents is in a JSON format
    //one of the keys (currentIndex) is the name of the current neustar ip info

    var options = {
        host: program.host,
        port: program.port,
        path: '/' + metadataIndexName + '/' + metadataTypeName + '/0/_source',
        method: 'GET'
    };

    var callback = function(response) {
        var httpResponseCode = '404';
        httpResponseCode = response.statusCode;
        console.log("Metadata get document response code=" + httpResponseCode);
        if (httpResponseCode == '404') {
            //Metadata Index does not exist
            console.log("Metadata index has no documents.");
            cb(null);
            return;
        }
        var str = '';
        //another chunk of data has been received, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been received, so we just print it out here
        response.on('end', function () {
            console.log(str);
            if (str == null) {
                console.log("Empty metadata");
                cb(null);
            }
            //pass the indexName to the callBack
            var jsonMetadataString = JSON.parse(str);
            var currentIpReputationIndex = jsonMetadataString.currentIndex;
            if (currentIpReputationIndex == null) {
                cb(null);
            }
            currentIpReputationIndex = currentIpReputationIndex.trim();
            console.log("Current index from metadata is " + currentIpReputationIndex);
            cb(currentIpReputationIndex);
        });
    }

    http.request(options, callback).end();
}

function createIndexAndParseCsv(indexName) {
    const putData = {
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

    var putOptions = {
        host: program.host,
        port: program.port,
        path: '/' + indexName + '?pretty',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'PUT'
    };

    var callback = function(response) {
        console.log("Neustar index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                console.log("Neustar index creation failed with error:" + str);
                console.log("Neustar index creation failed. Aborting upload.");
                return;
            }
            console.log(str);
            parseCsv(indexName, program.skiplines);
        });
    }

    var req = http.request(putOptions, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    console.log("Creating index " + indexName);
}


function createIndex(indexName, cb) {
    const putData = {
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

    var putOptions = {
        host: program.host,
        port: program.port,
        path: '/' + indexName + '?pretty',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'PUT'
    };

    var callback = function(response) {
        console.log("Neustar index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                console.log("Neustar index creation failed with error:" + str);
                console.log("Neustar index creation failed. Aborting upload.");
                cb(false);
            } else {
                console.log(str);
                cb(true);
            }
        });
    }

    var req = http.request(putOptions, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    console.log("Creating index " + indexName);
}


function updateSkipLinesIfNeededAndParse(indexName) {
    var origSkipLines;
    if ((program.skiplines) == null) {
        console.log("Lines skipped is null");
        program.skiplines = 0; //set it to 0
    } else {
        console.log("Lines skipped originally is " + program.skiplines);
    }
    origSkipLines = program.skiplines;

    //count number of docs already on index and update skipLines
    var options = {
        host: program.host,
        port: program.port,
        path: '/_cat/count/' + indexName + '?v&h=count&pretty',
        method: 'GET'
    };

    var callback = function(response) {
        var countSucceeded = response.statusCode;
        var str = '';
        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (countSucceeded != '200') {
                console.log("Counting docs on " + indexName + " failed with response code = " + countSucceeded);
                console.log("Aborting...");
                return;
            } else {
                //TODO: This is failing for some reason
                //not giving the right count
                var countArray = str.split(/\r\n|\r|\n/);
                var docCount = countArray[1];
                console.log("Number of docs on index " + indexName + " = " + docCount);
                //increment lines to skip by how many docs already loaded into ES index
                program.skiplines = program.skiplines + docCount;
                console.log("New skiplines = " + program.skiplines);
                parseCsv(indexName, origSkipLines);
            }
        });
    }

    http.request(options, callback).end();
}

function parseCsv(indexName, origSkipLines) {
    var newLine = new Buffer("\n");
    var output = [];
// Create the parser
    var parser = parse({delimiter: ',', columns: true, trim: true});
    var batchSize = program.batchsize !== null && program.batchsize > 0 ? program.batchsize : 1000;
    var byteCount = 0;

    if (origSkipLines == null) {
        origSkipLines = 0;
    }
    var currentMaxBlockSize = 0;
    var batchCount = 0;
    var indexCommand = new Buffer("{\"index\": {\"_index\": \"" + indexName + "\", \"_type\": \"" + objectType + "\" }}\n");
    // Use the writable stream api
    parser.on('readable', function() {
        while(record = parser.read()){
            ++total;
            if (program.skiplines !== null && total <= program.skiplines) {
                ++counterObject.skipped;
                //original skipped lines should be skipped and not checked
                //if it's resuming, then we need to check the maxBlockSize for the skipped rows too
                if (origSkipLines > 0) {
                    origSkipLines--;
                } else {
                    //once origSkipLines is 0, we should check maxBlockSize for the skipped lines as well
                    //since this will be a case of resuming loading
                    var recBlockSize = getBlockSizeOfRecord(convertIpToInt(record));
                    if (recBlockSize > currentMaxBlockSize) {
                        currentMaxBlockSize = recBlockSize;
                    }
                }
                continue;
            }
            ++recordCount;
            var jsonRec = convertIpToInt(record);
            var recBlockSize = getBlockSizeOfRecord(jsonRec);
            if (recBlockSize > currentMaxBlockSize) {
                currentMaxBlockSize = recBlockSize;
            }
            var jsonData = JSON.stringify(jsonRec);
            output.push(indexCommand);
            output.push(new Buffer(jsonData + "\n"));
            byteCount += indexCommand.byteLength + output[output.length - 1].byteLength;

            if (recordCount >= batchSize) {
                uploadBatches(false);
            }
        }
    });

    function convertIpToInt(jsonRecord) {
        replaceField(jsonRecord, 'startIP');
        replaceField(jsonRecord, 'endIP');
        return jsonRecord;
    }

    function replaceField(map, k) {
        var s = map[k];
        if (s !== null) {
            map[k] = tryConvertToInt(s);
        }
    }

    function tryConvertToInt(v) {
        try {
            return parseInt(v)
        } catch (e) {
            return v;
        }
    }

    function getBlockSizeOfRecord(map) {
        var startIp = map['startIP'];
        var endIp = map['endIP'];
        return (endIp - startIp);
    }

    function uploadBatches(lastUpload) {
        if (output.length <= 0) {
            if (lastUpload) {
                console.log("Parser Finish calling uploadBatches() and there's nothing in output");
                updateScratchSpaceWithLoadedIndex(indexName, currentMaxBlockSize);
            }
            return;
        }

        output.push(newLine);
        byteCount += newLine.byteLength;
        httpWrite(output, byteCount, lastUpload);
        output = [];
        byteCount = 0;
        recordCount = 0;
    }

    function httpWrite(bufferArray, byteLength, lastUpload) {
        var post_options = {
            host: program.host,
            port: program.port,
            path: '/' + indexName + '/' + objectType + '/_bulk',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': byteLength
            }
        };

        // Set up the request
        var post_req = http.request(post_options, function(res) {
            res.setEncoding('utf8');
            ++counterObject.httpWriteCount;
            var rr = makeResponseReader(counterObject, function () {
                --counterObject.httpWriteCount;
                if (rr.hasErrors) {
                    ++counterObject.httpBulkRetry;
                    setImmediate(function () {
                        httpWrite(bufferArray, byteLength, lastUpload);
                    });
                } else {
                    displayStatus();
                    if (lastUpload) {
                        console.log("Parser.finish finished uploading the last remnants of data to be parsed");
                        updateScratchSpaceWithLoadedIndex(indexName, currentMaxBlockSize);
                    }
                }
            });
            rr.pipe(res);
        });

        // post the data
        for (var i = 0; i < bufferArray.length; ++i) {
            post_req.write(bufferArray[i]);
        }

        post_req.end();
    }

    // Catch any error
    parser.on('error', function(err){
        console.log(err.message);
    });

    // When we are done, test that the parsed output matched what expected
    parser.on('finish', function(){
        uploadBatches(true);
    });

    // Now that setup is done, write data to the stream
    process.stdin.pipe(parser);

    var counterObject = {
        errorCount: 0,
        successCount: 0,
        httpWriteCount: 0,
        httpBulkRetry: 0,
        skipped: 0
    };

    function displayStatus() {
        process.stdout.write("\033[0G");
        process.stdout.write("error count: " + counterObject.errorCount + ", upload count: "
            + counterObject.successCount * batchSize + ", http pending write count: " + counterObject.httpWriteCount
            + ", http write retry: " + counterObject.httpBulkRetry + ", skipped: " + counterObject.skipped);
    }

    var timeout = setInterval(displayStatus, 1500);
    timeout.unref();

    function makeResponseReader(counterObject, onEnd) {
        var oThis = {
            co: counterObject,
            source: makeSource({}),
            objDepth: 0,
            jsonParseError: [],
            hasErrors: false,

            pipe: function (input) {
                input.pipe(this.source.input);
            }
        }

        function ifKeyErrors(value) {
            if (value == 'errors') {
                oThis.source.once("falseValue", function () {
                    oThis.source.removeListener("keyValue", ifKeyErrors);
                    ++oThis.co.successCount;
                });
            } else if (value == 'error') {
                ++oThis.co.errorCount;
                oThis.hasErrors = true;
            }
        }

        oThis.source.on("startObject", function() {
            if (oThis.objDepth == 0) {
                oThis.source.on("keyValue", ifKeyErrors);
            }
            ++oThis.objDepth;
        });

        oThis.source.on("error", function (e) {
            oThis.jsonParseError.push(e);
        });

        if (onEnd !== null) {
            oThis.source.on('end', function () {
                onEnd();
            });
        }

        return oThis;
    }
}

function updateScratchSpaceWithLoadedIndex(newIndex, newMaxBlockSize) {
    //update the doc on metadata index with new index name and new maxBlockSize
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0/_update',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'POST'
    };

    var callback = function(response) {
        var updateSucceeded = response.statusCode;
        var str = '';
        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (updateSucceeded != '200') {
                console.log("Updating scratch space loaded index with " + newIndex + " failed with response code = " + updateSucceeded);
                console.log(str);
                console.log("Aborting...");
                return;
            } else {
                console.log("Updating scratch space loaded index with " + newIndex + " succeeded.");
                console.log(str);
            }
        });
    }

    const postData = {
        "doc" : {
            "loadedIndex": newIndex,
            "loadedMaxBlockSize": newMaxBlockSize,
            "pausedIndex": null
        }
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(postData));
    req.end();

}

function removePausedIndexFromScratchSpace() {
    removeInfoFromScratchSpace(false, true);
}

function removeLoadedIndexFromScratchSpace() {
    removeInfoFromScratchSpace(true, false);
}

function removeInfoFromScratchSpace(loaded, paused) {
    //update the doc on metadata index with new index name and new maxBlockSize
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0/_update',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'POST'
    };

    var callback = function(response) {
        var updateSucceeded = response.statusCode;
        var str = '';
        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            var logString = loaded ? "loadedIndex info" : "pausedIndex info";
            if (updateSucceeded != '200') {
                console.log("Removing " + logString + " from scratch space failed with response code = " + updateSucceeded);
                console.log(str);
                return;
            } else {
                console.log("Removing " + logString + " from scratch space succeeded");
                console.log(str);
            }
        });
    }

    var postData;

    if (loaded) {
        postData = {
            "doc" : {
                "loadedIndex": null,
                "loadedMaxBlockSize": null
            }
        }
    } else {
        postData = {
            "doc" : {
                "pausedIndex": null
            }
        }
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(postData));
    req.end();

}

function updateMetadataIndex(create, newIndex, newMaxBlockSize) {
    if (create === true) {
        //there were no docs on the metadata index
        //create a new one with new index name
        var options = {
            host: program.host,
            port: program.port,
            path: '/' + metadataIndexName + '/' + metadataTypeName + '/0',
            headers: {
                'Content-Type': 'application/json'
            },
            method: 'PUT'
        };

        var callback = function(response) {
            var uploadSucceeded = response.statusCode;
            var str = '';
            //another chunk of data has been recieved, so append it to `str`
            response.on('data', function (chunk) {
                str += chunk;
            });

            //the whole response has been recieved, so we just print it out here
            response.on('end', function () {
                if (uploadSucceeded != '201') {
                    console.log("Creating new doc on metadata index with new index name " + newIndex + " failed with response code = " + uploadSucceeded);
                    console.log("Aborting...");
                    return;
                } else {
                    console.log("Creating new doc on metadata index with new index name " + newIndex + " succeeded.");
                    console.log(str);
                }
            });
        }

        const putData = {
            "maxBlockSize" : newMaxBlockSize,
            "currentIndex" : newIndex,
            "version" : "something" //do we need this??
        }

        var req = http.request(options, callback);
        //This is the data we are posting, it needs to be a string or a buffer
        req.write(JSON.stringify(putData));
        req.end();

    } else {
        //update the doc on metadata index with new index name and new maxBlockSize
        var options = {
            host: program.host,
            port: program.port,
            path: '/' + metadataIndexName + '/' + metadataTypeName + '/0/_update',
            headers: {
                'Content-Type': 'application/json'
            },
            method: 'POST'
        };

        var callback = function(response) {
            var updateSucceeded = response.statusCode;
            var str = '';
            //another chunk of data has been recieved, so append it to `str`
            response.on('data', function (chunk) {
                str += chunk;
            });

            //the whole response has been recieved, so we just print it out here
            response.on('end', function () {
                if (updateSucceeded != '200') {
                    console.log("Updating metadata index with new index name " + newIndex + " failed with response code = " + updateSucceeded);
                    console.log(str);
                    console.log("Aborting...");
                    return;
                } else {
                    console.log("Updating metadata index with new index name " + newIndex + " succeeded.");
                    console.log(str);
                }
            });
        }

        const postData = {
            "doc" : {
                "maxBlockSize" : newMaxBlockSize,
                "currentIndex" : newIndex
            }
        }

        var req = http.request(options, callback);
        //This is the data we are posting, it needs to be a string or a buffer
        req.write(JSON.stringify(postData));
        req.end();

    }

}