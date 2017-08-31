/*
 Neustar ipinfo index creation and loading of docs
 this script is idempotent.
 This script creates following indexes:
 - neustar.metadata which gives info currently used by monolith (if any), maxBlockSize for that index
 - neustar.scratch.space which stores intermediate info like latest uploaded index and its maxBlockSize, paused index, last index used by monolith and its maxBlockSize
 - a new index where Nuestar ip reputation data is loaded when the upload command is run

 The csv file that is passed as input must have a header (specifying the columns), that is skipped (even if you dont ask it to skip)
 every line after that is processed unless you skip them with the -s argument

 Options:
 Upload - uploads csv file to a new index (if there was not already an index that had a paused upload).
 If there was a paused index, then uploading continues on that until it is complete
 DeleteAll - Delete all indexes that are not used by the monolith (got from neustar.metadata)
 DeleteOld - Delete all indexes that are older than what the monolith is using (got from neustar.metadata)
 Switch - Switch the index being used by the monolith to the latest fully loaded index (this is updating neustar.metadata)
 SwitchToLastIndex - Switch the index being used by the monolith to the last index that the monolith used

 This script also allows for resuming of uploading docs loading into Neustar index (if it was paused because of a system crash or Ctrl + C)
 Neustar index is of the form neustar.ipinfo.* (where * is the timestamp of when the index was created)
 it is of type int (time form epoch)

 LogLevels
 Default is info
 Run with logLevel --verbose to see all logs
 */

var parse = require('csv-parse');
var http = require('http');
var program = require('commander');
var makeSource = require("stream-json");
var winston = require('winston');
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
    .option('--switchToLastIndex', 'start using new index for ip reputation')
    .option('--logLevel [winstonLogLevel]', 'log level for console logging')
    .parse(process.argv);

var indexRegExp = "neustar.ipinfo.*";
var metadataIndexName = "neustar.metadata";
var scratchSpaceIndexName = "neustar.scratch.space";
var metadataTypeName = "1";
var objectType = "1";//name of mapping for Neustar index

//all ip info indexes will have this prefix
//the suffix is the tiestamp (int measured from epoch)
const neustarIndexPrefix = "neustar.ipinfo."
var recordCount = 0;
var total = 0;

setLogLevel();
optionCheck();

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

function optionCheck() {
    if (program.host == null || program.port == null) {
        throw new Error("Elasticsearch host and port not set");
    } else {
        winston.log('verbose', "ElasticSearch host=" + program.host + " and port=" + program.port);
    }
    if (program.deleteAll == null && program.deleteOld == null && program.upload == null && program.switch == null && program.switchToLastIndex == null) {
        throw new Error("No option selected. Use --delete to clear old indexes, --upload to upload csv file " +
            "or --switch to enable the new index");
    } else if (program.deleteOld != null && program.deleteAll == null && program.upload == null && program.switch == null && program.switchToLastIndex == null) {
        winston.log('verbose', "Delete old indexes option selected. Program will now delete old unused elasticsearch indexes for Neustar IP reputation.");
    } else if (program.deleteOld == null && program.deleteAll != null && program.upload == null && program.switch == null && program.switchToLastIndex == null) {
        winston.log('verbose', "Delete all indexes option selected. Program will now delete all unused elasticsearch indexes for Neustar IP reputation.");
    } else if (program.deleteOld == null && program.deleteAll == null && program.upload != null && program.switch == null && program.switchToLastIndex == null) {
        winston.log('verbose', "Upload option selected. Program will upload Neustar IP reputation to a new index.");
    } else if (program.deleteOld == null && program.deleteAll == null && program.upload == null && program.switch != null && program.switchToLastIndex == null) {
        winston.log('verbose', "Switch option selected. Program will switch current index in metadata index to use latest Neustar IP reputation index.");
    } else if (program.deleteOld == null && program.deleteAll == null && program.upload == null && program.switch == null && program.switchToLastIndex != null) {
        winston.log('verbose', "Switch option to Last Index selected. Program will switch current index in metadata index to use last used index from metadata.");
    } else {
        throw new Error("More than one option among delete, upload, switch not allowed.");
    }
    //first check if metadata index exist
    doesMetadataIndexExist();

}

//creates index if it doesn't exist
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
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        winston.log('verbose', "Metadata index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            winston.log('verbose', "This means metadata index exists.");
            doOperationBasedOnOption();
        } else {
            winston.log('verbose' , "This means metadata index does not exist. Creating it...");
            winston.log('verbose' , str);
            createMetadataIndex();
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
        winston.log('verbose' , "Metadata scratch space index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            winston.log('verbose' , "This means metadata scratch space index exists.");
            cb(true);
        } else {
            winston.log('verbose' , "This means metadata scratch space index does not exist. Creating it...");
            createScratchSpaceIndex(cb);
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
        winston.log('verbose' , "Metadata index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200') {
                throw new Error("Metadata index creation failed with error:" + str);
            } else {
                doOperationBasedOnOption();
            }
        });
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    winston.log('info' , "Creating metadata index " + metadataIndexName);
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
    } else if (program.switch != null || program.switchToLastIndex != null) {
        switchIndex();
    } else {
        throw new Error("doOperationBasedOnOption failed. Aborting...");
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
                winston.log('warn' , indexToBeDeleted + "failed to be deleted with response code = " + indexDeleted);
                winston.log('warn' , "Index deletion error is " + str);
            } else {
                winston.log('info' , "Unused index " + indexToBeDeleted + " deleted");
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
                    winston.log('info' , "No indexes to delete.");
                    return;
                }
                var i;
                getCurrentIndexAndBlockSizeFromMetadata(function(currentMonolithIndex, currentMaxBlockSize) {
                    getScratchSpaceInfo(function (scratchSpaceInfo) {
                        var loadedIndex = null;
                        var pausedIndex = null;
                        var lastUsedIndex = null;
                        if (scratchSpaceInfo != null) {
                            loadedIndex = scratchSpaceInfo.loadedIndex;
                            pausedIndex = scratchSpaceInfo.pausedIndex;
                            lastUsedIndex = scratchSpaceInfo.lastUsedIndex;
                            if (loadedIndex != null) {
                                loadedIndex = loadedIndex.trim();
                            }
                            if (pausedIndex != null) {
                                pausedIndex = pausedIndex.trim();
                            }
                            if (lastUsedIndex != null) {
                                lastUsedIndex = lastUsedIndex.trim();
                            }
                        }
                        var somethingDeleted = false;
                        for (i = 0; i < arrayOfIndexes.length; i++) {
                            if (arrayOfIndexes[i] != currentMonolithIndex) {
                                somethingDeleted = true;
                                deleteIndex(arrayOfIndexes[i]);
                                if (arrayOfIndexes[i] === loadedIndex) {
                                    removeLoadedIndexFromScratchSpace();
                                }
                                if (arrayOfIndexes[i] === pausedIndex) {
                                    removePausedIndexFromScratchSpace();
                                }
                                if (arrayOfIndexes[i] === lastUsedIndex) {
                                    removeLastUsedIndexFromScratchSpace();
                                }
                            } else {
                                if (deleteAll) {
                                    //delete indexes newer than what's used by monolith
                                    //hence continue
                                    //otherwise break out of for loop
                                    continue;
                                } else {
                                    break;
                                }
                            }
                        }
                        if (!somethingDeleted) {
                            winston.log('info' , "Nothing to delete");
                        }
                    });
                })
            });
        } else {
            throw new Error("Scratch space creation failed");
        }
    });
}

function getAllNeustarIndexes(cb) {
    winston.log('verbose' , "Getting all Neustar indexes");
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
            winston.log('verbose' , "Neustar indexes on ES are: " + str);

            var arrayOfIndexes = str.split(/\r\n|\r|\n/);
            var indexToDelete = null;
            var indexCount = arrayOfIndexes.length - 2;
            winston.log('verbose' , "Number of indexes = " + indexCount);
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
                    winston.log('info' , "Creating new index for csv upload = " + ipReputationIndexName);
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
                    winston.log('info' , "Reusing paused index=" + ipReputationIndexName + " in scratch space for csv upload.");
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
                    winston.log('error' , str);
                    throw new Error("Creating new doc on scratch space with new index name " + indexName + " failed with response code = " + uploadSucceeded);
                } else {
                    winston.log('verbose' , "Creating new doc on scratch space with new index name " + indexName + " succeeded.");
                    cb(true);
                }
            });
        }

        const putData = {
            "loadedIndex": null,
            "loadedMaxBlockSize": null,
            "pausedIndex": indexName,
            "lastUsedIndex": null,
            "lastUsedMaxBlockSize" : null
        }

        var req = http.request(options, callback);
        //This is the data we are posting, it needs to be a string or a buffer
        req.write(JSON.stringify(putData));
        req.end();

    } else {
        //update the doc on scratch space paused index with new index name and new maxBlockSize
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
                    winston.log('error' , str);
                    throw new Error("Updating scratch space paused index with " + indexName + " failed with response code = " + updateSucceeded);
                } else {
                    winston.log('verbose' , "Updating scratch space paused index with " + indexName + " succeeded.");
                    winston.log('verbose' , str);
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
            updateMetadataCurrentIndex();
        }
    });
}

function updateMetadataCurrentIndex() {
    //get the only doc on the metadata index
    //its contents is in a JSON format
    //one of the keys (currentIndex) is the name of the current neustar ip info
    getScratchSpaceInfo(function (scratchSpaceInfo) {
        if (scratchSpaceInfo == null) {
            winston.log('info' , "No documents on scratch space. Cannot update metadata index.")
            return;
        }

        var indexToSwitchTo, switchMaxBlockSize;
        if(program.switchToLastIndex != null) {
            indexToSwitchTo = scratchSpaceInfo.lastUsedIndex;
            switchMaxBlockSize = scratchSpaceInfo.lastUsedMaxBlockSize;
            if (indexToSwitchTo == null) {
                winston.log('info' , "No last used index in scratch space.")
                winston.log('verbose' , "Paused index = " + scratchSpaceInfo.pausedIndex + " and loaded index = " + scratchSpaceInfo.loadedIndex);
                return;
            }
        } else {
            indexToSwitchTo = scratchSpaceInfo.loadedIndex;
            if (indexToSwitchTo == null) {
                winston.log('info' , "Nothing fully loaded so far into ES. Loaded index is null from scratch space.")
                winston.log('verbose' , "Paused index = " + scratchSpaceInfo.pausedIndex);
                return;
            }
            switchMaxBlockSize = scratchSpaceInfo.loadedMaxBlockSize;
        }
        indexToSwitchTo = indexToSwitchTo.trim();

        if (switchMaxBlockSize == null) {
            winston.log("error", "Index to switch to " + indexToSwitchTo + " has null maxBlockSize. Bad state! Aborting...");
            throw new Error("Index to switch to " + indexToSwitchTo + " has null maxBlockSize.");
            return;
        }

        //check if loaded index is present before modifying neustar.metadata
        doesNeustarIpReputationIndexExist(indexToSwitchTo, function (success) {
            if (success) {
                getCurrentIndexAndBlockSizeFromMetadata(function (currentMonolithIndex, currentMaxBlockSize) {
                    if (currentMonolithIndex == null) {
                        winston.log('info', "Metadata has no index in use currently. Setting it as the " +
                            "loaded index " + indexToSwitchTo + " from scratch space");
                        //create a doc
                        updateMetadataIndex(true, indexToSwitchTo, switchMaxBlockSize);
                    } else {
                        if (program.switchToLastIndex != null && indexToSwitchTo != currentMonolithIndex) {
                            //we are switching to last Used Index
                            //so it could be older than what is used by monolith, and we should allow that
                            updateMetadataIndex(false, indexToSwitchTo, switchMaxBlockSize);
                        } else if (indexToSwitchTo > currentMonolithIndex) {
                            winston.log('info' , "Metadata has index " + currentMonolithIndex + " in use currently. Setting it as the " +
                                "loaded index " + indexToSwitchTo + " from scratch space");
                            //update doc
                            updateMetadataIndex(false, indexToSwitchTo, switchMaxBlockSize);
                            updateScratchSpaceWithLastUsedIndex(currentMonolithIndex, currentMaxBlockSize);
                        } else {
                            winston.log('info' , "Metadata has index " + currentMonolithIndex + " in use currently. Not switching since " +
                                "loaded index " + indexToSwitchTo + " from scratch space is older or the same.");
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
        winston.log('verbose' , "Scratch Space get document response code=" + httpResponseCode);
        if (httpResponseCode == '404') {
            //Metadata Index does not exist
            winston.log('verbose' , "Metadata scratch space index has no documents.");
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
            winston.log('verbose' , str);
            if (str == null) {
                winston.log('verbose' , "Empty metadata");
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
        winston.log('verbose' , neustarIpInfoIndexName + " index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            winston.log('verbose' , "This means " + neustarIpInfoIndexName + " index exists.");
            cb(true);
        } else {
            winston.log('error' , "Aborting switching...");
            throw new Error("This means " + neustarIpInfoIndexName + " index does not exist.");
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
        winston.log('verbose' , "Metadata scratch space index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                winston.log('error' , str);
                throw new Error("Metadata scratch space index creation failed.");
            } else {
                winston.log('verbose' , str);
                cb(true);
            }
        });
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    winston.log('info' , "Creating metadata index " + scratchSpaceIndexName);
}


function getCurrentIndexAndBlockSizeFromMetadata(cb) {
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
        winston.log('verbose' , "Metadata get document response code=" + httpResponseCode);
        if (httpResponseCode == '404') {
            //Metadata Index does not exist
            winston.log('verbose' , "Metadata index has no documents.");
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
            winston.log('verbose' , str);
            if (str == null) {
                winston.log('verbose' , "Empty metadata");
                cb(null);
            }
            //pass the indexName to the callBack
            var jsonMetadataString = JSON.parse(str);
            var currentIpReputationIndex = jsonMetadataString.currentIndex;
            var maxBlockSizeOfCurrentIndex = jsonMetadataString.maxBlockSize;
            if (currentIpReputationIndex == null) {
                cb(null, null);
            }
            currentIpReputationIndex = currentIpReputationIndex.trim();
            winston.log('info' , "Current index from metadata is " + currentIpReputationIndex + " and maxBlckSize is " + maxBlockSizeOfCurrentIndex);
            cb(currentIpReputationIndex, maxBlockSizeOfCurrentIndex);
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
        winston.log('verbose' , "Neustar index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                winston.log('error' , "Neustar index creation failed. Aborting upload.");
                throw new Error("Neustar index creation failed with error:" + str);
                return;
            }
            winston.log('verbose' , str);
            parseCsv(indexName, program.skiplines);
        });
    }

    var req = http.request(putOptions, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    winston.log('info' , "Creating neustar ip reputation index " + indexName);
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
        winston.log('verbose' , "Neustar index creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                throw new Error("Neustar index creation failed with error:" + str);
            } else {
                winston.log('verbose' , str);
                cb(true);
            }
        });
    }

    var req = http.request(putOptions, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(putData));
    req.end();
    winston.log('info' , "Creating index " + indexName);
}


function updateSkipLinesIfNeededAndParse(indexName) {
    var origSkipLines;
    if ((program.skiplines) == null) {
        winston.log('verbose' , "Lines skipped is null");
        program.skiplines = 0; //set it to 0
    } else {
        winston.log('verbose' , "Lines skipped originally is " + program.skiplines);
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
                throw new Error("Counting docs on " + indexName + " failed with response code = " + countSucceeded);
            } else {
                var countArray = str.split(/\r\n|\r|\n/);
                var docCount = countArray[1];
                winston.log('verbose' , "Number of docs on index " + indexName + " = " + docCount);
                //increment lines to skip by how many docs already loaded into ES index
                program.skiplines = program.skiplines + docCount;
                winston.log('info' , "New skiplines = " + program.skiplines);
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
    var batchSize = program.batchsize != null && program.batchsize > 0 ? program.batchsize : 1000;
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
            if (program.skiplines != null && total <= program.skiplines) {
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
        if (s != null) {
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
                winston.log('verbose' , "Parser Finish calling uploadBatches() and there's nothing in output");
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
                    displayStatus();
                    ++counterObject.httpBulkRetry;
                    setImmediate(function () {
                        httpWrite(bufferArray, byteLength, lastUpload);
                    });
                } else {
                    displayStatus();
                    if (lastUpload) {
                        winston.log('verbose' , "Parser.finish finished uploading the last remnants of data to be parsed");
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
        winston.log('verbose' , err.message);
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
        process.stdout.write("error count: " + counterObject.errorCount + ", uploaded batch count: "
            + counterObject.successCount + ", http pending write count: " + counterObject.httpWriteCount
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

        if (onEnd != null) {
            oThis.source.on('end', function () {
                onEnd();
            });
        }

        return oThis;
    }
}

function updateScratchSpaceWithLoadedIndex(newIndex, newMaxBlockSize) {
    //update the doc on scratch space loaded Index with new index name and new maxBlockSize
    //and nullify paused index
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
                winston.log('error' , str);
                throw new Error("Updating scratch space loaded index with " + newIndex + " failed with response code = " + updateSucceeded);
            } else {
                winston.log('info' , "Updating scratch space loaded index with " + newIndex + " succeeded.");
                winston.log('verbose' , str);
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

function updateScratchSpaceWithLastUsedIndex(lastUsedIndex, lastUsedMaxBlockSize) {
    //update the doc on scratch space last used Index and last maxBlockSize
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
                winston.log('error' , str);
                throw new Error("Updating scratch space last used index with " + lastUsedIndex + " failed with response code = " + updateSucceeded);
            } else {
                winston.log('info' , "Updating scratch space loaded index with " + lastUsedIndex + " succeeded.");
                winston.log('verbose' , str);
            }
        });
    }

    const postData = {
        "doc" : {
            "lastUsedIndex": lastUsedIndex,
            "lastUsedMaxBlockSize": lastUsedMaxBlockSize
        }
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(postData));
    req.end();

}

function removeLastUsedIndexFromScratchSpace() {
    removeInfoFromScratchSpace("lastUsed");
}

function removePausedIndexFromScratchSpace() {
    removeInfoFromScratchSpace("paused");
}

function removeLoadedIndexFromScratchSpace() {
    removeInfoFromScratchSpace("loaded");
}

function removeInfoFromScratchSpace(indexToBeDeleted) {
    //update the doc on metadata index and remove loaded index or paused index based on above args
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0/_update',
        headers: {
            'Content-Type': 'application/json'
        },
        method: 'POST'
    };

    var logString;

    var callback = function(response) {
        var updateSucceeded = response.statusCode;
        var str = '';
        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been received, so we just print it out here
        response.on('end', function () {
            if (updateSucceeded != '200') {
                winston.log('error' , "Removing " + logString + " from scratch space failed with response code = " + updateSucceeded);
                winston.log('error' , str);
                return;
            } else {
                winston.log('verbose' , "Removing " + logString + " from scratch space succeeded");
                winston.log('verbose' , str);
            }
        });
    }

    var postData;

    switch(indexToBeDeleted) {
        case "loaded":
            postData = {
                "doc" : {
                    "loadedIndex": null,
                    "loadedMaxBlockSize": null
                }
            }
            logString = "loadedIndex info";
            break;
        case "paused":
            postData = {
                "doc" : {
                    "pausedIndex": null
                }
            }
            logString = "pausedIndex info";
            break;
        case "lastUsed":
            postData = {
                "doc" : {
                    "lastUsedIndex": null,
                    "lastUsedMaxBlockSize": null
                }
            }
            logString = "lastUsedIndex info";
            break;
    }

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(postData));
    req.end();

}

function updateMetadataIndex(create, newIndex, newMaxBlockSize, oldIndex, oldBlockSize) {
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
                    throw new Error("Creating new doc on metadata index with new index name " + newIndex + " failed with response code = " + uploadSucceeded);
                } else {
                    winston.log('info' , "Creating new doc on metadata index with new index name " + newIndex + " succeeded.");
                    winston.log('verbose' , str);
                }
            });
        }

        const putData = {
            "maxBlockSize" : newMaxBlockSize,
            "currentIndex" : newIndex,
            "version" : "something", //do we need this??
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
                    winston.log('error' , str);
                    throw new Error("Updating metadata index with new index name " + newIndex + " failed with response code = " + updateSucceeded);
                } else {
                    winston.log('info' , "Updating metadata index with new index name " + newIndex + " succeeded.");
                    winston.log('verbose' , str);
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
