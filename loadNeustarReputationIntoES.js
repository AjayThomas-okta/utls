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
 DeleteAll - Delete all indexes as long as they are not used by the monolith (got from neustar.metadata)
 or was the last used index by monolith (got from scratch space)
 DeleteOld - Delete all indexes that are older than what the monolith is using (got from neustar.metadata)
 except the last used index by monolith (got from scratch space)
 DeleteSingle - Deletes a single index that is given as paramter (as long as it is not what monolith is using or the last used index by monolith)
 Switch - Switch the index being used by the monolith to the latest fully loaded index (this is updating neustar.metadata)
 switchToLastUsedIndex - Switch the index being used by the monolith to the last index that the monolith used

 This script also allows for resuming of uploading docs loading into Neustar index (if it was paused because of a system crash or Ctrl + C)
 Neustar index is of the form neustar.ipinfo.* (where * is the timestamp of when the index was created)
 it is of type int (time form epoch)

 LogLevels
 Default is info
 Run with --logLevel verbose to see all logs

 --logBulkUpload size   (write BulkUploadResponse to log file)
 size is in MB so 20 means size of log file capped at 20 MB
 */

var parse = require('csv-parse');
var http = require('http');
var program = require('commander');
var makeSource = require("stream-json");
var winston = require('winston');
const fs = require('fs');
const fileName = '/tmp/neustarBulkUploadResponse.log';
program
    .version('0.0.1')
    .option('-s, --skiplines <n>', 'skip first n lines', parseInt)
    .option('-b, --batchsize <n>', 'upload in bulk size', parseInt)
    .option('--host [esHost]', 'elasticSearch Host')
    .option('--port [esPort]', 'elasticSearch port')
    .option('--deleteAll', 'delete all indexes except monolith one')
    .option('--deleteOld', 'delete indexes older that whats used by monolith')
    .option('--deleteSingle [indexToDelete]', 'delete single index')
    .option('--upload', 'upload csv file')
    .option('--switch', 'start using new index for ip reputation')
    .option('--switchToLastUsedIndex', 'start using new index for ip reputation')
    .option('--logLevel [winstonLogLevel]', 'log level for console logging')
    .option('--logBulkUpload <n>', 'write BulkUploadResponse to log file and cap its size', parseInt)
    .parse(process.argv);

//Regular expression used when getting all Neustar IP reputation indexes on ES
const indexRegExp = "neustar.ipinfo.*";
const metadataIndexName = "neustar.metadata";
const scratchSpaceIndexName = "neustar.scratch.space";
const metadataTypeName = "1";
const objectType = "1";//name of mapping for Neustar index

//all ip info indexes will have this prefix
//the suffix is the timestamp (int measured from epoch)
const neustarIndexPrefix = "neustar.ipinfo.";
var recordCount = 0;
var total = 0;

setLogLevel();
bulkUploadLogCheck();

function setLogLevel() {
    if (program.logLevel == null) {
        winston.level = 'info';
    } else {
        switch (program.logLevel.toString()) {
            case 'debug':
            case 'verbose':
                winston.level = program.logLevel.toString();
                break;
            default:
                winston.level = 'info';
                break;
        }
    }
    console.log("Logging level set to " + winston.level);
}

function bulkUploadLogCheck() {
    //check for logFile exist
    if (program.logBulkUpload != null) {
        fs.writeFile(fileName, '', {flag: 'w'}, function (err) {
            if (err) {
                throw err;
            }
            winston.log('info', "Created log file");
            optionCheck();
        });
    } else {
        optionCheck();
    }
}

function countNonNull(ary) {
    return ary.filter(function(elem) { return elem != null; }).length;
}

function optionCheck() {
    var startDateTime = new Date();
    winston.log('info', "Time started: " + " hours " + startDateTime.getUTCHours() +
        " minutes " + startDateTime.getUTCMinutes() + " seconds "  + startDateTime.getUTCSeconds());
    if (program.host == null || program.port == null || program.host == true || program.port == true) {
        //if the host or port options were not there or if they were not provided with parameters
        throw new Error("Elasticsearch host and port not set");
    } else {
        winston.log('verbose', "ElasticSearch host = " + program.host + " and port = " + program.port);
    }

    if (countNonNull([program.deleteAll, program.deleteOld, program.deleteSingle,
            program.upload, program.switch, program.switchToLastUsedIndex]) == 0) {
        throw new Error("No option selected. Use --delete to clear old indexes, --upload to upload csv file or --switch to enable the new index");
    } else if (countNonNull([program.deleteAll, program.deleteOld, program.deleteSingle,
            program.upload, program.switch, program.switchToLastUsedIndex]) > 1) {
        throw new Error("More than one option among delete, upload, switch not allowed.");
    }

    if (program.deleteOld != null) {
        winston.log('verbose', "Delete old indexes option selected. Program will now delete old unused elasticsearch indexes for Neustar IP reputation.");
    } else if (program.deleteAll != null) {
        winston.log('verbose', "Delete all indexes option selected. Program will now delete all unused elasticsearch indexes for Neustar IP reputation.");
    } else if (program.deleteSingle != null) {
        winston.log('verbose', "Delete single index option selected. Program will now delete elasticsearch index specified.");
    } else if (program.upload != null) {
        winston.log('verbose', "Upload option selected. Program will upload Neustar IP reputation to a new index.");
    } else if (program.switch != null) {
        winston.log('verbose', "Switch option selected. Program will switch current index in metadata index to use latest Neustar IP reputation index.");
    } else if (program.switchToLastUsedIndex != null) {
        winston.log('verbose', "Switch option to Last Index selected. Program will switch current index in metadata index to use last used index from metadata.");
    }

    //first check if metadata index exist
    assertMetadataIndexExists();
}

//creates index if it doesn't exist only if createData is not null
function assertIndexExists(indexToAssert, createData, cb) {
    var options = {
        host: program.host,
        port: program.port,
        path: '/' + indexToAssert,
        method: 'HEAD'
    };

    var callback = function(response) {
        var httpResponseCode = response.statusCode;
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        winston.log('verbose', indexToAssert + " index exist response code=" + httpResponseCode);
        if (httpResponseCode == '200') {
            winston.log('verbose', "This means " + indexToAssert + " index exists.");
            cb(true);
        } else {
            if (createData == null) {
                throw new Error("This means " + indexToAssert + " index does not exist.");
            } else {
                winston.log('verbose', "This means " + indexToAssert + " index does not exist. Creating it...");
                winston.log('verbose', str);
                createIndex(indexToAssert, createData, cb);
            }
        }
    };

    http.request(options, callback).end();
}

function assertMetadataIndexExists() {
    const createData = {
        settings : {
            index : {
                number_of_shards : 2,//compare times with more shards
                number_of_replicas : 1
            }
        },
        mappings : {
            1 : {
                properties: {
                    currentIndex: {
                        ignore_above: 10922,
                        type: "string" //prefix + timestamp (measured from epoch)
                    },
                    maxBlockSize: {
                        type: "integer"
                    },
                    //use this property for debugging - more data about the csv file itself
                    //also version is used as a cache key in redis by monolith
                    version: {
                        ignore_above: 10922,
                        type: "string"
                    }
                }
            }
        }
    };

    assertIndexExists(metadataIndexName, createData, function (success) {
        if (success) {
            doOperationBasedOnOption();
        }
    });
}

function assertScratchSpaceIndexExists(cb) {
    const createData = {
        settings : {
            index : {
                number_of_shards : 2,//compare times with more shards
                number_of_replicas : 1
            }
        },
        mappings : {
            1 : {
                properties: {
                    loadedIndex: {
                        ignore_above: 10922,
                        type: "string" //measured from epoch
                    },
                    loadedMaxBlockSize: {
                        type: "integer"
                    },
                    pausedIndex: {
                        ignore_above: 10922,
                        type: "string" //measured from epoch
                    },
                    lastUsedIndex: {
                        ignore_above: 10922,
                        type: "string" //measured from epoch
                    },
                    lastUsedMaxBlockSize: {
                        type: "integer"
                    }
                }
            }
        }
    };

    assertIndexExists(scratchSpaceIndexName, createData, function (success) {
        if (success) {
            cb(true);
        }
    });
}

function doOperationBasedOnOption() {
    if (program.deleteOld != null) {
        //clear old indexes
        deleteUnusedIndexes(false);
    } else if (program.deleteAll != null) {
        //clear all indexes
        deleteUnusedIndexes(true);
    } else if (program.deleteSingle != null) {
        deleteSingleIndex();
    } else if (program.upload != null) {
        uploadCsv();
    } else if (program.switch != null || program.switchToLastUsedIndex != null) {
        switchIndex();
    } else {
        throw new Error("doOperationBasedOnOption failed. Aborting...");
    }
}

function deleteIndex(indexToBeDeleted, cb) {
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
                winston.log('warn', indexToBeDeleted + "failed to be deleted with response code = " + indexDeleted);
                winston.log('warn', "Index deletion error is " + str);
                cb(false, indexToBeDeleted);
            } else {
                winston.log('info', "Unused index " + indexToBeDeleted + " deleted");
                cb(true, indexToBeDeleted);
            }
        });
    };

    http.request(options, callback).end();
}

function deleteUnusedIndexes(deleteAll) {
    assertScratchSpaceIndexExists(function (success) {
        if (success === true) {
            getAllNeustarIndexes(function(arrayOfIndexes) {
                if (arrayOfIndexes == null) {
                    winston.log('info', "No indexes to delete.");
                    return;
                }
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
                        for (var i = 0; i < arrayOfIndexes.length; i++) {
                            //we don't allow to delete the current index used by monolith
                            if (arrayOfIndexes[i] != currentMonolithIndex) {
                                //we also don't allow to delete the last used index by monolith since that could still be used in prod
                                //because a switch could have gone wrong and we want to revert back to lastUsed index
                                if (arrayOfIndexes[i] === lastUsedIndex) {
                                    winston.log('verbose', "Last used index " + lastUsedIndex + " must not be deleted.");
                                    continue;
                                }
                                somethingDeleted = true;
                                deleteIndex(arrayOfIndexes[i], function (deleteSuccess, indexDeleted) {
                                    if (deleteSuccess) {
                                        if (indexDeleted === loadedIndex) {
                                            removeLoadedIndexFromScratchSpace();
                                        }
                                        if (indexDeleted === pausedIndex) {
                                            removePausedIndexFromScratchSpace();
                                        }
                                    }
                                });
                            } else {
                                if (!deleteAll) {
                                    //delete indexes newer than what's used by monolith only if it is deleteAll
                                    //if deleteOld, then we must stop one we reach the index used by monolith
                                    //hence break out of for loop
                                    //FYI: arrayOfIndexes is already sorted according to timestamp inside getAllNeustarIndexes()
                                    break;
                                }
                            }
                        }
                        if (!somethingDeleted) {
                            winston.log('info', "Nothing to delete");
                        }
                    });
                });
            });
        } else {
            throw new Error("Scratch space creation failed");
        }
    });
}

function deleteSingleIndex() {
    if (program.deleteSingle == true) {
        winston.log('info', "No index specified to delete");
        return;
    }

    assertScratchSpaceIndexExists(function (success) {
        if (success === true) {
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

                    if (program.deleteSingle == currentMonolithIndex) {
                        winston.log('warn', "Failed to delete " + program.deleteSingle + " since it's currently being used by monolith.");
                    } else if (program.deleteSingle == lastUsedIndex) {
                        winston.log('warn', "Failed to delete " + program.deleteSingle + " since it was the last used index by monolith.");
                    } else {
                        winston.log('verbose', "About to delete " + program.deleteSingle);
                        deleteIndex(program.deleteSingle, function (deleteSuccess, indexDeleted) {
                            if (deleteSuccess) {
                                if (indexDeleted === loadedIndex) {
                                    removeLoadedIndexFromScratchSpace();
                                }
                                if (indexDeleted === pausedIndex) {
                                    removePausedIndexFromScratchSpace();
                                }
                            }
                        });
                    }
                });
            });
        } else {
            throw new Error("Scratch space creation failed");
        }
    });
}

function getAllNeustarIndexes(cb) {
    winston.log('verbose', "Getting all Neustar indexes");
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
            winston.log('verbose', "Neustar indexes on ES are: " + str);

            var arrayOfIndexes = str.split(/\r\n|\r|\n/);
            var indexToDelete = null;
            //we subtract 2 from teh length since there is a new line at the beginning of this list and at the end
            //need to omit those 2 newlines when getting the real count of indexes
            var indexCount = arrayOfIndexes.length - 2;
            winston.log('verbose', "Number of indexes = " + indexCount);
            if (indexCount === 0) {
                cb(null);
            } else {
                //remove the first element since that just says "index" (header of the table)
                arrayOfIndexes.shift();
                //remove last line which is a blank line
                arrayOfIndexes.pop();
                for (var i = 0; i < indexCount; i++) {
                    arrayOfIndexes[i] = arrayOfIndexes[i].toString().trim();
                }
                arrayOfIndexes.sort();
                cb(arrayOfIndexes);
            }
        });
    };

    http.request(options, callback).end();
}

function uploadCsv() {
    assertScratchSpaceIndexExists(function (success) {
        if (success === true) {
            getScratchSpaceInfo(function(scratchSpaceInfo) {
                var ipReputationIndexName;
                if (scratchSpaceInfo == null || scratchSpaceInfo.pausedIndex == null) {
                    ipReputationIndexName = neustarIndexPrefix + Math.round(new Date().getTime() / 1000);
                    winston.log('info', "Creating new index for csv upload = " + ipReputationIndexName);

                    const neustarIndexCreateData = {
                        settings : {
                            index : {
                                number_of_shards : 2,
                                number_of_replicas : 1
                            }
                        },
                        mappings : {
                            1 : {
                                //don't create new fields dynamically for mapping when docs loaded
                                dynamic: false,
                                properties: {
                                    end_ip_int: {
                                        type: "long"
                                    },
                                    carrier: {
                                        ignore_above: 10922,
                                        type: "string"
                                    },
                                    start_ip_int: {
                                        type: "long"
                                    },
                                    anonymizer_status: {
                                        ignore_above: 10922,
                                        type: "string"
                                    },
                                    organization: {
                                        ignore_above: 10922,
                                        type: "string"
                                    },
                                    proxy_type: {
                                        ignore_above: 10922,
                                        type: "string"
                                    },
                                    sld: {
                                        ignore_above: 10922,
                                        type: "string"
                                    },
                                    asn: {
                                        type: "integer"
                                    },
                                    tld: {
                                        ignore_above: 10922,
                                        type: "string"
                                    }
                                }
                            }
                        }
                    };


                    createIndex(ipReputationIndexName, neustarIndexCreateData, function (success) {
                        if (success === true) {
                            //get count of docs first to know whether to create a doc on scratch space or it's just an update
                            getDocCountOnIndex(scratchSpaceIndexName, function (count) {
                                var createDoc = true;
                                if (count == 1) {
                                    createDoc = false;
                                }
                                updateScratchSpaceWithNewInfo("paused", ipReputationIndexName, null, createDoc, function(result) {
                                    if (result === true) {
                                        parseCsv(ipReputationIndexName, program.skiplines);
                                    }
                                });
                            });
                        }
                    });
                } else {
                    pausedIndex = scratchSpaceInfo.pausedIndex.trim();
                    ipReputationIndexName = pausedIndex;
                    winston.log('info', "Reusing paused index=" + ipReputationIndexName + " in scratch space for csv upload.");
                    //TODO: Gotta be careful, we're not uploading paused index with new csv file
                    updateSkipLinesIfNeededAndParse(ipReputationIndexName);
                }
            });
        }
    });
}

function switchIndex() {
    assertScratchSpaceIndexExists(function (success) {
        if (success === true) {
            updateMetadataCurrentIndex();
        }
    });
}

function getDocCountOnIndex(indexName, cb) {
    winston.log('verbose', "Getting doc count on " + indexName);
    //get all indexes that match regexp 'indexName'
    var options = {
        host: program.host,
        port: program.port,
        ///_cat/count/neustar.scratch.space?v&pretty
        path: '/_cat/count/' + indexName + '?v&h=count&pretty',
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
            var countInfo = str.split(/\r\n|\r|\n/);
            winston.log('verbose', "Doc count on " + indexName + ": " + countInfo[1]);
            cb(countInfo[1]);
        });
    };

    http.request(options, callback).end();
}

function updateMetadataCurrentIndex() {
    //get the only doc on the metadata index
    //its contents is in a JSON format
    //one of the keys (currentIndex) is the name of the current neustar ip info
    getScratchSpaceInfo(function (scratchSpaceInfo) {
        if (scratchSpaceInfo == null) {
            winston.log('info', "No documents on scratch space. Cannot update metadata index.");
            return;
        }

        var indexToSwitchTo, switchMaxBlockSize;
        if(program.switchToLastUsedIndex != null) {
            indexToSwitchTo = scratchSpaceInfo.lastUsedIndex;
            switchMaxBlockSize = scratchSpaceInfo.lastUsedMaxBlockSize;
            if (indexToSwitchTo == null) {
                winston.log('info', "No last used index in scratch space.");
                winston.log('verbose', "Paused index = " + scratchSpaceInfo.pausedIndex + " and loaded index = " + scratchSpaceInfo.loadedIndex);
                return;
            }
        } else {
            indexToSwitchTo = scratchSpaceInfo.loadedIndex;
            if (indexToSwitchTo == null) {
                winston.log('info', "Nothing fully loaded so far into ES. Loaded index is null from scratch space.");
                winston.log('verbose', "Paused index = " + scratchSpaceInfo.pausedIndex);
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
        assertIndexExists(indexToSwitchTo, null, function (success) {
            if (success) {
                getCurrentIndexAndBlockSizeFromMetadata(function (currentMonolithIndex, currentMaxBlockSize) {
                    if (currentMonolithIndex == null) {
                        winston.log('info', "Metadata has no index in use currently. Setting it as the " +
                            "loaded index " + indexToSwitchTo + " from scratch space");
                        //create a doc
                        updateMetadataIndex(true, indexToSwitchTo, switchMaxBlockSize);
                    } else {
                        if (program.switchToLastUsedIndex != null && indexToSwitchTo != currentMonolithIndex) {
                            //we are switching to last Used Index
                            //so it could be older than what is used by monolith, and we should allow that
                            updateMetadataIndex(false, indexToSwitchTo, switchMaxBlockSize);
                        } else if (indexToSwitchTo > currentMonolithIndex) {
                            winston.log('info', "Metadata has index " + currentMonolithIndex + " in use currently. Setting it as the " +
                                "loaded index " + indexToSwitchTo + " from scratch space");
                            //update doc
                            updateMetadataIndex(false, indexToSwitchTo, switchMaxBlockSize);
                            updateScratchSpaceWithNewInfo("lastUsed", currentMonolithIndex, currentMaxBlockSize);
                        } else {
                            winston.log('info', "Metadata has index " + currentMonolithIndex + " in use currently. Not switching since " +
                                "loaded index " + indexToSwitchTo + " from scratch space is older or the same.");
                            return;
                        }
                    }
                });
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
        var httpResponseCode = response.statusCode;
        winston.log('verbose', "Scratch Space get document response code=" + httpResponseCode);
        if (httpResponseCode == '404') {
            //Metadata Index does not exist
            winston.log('verbose', "Metadata scratch space index has no documents.");
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
            winston.log('verbose', str);
            if (str == null) {
                winston.log('verbose', "Empty metadata");
                cb(null);
            } else {
                //pass the indexName to the callBack
                var jsonMetadataString = JSON.parse(str);
                cb(jsonMetadataString);
            }
        });
    };

    http.request(options, callback).end();
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
        var httpResponseCode = response.statusCode;
        winston.log('verbose', "Metadata get document response code=" + httpResponseCode);
        if (httpResponseCode == '404') {
            //Metadata Index does not exist
            winston.log('verbose', "Metadata index has no documents.");
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
            winston.log('verbose', str);
            if (str == null) {
                winston.log('verbose', "Empty metadata");
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
            winston.log('info', "Current index from metadata is " + currentIpReputationIndex + " and maxBlockSize is " + maxBlockSizeOfCurrentIndex);
            cb(currentIpReputationIndex, maxBlockSizeOfCurrentIndex);
        });
    };

    http.request(options, callback).end();
}

function createIndex(indexName, createData, cb) {
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
        winston.log('verbose', indexName + " creation response code = " + response.statusCode);
        var str = '';

        //another chunk of data has been recieved, so append it to `str`
        response.on('data', function (chunk) {
            str += chunk;
        });

        //the whole response has been recieved, so we just print it out here
        response.on('end', function () {
            if (response.statusCode != '200'){
                throw new Error(indexName + " creation failed with error:" + str);
            } else {
                winston.log('verbose', str);
                cb(true);
            }
        });
    };

    var req = http.request(putOptions, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(createData));
    req.end();
    winston.log('info', "Creating index " + indexName);
}

function updateSkipLinesIfNeededAndParse(indexName) {
    var origSkipLines;
    if ((program.skiplines) == null) {
        winston.log('verbose', "Lines skipped is null");
        program.skiplines = 0; //set it to 0
    } else {
        winston.log('verbose', "Lines skipped originally is " + program.skiplines);
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
                winston.log('verbose', "Number of docs on index " + indexName + " = " + docCount);
                //increment lines to skip by how many docs already loaded into ES index
                program.skiplines = program.skiplines + docCount;
                winston.log('info', "New skiplines = " + program.skiplines);
                parseCsv(indexName, origSkipLines);
            }
        });
    };

    http.request(options, callback).end();
}

function writeStreamToFile(res) {
    //write stream appends to file
    var writeStream = fs.createWriteStream(fileName, {flags : 'a'});
    res.pipe(writeStream);
}

function logToFile(res) {
    const stats = fs.statSync(fileName);
    const fileSizeInBytes = stats.size;
    //Convert the file size to megabytes
    const fileSizeInMegabytes = fileSizeInBytes / 1000000.0;
    if (fileSizeInMegabytes < program.logBulkUpload) {
        writeStreamToFile(res);
    }
    /*fs.stat(fileName, function (err, stats) {
        if (err) {
            winston.log('error', "Log file didn't exist");
        } else {
            const fileSizeInBytes = stats.size;
            winston.log('info', "File size in bytes " + fileSizeInBytes);
            //Convert the file size to megabytes
            const fileSizeInMegabytes = fileSizeInBytes / 1000000.0;
            winston.log('info', "File size in MB " + fileSizeInMegabytes);
            if (fileSizeInMegabytes > program.logBulkUpload) {
                fs.truncate(fileName, 0, function () {
                    writeStreamToFile(res);
                });
            } else {
                writeStreamToFile(res);
            }
        }
    });*/

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
        replaceField(jsonRecord, 'start_ip_int');
        replaceField(jsonRecord, 'end_ip_int');
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
        var startIp = map['start_ip_int'];
        var endIp = map['end_ip_int'];
        return (endIp - startIp);
    }

    function uploadBatches(lastUpload) {
        if (output.length <= 0) {
            if (lastUpload) {
                winston.log('verbose', "Parser Finish calling uploadBatches() and there's nothing in output");
                updateScratchSpaceWithNewInfo("loaded", indexName, currentMaxBlockSize);
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
            //pipe output into a file
            if (program.logBulkUpload != null) {
                logToFile(res);
            }
            var rr = makeResponseReader(counterObject, function () {
                --counterObject.httpWriteCount;
                if (rr.hasErrors) {
                    winston.log('verbose', " Bulk upload http statusCode: " + res.statusCode);
                    displayStatus();
                    ++counterObject.httpBulkRetry;
                    setImmediate(function () {
                        httpWrite(bufferArray, byteLength, lastUpload);
                    });
                } else {
                    displayStatus();
                    if (lastUpload) {
                        winston.log('verbose', "Parser.finish finished uploading the last remnants of data to be parsed");
                        updateScratchSpaceWithNewInfo("loaded", indexName, currentMaxBlockSize);
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
        winston.log('verbose', err.message);
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
        };

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

function updateScratchSpaceWithNewInfo(property, newIndex, newMaxBlockSize, create, cb) {
    //update the doc on scratch space loaded, lastUsed or paused Index with new index name and new maxBlockSize
    //or delete loaded or paused indexes form scratch space

    var errMsg, successMsg;
    var httpMethod, httpRequestData, httpRequestPath;
    httpMethod = 'POST';
    httpRequestPath = '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0/_update';

    if (property == "loaded") {
        httpRequestData = {
            doc : {
                loadedIndex: newIndex,
                loadedMaxBlockSize: newMaxBlockSize,
                pausedIndex: null
            }
        };
        errMsg = "Updating scratch space loaded index with " + newIndex + " failed with response code = ";
        successMsg = "Updating scratch space loaded index with " + newIndex + " succeeded.";
    } else if (property == "paused") {
        if (create === true) {
            //We are using a PUT because:
            //we have a specific URI path where we want to create a resource
            httpMethod = 'PUT';
            httpRequestData = {
                loadedIndex: null,
                loadedMaxBlockSize: null,
                pausedIndex: newIndex,
                lastUsedIndex: null,
                lastUsedMaxBlockSize : null
            };
            httpRequestPath = '/' + scratchSpaceIndexName + '/' + metadataTypeName + '/0';
            errMsg = "Updating scratch space paused index with " + newIndex + " failed with response code = ";
            successMsg = "Updating scratch space paused index with " + newIndex + " succeeded.";
        } else {
            //update doc is a POST because:
            //Elasticsearch does not actually do in-place updates under the hood.
            //Whenever we do an update, Elasticsearch deletes the old document and then indexes a new document
            // with the update applied to it in one shot.
            httpRequestData = {
                doc : {
                    pausedIndex: newIndex
                }
            };
            errMsg = "Updating scratch space paused index with " + newIndex + " failed with response code = ";
            successMsg = "Updating scratch space paused index with " + newIndex + " succeeded.";
        }
    } else if (property == "lastUsed") {
        httpRequestData = {
            doc : {
                lastUsedIndex: newIndex,
                lastUsedMaxBlockSize: newMaxBlockSize
            }
        };
        errMsg = "Updating scratch space lastUsed index with " + newIndex + " failed with response code = ";
        successMsg = "Updating scratch space lastUsed index with " + newIndex + " succeeded.";
    } else if (property == "deleteLoaded") {
        httpRequestData = {
            doc : {
                loadedIndex: null,
                loadedMaxBlockSize: null
            }
        };
        errMsg = "Deleting loaded index from  scratch space failed with response code = ";
        successMsg = "Deleting loaded index from  scratch space succeeded.";
    } else if (property == "deletePaused") {
        httpRequestData = {
            doc : {
                pausedIndex: null
            }
        };
        errMsg = "Deleting paused index from  scratch space failed with response code = ";
        successMsg = "Deleting paused index from  scratch space succeeded.";
    } else {
        //undefined action on scratch space
        winston.log('warn', "Property " + property + " not supported for updateScratchSpaceWithNewInfo()");
        return;
    }

    var options = {
        host: program.host,
        port: program.port,
        path: httpRequestPath,
        headers: {
            'Content-Type': 'application/json'
        },
        method: httpMethod
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
            if (updateSucceeded == '200' || updateSucceeded == '201') {
                winston.log('info', successMsg);
                winston.log('verbose', str);
                //This function is called at the end for both uploading and switching.
                //Hence, end time calculation here makes sense.
                if (property == "loaded" || property == "lastUsed") {
                    var endDateTime = new Date();
                    winston.log('info', "Time upload ended: " + " hours " + endDateTime.getUTCHours() +
                        " minutes " + endDateTime.getUTCMinutes() + " seconds "  + endDateTime.getUTCSeconds());
                } else if (property == "paused") {
                    cb(true);
                }
            } else {
                winston.log('error', str);
                throw new Error(errMsg + updateSucceeded);
            }
        });
    };

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(httpRequestData));
    req.end();
}

function removePausedIndexFromScratchSpace() {
    updateScratchSpaceWithNewInfo("deletePaused");
}

function removeLoadedIndexFromScratchSpace() {
    updateScratchSpaceWithNewInfo("deleteLoaded");
}

function updateMetadataIndex(create, newIndex, newMaxBlockSize) {
    var httpMethod, httpRequestData, httpRequestPath;
    if (create === true) {
        httpMethod = 'PUT';
        httpRequestPath = '/' + metadataIndexName + '/' + metadataTypeName + '/0';
        httpRequestData = {
            maxBlockSize : newMaxBlockSize,
            currentIndex : newIndex,
            version : "something" //do we need this??
        };
    } else {
        httpMethod = 'POST';
        httpRequestPath = '/' + metadataIndexName + '/' + metadataTypeName + '/0/_update';
        httpRequestData = {
            doc : {
                maxBlockSize : newMaxBlockSize,
                currentIndex : newIndex
            }
        };
    }

    //there were no docs on the metadata index
    //create a new one with new index name
    var options = {
        host: program.host,
        port: program.port,
        path: httpRequestPath,
        headers: {
            'Content-Type': 'application/json'
        },
        method: httpMethod
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
            if (uploadSucceeded == '201' || uploadSucceeded == '200') {
                winston.log('info', "Updating metadata index with new index name " + newIndex + " succeeded.");
                winston.log('verbose', str);
            } else {
                throw new Error("Updating metadata index with new index name " + newIndex + " failed with response code = " + uploadSucceeded);
            }
        });
    };

    var req = http.request(options, callback);
    //This is the data we are posting, it needs to be a string or a buffer
    req.write(JSON.stringify(httpRequestData));
    req.end();
}
