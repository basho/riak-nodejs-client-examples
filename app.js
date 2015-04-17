 /* jshint newcap: false */

'use strict';

/*
 * NOTE:
 * Be sure to run ./tools/devrel/setup-examples from the
 * riak-nodejs-client repo prior to running these. It will
 * create the expected bucket types and other settings.
 */

var async = require('async');
var parseArgs = require('minimist');
var logger = require('winston');
var Riak = require('basho-riak-client');

var config = require('./config');

var TasteOfRiakIntroduction = require('./dev/taste-of-riak/introduction');

var DevUsingBasics = require('./dev/using/basics');
var DevUsingUpdates = require('./dev/using/updates');
var DevUsing2i = require('./dev/using/2i');
var DevUsingConflictResolution = require('./dev/using/conflict-resolution');
var DevUsingDataTypes = require('./dev/using/data-types');

var DevAdvancedBucketTypes = require('./dev/advanced/bucket-types');

var DevDataModelingDataTypes = require('./dev/data-modeling/data-types');

var DevSearchDocumentStore = require('./dev/search/document-store');
var DevSearchDataTypes = require('./dev/search/search-data-types');

var examples = {
    TasteOfRiak: [ '               Taste Of Riak Intro', TasteOfRiakIntroduction],
    DevUsingBasics: [ '            Dev/Using/Basics', DevUsingBasics ],
    DevUsingUpdates: [ '           Dev/Using/Updates', DevUsingUpdates ],
    DevUsing2i: [ '                Dev/Using/2i', DevUsing2i ],
    DevUsingConflictResolution: [ 'Dev/Using/Conflict-Resolution', DevUsingConflictResolution ],
    DevUsingDataTypes: [ '         Dev/Using/Data-Types', DevUsingDataTypes] ,
    DevAdvancedBucketTypes: [ '    Dev/Advanced/Bucket-Types', DevAdvancedBucketTypes ],
    DevDataModelingDataTypes: [ '  Dev/Data-Modeling/Data-Types', DevDataModelingDataTypes ],
    DevSearchDocumentStore: [ '    Dev/Search/Document-Store', DevSearchDocumentStore ],
    DevSearchDataTypes: [ '        Dev/Search/Search-Data-Types', DevSearchDataTypes, DevSearchDataTypes.Init ]
};

function client_shutdown() {
    var client = config.createClient();
    client.shutdown(function (state) {
        if (state === Riak.Cluster.State.SHUTDOWN) {
            process.exit();
        }
    });
}

function usage() {
    console.log('Argument                     Docs Page');
    console.log('--------------------------------------------------------------');
    console.log('--all                        Run all examples');
    console.log('--init                       Init all examples');
    Object.keys(examples).forEach(function (ex) {
        var name = examples[ex][0];
        console.log("--" + ex + " " + name);
    });
    console.log('');
}

var args_options = {
    alias: {
        usage: [ 'help', 'h' ],
        debug: [ 'verbose', 'd', 'v' ]
    }
};
var argv = parseArgs(process.argv, args_options);

if (argv.debug) {
    logger.remove(logger.transports.Console);
    logger.add(logger.transports.Console, {
        level : 'debug',
        colorize: true,
        timestamp: true
    });
}

logger.debug("parsed argv: '%s'", JSON.stringify(argv));

if (argv.usage) {
    usage();
    process.exit();
}

function maybeRunExampleInit(example, doneFunc) {
    var ex_name = example[0].trim();
    logger.debug("maybe running init for '%s'", ex_name);
    if (example.length === 3) {
        logger.debug("running init for '%s'", ex_name);
        var initFunc = example[2];
        initFunc(doneFunc);
    } else {
        doneFunc();
    }
}

function executeExample(example, doneFunc) {
    var ex_func = example[1];
    maybeRunExampleInit(example, function () {
        logger.debug("running ex_func for '%s'", ex_name);
        ex_func(doneFunc);
    });
}

if (argv.init) {
    logger.info("Init ALL examples!");
    var funcs = [];
    Object.keys(examples).forEach(function (ex) {
        var async_runner = function (async_cb) {
            maybeRunExampleInit(examples[ex], async_cb);
        };
        funcs.push(async_runner);
    });
    async.parallel(funcs, function (err, rslts) {
        if (err) {
            logger.err("error: '%s'", err);
        }
        client_shutdown();
    });
}

if (argv.all) {
    logger.info("Running ALL examples!");
    var funcs = [];
    Object.keys(examples).forEach(function (ex) {
        var async_runner = function (async_cb) {
            executeExample(examples[ex], async_cb);
        };
        funcs.push(async_runner);
    });
    async.parallel(funcs, function (err, rslts) {
        if (err) {
            logger.err("error: '%s'", err);
        }
        client_shutdown();
    });
}

var example_found = false;
Object.keys(argv).forEach(function (arg) {
    if (examples[arg]) {
        example_found = true;
        var ex_func = examples[arg][1];
        logger.info("Running '%s'", arg);
        executeExample(examples[arg], function (err, rslt) {
            client_shutdown();
        });
    }
});

if (! example_found) {
    usage();
}

