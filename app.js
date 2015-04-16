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

var examples = {
    TasteOfRiak: [ '               Taste Of Riak Intro', TasteOfRiakIntroduction],
    DevUsingBasics: [ '            Dev/Using/Basics', DevUsingBasics ],
    DevUsingUpdates: [ '           Dev/Using/Updates', DevUsingUpdates ],
    DevUsing2i: [ '                Dev/Using/2i', DevUsing2i ],
    DevUsingConflictResolution: [ 'Dev/Using/Conflict-Resolution', DevUsingConflictResolution ],
    DevUsingDataTypes: [ '         Dev/Using/Data-Types', DevUsingDataTypes] ,
    DevAdvancedBucketTypes: [ '    Dev/Advanced/Bucket-Types', DevAdvancedBucketTypes ],
    DevDataModelingDataTypes: [ '  Dev/Data-Modeling/Data-Types', DevDataModelingDataTypes ]
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

if (argv.all) {
    logger.info("Running ALL examples!");
    var funcs = [];
    Object.keys(examples).forEach(function (ex) {
        var ex_func = examples[ex][1];
        var async_runner = function (async_cb) {
            ex_func(async_cb);
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

/*
TasteOfRiakIntroduction();
DevUsingBasics();
DevUsingUpdates();
DevUsing2i();
DevUsingConflictResolution();
DevUsingDataTypes();
DevAdvancedBucketTypes();
DevDataModelingDataTypes();
*/

