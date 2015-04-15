 /* jshint newcap: false */

'use strict';

/*
 * NOTE:
 * Be sure to run ./tools/devrel/setup-examples from the
 * riak-nodejs-client repo prior to running these. It will
 * create the expected bucket types and other settings.
 */

var config = require('./config');
var Riak = require('basho-riak-client');

var TasteOfRiakIntroduction = require('./dev/taste-of-riak/introduction');

var DevUsingBasics = require('./dev/using/basics');
var DevUsingUpdates = require('./dev/using/updates');
var DevUsing2i = require('./dev/using/2i');
var DevUsingConflictResolution = require('./dev/using/conflict-resolution');
var DevUsingDataTypes = require('./dev/using/data-types');

var DevAdvancedBucketTypes = require('./dev/advanced/bucket-types');

TasteOfRiakIntroduction();

DevUsingBasics();
DevUsingUpdates();
DevUsing2i();
DevUsingConflictResolution();
DevUsingDataTypes();

DevAdvancedBucketTypes();

var client = config.createClient();
client.shutdown(function (state) {
    if (state === Riak.Cluster.State.SHUTDOWN) {
        process.exit();
    }
});

