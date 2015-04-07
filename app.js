'use strict';

/*
 * NOTE:
 * Be sure to run ./tools/devrel/setup-examples from the
 * riak-nodejs-client repo prior to running these. It will
 * create the expected bucket types and other settings.
 */

var TasteOfRiakIntroduction = require('./dev/taste-of-riak/introduction');
var DevUsingBasics = require('./dev/using/basics');
var DevUsingUpdates = require('./dev/using/updates');

TasteOfRiakIntroduction();

DevUsingBasics();

DevUsingUpdates();
