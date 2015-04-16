'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/data-modeling/data-types/
 */

var async = require('async');
var clone = require('clone');
var logger = require('winston');
var Riak = require('basho-riak-client');

function checkErr(err) {
    if (err) {
        throw new Error(err);
    }
}

function User(options, callback) {
    var client;
    if (options.client && options.client instanceof Riak.Client) {
        client = options.client;
    } else {
        throw new Error('options.client must be instance of Riak.Client');
    }

    var key = options.firstName.toLowerCase() + '_' + options.lastName.toLowerCase();

    var default_options = {
        bucketType: 'maps',
        bucket: 'users',
        key: key
    };

    /*
     * Fetches our map
     */
    function getMap(callback) {
        client.fetchMap(default_options, callback);
    }

    /*
     * Updates our map using the abstract context object fetched using
     * the private getMap() function above.
     */
    this.updateMapWithContext = function (mapOp, callback) {
        getMap(function (err, rslt) {
            checkErr(err);
            var options = clone(default_options);
            options.context = rslt.context;
            client.updateMap(options, callback);
        });
    }

    /*
     * Updates our map without an abstract context object. Context is not
     * needed for some map updates.
     */
    this.updateMapWithoutContext = function (mapOp, callback) {
        var options = clone(default_options);
        options.op = mapOp;
        client.updateMap(options, callback);
    }

    /*
     * Create the user in Riak
     */
    var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
    mapOp.setRegister('first_name', options.firstName);
    mapOp.setRegister('last_name', options.lastName);
    if (options.interests) {
        options.interests.forEach(function (interest) {
            mapOp.addToSet('interests', interest);
        });
    }

    this.updateMapWithoutContext(mapOp, callback);
}

User.prototype.getFirstName = function (callback) {
};

User.prototype.visitPage = function (callback) {
    var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
    mapOp.incrementCounter(1);
    this.updateMapWithoutContext(mapOp, callback);
};

User.prototype.upgradeAccount = function (callback) {
    var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
    mapOp.setFlag('paid_account', true);
    this.updateMapWithContext(mapOp, callback);
};

User.prototype.downgradeAccount = function (callback) {
    var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
    mapOp.setFlag('paid_account', false);
    this.updateMapWithContext(mapOp, callback);
};

function DevDataModelingDataTypes(done) {
    var client = config.createClient();

    var userOptions = {
        client: client,
        firstName: 'Bruce',
        lastName: 'Wayne',
        interests: []
    };

    var bruce = new User(userOptions, function (err, rslt) {
        logger.info("[DevDataModelingDataTypes] stored Bruce Wayne");
        done(err, rslt);
    });
}

module.exports = DevDataModelingDataTypes;

