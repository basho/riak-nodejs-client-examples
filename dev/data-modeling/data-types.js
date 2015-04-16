'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/data-modeling/data-types/
 */

var async = require('async');
var clone = require('clone');
var logger = require('winston');

var User = require('./models/user');
var UserRepository = require('./repositories/user-repository');

function throwIfErr(err) {
    if (err) {
        throw new Error(err);
    }
}

function DevDataModelingDataTypes(done) {
    var client = config.createClient();

    var userRepo = new UserRepository(client);
    var bruce = new User('Bruce', 'Wayne', ['bats', 'crime fighting']);

    userRepo.save(bruce, function (err, rslt) {
        throwIfErr(err);

        logger.info("[DevDataModelingDataTypes] saved bruce: '%s'", JSON.stringify(rslt));

        done();
    });
}

module.exports = DevDataModelingDataTypes;

