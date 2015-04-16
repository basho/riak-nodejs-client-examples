'use strict';

var Riak = require('basho-riak-client');
var inherits = require('util').inherits;

var Repository = require('./repository');
var User = require('../models/user');

function UserRepository(client) {
    Repository.call(this, client);
    this.bucketType = 'maps';
    this.bucketName = 'Users';
}

inherits(UserRepository, Repository);

UserRepository.prototype.buildModel = function (rslt) {
    var firstName = rslt.map.registers.first_name.toString('utf8');
    var lastName = rslt.map.registers.last_name.toString('utf8');

    var interests = [];
    if (rslt.map.sets.interests) {
        rslt.map.sets.interests.forEach(function (interest) {
            interests.push(interest);
        });
    }

    var visits = 0;
    if (rslt.map.counters.visits) {
        visits = rslt.map.counters.visits;
    }

    var paid_account = false;
    if (rslt.map.flags.paid_account) {
        paid_account = rslt.map.flags.paid_account;
    }

    return new User(firstName, lastName, interests, visits, paid_account);
};

UserRepository.prototype.getMapOperation = function (model) {
    var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
    mapOp.setRegister('first_name', model.firstName);
    mapOp.setRegister('last_name', model.lastName);
    if (model.interests) {
        model.interests.forEach(function (interest) {
            mapOp.addToSet('interests', interest);
        });
    }
    if (model.paid_account) {
        mapOp.setFlag('paid_account', model.paid_account);
    }
    // Note: visits are taken care of on a per-visit basis
    return mapOp;
};

module.exports = UserRepository;

