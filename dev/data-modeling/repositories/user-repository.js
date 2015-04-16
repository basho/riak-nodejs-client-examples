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
    var rv = null;
    if (rslt.map) {
        var firstName = rslt.map.registers.first_name.toString('utf8');
        var lastName = rslt.map.registers.last_name.toString('utf8');

        var interests = [];
        if (rslt.map.sets.interests) {
            rslt.map.sets.interests.forEach(function (interest) {
                interests.push(interest);
            });
        }

        rv = new User(firstName, lastName, interests);
    }
    return rv;
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
    return mapOp;
};

module.exports = UserRepository;

