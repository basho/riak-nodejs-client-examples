'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/data-modeling/data-types/
 */

var async = require('async');
var clone = require('clone');
var logger = require('winston');

var EntityManager = require('./entity-manager');
var User = require('./models/user');
var UserRepository = require('./repositories/user-repository');

function throwIfErr(err) {
    if (err) {
        throw new Error(err);
    }
}

function DevDataModelingDataTypes(done) {
    config.createClient(function (err, client) {
        if (err) {
            throw new Error(err);
        }

        var entityManager = new EntityManager(client);

        var userRepo = new UserRepository(client);

        var bruce = new User('Bruce', 'Wayne', ['bats', 'crime fighting']);
        entityManager.addModel(bruce);

        var joeInterests = [ 'distributed systems', 'Erlang' ];
        var joe = new User('Joe', 'Armstrong', joeInterests);
        entityManager.addModel(joe);

        var funcs = [
            function (async_cb) {
                userRepo.save(joe, function (err, rslt) {
                    throwIfErr(err);
                    logger.debug("[DevDataModelingDataTypes] joe save rslt: '%s'", JSON.stringify(rslt));
                    logger.info("Joe.firstName: '%s'", rslt.firstName);
                    logger.info("Joe.lastName: '%s'", rslt.lastName);
                    logger.info("Joe.interests: '%s'", JSON.stringify(rslt.interests));
                    async_cb();
                });
            },
            function (async_cb) {
                userRepo.save(bruce, function (err, rslt) {
                    throwIfErr(err);

                    // Note: *must* use bruce here rather than rslt, since
                    // bruce is the entity emitting events. The EntityManager
                    // could be extended to update the in-memory entity should
                    // differences arise from concurrent saves in Riak
                    bruce.visitPage();
                    bruce.addInterest('catwoman');
                    bruce.addInterest('oranges');
                    bruce.upgradeAccount();
                    bruce.removeInterest('bats');

                    // Note: since the updates to Riak happen async a timer is added to
                    // allow operations to complete before re-fetching the object for
                    // display. This is for instructional purposes here and would not be
                    // done in production.
                    setTimeout(function () {
                        var bruce_id = rslt.key;
                        logger.debug("[DevDataModelingDataTypes] bruce save rslt: '%s'", JSON.stringify(rslt));
                        userRepo.get(bruce_id, false, function (err, updated) {
                            logger.info("[DevDataModelingDataTypes] bruce entity: '%s'", JSON.stringify(bruce));
                            logger.info("[DevDataModelingDataTypes] bruce in Riak: '%s'", JSON.stringify(updated));
                            async_cb();
                        });
                    }, 1000);
                });
            }
        ];
        async.parallel(funcs, function (err, rslts) {
            throwIfErr(err);
            entityManager.evictModel(bruce);
            entityManager.evictModel(joe);
            logger.info("[DevDataModelingDataTypes] async.parallel done");
            client.stop(function (err) {
                if (err) {
                    logger.error('[DevSearchDataTypes] err: %s', err);
                }
                done();
            });
        });
    });
}

module.exports = DevDataModelingDataTypes;
