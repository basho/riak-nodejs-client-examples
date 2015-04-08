'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/conflict-resolution/
 * http://docs.basho.com/riak/latest/dev/using/conflict-resolution/nodejs/
 */

var assert = require('assert');
var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingConflictResolution() {
    var client = config.createClient();

    siblings_in_action();

    function siblings_in_action() {
        var obj1 = new Riak.Commands.KV.RiakObject();
        obj1.setContentType('text/plain');
        obj1.setBucketType('siblings_allowed');
        obj1.setBucket('nickolodeon');
        obj1.setKey('best_character');
        obj1.setValue('Ren');

        var obj2 = new Riak.Commands.KV.RiakObject();
        obj2.setContentType('text/plain');
        obj2.setBucketType('siblings_allowed');
        obj2.setBucket('nickolodeon');
        obj2.setKey('best_character');
        obj2.setValue('Ren');

        var storeFuncs = [];
        [obj1, obj2].forEach(function (obj) {
            storeFuncs.push(
                function (async_cb) {
                    client.storeValue({ value: obj }, function (err, rslt) {
                        async_cb(err, rslt);
                    });
                }
            );
        });

        async.parallel(storeFuncs, function (err, rslts) {
            if (err) {
                throw new Error(err);
            }
            read_siblings();
        });
    }

    function read_siblings() {
        client.fetchValue({
            bucketType: 'siblings_allowed', bucket:
                'nickolodeon', key: 'best_character'
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("nickolodeon/best_character has '%d' siblings",
                rslt.values.length);

            resolve_siblings();
        });
    }

    function resolve_siblings() {
        client.fetchValue({
            bucketType: 'siblings_allowed', bucket:
                'nickolodeon', key: 'best_character'
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var riakObj = rslt.values.shift();
            riakObj.setValue('Stimpy');
            client.storeValue({ value: riakObj, returnBody: true },
                function (err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }

                    assert(rslt.values.length === 1);
                });
        });
    }
}

module.exports = DevUsingConflictResolution;

