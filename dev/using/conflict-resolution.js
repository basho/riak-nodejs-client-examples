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

function DevUsingConflictResolution(done) {
    var client = config.createClient(function (err, c) {
        var f1 = function (async_cb) {
            siblings_in_action('nickelodeon', read_siblings, async_cb);
        };

        var f2 = function (async_cb) {
            siblings_in_action('nickelodeon2', resolve_choosing_first, async_cb);
        };

        var f3 = function (async_cb) {
            siblings_in_action('nickelodeon3', resolve_using_resolver, async_cb);
        };

        async.parallel([f1, f2, f3], function (err, rslts) {
            if (err) {
                logger.error('[DevUsingConflictResolution] err: %s', err);
            }
            c.stop(function (err) {
                if (err) {
                    logger.error('[DevUsingConflictResolution] err: %s', err);
                }
                done();
            });
        });
    });

    function siblings_in_action(bucket_name, next_step_func, async_cb) {
        var obj1 = new Riak.Commands.KV.RiakObject();
        obj1.setContentType('text/plain');
        obj1.setBucketType('siblings_allowed');
        obj1.setBucket(bucket_name);
        obj1.setKey('best_character');
        obj1.setValue('Ren');

        var obj2 = new Riak.Commands.KV.RiakObject();
        obj2.setContentType('text/plain');
        obj2.setBucketType('siblings_allowed');
        obj2.setBucket(bucket_name);
        obj2.setKey('best_character');
        obj2.setValue('Ren');

        var storeFuncs = [];
        [obj1, obj2].forEach(function (obj) {
            storeFuncs.push(
                function (acb) {
                    client.storeValue({ value: obj }, function (err, rslt) {
                        acb(err, rslt);
                    });
                }
            );
        });

        async.parallel(storeFuncs, function (err, rslts) {
            if (err) {
                throw new Error(err);
            }
            next_step_func(bucket_name, async_cb);
        });
    }

    function read_siblings(bucket_name, async_cb) {
        client.fetchValue({
            bucketType: 'siblings_allowed',
            bucket: bucket_name, key: 'best_character'
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("[DevUsingConflictRes] %s/best_character has '%d' siblings",
                bucket_name, rslt.values.length);
            resolve_siblings(bucket_name, async_cb);
        });
    }

    function resolve_siblings(bucket_name, async_cb) {
        client.fetchValue({
            bucketType: 'siblings_allowed',
            bucket: bucket_name, key: 'best_character'
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
                    async_cb();
                });
        });
    }

    function resolve_choosing_first(bucket_name, async_cb) {
        client.fetchValue({
            bucketType: 'siblings_allowed',
            bucket: bucket_name, key: 'best_character'
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var riakObj = rslt.values.shift();
            client.storeValue({ value: riakObj, returnBody: true },
                function (err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }

                    assert(rslt.values.length === 1);
                    async_cb();
                });
        });
    }

    function resolve_using_resolver(bucket_name, async_cb) {
        
        function conflict_resolver(objects) {
            /*
             * Note: a more sophisticated resolver would
             * look into the objects to pick one, or perhaps
             * present the list to a user to choose
             */
            return objects.shift();
        }

        client.fetchValue({
            bucketType: 'siblings_allowed',
            bucket: bucket_name, key: 'best_character',
            conflictResolver: conflict_resolver
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            // Test that resolver just returned one
            // RiakObject
            assert(rslt.values.length === 1);

            var riakObj = rslt.values.shift();
            client.storeValue({ value: riakObj, returnBody: true },
                function (err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }

                    assert(rslt.values.length === 1);
                    async_cb();
                });
        });
    }
}

module.exports = DevUsingConflictResolution;
