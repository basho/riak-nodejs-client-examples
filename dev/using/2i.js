'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/2i/
 */

var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsing2i(done) {
    var client = config.createClient();

    inserting_objects();

    indexing_objects();

    done();

    function inserting_objects() {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setBucket('users');
        riakObj.setKey('john_smith');
        riakObj.setValue('...user data...');
        riakObj.addToIndex('twitter_bin', 'jsmith123');
        riakObj.addToIndex('email_bin', 'jsmith@basho.com');
        client.storeValue({ value: riakObj }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            logger.info('[DevUsing2i] Stored john_smith with index data');
            querying_indexes();
        });
    }

    function querying_indexes() {

        var cmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucket('users')
            .withIndexName('twitter_bin')
            .withIndexKey('jsmith123')
            .withCallback(query_cb)
            .build();
        client.execute(cmd);
    }

    function indexing_objects() {

        function store_cb(err, rslt, async_cb) {
            if (err) {
                throw new Error(err);
            }
            async_cb(null, rslt);
        }

        var storeFuncs = [
            function (async_cb) {
                var riakObj = new Riak.Commands.KV.RiakObject();
                riakObj.setContentType('text/plain');
                riakObj.setBucketType('indexes');
                riakObj.setBucket('people');
                riakObj.setKey('larry');
                riakObj.setValue('My name is Larry');
                riakObj.addToIndex('field1_bin', 'val1');
                riakObj.addToIndex('field2_int', 1001);
                client.storeValue({ value: riakObj }, function (err, rslt) {
                    store_cb(err, rslt, async_cb);
                });
            },
            function (async_cb) {
                var riakObj = new Riak.Commands.KV.RiakObject();
                riakObj.setContentType('text/plain');
                riakObj.setBucketType('indexes');
                riakObj.setBucket('people');
                riakObj.setKey('moe');
                riakObj.setValue('My name is Moe');
                riakObj.addToIndex('Field1_bin', 'val2');
                riakObj.addToIndex('Field2_int', 1002);
                client.storeValue({ value: riakObj }, function (err, rslt) {
                    store_cb(err, rslt, async_cb);
                });
            },
            function (async_cb) {
                var riakObj = new Riak.Commands.KV.RiakObject();
                riakObj.setContentType('text/plain');
                riakObj.setBucketType('indexes');
                riakObj.setBucket('people');
                riakObj.setKey('curly');
                riakObj.setValue('My name is Curly');
                riakObj.addToIndex('FIELD1_BIN', 'val3');
                riakObj.addToIndex('FIELD2_INT', 1003);
                client.storeValue({ value: riakObj }, function (err, rslt) {
                    store_cb(err, rslt, async_cb);
                });
            },
            function (async_cb) {
                var riakObj = new Riak.Commands.KV.RiakObject();
                riakObj.setContentType('text/plain');
                riakObj.setBucketType('indexes');
                riakObj.setBucket('people');
                riakObj.setKey('veronica');
                riakObj.setValue('My name is Veronica');
                riakObj.addToIndex('FIELD1_bin', 'val4');
                riakObj.addToIndex('FIELD1_bin', 'val4');
                riakObj.addToIndex('FIELD1_bin', 'val4a');
                riakObj.addToIndex('FIELD1_bin', 'val4b');
                riakObj.addToIndex('FIELD2_int', 1004);
                riakObj.addToIndex('FIELD2_int', 1005);
                riakObj.addToIndex('FIELD2_int', 1006);
                riakObj.addToIndex('FIELD2_int', 1004);
                riakObj.addToIndex('FIELD2_int', 1004);
                riakObj.addToIndex('FIELD2_int', 1007);
                client.storeValue({ value: riakObj }, function (err, rslt) {
                    store_cb(err, rslt, async_cb);
                });
            }
        ];
        async.parallel(storeFuncs, function (err, rslts) {
            if (err) {
                throw new Error(err);
            }
            invalid_field_names();
            incorrect_data_type();
            querying_exact_match();
            querying_range();
            querying_range_with_terms();
            querying_pagination();
        });
    }

    function querying_pagination() {

        function do_query(continuation) {
            var binIdxCmdBuilder = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
                .withBucketType('indexes')
                .withBucket('tweets')
                .withIndexName('hashtags_bin')
                .withRange('ri', 'ru')
                .withMaxResults(5)
                .withCallback(pagination_cb);

            if (continuation) {
                binIdxCmdBuilder.withContinuation(continuation);
            }

            client.execute(binIdxCmdBuilder.build());
        }

        var query_keys = [];
        function pagination_cb(err, rslt) {
            if (err) {
                logger.error("[DevUsing2i] query_cb err: '%s'", err);
                return;
            }

            if (rslt.done) {
                query_keys.forEach(function (key) {
                    logger.info("[DevUsing2i] 2i query key: '%s'", key);
                });
                query_keys = [];

                if (rslt.continuation) {
                    do_query(rslt.continuation);
                }
            }

            if (rslt.values.length > 0) {
                Array.prototype.push.apply(query_keys,
                    rslt.values.map(function (value) {
                        return value.objectKey;
                    }));
            }
        }

        do_query();
    }

    function querying_exact_match() {
        var binIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('people')
            .withIndexName('field1_bin')
            .withIndexKey('val1')
            .withCallback(query_cb)
            .build();
        client.execute(binIdxCmd);

        var intIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('people')
            .withIndexName('field2_int')
            .withIndexKey(1001)
            .withCallback(query_cb)
            .build();
        client.execute(intIdxCmd);
    }

    function querying_range_with_terms() {
        var binIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('tweets')
            .withIndexName('hashtags_bin')
            .withRange('rock', 'rocl')
            .withReturnKeyAndIndex(true)
            .withCallback(query_cb)
            .build();
        client.execute(binIdxCmd);
    }   

    function querying_range() {
        var binIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('people')
            .withIndexName('field1_bin')
            .withRange('val2', 'val4')
            .withCallback(query_cb)
            .build();
        client.execute(binIdxCmd);

        var intIdxCmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('people')
            .withIndexName('field2_int')
            .withRange(1002, 1004)
            .withCallback(query_cb)
            .build();
        client.execute(intIdxCmd);
    }

    function invalid_field_names() {
        var cmd = new Riak.Commands.KV.SecondaryIndexQuery.Builder()
            .withBucketType('indexes')
            .withBucket('people')
            .withIndexName('field2_foo')
            .withIndexKey('jsmith123')
            .withCallback(query_cb)
            .build();
        client.execute(cmd);
    }

    function incorrect_data_type() {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setBucketType('indexes');
        riakObj.setBucket('people');
        riakObj.setKey('larry');
        riakObj.addToIndex('field2_int', 'bar');
        try {
            client.storeValue({ value: riakObj }, function (err, rslt) {
                logger.error("[DevUsing2i] incorrect_data_type err: '%s'", err);
            });
        } catch (e) {
            logger.error("[DevUsing2i] incorrect_data_type err: '%s'", e);
        }
    }

    var query_keys = [];
    function query_cb(err, rslt) {
        if (err) {
            logger.error("[DevUsing2i] query_cb err: '%s'", err);
            return;
        }

        if (rslt.done) {
            query_keys.forEach(function (key) {
                logger.info("[DevUsing2i] 2i query key: '%s'", key);
            });
            query_keys = [];
        }

        if (rslt.values.length > 0) {
            Array.prototype.push.apply(query_keys,
                rslt.values.map(function (value) {
                    return value.objectKey;
                }));
        }
    }
}

module.exports = DevUsing2i;

