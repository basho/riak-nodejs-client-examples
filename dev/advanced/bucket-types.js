'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/advanced/bucket-types/
 */

var assert = require('assert');
var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevAdvancedBucketTypes() {
    var client = config.createClient();

    default_type_example();

    client_usage_example();

    memes_example();

    function default_type_example() {
        var obj1 = new Riak.Commands.KV.RiakObject();
        obj1.setContentType('text/plain');
        obj1.setBucketType('default');
        obj1.setBucket('my_bucket');
        obj1.setKey('my_key');
        obj1.setValue('value');
        client.storeValue({ value: obj1 }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            client.fetchValue({
                bucketType: 'default', bucket: 'my_bucket', key: 'my_key'
            }, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                var obj2 = rslt.values.shift();
                assert(obj1.value == obj2.value);
                logger.info("[DevAdvancedBucketTypes] obj1 val: '%s', obj2 val: '%s'",
                    obj1.value.toString('utf8'), obj2.value.toString('utf8'));
            });
        });
    }

    function client_usage_example() {
        var obj = { name: 'Bob' };
        client.storeValue({
            bucketType: 'no_siblings', bucket: 'sensitive_user_data', key: 'user19735',
            value: obj
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
        });
    }

    function memes_example() {
        var obj = new Riak.Commands.KV.RiakObject();
        obj.setContentType('text/plain');
        obj.setBucketType('no_siblings');
        obj.setBucket('old_memes');
        obj.setKey('all_your_base');
        obj.setValue('all your base are belong to us');
        client.storeValue({ value: obj }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
        });
    }
}

module.exports = DevAdvancedBucketTypes;

