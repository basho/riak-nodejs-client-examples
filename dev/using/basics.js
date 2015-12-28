'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/basics/
 */

var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingBasics(done) {
    var client = config.createClient(function (err, c) {
        var funcs = [
            read_rufus,
            put_oscar_wilde,
            causal_context,
            store_dodge_viper,
            riak_generated_key,
            fetch_bucket_props
        ];
        async.parallel(funcs, function (err, rslt) {
            if (err) {
                logger.error("[DevUsingBasics] err: %s", err);
            }
            c.stop(function () {
                done();
            });
        });
    });

    var rufus_rvalue = null;

    function put_rufus(async_cb) {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setValue('WOOF!');
        client.storeValue({
            bucketType: 'animals', bucket: 'dogs', key: 'rufus',
            value: riakObj
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            rufus_rvalue = 3;
            read_rufus(async_cb);
        });
    }

    function put_oscar_wilde(async_cb) {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setValue('I have nothing to declare but my genius');
        client.storeValue({
            bucketType: 'quotes', bucket: 'oscar_wilde', key: 'genius',
            value: riakObj
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            async_cb();
        });
    }

    function read_rufus(async_cb) {
        var fetchOptions = {
            bucketType: 'animals', bucket: 'dogs', key: 'rufus'
        };
        if (rufus_rvalue) {
            fetchOptions.r = rufus_rvalue;
        }
        client.fetchValue(fetchOptions, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (rslt.isNotFound) {
                logger.info("[DevUsingBasics] read_rufus: isNotFound %s", rslt.isNotFound);
                put_rufus(async_cb);
            } else {
                var riakObj = rslt.values.shift();
                var rufusValue = riakObj.value.toString("utf8");
                logger.info("[DevUsingBasics] read_rufus: %s", rufusValue);
                async_cb();
            }
        });
    }

    function causal_context(async_cb) {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setValue('Washington Generals');

        var options = {
            bucketType: 'sports', bucket: 'nba', key: 'champion',
            value: riakObj
        };
        client.storeValue(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            delete options.value;
            client.fetchValue(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                var fetchedObj = rslt.values.shift();
                logger.info("[DevUsingBasics] vclock: %s", fetchedObj.getVClock().toString('base64'));
                fetchedObj.setValue('Harlem Globetrotters');
                options.value = fetchedObj;
                options.returnBody = true;
                client.storeValue(options, function (err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }
                    var updatedObj = rslt.values.shift();
                    logger.info("[DevUsingBasics] champion: %s", updatedObj.value.toString('utf8'));
                    async_cb();
                });
            });
        });
    }

    function store_dodge_viper(async_cb) {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setValue('vroom');

        var options = {
            bucketType: 'cars', bucket: 'dodge', key: 'viper',
            w: 3, returnBody: true, value: riakObj
        };
        client.storeValue(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            var riakObj = rslt.values.shift();
            var viper = riakObj.value;
            logger.info("[DevUsingBasics] dodge viper: %s", viper.toString('utf8'));
            async_cb();
        });
    }

    function riak_generated_key(async_cb) {
        var user = {
            user: 'data'
        };
        var options = {
            bucketType: 'users', bucket: 'random_user_keys',
            returnBody: true, value: user
        };
        client.storeValue(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            var riakObj = rslt.values.shift();
            var generatedKey = riakObj.getKey();
            logger.info("[DevUsingBasics] Generated key: %s", generatedKey);

            options = {
                bucketType: 'users', bucket: 'random_user_keys',
                key: generatedKey
            };
            client.deleteValue(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                async_cb();
            });
        });
    }

    function fetch_bucket_props(async_cb) {
        client.fetchBucketProps({
            bucketType: 'n_val_of_5', bucket: 'any_bucket_name'
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("[DevUsingBasics] props: %s", JSON.stringify(rslt));
            async_cb();
        });
    }
}

module.exports = DevUsingBasics;
