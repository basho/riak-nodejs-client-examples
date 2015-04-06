'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/basics/
 */

var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingBasics() {
    var client = config.createClient();

    read_rufus();

    put_oscar_wilde();

    causal_context();

    store_dodge_viper();

    riak_generated_key();

    fetch_bucket_props();

    function put_rufus() {
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
            read_rufus(3);
        });
    }

    function put_oscar_wilde() {
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
        });
    }

    function read_rufus(rvalue) {
        var fetchOptions = {
            bucketType: 'animals', bucket: 'dogs', key: 'rufus'
        };
        if (rvalue) {
            fetchOptions.r = rvalue;
        }
        client.fetchValue(fetchOptions, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (rslt.isNotFound) {
                logger.info("read_rufus: isNotFound %s", rslt.isNotFound);
                put_rufus();
            } else {
                var riakObj = rslt.values.shift();
                var rufusValue = riakObj.value.toString("utf8");
                logger.info("read_rufus: %s", rufusValue);
            }
        });
    }

    function causal_context() {
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
                logger.info("vclock: %s", fetchedObj.getVClock().toString('base64'));
                fetchedObj.setValue('Harlem Globetrotters');
                options.value = fetchedObj;
                options.returnBody = true;
                client.storeValue(options, function (err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }
                    var updatedObj = rslt.values.shift();
                    logger.info("champion: %s", updatedObj.value.toString('utf8'));
                });
            });
        });
    }

    function store_dodge_viper() {
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
            logger.info("dodge viper: %s", viper.toString('utf8'));
        });
    }

    function riak_generated_key() {
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
            logger.info("Generated key: %s", generatedKey);

            options = {
                bucketType: 'users', bucket: 'random_user_keys',
                key: generatedKey
            };
            client.deleteValue(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
            });
        });
    }

    function fetch_bucket_props() {
        client.fetchBucketProps({
            bucketType: 'n_val_of_5', bucket: 'any_bucket_name'
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            logger.info("props: %s", JSON.stringify(rslt));
        });
    }
}

module.exports = DevUsingBasics;

