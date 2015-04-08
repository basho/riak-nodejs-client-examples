'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/data-types/
 */

var async = require('async');
var clone = require('clone');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingDataTypes() {
    var client = config.createClient();

    var counter_options = {
        bucketType: 'counters',
        bucket: 'counter',
        key: 'traffic_tickets'
    };

    var set_options = {
        bucketType: 'sets',
        bucket: 'travel',
        key: 'cities'
    };

    demonstrate_empty_set(set_options);

    function demonstrate_empty_set(options) {
        client.fetchSet(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            if (rslt.notFound) {
                logger.info("[DevUsingDataTypes] set 'cities' is not found!");
            }
            add_to_cities_set();
        });
    }

    function add_to_cities_set() {
        var options = {
            bucketType: 'sets',
            bucket: 'travel',
            key: 'cities'
        };
        var cmd = new Riak.Commands.CRDT.UpdateSet.Builder()
            .withBucketType(options.bucketType)
            .withBucket(options.bucket)
            .withKey(options.key)
            .withAdditions(['Toronto', 'Montreal'])
            .withCallback(
                function (err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }

                    change_cities_set();
                }
            )
            .build();
        client.execute(cmd);
    }

    function change_cities_set() {
        var options = {
            bucketType: 'sets',
            bucket: 'travel',
            key: 'cities'
        };
        client.fetchSet(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var update_opts = clone(options);
            update_opts.context = rslt.context;
            update_opts.additions = ['Hamilton', 'Ottawa'];
            update_opts.removals = ['Montreal', 'Ottawa'];

            client.updateSet(update_opts, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                client.fetchSet(options, function(err, rslt) {
                    if (err) {
                        throw new Error(err);
                    }

                    logger.info("[DevUsingDataTypes] cities set values: '%s'",
                        rslt.values.join(', '));
                });
            });
        });
    }

    create_or_update_counter(counter_options, 1, function(opt1) {
        display_counter(opt1, function(opt2) {
            create_or_update_counter(opt2, -1, function(opt3) {
                display_counter(opt3);
            });
        });
    });

    function create_or_update_counter(options, amount, next_func) {
        var my_opts = clone(options);
        my_opts.increment = amount;
        client.updateCounter(my_opts,
            function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                if (next_func) {
                    next_func(options);
                }
            }
        );
    }

    function display_counter(options, next_func) {
        client.fetchCounter(options,
            function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                if (rslt.notFound) {
                    logger.error("[DevUsingDataTypes] bt: %s, b: %s, k: %s, counter: NOT FOUND",
                        options.bucketType, options.bucket, options.key);
                } else {
                    logger.info("[DevUsingDataTypes] bt: %s, b: %s, k: %s, counter: %d",
                        options.bucketType, options.bucket, options.key, rslt.counterValue);
                }

                if (next_func) {
                    next_func(options);
                }
            }
        );
    }
}

module.exports = DevUsingDataTypes;

