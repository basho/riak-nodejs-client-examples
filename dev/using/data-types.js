'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/data-types/
 */

var assert = require('assert');
var async = require('async');
var clone = require('clone');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingDataTypes() {
    var client = config.createClient();

    counter_examples();

    set_examples();

    map_examples();

    function set_examples() {
        var options = {
            bucketType: 'sets',
            bucket: 'travel',
            key: 'cities'
        };
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

    function counter_examples() {
        var options = {
            bucketType: 'counters',
            bucket: 'counter',
            key: 'traffic_tickets'
        };
        create_or_update_counter(options, 1, function(opt1) {
            display_counter(opt1, function(opt2) {
                create_or_update_counter(opt2, -1, function(opt3) {
                    display_counter(opt3);
                });
            });
        });
    }

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

    function map_examples() {

        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        mapOp.setRegister('first_name', new Buffer('Ahmed'));
        mapOp.setRegister('phone_number', new Buffer('5551234567'));

        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info',
            op: mapOp
        };

        client.updateMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            
            flags_within_maps();
        });
    }

    function print_map(next_func) {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            logger.info("[DevUsingDataTypes] fetched map: %s", JSON.stringify(rslt));

            if (next_func) {
                next_func();
            }
        });
    }

    function flags_within_maps() {
        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        mapOp.setFlag('enterprise_customer', false);

        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info',
            op: mapOp
        };

        client.updateMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            print_map(counters_within_maps);
        });
    }

    function counters_within_maps() {
        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        mapOp.incrementCounter('page_visits', 1);

        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info',
            op: mapOp
        };

        client.updateMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            print_map(sets_within_maps);
        });
    }

    function sets_within_maps() {
        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        mapOp.addToSet('interests', 'robots');
        mapOp.addToSet('interests', 'opera');
        mapOp.addToSet('interests', 'motorcycles');

        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info',
            op: mapOp
        };

        client.updateMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            verify_interests_set();
        });
    }

    function verify_interests_set() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            assert(rslt.map.sets['interests'].indexOf('robots') !== -1);
        });
    }
}

module.exports = DevUsingDataTypes;

