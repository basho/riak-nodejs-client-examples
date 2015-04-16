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

function DevUsingDataTypes(done) {
    var client = config.createClient();

    counter_examples();

    set_examples();

    map_examples();

    done();

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
        mapOp.setRegister('first_name', 'Ahmed');
        mapOp.setRegister('phone_number', '5551234567');

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

            counters_within_maps();
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

            sets_within_maps();
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

            /* jshint sub:true */
            assert(rslt.map.sets['interests'].indexOf('robots') !== -1);
            /* jshint sub:false */

            update_interests_set();
        });
    }

    function update_interests_set() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
            mapOp.removeFromSet('interests', 'opera');
            mapOp.addToSet('interests', 'indie pop');

            options.context = rslt.context;
            options.op = mapOp;

            client.updateMap(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                maps_within_maps();
            });
        });
    }

    function maps_within_maps() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        mapOp.map('annika_info')
            .setRegister('first_name', 'Annika')
            .setRegister('last_name', 'Weiss')
            .setRegister('phone_number', '5559876543');

        options.op = mapOp;

        client.updateMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            examine_map_register();
        });
    }

    function examine_map_register() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var first_name_register =
                rslt.map.maps.annika_info.registers.first_name;

            logger.info("[DevUsingDataTypes] Annika's first name is '%s'",
                first_name_register.toString('utf8'));

            remove_register_from_inner_map(options, rslt);
        });
    }

    function remove_register_from_inner_map() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
            mapOp.map('annika_info').removeRegister('first_name');

            var options = {
                bucketType: 'maps',
                bucket: 'customers',
                key: 'ahmed_info',
                op: mapOp,
                context: rslt.context,
            };

            client.updateMap(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                subscribe_annika_to_plans();
            });
        });
    }

    function subscribe_annika_to_plans() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
            var annika_map = mapOp.map('annika_info');
            annika_map.setFlag('enterprise_plan', false);
            annika_map.setFlag('family_plan', false);
            annika_map.setFlag('free_plan', true);

            var options = {
                bucketType: 'maps',
                bucket: 'customers',
                key: 'ahmed_info',
                op: mapOp,
                context: rslt.context,
            };

            client.updateMap(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                retrieve_annika_plans();
            });
        });
    }

    function retrieve_annika_plans() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };

        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var enterprisePlan =
                rslt.map.maps.annika_info.flags.enterprise_plan;

            logger.info("[DevUsingDataTypes] Annika enterprise plan '%s'",
                enterprisePlan);

            add_counter_to_inner_map();
        });
    }

    function add_counter_to_inner_map() {
        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        mapOp.map('annika_info').incrementCounter('widget_purchases', 1);

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

            store_annikas_interest();
        });
    }
    
    function store_annikas_interest() {
        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        var annika_map = mapOp.map('annika_info');
        annika_map.addToSet('interests', 'tango dancing');

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

            remove_annikas_interest();
        });
    }

    function remove_annikas_interest() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };
        
        client.fetchMap(options, function (err, rslt) {
            var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
            var annika_map = mapOp.map('annika_info');
            annika_map.removeFromSet('interests', 'tango dancing');

            options = {
                bucketType: 'maps',
                bucket: 'customers',
                key: 'ahmed_info',
                op: mapOp,
                context: rslt.context
            };

            client.updateMap(options, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                store_annikas_purchase_info();
            });
        });
    }

    function store_annikas_purchase_info() {
        var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
        var annika_map = mapOp.map('annika_info');
        var annika_purchase_map = annika_map.map('purchase');
        annika_purchase_map.setFlag('first_purchase', true);
        annika_purchase_map.setRegister('amount', '1271');
        annika_purchase_map.addToSet('items', 'large widget');

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

            fetch_and_display_context();
        });
    }

    function fetch_and_display_context() {
        var options = {
            bucketType: 'maps',
            bucket: 'customers',
            key: 'ahmed_info'
        };
        
        client.fetchMap(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            logger.info("[DevUsingDataTypes] context: '%s'",
                rslt.context.toString('base64'));
        });
    }
}

module.exports = DevUsingDataTypes;

