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

    var default_options = {
        bucketType: 'counters',
        bucket: 'counter',
        key: 'traffic_tickets'
    };

    create_or_update_counter(default_options, 1, function(opt1) {
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

