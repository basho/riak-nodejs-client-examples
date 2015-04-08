'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/data-types/
 */

var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingDataTypes() {
    var client = config.createClient();

    create_counter();

    function create_counter() {
        client.updateCounter({
                bucketType: 'counters',
                bucket: 'counters',
                key: 'traffic_tickets',
                increment: 1
            }, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                display_counter('counters', 'counter', 'traffic_tickets');
            }
        );
    }

    function display_counter(bucketType, bucket, key) {
        client.fetchCounter({
                bucketType: 'counters',
                bucket: 'counters',
                key: 'traffic_tickets'
            }, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }

                if (rslt.notFound) {
                    logger.error("bt: %s, b: %s, k: %s, counter: NOT FOUND",
                        bucketType, bucket, key);
                } else {
                    logger.info("bt: %s, b: %s, k: %s, counter: %d",
                        bucketType, bucket, key, rslt.counterValue);
                }
            }
        );
    }
}

module.exports = DevUsingDataTypes;

