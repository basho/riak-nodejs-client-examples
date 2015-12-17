'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/updates/
 */

var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingUpdates(done) {
    var client = config.createClient(function (err, c) {
        if (err) {
            logger.error('[DevUsingUpdates] err: %s', err);
        }
        put_coach();
    });

    function put_coach() {
        var riakObj = new Riak.Commands.KV.RiakObject();
        riakObj.setContentType('text/plain');
        riakObj.setBucketType('siblings');
        riakObj.setBucket('coaches');
        riakObj.setKey('seahawks');
        riakObj.setValue('Pete Carroll');
        client.storeValue({ value: riakObj }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            } else {
                logger.info('[DevUsingUpdates] Stored Pete Carroll');
            }
            update_coach('seahawks', 'Bob Barker');
        });
    }

    function update_coach(team, newCoach) {
        client.fetchValue({
            bucketType: 'siblings', bucket: 'coaches', key: team
        }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            var riakObj = rslt.values.shift();
            riakObj.setValue(newCoach);
            client.storeValue({ value: riakObj }, function (err, rslt) {
                if (err) {
                    throw new Error(err);
                }
                client.stop(function (err) {
                    if (err) {
                        logger.error('[DevUsingUpdates] err: %s', err);
                    }
                    done(err, rslt);
                });
            });
        });
    }
}

module.exports = DevUsingUpdates;

