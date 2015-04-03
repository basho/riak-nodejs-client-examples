'use strict';

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/taste-of-riak/nodejs/
 */

var assert = require('assert');
var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function TasteOfRiakIntroduction() {
    var client = new Riak.Client([
        // Note: this is for a connection to a devrel running on
        // host 'riak-test'. You may wish to use this instead:
        // 'localhost:8087'
        'riak-test:10017',
        // Can comment out the following nodes if only using
        // a one-node cluster
        'riak-test:10027',
        'riak-test:10037',
        'riak-test:10047'
    ]);

    client.ping(function (err, rslt) {
        if (err) {
            throw new Error(err);
        } else {
            // On success, ping returns true
            assert(rslt === true);
            logger.info('ping is successful');
            save_people();
        }
    });

    function save_people() {
        var people = [
            {
                emailAddress: "bashoman@basho.com",
                firstName: "Basho",
                lastName: "Man"
            },
            {
                emailAddress: "johndoe@gmail.com",
                firstName: "John",
                lastName: "Doe"
            }
        ];

        var storeFuncs = [];
        people.forEach(function (person) {
            // Create functions to execute in parallel to store people
            storeFuncs.push(function (async_cb) {
                client.storeValue({
                        bucket: 'contributors',
                        key: person.emailAddress,
                        value: person
                    },
                    function(err, rslt) {
                        async_cb(err, rslt);
                    }
                );
            });
        });

        async.parallel(storeFuncs, function (err, rslts) {
            if (err) {
                throw new Error(err);
            } else {
                logger.info('people stored in Riak');
                read_person();
            }
        });
    }

    function read_person() {
        client.fetchValue({ bucket: 'contributors', key: 'bashoman@basho.com', convertToJs: true },
            function (err, rslt) {
                if (err) {
                    throw new Error(err);
                } else {
                    var riakObj = rslt.values.shift();
                    var bashoman = riakObj.value;
                    logger.info("I found %s in 'contributors'", bashoman.emailAddress);

                    update_person(riakObj);
                }
            }
        );
    }

    function update_person(riakObj) {
        var bashoman = riakObj.value;
        bashoman.firstName = "Riak";
        riakObj.setValue(bashoman);

        client.storeValue({ value: riakObj, returnBody: true, convertToJs: true }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            } else {
                var updated = rslt.values.shift().value;
                logger.info("updated bashoman first name: %s", updated.firstName);
                delete_example();
            }
        });
    }

    function delete_example() {
        client.deleteValue({ bucket: 'contributors', key: 'johndoe@gmail.com' }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            } else {
                logger.info('john doe deleted from Riak');
                client_shutdown();
            }
        });
    }

    function client_shutdown() {
        client.shutdown(function (state) {
            if (state === Riak.Cluster.State.SHUTDOWN) {
                process.exit();
            }
        });
    }
}

module.exports = TasteOfRiakIntroduction;
