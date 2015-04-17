'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/taste-of-riak/nodejs/
 */

var assert = require('assert');
var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function TasteOfRiakIntroduction(done) {
    var client = config.createClient();

    client.ping(function (err, rslt) {
        if (err) {
            throw new Error(err);
        } else {
            // On success, ping returns true
            assert(rslt === true);
            logger.info('[TasteOfRiakIntro] ping is successful');
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
                logger.info('[TasteOfRiakIntro] people stored in Riak');
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
                    logger.info("[TasteOfRiakIntro] I found %s in 'contributors'", bashoman.emailAddress);

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
                logger.info("[TasteOfRiakIntro] updated bashoman first name: %s", updated.firstName);
                delete_example();
            }
        });
    }

    function delete_example() {
        client.deleteValue({ bucket: 'contributors', key: 'johndoe@gmail.com' }, function (err, rslt) {
            if (err) {
                throw new Error(err);
            } else {
                logger.info('[TasteOfRiakIntro] john doe deleted from Riak');
            }
            done(err, rslt);
        });
    }
}

module.exports = TasteOfRiakIntroduction;

