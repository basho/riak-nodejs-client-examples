'use strict';

var config = require('../../config');
var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function GitHubIssue52(done) {
    config.createClient(function (err, client) {
        if (err) {
            logger.error('[GitHubIssue52] err: %s', err);
        }
        client.fetchValue({
                notFoundOk: true,
                bucketType: 'github', bucket: 'issues', key: '52'
            }, function (err, rslt) {
                logger.debug("fetch err: %s", JSON.stringify(err));
                logger.debug("fetch rslt: %s", JSON.stringify(rslt));

                if (err) {
                    throw new Error(err);
                }

                var value = {
                    abc: 1,
                    def: 'hello world'
                };

                var userMeta = [
                    {
                        key: 'a',
                        value: 'b'
                    },
                    {
                        key: 'c',
                        value: 'd'
                    }
                ];

                var riakObj;
                if (rslt.isNotFound) {
                    var riakObj = new Riak.Commands.KV.RiakObject();
                    riakObj.setBucketType('github')
                    riakObj.setBucket('issues')
                    riakObj.setKey('52')
                    riakObj.setContentType('application/json');
                    riakObj.setValue(value);
                    riakObj.setUserMeta(userMeta);
                } else {
                    riakObj = rslt.values.shift();
                    var updatedValue = {
                        abc: 1,
                        def: 'hello world updated at: ' + Date.now()
                    };
                    riakObj.setValue(updatedValue);
                }

                client.storeValue({ returnBody: true, value: riakObj }, function (err, rslt) {
                    logger.debug("store err: %s", JSON.stringify(err));
                    logger.debug("store rslt: %s", JSON.stringify(rslt));
                    if (err) {
                        throw new Error(err);
                    }
                    client.stop(function (err) {
                        if (err) {
                            logger.error('[GitHubIssue52] err: %s', err);
                        }
                        done();
                    });
                });
            }
        );
    });
}

module.exports = GitHubIssue52;
