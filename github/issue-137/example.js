'use strict';

var logger = require('winston');
var Riak = require('basho-riak-client');

function GitHubIssue137(done) {
    var nodes = [
        'riak-test:10017',
        'riak-test:10027',
        'riak-test:10037',
        'riak-test:10047',
    ];

    var c = new Riak.Client(nodes, function (err, client) {
        if (err) {
            logger.error('[GitHubIssue137] err: %s', err);
        }

        var keys = [];
        function storeValues() {
            var obj = new Riak.Commands.KV.RiakObject();
            obj.setContentType('text/plain');
            obj.setValue('github-issue-137');
            var o = {
                bucketType: 'default',
                bucket: 'gh-137',
                value: obj
            };
            client.storeValue(o, function (err, rslt) {
                keys.push(rslt.generatedKey);
                setTimeout(storeValues, 5);
            });
        };

        var finalCount = Math.pow(2, 20);
        var fetchCount = 0;
        function fetchValues() {
            fetchCount++;
            if (fetchCount > finalCount) {
                stopClient();
            }
            if (fetchCount % 1024 === 0) {
                logger.info('[GitHubIssue137] fetchCount: %d', fetchCount);
            }

            var k = 'frazzle';
            if (keys.length > 0) {
                k = keys.shift();
            }

            // Do fetch of random key
            var o = {
                bucketType: 'default',
                bucket: 'gh-137',
                key: k
            };
            client.fetchValue(o, function (err, rslt) {
                if (err) {
                    logger.error('[GitHubIssue137] fv err: %s', err);
                }
                setTimeout(fetchValues, 5);
            });
        };

        function stopClient() {
            client.stop(function (err) {
                if (err) {
                    logger.error('[GitHubIssue137] err: %s', err);
                }
                done();
            });
        };

        client.ping(function (err, rslt) {
            if (rslt) {
                logger.debug('[GitHubIssue137] starting store/fetch');
                storeValues();
                fetchValues();
            } else {
                stopClient();
            }
        });
    });
}

module.exports = GitHubIssue137;
