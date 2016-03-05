'use strict';

var logger = require('winston');
var Riak = require('basho-riak-client');

function GitHubIssue137(done) {

	process.on('uncaughtException', function(err) {
		logger.error('[GitHubIssue137] unhandled exception:', err);
		logger.error(err.stack);
        process.exit(1);
	});

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

        var finalCount = Math.pow(2, 20);

        var storeIntervalMs = 16;
        var storeValueInterval = null;
        var fetchIntervalMs = 16;

        var key = 1;
        var storeCount = 0;
        function storeValues() {
            if (storeCount > finalCount) {
                logger.info('[GitHubIssue137] all values stored!');
                if (storeValueInterval) {
                    clearInterval(storeValueInterval);
                }
                return;
            }
            var obj = new Riak.Commands.KV.RiakObject();
            obj.setContentType('text/plain');
            obj.setValue('github-issue-137');
            var o = {
                bucketType: 'default',
                bucket: 'gh-137',
                key: key.toString(),
                value: obj
            };
            key++;
            client.storeValue(o, function (err, rslt) {
                storeCount++;
                if (err) {
                    logger.info('[GitHubIssue137] storeValue err:', err);
                }
            });
        };

        var fetchCount = 0;
        function fetchValues() {
            fetchCount++;
            if (fetchCount > finalCount) {
                stopClient();
            }
            if (fetchCount % 1024 === 0) {
                logger.info('[GitHubIssue137] fetchCount:', fetchCount);
            }

            var k = Math.floor(Math.random() * storeCount) + 1;
            // Do fetch of random key
            var o = {
                bucketType: 'default',
                bucket: 'gh-137',
                key: k.toString()
            };
            client.fetchValue(o, function (err, rslt) {
                if (err) {
                    logger.error('[GitHubIssue137] fv err: %s', err);
                }

                if (!rslt) {
                    logger.error('[GitHubIssue137] no fv rslt!');
                    return
                }
                if (rslt.isNotFound) {
                    logger.error('[GitHubIssue137] key not found:', k);
                }
            });
        };

        logger.debug('[GitHubIssue137] starting store/fetch');
        storeValueInterval = setInterval(storeValues, storeIntervalMs);
        var intervals = [
            storeValueInterval,
            setInterval(fetchValues, fetchIntervalMs),
        ];

        function stopClient() {
            intervals.forEach(function (i) {
                clearInterval(i);
            });
            client.stop(function (err) {
                if (err) {
                    logger.error('[GitHubIssue137] err: %s', err);
                }
                done();
            });
        };

        client.ping(function (err, rslt) {
            if (rslt) {
            } else {
                stopClient();
            }
        });
    });
}

module.exports = GitHubIssue137;
