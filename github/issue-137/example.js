'use strict';

var async = require('async');
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
        'riak-test:10057' //,
        // 'riak-test:10067',
        // 'riak-test:10077',
        // 'riak-test:10087'
    ];

    var c = new Riak.Client(nodes, function (err, client) {
        if (err) {
            logger.error('[GitHubIssue137] err: %s', err);
        }

        var batch_size = 64;
        var finalCount = Math.pow(2, 20);

        var storeIntervalMs = 10;
        var storeValueInterval = null;
        var fetchIntervalMs = 10;
        var fetchValueInterval = null;

        var key = 1;
        var storeCount = 0;
        function makeStoreValueFunc() {
            return function(acb) {
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
                    acb(err, rslt);
                });
            };
        };

        function storeValues() {
            if (storeCount > finalCount) {
                logger.info('[GitHubIssue137] all values stored!');
                if (storeValueInterval) {
                    clearInterval(storeValueInterval);
                }
                return;
            }
            var storeFuncs = [];
            for (var i = 0; i < batch_size; i++) {
                storeFuncs.push(makeStoreValueFunc());
            }
            async.parallel(storeFuncs, function (err, rslts) {
                if (err) {
                    logger.error('[GitHubIssue137] async storeValue err:', err);
                }
                if (storeIntervalMs === 0) {
                    setImmediate(storeValues);
                }
            });
        };

        var fetchCount = 0;
        function makeFetchValueFunc() {
            return function(acb) {
                var k = Math.floor(Math.random() * storeCount) + 1;
                // Do fetch of random key
                var o = {
                    bucketType: 'default',
                    bucket: 'gh-137',
                    key: k.toString(),
                    r: 1
                };
                client.fetchValue(o, function (err, rslt) {
                    fetchCount++;
                    if (rslt && rslt.isNotFound) {
                        logger.error('[GitHubIssue137] key not found:', k);
                    }
                    acb(err, rslt);
                });
            };
        };

        function fetchValues() {
            if (fetchCount > finalCount) {
                stopClient();
            }
            if (fetchCount % 1024 === 0) {
                logger.info('[GitHubIssue137] fetchCount:', fetchCount);
            }

            var fetchFuncs = [];
            for (var i = 0; i < batch_size; i++) {
                fetchFuncs.push(makeFetchValueFunc());
            }
            async.parallel(fetchFuncs, function (err, rslts) {
                if (err) {
                    logger.error('[GitHubIssue137] async fetchValue err:', err);
                }
                if (fetchIntervalMs === 0) {
                    setImmediate(fetchValues);
                }
            });
        };

        logger.debug('[GitHubIssue137] starting store/fetch');

        if (storeIntervalMs === 0) {
            setImmediate(storeValues);
        } else {
            storeValueInterval = setInterval(storeValues, storeIntervalMs);
        }

        if (fetchIntervalMs === 0) {
            setImmediate(fetchValues);
        } else {
            fetchValueInterval = setInterval(fetchValues, fetchIntervalMs);
        }

        var intervals = [ storeValueInterval, fetchValueInterval ];

        function stopClient() {
            intervals.forEach(function (i) {
                if (i) {
                    clearInterval(i);
                }
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
