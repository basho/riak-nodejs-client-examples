'use strict';

var async = require('async');
var logger = require('winston');
var util = require('util');
var Riak = require('basho-riak-client');

var c = null;
var intervals = null;

function stopClient() {
    if (intervals) {
        intervals.forEach(function (i) {
            if (i) {
                clearInterval(i);
            }
        });
    }
    if (c) {
        c.stop(function (err) {
            if (err) {
                logger.error('[GitHubIssue137] err: %s', err);
            }
        });
    }
}

/*
var nodes = [
    'riak-test:10117',
    'riak-test:10127',
    'riak-test:10137',
    'riak-test:10147',
    'riak-test:10157'
];
var nodes = [
    'riak-test:10017',
    'riak-test:10027',
    'riak-test:10037',
    'riak-test:10047',
    'riak-test:10057'
];
*/
var nodeAddresses = [
    'riak-test:10117',
];

var nodeBuilder = new Riak.Node.Builder();
var nodeTemplate = nodeBuilder.withExternalLoadBalancer(true);
var nodes = Riak.Node.buildNodes(nodeAddresses, nodeTemplate);

var cb = new Riak.Cluster.Builder();
cb.withRiakNodes(nodes);
var cluster = cb.build();

c = new Riak.Client(cluster, function (err, client) {
    if (err) {
        logger.error('[GitHubIssue137] err: %s', err);
    }

    var batch_size = 64;
    var finalCount = Math.pow(2, 20);

    var storeIntervalMs = 250;
    var storeValueInterval = null;
    var fetchIntervalMs = 250;
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
    }

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
    }

    var fetchCount = 0;
    function makeFetchValueFunc() {
        return function(async_cb) {
            var k = Math.floor(Math.random() * storeCount) + 1;
            // Do fetch of random key
            var o = {
                bucketType: 'default',
                bucket: 'gh-137',
                key: k.toString()
            };
            var tries = 0;
            var fv = function(acb) {
                // NB: this has an interesting effect
                // o.r = 1;
                client.fetchValue(o, function (err, rslt) {
                    if (!rslt) {
                        var rslt_err = new Error('[GitHubIssue137] no result for fetch of key: ' + k.toString());
                        if (err) {
                            rslt_err = err;
                        }
                        acb(rslt_err, null);
                        return;
                    }

                    if (rslt.isNotFound) {
                        tries++;
                        if (tries > 3) {
                            var retry_err = util.format('[GitHubIssue137] key not found after 3 tries:', k);
                            acb(retry_err, null);
                        } else {
                            setTimeout(fv.bind(this, acb), fetchIntervalMs * tries);
                        }
                        return;
                    }

                    fetchCount++;
                    acb(err, rslt);
                });
            };
            fv(async_cb);
        };
    }

    function fetchValues() {
        if (fetchCount > finalCount) {
            logger.info('[GitHubIssue137] all values fetched!');
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
    }

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

    intervals = [ storeValueInterval, fetchValueInterval ];

    client.ping(function (err, rslt) {
        if (rslt) {
        } else {
            stopClient();
        }
    });
});

process.on('uncaughtException', function(err) {
    logger.error('[GitHubIssue137] unhandled exception:', err);
    logger.error(err.stack);
    process.exit(1);
});

process.on('SIGINT', function() {
    logger.info('[GitHubIssue137] EXITING');
    stopClient();
});
