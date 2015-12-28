'use strict';

var logger = require('winston');
var Riak = require('basho-riak-client');
var config = require('../../config');

function GitHubIssue77(done) {
    var nodes = [];
    config.riakNodes.forEach(function (n) {
        var a = n.split(':');
        var host = a[0];
        var port = a[1];
        var node = new Riak.Node({ remoteAddress: host, remotePort: port, cork: false  });
        nodes.push(node);
    });
    var cluster = new Riak.Cluster({ nodes: nodes });
    var c = new Riak.Client(cluster, function (err, client) {
        if (err) {
            logger.error('[GitHubIssue77] err: %s', err);
        }
        client.ping(function (err, rslt) {
            logger.info('[GitHubIssue77] rslt: %s', rslt);
            client.stop(function (err) {
                if (err) {
                    logger.error('[GitHubIssue77] err: %s', err);
                }
                done();
            });
        });
    });
}

module.exports = GitHubIssue77;
