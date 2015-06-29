'use strict';

var logger = require('winston');
var Riak = require('basho-riak-client');

function GitHubIssue77(done) {

    var nodes = [];
    var addrs = [ [ 'riak-test', 10017 ], [ 'riak-test', 10027 ], [ 'riak-test', 10037 ], [ 'riak-test', 10047 ] ];
    addrs.forEach(function (a) {
        var host = a[0];
        var port = a[1];
        var node = new Riak.Node({ remoteAddress: host, remotePort: port, cork: false  });
        nodes.push(node);
    });
    var cluster = new Riak.Cluster({ nodes: nodes });
    var client = new Riak.Client(cluster);
    client.ping(function (err, rslt) {
        logger.info('[GitHubIssue77] rslt: %s', rslt);
        done();
    });
}

module.exports = GitHubIssue77;
