'use strict';

var Riak = require('basho-riak-client');

// Note: this is for a connection to a devrel running on
// host 'riak-test'. You may wish to use the following instead if
// you are running a one-node cluster installed from official
// installation packages:
//
// var riakNodes = [ 'localhost:8087' ];
// var riakNodes = [ 'riak-test:10017', 'riak-test:10027', 'riak-test:10037', 'riak-test:10047' ];
var riakNodes = [ 'riak-test:10017' ];

function Config() { }

var createClient = function (cb) {
    return new Riak.Client(riakNodes, cb);
};

module.exports = Config;
module.exports.createClient = createClient;
module.exports.riakNodes = riakNodes;
