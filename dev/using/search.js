'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/using/data-types/
 */

var assert = require('assert');
var logger = require('winston');
var Riak = require('basho-riak-client');

function DevUsingSearch(done) {
    var client = config.createClient();

    create_famous_index();

    function create_famous_index() {
        var storeIndex_cb = function (err, rslt) {
            if (err) {
                throw new Error(err);
            }
            if (!rslt) {
                logger.info("[DevUsingSearch] rslt === false");
            }
            done();
        };

        var store = new Riak.Commands.YZ.StoreIndex.Builder()
            .withIndexName("famous")
            .withCallback(storeIndex_cb)
            .build();

        client.execute(store);
    }
}


module.exports = DevUsingSearch;

