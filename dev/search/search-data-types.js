'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/search/search-data-types/
 */

var async = require('async');
var logger = require('winston');
var Riak = require('basho-riak-client');

function throwIfErr(err) {
    if (err) {
        throw new Error(err);
    }
}

function waitForSearch(nextFunc) {
    setTimeout(nextFunc, 1250);
}

function Init(done) {
    var client = config.createClient();

    var indexFuncs = [];
    ['scores', 'hobbies', 'customers'].forEach(function (idxName) {
        indexFuncs.push(function (async_cb) {
            var options = {
                schemaName: '_yz_default',
                indexName: idxName
            };
            client.storeIndex(options, function (err, rslt) {
                throwIfErr(err);
                async_cb();
            });
        });
    });

    async.parallel(indexFuncs, function (err, rslts) {
        throwIfErr(err);
        done();
    });
}

function DevSearchDataTypes(done) {
    var client = config.createClient();

    async.parallel([
            counterExample,
            setExample,
            mapExample
        ], function (err, rslts) {
            throwIfErr(err);
            done();
        }
    );

    function counterExample(counterExampleDone) {
        var funcs = [
            function (async_cb) {
                var options = {
                    bucketType: 'counters',
                    bucket: 'people',
                    key: 'christ_hitchens',
                    increment: 10
                };

                client.updateCounter(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            },
            function (async_cb) {
                var options = {
                    bucketType: 'counters',
                    bucket: 'people',
                    key: 'joan_rivers',
                    increment: 25
                };

                client.updateCounter(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            }
        ];

        async.parallel(funcs, function (err, rslts) {
            throwIfErr(err);

            waitForSearch(function () {
                function search_cb(err, rslt) {
                    logger.info("[DevSearchDataTypes] counter numFound: '%d', docs: '%s'",
                        rslt.numFound, JSON.stringify(rslt.docs));

                    var doc = rslt.docs[0];
                    /* jshint sub:true */
                    var key = doc['_yz_rk'];
                    var bucket = doc['_yz_rb'];
                    var bucketType = doc['_yz_rt'];
                    /* jshint sub:false */

                    counterExampleDone();
                }

                var searchCmd = new Riak.Commands.YZ.Search.Builder()
                    .withIndexName('scores')
                    .withQuery('counter:[20 TO *]')
                    .withCallback(search_cb)
                    .build();

                client.execute(searchCmd);
            });
        });
    }

    function setExample(setExampleDone) {
        var funcs = [
            function (async_cb) {
                var options = {
                    bucketType: 'sets',
                    bucket: 'people',
                    key: 'ditka',
                    additions: ['football', 'winning']
                };

                client.updateSet(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            },
            function (async_cb) {
                var options = {
                    bucketType: 'sets',
                    bucket: 'people',
                    key: 'dio',
                    additions: ['wailing', 'rocking', 'winning']
                };

                client.updateSet(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            }
        ];

        async.parallel(funcs, function (err, rslts) {
            throwIfErr(err);

            waitForSearch(function () {
                function search_cb(err, rslt) {
                    logger.info("[DevSearchDataTypes] sets numFound: '%d', docs: '%s'",
                        rslt.numFound, JSON.stringify(rslt.docs));

                    var doc = rslt.docs[0];
                    /* jshint sub:true */
                    var key = doc['_yz_rk'];
                    var bucket = doc['_yz_rb'];
                    var bucketType = doc['_yz_rt'];
                    /* jshint sub:false */

                    setExampleDone();
                }

                var searchCmd = new Riak.Commands.YZ.Search.Builder()
                    .withIndexName('hobbies')
                    .withQuery('set:football')
                    .withCallback(search_cb)
                    .build();

                client.execute(searchCmd);
            });
        });
    }

    function mapExample(mapExampleDone) {
        var funcs = [
            function (async_cb) {
                var options = {
                    bucketType: 'maps',
                    bucket: 'customers',
                    key: 'idris_elba'
                };

                var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
                mapOp.setRegister('first_name', 'Idris');
                mapOp.setRegister('last_name', 'Elba');
                mapOp.setFlag('enterprise_customer', false);
                mapOp.incrementCounter('page_visits', 10);
                mapOp.addToSet('interests', 'acting');
                mapOp.addToSet('interests', 'being Stringer Bell');

                options.op = mapOp;

                client.updateMap(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            },
            function (async_cb) {
                var options = {
                    bucketType: 'maps',
                    bucket: 'customers',
                    key: 'joan_jett'
                };

                var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
                mapOp.setRegister('first_name', 'Joan');
                mapOp.setRegister('last_name', 'Jett');
                mapOp.setFlag('enterprise_customer', false);
                mapOp.incrementCounter('page_visits', 25);
                mapOp.addToSet('interests', 'loving rock and roll');
                mapOp.addToSet('interests', 'being in the Blackhearts');

                options.op = mapOp;

                client.updateMap(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            }
        ];

        async.parallel(funcs, function (err, rslts) {
            throwIfErr(err);

            waitForSearch(function () {
                function search_cb(err, rslt) {
                    logger.info("[DevSearchDataTypes] maps numFound: '%d', docs: '%s'",
                        rslt.numFound, JSON.stringify(rslt.docs));

                    var doc = rslt.docs[0];
                    /* jshint sub:true */
                    var key = doc['_yz_rk'];
                    var bucket = doc['_yz_rb'];
                    var bucketType = doc['_yz_rt'];
                    /* jshint sub:false */
                }

                var searchFuncs = [
                    function (async_cb) {
                        var searchCmd = new Riak.Commands.YZ.Search.Builder()
                            .withIndexName('customers')
                            .withQuery('page_visits_counter:[15 TO *]')
                            .withCallback(function (err, rslt) {
                                search_cb(err, rslt);
                                async_cb(err, rslt);
                            }).build();

                        client.execute(searchCmd);
                    },
                    function (async_cb) {
                        var searchCmd = new Riak.Commands.YZ.Search.Builder()
                            .withIndexName('customers')
                            .withQuery('interests_set:*')
                            .withCallback(function (err, rslt) {
                                search_cb(err, rslt);
                                async_cb(err, rslt);
                            }).build();

                        client.execute(searchCmd);
                    },
                    function (async_cb) {
                        var searchCmd = new Riak.Commands.YZ.Search.Builder()
                            .withIndexName('customers')
                            .withQuery('interests_set:loving*')
                            .withCallback(function (err, rslt) {
                                search_cb(err, rslt);
                                async_cb(err, rslt);
                            }).build();

                        client.execute(searchCmd);
                    }
                ];

                async.parallel(searchFuncs, function (err, rslts) {
                    throwIfErr(err);
                    mapWithinMapExample(mapExampleDone);
                });
            });
        });
    }

    function mapWithinMapExample(mapWithinMapExampleDone) {
        var funcs = [
            function (async_cb) {
                var options = {
                    bucketType: 'maps',
                    bucket: 'customers',
                    key: 'idris_elba'
                };

                var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
                var alterEgoMap = mapOp.map('alter_ego');
                alterEgoMap.setRegister('name', 'John Luther');

                options.op = mapOp;

                client.updateMap(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            },
            function (async_cb) {
                var options = {
                    bucketType: 'maps',
                    bucket: 'customers',
                    key: 'joan_jett'
                };

                var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
                var alterEgoMap = mapOp.map('alter_ego');
                alterEgoMap.setRegister('name', 'Robert Plant');

                options.op = mapOp;

                client.updateMap(options, function (err, rslt) {
                    throwIfErr(err);
                    async_cb();
                });
            }
        ];

        async.parallel(funcs, function (err, rslts) {
            throwIfErr(err);

            waitForSearch(function () {
                function search_cb(err, rslt) {
                    logger.info("[DevSearchDataTypes] mapWithinMap numFound: '%d', docs: '%s'",
                        rslt.numFound, JSON.stringify(rslt.docs));

                    var doc = rslt.docs[0];
                    /* jshint sub:true */
                    var key = doc['_yz_rk'];
                    var bucket = doc['_yz_rb'];
                    var bucketType = doc['_yz_rt'];
                    /* jshint sub:false */
                }

                var searchFuncs = [
                    function (async_cb) {
                        var searchCmd = new Riak.Commands.YZ.Search.Builder()
                            .withIndexName('customers')
                            .withQuery('alter_ego_map.name_register:*')
                            .withCallback(function (err, rslt) {
                                search_cb(err, rslt);
                                async_cb(err, rslt);
                            }).build();

                        client.execute(searchCmd);
                    },
                    function (async_cb) {
                        var searchCmd = new Riak.Commands.YZ.Search.Builder()
                            .withIndexName('customers')
                            .withQuery('alter_ego_map.name_register:*Plant')
                            .withCallback(function (err, rslt) {
                                search_cb(err, rslt);
                                async_cb(err, rslt);
                            }).build();

                        client.execute(searchCmd);
                    }
                ];

                async.parallel(searchFuncs, function (err, rslts) {
                    throwIfErr(err);
                    mapWithinMapExampleDone();
                });
            });
        });
    }
}

module.exports = DevSearchDataTypes;
module.exports.Init = Init;

