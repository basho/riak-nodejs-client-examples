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

    storeCounterData();

    function storeCounterData() {
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
                    logger.info("[DevSearchDataTypes] numFound: '%d', docs: '%s'",
                        rslt.numFound, JSON.stringify(rslt.docs));

                    var doc = rslt.docs[0];
                    /* jshint sub:true */
                    var key = doc['_yz_rk'];
                    var bucket = doc['_yz_rb'];
                    var bucketType = doc['_yz_rt'];
                    /* jshint sub:false */

                    done();
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
}

module.exports = DevSearchDataTypes;
module.exports.Init = Init;

/*
    public sealed class SearchDataTypes : ExampleBase
    {
        [Test]
        public void SearchForCountersWithValueGreaterThan25()
        {
            var christopherHitchensId = new RiakObjectId("counters", "people", "christ_hitchens");
            var hitchensRslt = client.DtUpdateCounter(christopherHitchensId, 10);
            CheckResult(hitchensRslt.Result);

            var joanRiversId = new RiakObjectId("counters", "people", "joan_rivers");
            var joanRiversRslt = client.DtUpdateCounter(joanRiversId, 25);
            CheckResult(joanRiversRslt.Result);

            WaitForSearch();

            DoSearch("scores", "counter:[20 TO *]");
        }

        [Test]
        public void SearchForSetsContainingFootballString()
        {
            var mikeDitkaId = new RiakObjectId("sets", "people", "ditka");
            var ditkaAdds = new List<string> { "football", "winning" };
            var ditkaRslt = client.DtUpdateSet(mikeDitkaId, Serializer, null, ditkaAdds);
            CheckResult(ditkaRslt.Result);

            var dioId = new RiakObjectId("sets", "people", "dio");
            var dioAdds = new List<string> { "wailing", "rocking", "winning" };
            var dioRslt = client.DtUpdateSet(dioId, Serializer, null, dioAdds);
            CheckResult(dioRslt.Result);

            WaitForSearch();

            DoSearch("hobbies", "set:football");
        }

        [Test]
        public void Maps()
        {
            const string firstNameRegister = "first_name";
            const string lastNameRegister = "last_name";
            const string enterpriseCustomerFlag = "enterprise_customer";
            const string pageVisitsCounter = "page_visits";
            const string interestsSet = "interests";

            var idrisElbaId = new RiakObjectId("maps", "customers", "idris_elba");
            var idrisMapUpdates = new List<MapUpdate>();
            idrisMapUpdates.Add(new MapUpdate
            {
                register_op = Serializer("Idris"),
                field = new MapField
                {
                    name = Serializer(firstNameRegister),
                    type = MapField.MapFieldType.REGISTER
                }
            });

            idrisMapUpdates.Add(new MapUpdate
            {
                register_op = Serializer("Elba"),
                field = new MapField
                {
                    name = Serializer(lastNameRegister),
                    type = MapField.MapFieldType.REGISTER
                }
            });

            idrisMapUpdates.Add(new MapUpdate
            {
                flag_op = MapUpdate.FlagOp.DISABLE,
                field = new MapField
                {
                    name = Serializer(enterpriseCustomerFlag),
                    type = MapField.MapFieldType.FLAG
                }
            });

            idrisMapUpdates.Add(new MapUpdate
            {
                counter_op = new CounterOp { increment = 10 },
                field = new MapField
                {
                    name = Serializer(pageVisitsCounter),
                    type = MapField.MapFieldType.COUNTER
                }
            });

            var idrisAdds = new[] { "acting", "being Stringer Bell" };
            var idrisSetOp = new SetOp();
            idrisSetOp.adds.AddRange(idrisAdds.Select(x => Serializer(x)));
            idrisMapUpdates.Add(new MapUpdate
            {
                set_op = idrisSetOp,
                field = new MapField
                {
                    name = Serializer(interestsSet),
                    type = MapField.MapFieldType.SET
                }
            });

            var idrisRslt = client.DtUpdateMap(idrisElbaId, Serializer, null, null, idrisMapUpdates);
            CheckResult(idrisRslt.Result);

            var joanJettId = new RiakObjectId("maps", "customers", "joan_jett");
            var joanJettMapUpdates = new List<MapUpdate>();
            joanJettMapUpdates.Add(new MapUpdate
            {
                register_op = Serializer("Joan"),
                field = new MapField
                {
                    name = Serializer(firstNameRegister),
                    type = MapField.MapFieldType.REGISTER
                }
            });

            joanJettMapUpdates.Add(new MapUpdate
            {
                register_op = Serializer("Jett"),
                field = new MapField
                {
                    name = Serializer(lastNameRegister),
                    type = MapField.MapFieldType.REGISTER
                }
            });

            joanJettMapUpdates.Add(new MapUpdate
            {
                flag_op = MapUpdate.FlagOp.DISABLE,
                field = new MapField
                {
                    name = Serializer(enterpriseCustomerFlag),
                    type = MapField.MapFieldType.FLAG
                }
            });

            joanJettMapUpdates.Add(new MapUpdate
            {
                counter_op = new CounterOp { increment = 25 },
                field = new MapField
                {
                    name = Serializer(pageVisitsCounter),
                    type = MapField.MapFieldType.COUNTER
                }
            });

            var joanJettAdds = new[] { "loving rock and roll", "being in the Blackhearts" };
            var joanJettSetOp = new SetOp();
            joanJettSetOp.adds.AddRange(joanJettAdds.Select(x => Serializer(x)));
            joanJettMapUpdates.Add(new MapUpdate
            {
                set_op = joanJettSetOp,
                field = new MapField
                {
                    name = Serializer(interestsSet),
                    type = MapField.MapFieldType.SET
                }
            });

            var joanJettRslt = client.DtUpdateMap(joanJettId, Serializer, null, null, joanJettMapUpdates);
            CheckResult(joanJettRslt.Result);

            WaitForSearch();

            DoSearch("customers", "page_visits_counter:[15 TO *]");

            // Add "alter ego" sub-map
            const string nameRegister = "name";
            const string alterEgoMap = "alter_ego";

            idrisElbaId = new RiakObjectId("maps", "customers", "idris_elba");
            var idrisGetRslt = client.DtFetchMap(idrisElbaId);
            CheckResult(idrisGetRslt.Result);

            var alterEgoMapOp = new MapOp();
            alterEgoMapOp.updates.Add(new MapUpdate
            {
                register_op = Serializer("John Luther"),
                field = new MapField
                {
                    name = Serializer(nameRegister),
                    type = MapField.MapFieldType.REGISTER
                }
            });

            var alterEgoMapUpdate = new MapUpdate
            {
                map_op = alterEgoMapOp,
                field = new MapField
                {
                    name = Serializer(alterEgoMap),
                    type = MapField.MapFieldType.MAP
                }
            };

            var idrisUpdateRslt = client.DtUpdateMap(idrisElbaId, Serializer,
                idrisGetRslt.Context, null, new List<MapUpdate> { alterEgoMapUpdate });
            CheckResult(idrisUpdateRslt.Result);
            PrintMapValues(idrisUpdateRslt.Values);

            DoSearch("customers", "alter_ego_map.name_register:*");
        }

        private void DoSearch(string index, string solrQuery)
        {
            var search = new RiakSearchRequest(index, solrQuery);
            var rslt = client.Search(search);
            CheckResult(rslt);

            RiakSearchResult searchResult = rslt.Value;
            Console.WriteLine("Num found: {0}", searchResult.NumFound);
            Assert.GreaterOrEqual(searchResult.Documents.Count, 1);

            Console.WriteLine("Search results for '{0}':", solrQuery);
            foreach (var doc in searchResult.Documents)
            {
                Console.WriteLine("\tKey: {0} Bucket Type: {1} Bucket: {2}",
                    doc.Key, doc.BucketType, doc.Bucket);
            }
        }
    }
}
*/
