'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/search/document-store/
 */

var async = require('async');
var fs = require('fs');
var http = require('http');
var logger = require('winston');
var path = require('path');
var Riak = require('basho-riak-client');

function maybeDownloadSchemaFile(callback) {

    var blogPostSchema =
        'http://github.com/basho/basho_docs/raw/master/source/data/blog_post_schema.xml';
    var blogPostSchemaXmlFile = 'blog_post_schema.xml';

    fs.exists(blogPostSchemaXmlFile, function (exists) {
        var filePath = path.join(process.cwd(), blogPostSchemaXmlFile);
        if (exists) {
            callback(filePath);
        } else {
            logger.info("[DevSearchDocumentStore] downloading '%s'", filePath);
            var file = fs.createWriteStream(blogPostSchemaXmlFile);
            var request = http.get(blogPostSchema, function(response) {
                response.pipe(file);
                file.on('finish', function () {
                    file.close(callback(filePath));
                });
            });
        }
    });

}

function throwIfErr(err) {
    if (err) {
        throw new Error(err);
    }
}

function DevSearchDocumentStore(done) {
    var client = config.createClient();

    maybeDownloadSchemaFile(function (schemaFile) {
        fs.readFile(schemaFile, function (err, data) {
            throwIfErr(err);
            storeSchemaInRiak(data);
        });
    });

    function storeSchemaInRiak(schemaXml) {

        var options = {
            schemaName: 'blog_post_schema',
            schema: schemaXml
        };
        client.storeSchema(options, function (err, rslt) {
            throwIfErr(err);
        });
    }
}

module.exports = DevSearchDocumentStore;

