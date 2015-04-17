'use strict';

var config = require('../../config');

/*
 * Code samples from:
 * http://docs.basho.com/riak/latest/dev/search/document-store/
 */

var async = require('async');
var fs = require('fs');
var https = require('https');
var logger = require('winston');
var path = require('path');

var Riak = require('basho-riak-client');

var BlogPost = require('./blog-post');
var BlogPostRepository = require('./blog-post-repository');

function maybeDownloadSchemaFile(callback) {

    var blogPostSchema =
        'https://raw.githubusercontent.com/basho/basho_docs/master/source/data/blog_post_schema.xml';
    var blogPostSchemaXmlFile = 'blog_post_schema.xml';

    fs.exists(blogPostSchemaXmlFile, function (exists) {
        var filePath = path.join(process.cwd(), blogPostSchemaXmlFile);
        if (exists) {
            callback(filePath);
        } else {
            logger.info("[DevSearchDocumentStore] downloading '%s'", filePath);
            var file = fs.createWriteStream(blogPostSchemaXmlFile);
            var request = https.get(blogPostSchema, function(response) {
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
            storeSchemaInRiak(data.toString('utf8'));
        });
    });

    function storeSchemaInRiak(schemaXml) {

        var options = {
            schemaName: 'blog_post_schema',
            schema: schemaXml
        };
        client.storeSchema(options, function (err, rslt) {
            throwIfErr(err);

            createIndexInRiak();
        });
    }

    function createIndexInRiak() {
        var options = {
            schemaName: 'blog_post_schema',
            indexName: 'blog_posts'
        };
        client.storeIndex(options, function (err, rslt) {
            if (err) {
                throw new Error(err);
            }

            storeBlogPost();
        });
    };

    function storeBlogPost() {
        var post = new BlogPost(
            'This one is so lulz!',
            'Cat Stevens',
            'Please check out these cat pics!',
            [ 'adorbs', 'cheshire' ],
            new Date(),
            true
        );

        var repo = new BlogPostRepository(client, 'cat_pics_quarterly');

        repo.save(post, function (err, rslt) {
            logger.info("[DevSearchDocumentStore] key: '%s', model: '%s'",
                rslt.key, JSON.stringify(rslt.model));
            done();
        });

    }

}

module.exports = DevSearchDocumentStore;

