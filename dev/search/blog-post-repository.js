'use strict';

var inherits = require('util').inherits;
var logger = require('winston');

var Riak = require('basho-riak-client');

var Repository = require('../repository');
var BlogPost = require('./blog-post');

function BlogPostRepository(client, bucketName) {
    Repository.call(this, client);
    this.bucketType = 'cms';
    this.bucketName = bucketName;
}

inherits(BlogPostRepository, Repository);

BlogPostRepository.prototype.buildModel = function (rslt) {
    var title = rslt.map.registers.title.toString('utf8');
    var author = rslt.map.registers.author.toString('utf8');
    var content = rslt.map.registers.content.toString('utf8');

    var keywords = [];
    rslt.map.sets.keywords.forEach(function (keyword) {
        keywords.push(keyword);
    });

    var datePosted = Date.parse(rslt.map.registers.date.toString('utf8'));
    var published = rslt.map.flags.published;

    return new BlogPost(
        title, author, content, keywords, datePosted, published);
};

BlogPostRepository.prototype.getMapOperation = function (model) {
    var mapOp = new Riak.Commands.CRDT.UpdateMap.MapOperation();
    mapOp.setRegister('title', model.title);

    mapOp.setRegister('author', model.author);
    mapOp.setRegister('content', model.content);

    model.keywords.forEach(function (keyword) {
        mapOp.addToSet('keywords', keyword);
    });

    mapOp.setRegister('date', model.datePosted.toISOString());
    mapOp.setFlag('published', model.published);

    logger.debug("[BlogPostRepository.getMapOperation] mapOp: '%s'", JSON.stringify(mapOp));

    return mapOp;
};

module.exports = BlogPostRepository;
