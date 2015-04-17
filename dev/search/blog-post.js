'use strict';

var Model = require('../../model');
var inherits = require('util').inherits;

function BlogPost(title, author, content, keywords, datePosted, published) {
    Model.call(this);

    var obj = {
        title: title,
        author: author,
        content: content,
        keywords: keywords,
        datePosted: datePosted,
        published: published
    };

    this.data = Object.freeze(obj);

    Model.defineProperties(this, this.data);
}

inherits(BlogPost, Model);

module.exports = User;

