'use strict';

var logger = require('winston');
var Model = require('./model');

function Repository(client) {

    this.client = client;

    this.fetch = function (key, notFoundOk, callback) {

        var bucketType = this.getBucketType(),
            bucket = this.getBucketName(),
            options = {
                bucketType: bucketType,
                bucket: bucket,
                key: key
            };

        this.client.fetchMap(options, callback);

    };
}

Repository.prototype.get = function (key, notFoundOk, callback) {

    var self = this;

    this.fetch(key, notFoundOk, function (err, riakRslt) {
        if (err) {
            callback(err, null);
        } else {
            if (riakRslt.notFound) {
                if (notFoundOk) {
                    callback(err, null);
                } else {
                    callback('key ' + key + ' not found', null);
                }
            } else {
                callback(err, { key: key, model: self.buildModel(riakRslt) });
            }
        }
    });

};

Repository.prototype.save = function (model, callback) {
    if (!(model instanceof Model)) {
        throw new Error('argument must be a Model object');
    }

    var self = this;

    logger.debug("[Repository.save] model: '%s'", JSON.stringify(model));

    function updateModel(options) {
        self.client.updateMap(options, function (err, update_rslt) {
            if (err) {
                callback(err, null);
            } else {
                var key = update_rslt.generatedKey ? update_rslt.generatedKey : options.key;
                callback(err, { key: key, model: self.buildModel(update_rslt) });
            }
        });
    }

    var bucketType = self.getBucketType(),
        bucket = self.getBucketName(),
        options = {
            bucketType: bucketType,
            bucket: bucket,
            returnBody: true,
            op: self.getMapOperation(model)
        };

    if (model.id) {

        options.key = model.id;

        this.fetch(model.id, true, function (err, riakRslt) {
            if (riakRslt && riakRslt.context) {
                options.context = riakRslt.context;
            }
            updateModel(options);
        });

    } else {
        updateModel(options);
    }

};

Repository.prototype.getBucketType = function () {
    if (this.hasOwnProperty('bucketType')) {
        return this.bucketType;
    } else {
        throw 'not implemented';
    }
};

Repository.prototype.getBucketName = function () {
    if (this.hasOwnProperty('bucketName')) {
        return this.bucketName;
    } else {
        throw 'not implemented';
    }
};

Repository.prototype.buildModel = function (rslt) {
    throw 'not implemented';
};

Repository.prototype.getMapOperation = function (model) {
    throw 'not implemented';
};

module.exports = Repository;

