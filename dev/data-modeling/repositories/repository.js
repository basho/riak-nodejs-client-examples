'use strict';

var logger = require('winston');
var Model = require('../models/model');

function Repository(client_arg) {

    this.client = client_arg;

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
                callback(err, self.buildModel(riakRslt));
            }
        }
    });

};

Repository.prototype.save = function (model, callback) {
    if (!(model instanceof Model)) {
        throw new Error('argument must be a Model object');
    }

    var self = this;

    this.get(model.id, true, function (err, rslt) {

        var bucketType = self.getBucketType(),
            bucket = self.getBucketName(),
            options = {
                bucketType: bucketType,
                bucket: bucket,
                key: model.id,
                returnBody: true,
                op: self.getMapOperation(model)
            };

        if (rslt && rslt.context) {
            options.context = rslt.context;
        }

        self.client.updateMap(options, function (err, update_rslt) {
            if (err) {
                callback(err, null);
            } else {
                callback(err, self.buildModel(update_rslt));
            }
        });
    });
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

