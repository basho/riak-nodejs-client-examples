'use strict';

var logger = require('winston');
var Model = require('../models/model');

function Repository(client_arg) {
    this.client = client_arg;
}

Repository.prototype.get = function (key, notFoundOk, callback) {
    var self = this,
        bucketType = this.getBucketType(),
        bucket = this.getBucketName(),
        options = {
            bucketType: bucketType,
            bucket: bucket,
            key: model.id
        };

    self.client.fetchMap(options, function (err, rslt) {
        if (err) {
            callback(err, null);
        } else {
            callback(err, self.buildModel(rslt));
        }
    });
};

Repository.prototype.save = function (model, callback) {
    if (!(model instanceof Model)) {
        throw new Error('argument must be a Model object');
    }

    var self = this,
        bucketType = this.getBucketType(),
        bucket = this.getBucketName(),
        options = {
            bucketType: bucketType,
            bucket: bucket,
            key: model.id
        };

    self.client.fetchValue(options, function (err, rslt) {
        if (err) {
            callback(err, null);
        } else {
            options.returnBody = true;
            options.op = self.getMapOperation(model);

            if (rslt.context) {
                options.context = rslt.context;
            }

            self.client.updateMap(options, function (err, rslt) {
                if (err) {
                    callback(err, null);
                } else {
                    logger.debug("rslt: '%s'", JSON.stringify(rslt));
                    callback(err, self.buildModel(rslt));
                }
            });
        }
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

