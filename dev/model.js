'use strict';

var events = require('events');
var logger = require('winston');
var util = require('util');

function Model() {
    events.EventEmitter.call(this);

    this.propertyChanged = function (propertyName, value) {
        var args = {
            sender: this,
            propertyName: propertyName,
            value: value
        };
        this.emit('propertyChanged', args);
    };
}

util.inherits(Model, events.EventEmitter);

/*
 * Used to create immutable Model objects
 */
function defineProperties(obj, data) {
    Object.keys(data).forEach(function (prop) {
        Object.defineProperty(obj, prop, {
            get: function () { return data[prop] }
        });
    });
}

Model.prototype.getId = function () {
    if (this.hasOwnProperty('id')) {
        return this.id;
    }
    // If a Model does not provide an ID, we must want Riak to generate it
};

Object.defineProperty(Model.prototype, "id", {
    get: function () { return this.getId() }
});

module.exports = Model;
module.exports.defineProperties = defineProperties;

