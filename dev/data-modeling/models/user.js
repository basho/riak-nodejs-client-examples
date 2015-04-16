'use strict';

var Model = require('./model');
var inherits = require('util').inherits;

function User(firstName, lastName, interests) {
    Model.call(this);

    var obj = {
        firstName: firstName,
        lastName: lastName
    };

    if (Array.isArray(interests)) {
        obj.interests = interests;
    }

    this.data = Object.freeze(obj);

    Model.defineProperties(this, this.data);
}

inherits(User, Model);

User.prototype.getId = function () {
    return this.firstName.toLowerCase() + '_' + this.lastName.toLowerCase();
};

module.exports = User;

