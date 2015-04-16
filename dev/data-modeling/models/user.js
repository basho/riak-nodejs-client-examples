'use strict';

var Model = require('./model');
var inherits = require('util').inherits;

function User(firstName, lastName, interests, visits, paid_account) {
    Model.call(this);

    var obj = {
        firstName: firstName,
        lastName: lastName
    };

    if (Array.isArray(interests)) {
        obj.interests = interests;
    }

    if (visits > 0) {
        // NB: mutable
        this.visits = visits;
    }

    if (paid_account) {
        obj.paid_account = paid_account;
    }

    this.data = Object.freeze(obj);

    Model.defineProperties(this, this.data);
}

inherits(User, Model);

User.prototype.getId = function () {
    return this.firstName.toLowerCase() + '_' + this.lastName.toLowerCase();
};

User.prototype.visitPage = function () {
    if (this.visits) {
        this.visits++;
    }
    this.propertyChanged('visits', 1);
};

module.exports = User;

