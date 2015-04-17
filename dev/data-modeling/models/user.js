'use strict';

var Model = require('../../model');
var inherits = require('util').inherits;

function User(firstName, lastName, interests, visits, paid_account) {
    Model.call(this);

    var obj = {
        firstName: firstName,
        lastName: lastName
    };

    if (Array.isArray(interests)) {
        // NB: mutable
        this.interests = interests;
    } else {
        throw new Error('interests argument must be an Array');
    }

    if (visits) {
        // NB: mutable
        this.visits = visits;
    } else {
        this.visits = 0;
    }

    if (paid_account) {
        // NB: mutable
        this.paid_account = paid_account;
    } else {
        this.paid_account = false;
    }

    this.data = Object.freeze(obj);

    Model.defineProperties(this, this.data);
}

inherits(User, Model);

User.prototype.getId = function () {
    return this.firstName.toLowerCase() + '_' + this.lastName.toLowerCase();
};

User.prototype.visitPage = function () {
    this.visits += 1;
    this.propertyChanged('visits', 1);
};

User.prototype.addInterest = function (interest) {
    if (this.interests) {
        this.interests.push(interest);
    } else {
        this.interests = [ interest ];
    }
    this.propertyChanged('interest:add', interest);
};

User.prototype.removeInterest = function (interest) {
    if (this.interests) {
        var index = this.interests.indexOf(interest);
        if (index > -1) {
            this.interests.splice(index, 1);
            this.propertyChanged('interest:remove', interest);
        }
    }
};

User.prototype.upgradeAccount = function () {
    this.paid_account = true;
    this.propertyChanged('paid_account', this.paid_account);
};

User.prototype.downgradeAccount = function () {
    this.paid_account = false;
    this.propertyChanged('paid_account', this.paid_account);
};

module.exports = User;

