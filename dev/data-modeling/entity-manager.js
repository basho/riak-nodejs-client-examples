'use strict';

var logger = require('winston');

var User = require('./models/user');
var UserRepository = require('./repositories/user-repository');

function EntityManager(client) {
    this.client = client;

    this.propertyChangedHandler = function(args) {
        logger.debug("[EntityManager.propertyChangedHandler] propertyName: '%s', value: '%s'",
                args.propertyName,
                JSON.stringify(args.value));

        var sender = args.sender;
        if (sender instanceof User) {
            
            var repository = new UserRepository(client);

            switch (args.propertyName) {
                case 'paid_account' :
                    repository.setPaidAccount(sender, args.value);
                    break;
                case 'visits' :
                    repository.incrementPageVisits(sender);
                    break;
                case 'interest:add' :
                    repository.addInterest(sender, args.value);
                    break;
                case 'interest:remove' :
                    repository.removeInterest(sender, args.value);
                    break;
                default:
                    logger.debug("[EntityManager.propertyChangedHandler] unknown propertyName: '%s'",
                        propertyName);
            }

        }
    }
}

EntityManager.prototype.addModel = function (model) {
    model.on('propertyChanged', this.propertyChangedHandler);
};

EntityManager.prototype.evictModel = function (model) {
    model.removeListener('propertyChanged', this.propertyChangedHandler);
};

module.exports = EntityManager;

