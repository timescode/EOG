var util = require('util'),
	async = require('async'),
	path = require('path'),
	Router = require('../helpers/router.js'),
	sandboxHelper = require('../helpers/sandbox.js');

// private fields
var modules, library, self, privated = {}, shared = {};

privated.loaded = false;

// Constructor
function Server(cb, scope) {
	library = scope;
	self = this;
	self.__private = privated;
	privated.attachApi();

	setImmediate(cb, null, self);
}

// private methods
privated.attachApi = function() {
	var router = new Router();

	router.use(function (req, res, next) {
		if (modules) return next();
		res.status(500).send({success: false, error: "Blockchain is loading"});
	});

	router.get('/', function (req, res) {
		if (privated.loaded) {
			res.render('wallet.html', {layout: false});
		} else {
			res.render('loading.html');
		}
	});

	router.use(function (req, res, next) {
		if (req.url.indexOf('/api/') == -1 && req.url.indexOf('/peer/') == -1) {
			return res.redirect('/');
		}
		next();
		// res.status(500).send({ success: false, error: 'api not found' });
	});

	library.network.app.use('/', router);
}

// Public methods

Server.prototype.sandboxApi = function (call, args, cb) {
	sandboxHelper.callMethod(shared, call, args, cb);
}

// Events
Server.prototype.onBind = function (scope) {
	modules = scope;
}

Server.prototype.onBlockchainReady = function () {
	privated.loaded = true;
}

Server.prototype.cleanup = function (cb) {
	privated.loaded = false;
	cb();
}

// Shared

// Export
module.exports = Server;
