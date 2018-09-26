var async = require('async');
var Router = require('../helpers/router.js');
var util = require('util');
var ip = require("ip");
var bignum = require('../helpers/bignum.js');
var sandboxHelper = require('../helpers/sandbox.js');

require('colors');

// private fields
var modules, library, self, privated = {}, shared = {};

privated.loaded = false;
privated.isActive = false;
privated.loadingLastBlock = null;
privated.genesisBlock = null;
privated.total = 0;
privated.blocksToSync = 0;
privated.syncIntervalId = null;

// Constructor
function Loader(cb, scope) {
	library = scope;
	privated.genesisBlock = privated.loadingLastBlock = library.genesisblock;
	self = this;
	self.__private = privated;
	privated.attachApi();

	setImmediate(cb, null, self);
}

// private methods
privated.attachApi = function () {
	var router = new Router();

	router.map(shared, {
		"get /status": "status",
		"get /status/sync": "sync"
	});

	library.network.app.use('/api/loader', router);
	library.network.app.use(function (err, req, res, next) {
		if (!err) return next();
		library.logger.error(req.url, err.toString());
		res.status(500).send({success: false, error: err.toString()});
	});
};

privated.syncTrigger = function (turnOn) {
	if (turnOn === false && privated.syncIntervalId) {
		clearTimeout(privated.syncIntervalId);
		privated.syncIntervalId = null;
	}
	if (turnOn === true && !privated.syncIntervalId) {
		setImmediate(function nextSyncTrigger() {
			library.network.io.sockets.emit('loader/sync', {
				blocks: privated.blocksToSync,
				height: modules.blocks.getLastBlock().height
			});
			privated.syncIntervalId = setTimeout(nextSyncTrigger, 1000);
		});
	}
};

privated.loadFullDb = function (peer, cb) {
	var peerStr = peer ? ip.fromLong(peer.ip) + ":" + peer.port : 'unknown';

	var commonBlockId = privated.genesisBlock.block.id;

	library.logger.debug("Loading blocks from genesis from " + peerStr);

	modules.blocks.loadBlocksFromPeer(peer, commonBlockId, cb);
};

privated.findUpdate = function (lastBlock, peer, cb) {
	var peerStr = peer ? ip.fromLong(peer.ip) + ":" + peer.port : 'unknown';

	library.logger.info("Looking for common block with " + peerStr);

	modules.blocks.getCommonBlock(peer, lastBlock.height, function (err, commonBlock) {
		if (err || !commonBlock) {
			return cb(err);
		}

		library.logger.info("Found common block " + commonBlock.id + " (at " + commonBlock.height + ")" + " with peer " + peerStr);
		var toRemove = lastBlock.height - commonBlock.height;

		if (toRemove > 1010) {
			library.logger.log("long fork, ban 60 min", peerStr);
			modules.peer.state(peer.ip, peer.port, 0, 3600);
			return cb();
		}

		var overTransactionList = [];
		modules.transactions.undoUnconfirmedList(function (err, unconfirmedList) {
			if (err) {
				return process.exit(0);
			}

			for (var i = 0; i < unconfirmedList.length; i++) {
				var transaction = modules.transactions.getUnconfirmedTransaction(unconfirmedList[i]);
				overTransactionList.push(transaction);
				modules.transactions.removeUnconfirmedTransaction(unconfirmedList[i]);
			}

			async.series([
				function (cb) {
					if (commonBlock.id != lastBlock.id) {
						modules.round.directionSwap('backward', lastBlock, cb);
					} else {
						cb();
					}
				},
				function (cb) {
					library.bus.message('deleteBlocksBefore', commonBlock);

					modules.blocks.deleteBlocksBefore(commonBlock, cb);
				},
				function (cb) {
					if (commonBlock.id != lastBlock.id) {
						modules.round.directionSwap('forward', lastBlock, cb);
					} else {
						cb();
					}
				},
				function (cb) {
					library.logger.debug("Loading blocks from peer " + peerStr);

					modules.blocks.loadBlocksFromPeer(peer, commonBlock.id, function (err, lastValidBlock) {
						if (err) {
							modules.transactions.deleteHiddenTransaction();
							library.logger.error(err);
							library.logger.log("Failed to load blocks, ban 60 min", peerStr);
							modules.peer.state(peer.ip, peer.port, 0, 3600);

							if (lastValidBlock) {
								var uploaded = lastValidBlock.height - commonBlock.height;

								if (toRemove < uploaded) {
									library.logger.info("Remove blocks again until " + lastValidBlock.id + " (at " + lastValidBlock.height + ")");

									async.series([
										function (cb) {
											if (lastValidBlock.id != lastBlock.id) {
												modules.round.directionSwap('backward', lastBlock, cb);
											} else {
												cb();
											}
										},
										function (cb) {
											modules.blocks.deleteBlocksBefore(lastValidBlock, function (err) {
												async.series([
													function (cb) {
														if (lastValidBlock.id != lastBlock.id) {
															modules.round.directionSwap('forward', lastBlock, cb);
														}
													},
													function (cb) {
														async.eachSeries(overTransactionList, function (trs, cb) {
															modules.transactions.processUnconfirmedTransaction(trs, false, cb);
														}, cb);
													}
												], cb);
											});
										}
									], cb);

								} else {
									library.logger.info("Remove blocks again until common " + commonBlock.id + " (at " + commonBlock.height + ")");

									async.series([
										function (cb) {
											if (commonBlock.id != lastBlock.id) {
												modules.round.directionSwap('backward', lastBlock, cb);
											} else {
												cb();
											}
										},
										function (cb) {
											modules.blocks.deleteBlocksBefore(commonBlock, cb);
										},
										function (cb) {
											if (commonBlock.id != lastBlock.id) {
												modules.round.directionSwap('forward', lastBlock, cb);
											} else {
												cb();
											}
										},
										function (cb) {
											async.eachSeries(overTransactionList, function (trs, cb) {
												modules.transactions.processUnconfirmedTransaction(trs, false, cb);
											}, cb);
										}
									], cb);
								}
							} else {
								async.eachSeries(overTransactionList, function (trs, cb) {
									modules.transactions.processUnconfirmedTransaction(trs, false, cb);
								}, cb);
							}
						} else {
							for (var i = 0; i < overTransactionList.length; i++) {
								modules.transactions.pushHiddenTransaction(overTransactionList[i]);
							}

							var trs = modules.transactions.shiftHiddenTransaction();
							async.whilst(
								function () {
									return trs;
								},
								function (next) {
									modules.transactions.processUnconfirmedTransaction(trs, true, function () {
										trs = modules.transactions.shiftHiddenTransaction();
										next();
									});
								}, cb);
						}
					});
				}
			], cb);
		});
	});
};

privated.loadBlocks = function (lastBlock, cb) {
	modules.transport.getFromRandomPeer({
		api: '/height',
		method: 'GET'
	}, function (err, data) {
		var peerStr = data && data.peer ? ip.fromLong(data.peer.ip) + ":" + data.peer.port : 'unknown';
		if (err || !data.body) {
			library.logger.log("Failed to get height from peer: " + peerStr);
			return cb();
		}

		library.logger.info("Check blockchain on " + peerStr);

		data.body.height = parseInt(data.body.height);

		var report = library.scheme.validate(data.body, {
			type: "object",
			properties: {
				"height": {
					type: "integer",
					minimum: 0
				}
			}, required: ['height']
		});

		if (!report) {
			library.logger.log("Failed to parse blockchain height: " + peerStr + "\n" + library.scheme.getLastError());
			return cb();
		}

		if (bignum(modules.blocks.getLastBlock().height).lt(data.body.height)) { // Diff in chainbases
			privated.blocksToSync = data.body.height;

			if (lastBlock.id != privated.genesisBlock.block.id) { // Have to find common block
				privated.findUpdate(lastBlock, data.peer, cb);
			} else { // Have to load full db
				privated.loadFullDb(data.peer, cb);
			}
		} else {
			cb();
		}
	});
};

privated.loadSignatures = function (cb) {
	modules.transport.getFromRandomPeer({
		api: '/signatures',
		method: 'GET',
		not_ban: true
	}, function (err, data) {
		if (err) {
			return cb();
		}

		library.scheme.validate(data.body, {
			type: "object",
			properties: {
				signatures: {
					type: "array",
					uniqueItems: true
				}
			},
			required: ['signatures']
		}, function (err) {
			if (err) {
				return cb();
			}

			library.sequence.add(function (cb) {
				async.eachSeries(data.body.signatures, function (signature, cb) {
					async.eachSeries(signature.signatures, function (s, cb) {
						modules.multisignatures.processSignature({
							signature: s,
							transaction: signature.transaction
						}, function (err) {
							setImmediate(cb);
						});
					}, cb);
				}, cb);
			}, cb);
		});
	});
};

privated.loadUnconfirmedTransactions = function (cb) {
	modules.transport.getFromRandomPeer({
		api: '/transactions',
		method: 'GET'
	}, function (err, data) {
		if (err) {
			return cb();
		}

		var report = library.scheme.validate(data.body, {
			type: "object",
			properties: {
				transactions: {
					type: "array",
					uniqueItems: true
				}
			},
			required: ['transactions']
		});

		if (!report) {
			return cb();
		}

		var transactions = data.body.transactions;

		for (var i = 0; i < transactions.length; i++) {
			try {
				transactions[i] = library.logic.transaction.objectNormalize(transactions[i]);
			} catch (e) {
				var peerStr = data.peer ? ip.fromLong(data.peer.ip) + ":" + data.peer.port : 'unknown';
				library.logger.log('Transaction ' + (transactions[i] ? transactions[i].id : 'null') + ' is not valid, ban 60 min', peerStr);
				modules.peer.state(data.peer.ip, data.peer.port, 0, 3600);
				return setImmediate(cb);
			}
		}


		library.balancesSequence.add(function (cb) {
			modules.transactions.receiveTransactions(transactions, cb);
		}, cb);
	});
};

privated.loadBlockChain = function () {
	var offset = 0, limit = library.config.loading.loadPerIteration;
	var verify = library.config.loading.verifyOnLoading;

	function load(count) {
		verify = true;
		privated.total = count;

		library.logic.account.removeTables(function (err) {
			if (err) {
				throw err;
			} else {
				library.logic.account.createTables(function (err) {
					if (err) {
						throw err;
					} else {
						async.until(
							function () {
								return count < offset;
							}, function (cb) {
								library.logger.info('Current ' + offset);
								setImmediate(function () {
									modules.blocks.loadBlocksOffset(limit, offset, verify, function (err, lastBlockOffset) {
										if (err) {
											return cb(err);
										}

										offset = offset + limit;
										privated.loadingLastBlock = lastBlockOffset;

										cb();
									});
								});
							}, function (err) {
								if (err) {
									library.logger.error('loadBlocksOffset', err);
									if (err.block) {
										library.logger.error('Blockchain failed at ', err.block.height);
										modules.blocks.simpleDeleteAfterBlock(err.block.id, function (err, res) {
											library.logger.error('Blockchain clipped');
											library.bus.message('blockchainReady');
										});
									}
								} else {
									library.logger.info('Blockchain ready');
									library.bus.message('blockchainReady');
								}
							}
						);
					}
				});
			}
		});
	}

	library.logic.account.createTables(function (err) {
		if (err) {
			throw err;
		} else {
			library.dbLite.query("select count(*) from mem_accounts where blockId = (select id from blocks where numberOfTransactions > 0 order by height desc limit 1)", {'count': Number}, function (err, rows) {
				if (err) {
					throw err;
				}

				var reject = !(rows[0].count);

				modules.blocks.count(function (err, count) {
					if (err) {
						return library.logger.error('Failed to count blocks', err);
					}

					library.logger.info('Blocks ' + count);

					// Check if previous loading missed
					if (reject || verify || count == 1) {
						load(count);
					} else {
						library.dbLite.query(
							"UPDATE mem_accounts SET u_isDelegate=isDelegate,u_secondSignature=secondSignature,u_username=username,u_balance=balance,u_delegates=delegates,u_contacts=contacts,u_followers=followers,u_multisignatures=multisignatures"
							, function (err, updated) {
								if (err) {
									library.logger.error(err);
									library.logger.info("Unable to load without verifying, clearing accounts from database and loading");
									load(count);
								} else {
									library.dbLite.query("select a.blockId, b.id from mem_accounts a left outer join blocks b on b.id = a.blockId where b.id is null", {}, ['a_blockId', 'b_id'], function (err, rows) {
										if (err || rows.length > 0) {
											library.logger.error(err || "Encountered missing block, looks like node went down during block processing");
											library.logger.info("Unable to load without verifying, clearing accounts from database and loading");
											load(count);
										} else {
											// Load delegates
											library.dbLite.query("SELECT lower(hex(publicKey)) FROM mem_accounts WHERE isDelegate=1", ['publicKey'], function (err, delegates) {
												if (err || delegates.length === 0) {
													library.logger.error(err || "No delegates, reload database");
													library.logger.info("Unable to load without verifying, clearing accounts from database and loading");
													load(count);
												} else {
													modules.blocks.loadBlocksOffset(1, count, verify, function (err, lastBlock) {
														if (err) {
															library.logger.error(err || "Unable to load last block");
															library.logger.info("Unable to load without verifying, clearing accounts from database and loading");
															load(count);
														} else {
															modules.blocks.loadLastBlock(function (err, block) {
																if (err) {
																	return load(count);
																}
																privated.lastBlock = block;
																library.logger.info('Blockchain ready');
																library.bus.message('blockchainReady');
															});
														}
													});
												}
											});
										}
									});
								}
							});
					}

				});
			});
		}
	});

};

// Public methods
Loader.prototype.syncing = function () {
	return !!privated.syncIntervalId;
};

Loader.prototype.sandboxApi = function (call, args, cb) {
	sandboxHelper.callMethod(shared, call, args, cb);
};

// Events
Loader.prototype.onPeerReady = function () {
	setImmediate(function nextLoadBlock() {
		if (!privated.loaded) return;
		privated.isActive = true;
		library.sequence.add(function (cb) {
			privated.syncTrigger(true);
			var lastBlock = modules.blocks.getLastBlock();
			privated.loadBlocks(lastBlock, cb);
		}, function (err) {
			err && library.logger.error('loadBlocks timer', err);
			privated.syncTrigger(false);
			privated.blocksToSync = 0;

			privated.isActive = false;
			if (!privated.loaded) return;

			setTimeout(nextLoadBlock, 9 * 1000);
		});
	});

	setImmediate(function nextLoadUnconfirmedTransactions() {
		if (!privated.loaded) return;
		privated.loadUnconfirmedTransactions(function (err) {
			err && library.logger.error('loadUnconfirmedTransactions timer', err);
			setTimeout(nextLoadUnconfirmedTransactions, 14 * 1000);
		});

	});

	setImmediate(function nextLoadSignatures() {
		if (!privated.loaded) return;
		privated.loadSignatures(function (err) {
			err && library.logger.error('loadSignatures timer', err);

			setTimeout(nextLoadSignatures, 14 * 1000);
		});
	});
};

Loader.prototype.onBind = function (scope) {
	modules = scope;

	privated.loadBlockChain();
};

Loader.prototype.onBlockchainReady = function () {
	privated.loaded = true;
};

Loader.prototype.cleanup = function (cb) {
	privated.loaded = false;
	if (!privated.isActive) {
		cb();
	} else {
		setImmediate(function nextWatch() {
			if (privated.isActive) {
				setTimeout(nextWatch, 1 * 1000);
			} else {
				cb();
			}
		});
	}
};

// Shared
shared.status = function (req, cb) {
	cb(null, {
		loaded: privated.loaded,
		now: privated.loadingLastBlock.height,
		blocksCount: privated.total
	});
};

shared.sync = function (req, cb) {
	cb(null, {
		sync: self.syncing(),
		blocks: privated.blocksToSync,
		height: modules.blocks.getLastBlock().height
	});
};

// Export
module.exports = Loader;
