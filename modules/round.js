var async = require('async');
var	util = require('util');
var	slots = require('../helpers/slots.js');
var	sandboxHelper = require('../helpers/sandbox.js');
var	constants = require('../helpers/constants.js');

// private fields
var modules, library, self, privated = {}, shared = {};

privated.loaded = false;

privated.feesByRound = {};
privated.rewardsByRound = {};
privated.delegatesByRound = {};
privated.unFeesByRound = {};
privated.unRewardsByRound = {};
privated.unDelegatesByRound = {};

// Constructor
function Round(cb, scope) {
	library = scope;
	self = this;
	self.__private = privated;
	setImmediate(cb, null, self);
}

// Round changes
function RoundChanges (round) {
  var roundFees = parseInt(privated.feesByRound[round]) || 0;
  var roundRewards = (privated.rewardsByRound[round] || []);

  this.at = function (index) {
    var fees = Math.floor(roundFees / constants.delegates),
        feesRemaining = roundFees - (fees * constants.delegates),
        rewards = parseInt(roundRewards[index]) || 0;

    return {
      fees : fees,
      feesRemaining : feesRemaining,
      rewards : rewards,
      balance : fees + rewards
    };
  };
}

Round.prototype.loaded = function () {
	return privated.loaded;
};

// Public methods
Round.prototype.calc = function (height) {
	return Math.floor(height / constants.delegates) + (height % constants.delegates > 0 ? 1 : 0);
};

Round.prototype.getVotes = function (round, cb) {
	library.dbLite.query("select delegate, amount from ( " +
		"select m.delegate, sum(m.amount) amount, m.round from mem_round m " +
		"group by m.delegate, m.round " +
		") where round = $round", {round: round}, {delegate: String, amount: Number}, function (err, rows) {
		cb(err, rows);
	});
};

Round.prototype.flush = function (round, cb) {
	library.dbLite.query("delete from mem_round where round = $round", {round: round}, cb);
};

Round.prototype.directionSwap = function (direction, lastBlock, cb) {
	if (direction == 'backward') {
		privated.feesByRound = {};
		privated.rewardsByRound = {};
		privated.delegatesByRound = {};
		self.flush(self.calc(lastBlock.height), cb);
	} else {
		privated.unFeesByRound = {};
		privated.unRewardsByRound = {};
		privated.unDelegatesByRound = {};
		self.flush(self.calc(lastBlock.height), cb);
	}
};

Round.prototype.backwardTick = function (block, previousBlock, cb) {
	function done(err) {
		cb && cb(err);
	}

	modules.accounts.mergeAccountAndGet({
		publicKey: block.generatorPublicKey,
		producedblocks: -1,
		blockId: block.id,
		round: modules.round.calc(block.height)
	}, function (err) {
		if (err) {
			return done(err);
		}

		var round = self.calc(block.height);

		var prevRound = self.calc(previousBlock.height);

		privated.unFeesByRound[round] = (privated.unFeesByRound[round] || 0);
		privated.unFeesByRound[round] += block.totalFee;

		privated.unRewardsByRound[round] = (privated.rewardsByRound[round] || []);
		privated.unRewardsByRound[round].push(block.reward);

		privated.unDelegatesByRound[round] = privated.unDelegatesByRound[round] || [];
		privated.unDelegatesByRound[round].push(block.generatorPublicKey);

		if (prevRound !== round || previousBlock.height == 1) {
			if (privated.unDelegatesByRound[round].length == constants.delegates || previousBlock.height == 1) {
				var outsiders = [];
				async.series([
					function (cb) {
						if (block.height != 1) {
							modules.delegates.generateDelegateList(block.height, function (err, roundDelegates) {
								if (err) {
									return cb(err);
								}
								for (var i = 0; i < roundDelegates.length; i++) {
									if (privated.unDelegatesByRound[round].indexOf(roundDelegates[i]) == -1) {
										outsiders.push(modules.accounts.generateAddressByPublicKey(roundDelegates[i]));
									}
								}
								cb();
							});
						} else {
							cb();
						}
					},
					function (cb) {
						if (!outsiders.length) {
							return cb();
						}
						var escaped = outsiders.map(function (item) {
							return "'" + item + "'";
						});
						library.dbLite.query('update mem_accounts set missedblocks = missedblocks + 1 where address in (' + escaped.join(',') + ')', function (err, data) {
							cb(err);
						});
					},
					function (cb) {
						self.getVotes(round, function (err, votes) {
							if (err) {
								return cb(err);
							}
							async.eachSeries(votes, function (vote, cb) {
								library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
									address: modules.accounts.generateAddressByPublicKey(vote.delegate),
									amount: vote.amount
								}, cb);
							}, function (err) {
								self.flush(round, function (err2) {
									cb(err || err2);
								});
							});
						});
					},
					function (cb) {
						var roundChanges = new RoundChanges(round);

						async.forEachOfSeries(privated.unDelegatesByRound[round], function (delegate, index, cb) {
							var changes = roundChanges.at(index);

							modules.accounts.mergeAccountAndGet({
								publicKey: delegate,
								balance: -changes.balance,
								u_balance: -changes.balance,
								blockId: block.id,
								round: modules.round.calc(block.height),
								fees: -changes.fees,
								rewards: -changes.rewards
							}, function (err) {
								if (err) {
									return cb(err);
								}
								if (index === 0) {
									modules.accounts.mergeAccountAndGet({
										publicKey: delegate,
										balance: -changes.feesRemaining,
										u_balance: -changes.feesRemaining,
										blockId: block.id,
										round: modules.round.calc(block.height),
										fees: -changes.feesRemaining,
									}, cb);
								} else {
									cb();
								}
							});
						}, cb);
					},
					function (cb) {
						self.getVotes(round, function (err, votes) {
							if (err) {
								return cb(err);
							}
							async.eachSeries(votes, function (vote, cb) {
								library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
									address: modules.accounts.generateAddressByPublicKey(vote.delegate),
									amount: vote.amount
								}, cb);
							}, function (err) {
								self.flush(round, function (err2) {
									cb(err || err2);
								});
							});
						});
					}
				], function (err) {
					delete privated.unFeesByRound[round];
					delete privated.unRewardsByRound[round];
					delete privated.unDelegatesByRound[round];
					done(err)
				});
			} else {
				done();
			}
		} else {
			done();
		}
	});
};

Round.prototype.tick = function (block, cb) {
	function done(err) {
		cb && setImmediate(cb, err);
	}

	modules.accounts.mergeAccountAndGet({
		publicKey: block.generatorPublicKey,
		producedblocks: 1,
		blockId: block.id,
		round: modules.round.calc(block.height)
	}, function (err) {
		if (err) {
			return done(err);
		}
		var round = self.calc(block.height);

		privated.feesByRound[round] = (privated.feesByRound[round] || 0);
		privated.feesByRound[round] += block.totalFee;

		privated.rewardsByRound[round] = (privated.rewardsByRound[round] || []);
		privated.rewardsByRound[round].push(block.reward);

		privated.delegatesByRound[round] = privated.delegatesByRound[round] || [];
		privated.delegatesByRound[round].push(block.generatorPublicKey);

		var nextRound = self.calc(block.height + 1);

		if (round !== nextRound || block.height == 1) {
			if (privated.delegatesByRound[round].length == constants.delegates || block.height == 1 || block.height == 101) {
				var outsiders = [];

				async.series([
					function (cb) {
						if (block.height != 1) {
							modules.delegates.generateDelegateList(block.height, function (err, roundDelegates) {
								if (err) {
									return cb(err);
								}
								for (var i = 0; i < roundDelegates.length; i++) {
									if (privated.delegatesByRound[round].indexOf(roundDelegates[i]) == -1) {
										outsiders.push(modules.accounts.generateAddressByPublicKey(roundDelegates[i]));
									}
								}
								cb();
							});
						} else {
							cb();
						}
					},
					function (cb) {
						if (!outsiders.length) {
							return cb();
						}
						var escaped = outsiders.map(function (item) {
							return "'" + item + "'";
						});
						library.dbLite.query('update mem_accounts set missedblocks = missedblocks + 1 where address in (' + escaped.join(',') + ')', function (err, data) {
							cb(err);
						});
					},
					function (cb) {
						self.getVotes(round, function (err, votes) {
							if (err) {
								return cb(err);
							}
							async.eachSeries(votes, function (vote, cb) {
								library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
									address: modules.accounts.generateAddressByPublicKey(vote.delegate),
									amount: vote.amount
								}, cb);
							}, function (err) {
								self.flush(round, function (err2) {
									cb(err || err2);
								});
							})
						});
					},
					function (cb) {
						var roundChanges = new RoundChanges(round);

						async.forEachOfSeries(privated.delegatesByRound[round], function (delegate, index, cb) {
							var changes = roundChanges.at(index);

							modules.accounts.mergeAccountAndGet({
								publicKey: delegate,
								balance: changes.balance,
								u_balance: changes.balance,
								blockId: block.id,
								round: modules.round.calc(block.height),
								fees: changes.fees,
								rewards: changes.rewards
							}, function (err) {
								if (err) {
									return cb(err);
								}
								if (index === privated.delegatesByRound[round].length - 1) {
									modules.accounts.mergeAccountAndGet({
										publicKey: delegate,
										balance: changes.feesRemaining,
										u_balance: changes.feesRemaining,
										blockId: block.id,
										round: modules.round.calc(block.height),
										fees: changes.feesRemaining
									}, cb);
								} else {
									cb();
								}
							});
						}, cb);
					},
					function (cb) {
						self.getVotes(round, function (err, votes) {
							if (err) {
								return cb(err);
							}
							async.eachSeries(votes, function (vote, cb) {
								library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
									address: modules.accounts.generateAddressByPublicKey(vote.delegate),
									amount: vote.amount
								}, cb);
							}, function (err) {
								library.bus.message('finishRound', round);
								self.flush(round, function (err2) {
									cb(err || err2);
								});
							})
						});
					}
				], function (err) {
					delete privated.feesByRound[round];
					delete privated.rewardsByRound[round];
					delete privated.delegatesByRound[round];

					done(err);
				});
			} else {
				done();
			}
		} else {
			done();
		}
	});
}

Round.prototype.sandboxApi = function (call, args, cb) {
	sandboxHelper.callMethod(shared, call, args, cb);
}

// Events
Round.prototype.onBind = function (scope) {
	modules = scope;
}

Round.prototype.onBlockchainReady = function () {
	var round = self.calc(modules.blocks.getLastBlock().height);
	library.dbLite.query("select sum(b.totalFee), GROUP_CONCAT(b.reward), GROUP_CONCAT(lower(hex(b.generatorPublicKey))) from blocks b where (select (cast(b.height / 101 as integer) + (case when b.height % 101 > 0 then 1 else 0 end))) = $round",
		{
			round: round
		},
		{
			fees: Number,
			rewards: Array,
			delegates: Array
		}, function (err, rows) {
			privated.feesByRound[round] = rows[0].fees;
			privated.rewardsByRound[round] = rows[0].rewards;
			privated.delegatesByRound[round] = rows[0].delegates;
			privated.loaded = true;
		});
}

Round.prototype.onFinishRound = function (round) {
	library.network.io.sockets.emit('rounds/change', {number: round});
}

Round.prototype.cleanup = function (cb) {
	privated.loaded = false;
	cb();
}

// Shared

// Export
module.exports = Round;
