var __assign = (this && this.__assign) || Object.assign || function(t) {
    for (var s, i = 1, n = arguments.length; i < n; i++) {
        s = arguments[i];
        for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
            t[p] = s[p];
    }
    return t;
};
Object.defineProperty(exports, "__esModule", { value: true });
var lie_ts_1 = require("lie-ts");
var utilities_1 = require("nano-sql/lib/utilities");
var redis = require("redis");
var RedisAdapter = (function () {
    function RedisAdapter(connectArgs, multipleDBs) {
        this.connectArgs = connectArgs;
        this.multipleDBs = multipleDBs;
        this._pkKey = {};
        this._pkType = {};
        this._doAI = {};
        this._DBIds = {};
        this._clientID = utilities_1.uuid();
    }
    RedisAdapter.prototype.setID = function (id) {
        this._id = id;
    };
    RedisAdapter.prototype._key = function (table, pk) {
        if (this.multipleDBs) {
            return table + "::" + String(pk);
        }
        else {
            return this._id + "::" + table + "::" + String(pk);
        }
    };
    RedisAdapter.prototype._getDB = function (table) {
        if (this.multipleDBs) {
            return this._dbClients[table];
        }
        return this._db;
    };
    RedisAdapter.prototype.connect = function (complete) {
        var _this = this;
        this._dbClients = {};
        this._db = redis.createClient(this.connectArgs);
        this._pub = redis.createClient(this.connectArgs);
        this._sub = redis.createClient(this.connectArgs);
        utilities_1.fastALL([this._db, this._pub, this._sub], function (item, i, done) {
            item.on("ready", done);
        }).then(function () {
            if (_this.multipleDBs) {
                _this._db.get("_db_idx_", function (err, result) {
                    var dbIDX = result ? JSON.parse(result) : {};
                    var maxID = Object.keys(dbIDX).reduce(function (prev, cur) {
                        return Math.max(prev, dbIDX[cur]);
                    }, 0) || 0;
                    var doUpdate = false;
                    Object.keys(_this._pkKey).forEach(function (table) {
                        var tableKey = _this._id + "::" + table;
                        if (dbIDX[tableKey] !== undefined) {
                            _this._DBIds[table] = dbIDX[tableKey];
                        }
                        else {
                            doUpdate = true;
                            _this._DBIds[table] = maxID;
                            dbIDX[tableKey] = maxID;
                            maxID++;
                        }
                    });
                    var genClients = function () {
                        utilities_1.fastCHAIN(Object.keys(_this._pkKey), function (item, i, next) {
                            _this._dbClients[item] = redis.createClient(_this.connectArgs);
                            _this._dbClients[item].on("ready", function () {
                                _this._dbClients[item].select(_this._DBIds[item], next);
                            });
                        }).then(complete);
                    };
                    if (doUpdate) {
                        _this._db.set("_db_idx_", JSON.stringify(dbIDX), genClients);
                    }
                    else {
                        genClients();
                    }
                });
            }
            else {
                complete();
            }
        });
    };
    RedisAdapter.prototype._getIndex = function (table, complete) {
        var isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
        var indexCallback = function (err, keys) {
            if (err) {
                complete([]);
                return;
            }
            complete(isNum ? keys.map(function (k) { return parseFloat(k); }) : keys);
        };
        this._getDB(table).zrange(this._key(table, "_index"), 0, -1, indexCallback);
    };
    RedisAdapter.prototype.makeTable = function (tableName, dataModels) {
        var _this = this;
        dataModels.forEach(function (d) {
            if (d.props && d.props.indexOf("pk") > -1) {
                _this._pkType[tableName] = d.type;
                _this._pkKey[tableName] = d.key;
                if (d.type === "int" && d.props.indexOf("ai") !== -1) {
                    _this._doAI[tableName] = true;
                }
            }
        });
    };
    RedisAdapter.prototype.write = function (table, pk, newData, complete, skipReadBeforeWrite) {
        var _this = this;
        if (!this._doAI[table]) {
            pk = pk || utilities_1.generateID(this._pkType[table], 0);
            if (!pk) {
                throw new Error("Can't add a row without a primary key!");
            }
        }
        var pkKey = this._pkKey[table];
        var isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
        var doInsert = function (oldData) {
            var r = __assign({}, oldData, newData, (_a = {}, _a[_this._pkKey[table]] = pk, _a));
            utilities_1.fastALL([0, 1], function (item, i, done) {
                if (i === 0) {
                    _this._getDB(table).zadd(_this._key(table, "_index"), isNum ? pk : 0, String(pk), done);
                }
                else {
                    _this._getDB(table).set(_this._key(table, r[pkKey]), JSON.stringify(r), function (err, reply) {
                        if (err)
                            throw err;
                        done();
                    });
                }
            }).then(function () {
                complete(r);
            });
            var _a;
        };
        if (pk) {
            if (skipReadBeforeWrite) {
                doInsert({});
            }
            else {
                this.read(table, pk, function (row) {
                    doInsert(row);
                });
            }
        }
        else {
            this._getDB(table).incr(this._key(table, "_AI"), function (err, result) {
                pk = result;
                doInsert({});
            });
        }
    };
    RedisAdapter.prototype.delete = function (table, pk, complete) {
        var _this = this;
        utilities_1.fastALL([0, 1], function (item, i, done) {
            if (i === 0) {
                _this._getDB(table).zrem(_this._key(table, "_index"), String(pk), done);
            }
            else {
                _this._getDB(table).del(_this._key(table, pk), done);
            }
        }).then(function () {
            complete();
        });
    };
    RedisAdapter.prototype.batchRead = function (table, pks, callback) {
        var _this = this;
        var keys = pks.map(function (k) { return _this._key(table, k); });
        var pkKey = this._pkKey[table];
        this._getDB(table).mget(keys, function (err, result) {
            callback(result && result.length ? result.map(function (r) { return JSON.parse(r); }).sort(function (a, b) { return a[pkKey] > b[pkKey] ? 1 : -1; }) : []);
        });
    };
    RedisAdapter.prototype.read = function (table, pk, callback) {
        this._getDB(table).get(this._key(table, pk), function (err, result) {
            if (err)
                throw err;
            callback(result ? JSON.parse(result) : undefined);
        });
    };
    RedisAdapter.prototype._getIndexRange = function (table, complete, from, to, usePK) {
        var usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;
        var pkKey = this._pkKey[table];
        var isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
        var queryCallback = function (err, reply) {
            if (err) {
                complete([]);
                return;
            }
            complete(reply);
        };
        if (usefulValues && usePK) {
            if (isNum) {
                this._getDB(table).zrangebyscore(this._key(table, "_index"), from, to, queryCallback);
            }
            else {
                this._getDB(table).zrangebylex(this._key(table, "_index"), "[" + from, "[" + to, queryCallback);
            }
        }
        else if (usefulValues) {
            this._getDB(table).zrange(this._key(table, "_index"), from, to, queryCallback);
        }
        else {
            this._getDB(table).zrange(this._key(table, "_index"), 0, -1, queryCallback);
        }
    };
    RedisAdapter.prototype.rangeRead = function (table, rowCallback, complete, from, to, usePK) {
        var _this = this;
        var usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;
        var pkKey = this._pkKey[table];
        this._getIndexRange(table, function (index) {
            index = index.map(function (k) { return _this._key(table, k); });
            var getBatch = function (keys, callback) {
                _this._getDB(table).mget(keys, function (err, result) {
                    if (err) {
                        callback();
                        return;
                    }
                    var rows = result.map(function (r) { return JSON.parse(r); }).sort(function (a, b) { return a[pkKey] > b[pkKey] ? 1 : -1; });
                    var i = 0;
                    var getRow = function () {
                        if (rows.length > i) {
                            rowCallback(rows[i], i, function () {
                                i++;
                                i > 1000 ? lie_ts_1.setFast(getRow) : getRow();
                            });
                        }
                        else {
                            callback();
                        }
                    };
                    getRow();
                });
            };
            if (index.length < 5000) {
                getBatch(index, complete);
            }
            else {
                var batchKeys = [];
                var batchKeyIdx = 0;
                for (var i = 0; i < index.length; i++) {
                    if (i > 0 && i % 1000 === 0) {
                        batchKeyIdx++;
                    }
                    if (!batchKeys[batchKeyIdx]) {
                        batchKeys[batchKeyIdx] = [];
                    }
                    batchKeys[batchKeyIdx].push(index[i]);
                }
                utilities_1.fastCHAIN(batchKeys, function (keys, i, done) {
                    getBatch(keys, done);
                }).then(complete);
            }
        }, from, to, usePK);
    };
    RedisAdapter.prototype.drop = function (table, callback) {
        var _this = this;
        this._getIndex(table, function (idx) {
            _this._getDB(table).del(_this._key(table, "_index"), function () {
                utilities_1.fastALL(idx, function (item, i, done) {
                    _this._getDB(table).del(item, done);
                }).then(callback);
            });
        });
    };
    RedisAdapter.prototype.getIndex = function (table, getLength, complete) {
        this._getIndex(table, function (idx) {
            complete(getLength ? idx.length : idx);
        });
    };
    RedisAdapter.prototype.destroy = function (complete) {
        var _this = this;
        if (this.multipleDBs) {
            utilities_1.fastALL(Object.keys(this._DBIds), function (table, i, done) {
                _this._getDB(table).flushall(done);
            }).then(complete);
        }
        else {
            this._db.flushall(function () {
                complete();
            });
        }
    };
    RedisAdapter.prototype.setNSQL = function (nsql) {
        var _this = this;
        this._sub.on("message", function (channel, msg) {
            if (channel === "nsql") {
                var data = JSON.parse(msg);
                if (data.source !== _this._clientID) {
                    nsql.triggerEvent(data.event);
                }
            }
        });
        nsql.table("*").on("*", function (event) {
            if (event.table && event.table.indexOf("_") !== 0) {
                _this._pub.publish("nsql", JSON.stringify({
                    source: _this._clientID,
                    event: __assign({}, event, { affectedRows: [] })
                }));
            }
        });
    };
    return RedisAdapter;
}());
exports.RedisAdapter = RedisAdapter;
