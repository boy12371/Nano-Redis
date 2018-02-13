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
var db_idx_1 = require("nano-sql/lib/database/db-idx");
var really_small_events_1 = require("really-small-events");
var RedisAdapter = (function () {
    function RedisAdapter(connectArgs, multipleDBs) {
        this.connectArgs = connectArgs;
        this.multipleDBs = multipleDBs;
        this._retryCounter = 0;
        if (!this.connectArgs.retry_strategy) {
            this.connectArgs.retry_strategy = function (options) {
                if (options.error && options.error.code === 'ECONNREFUSED') {
                    return new Error('The server refused the connection');
                }
                return Math.min(options.attempt * 100, 5000);
            };
        }
        this._poolPtr = 0;
        this._pkKey = {};
        this._pkType = {};
        this._dbIndex = {};
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
    RedisAdapter.prototype._getDB = function (table, increaseRetryCounter) {
        if (this.multipleDBs) {
            return this._dbClients[table];
        }
        if (!increaseRetryCounter) {
            this._retryCounter = 0;
        }
        var db = this._dbPool[this._poolPtr];
        this._poolPtr++;
        if (this._poolPtr >= this._dbPool.length) {
            this._poolPtr = 0;
        }
        if (!db.connected) {
            if (increaseRetryCounter) {
                this._retryCounter++;
            }
            if (this._retryCounter > this._dbPool.length) {
                throw new Error("No active client connections!");
            }
            return this._getDB(table, true);
        }
        return db;
    };
    RedisAdapter.prototype.connect = function (complete) {
        var _this = this;
        this._dbClients = {};
        this._dbPool = [];
        if (this.multipleDBs) {
            this._dbPool.push(redis.createClient(this.connectArgs));
        }
        else {
            for (var i = 0; i < 30; i++) {
                this._dbPool.push(redis.createClient(this.connectArgs));
            }
        }
        this._pub = redis.createClient(this.connectArgs);
        this._sub = redis.createClient(this.connectArgs);
        var getIndexes = function () {
            _this.updateIndexes(true).then(function () {
                complete();
                if (_this.multipleDBs) {
                    _this._dbPool[0].quit();
                }
                var hash = _this._clientID.split("").reduce(function (prev, cur) { return prev + cur.charCodeAt(0); }, 0) % 10;
                setInterval(function () {
                    if (new Date().getMinutes() % 10 === hash && new Date().getSeconds() === hash * 5) {
                        _this.updateIndexes();
                    }
                }, 1000);
            });
        };
        utilities_1.fastALL([this._pub, this._sub].concat(this._dbPool), function (item, i, done) {
            if (item.connected) {
                done();
            }
            else {
                item.on("ready", done);
            }
            var doCheck = function () {
                if (item.connected) {
                    done();
                }
                else {
                    setTimeout(doCheck, 50);
                }
            };
            setTimeout(doCheck, 50);
        }).then(function () {
            if (_this.multipleDBs) {
                _this._dbPool[0].get("_db_idx_", function (err, result) {
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
                        }).then(function () {
                            getIndexes();
                        });
                    };
                    if (doUpdate) {
                        _this._dbPool[0].set("_db_idx_", JSON.stringify(dbIDX), genClients);
                    }
                    else {
                        genClients();
                    }
                });
            }
            else {
                getIndexes();
            }
        });
    };
    RedisAdapter.prototype.updateIndexes = function (getThemFast) {
        var _this = this;
        if (getThemFast) {
            return utilities_1.fastALL(Object.keys(this._dbIndex), function (table, i, next) {
                _this._getDB(table).zrange(_this._key(table, "_index"), 0, -1, function (err, result) {
                    if (err)
                        throw err;
                    _this._dbIndex[table].set(result.sort(function (a, b) { return a > b ? 1 : -1; }));
                    next();
                });
            });
        }
        else {
            return utilities_1.fastCHAIN(Object.keys(this._dbIndex), function (table, i, next) {
                var ptr = "0";
                var index = [];
                var getNextPage = function () {
                    _this._getDB(table).zscan(_this._key(table, "_index"), ptr, function (err, result) {
                        if (err)
                            throw err;
                        if (!result[1].length) {
                            ptr = result[0];
                            getNextPage();
                        }
                        if (result[0] === "0") {
                            _this._dbIndex[table].set(index.sort(function (a, b) { return a > b ? 1 : -1; }));
                            next();
                        }
                        else {
                            ptr = result[0];
                            index = index.concat(result[1].filter(function (val, i) { return i % 2 === 0; }));
                            getNextPage();
                        }
                    });
                };
                getNextPage();
            });
        }
    };
    RedisAdapter.prototype.makeTable = function (tableName, dataModels) {
        var _this = this;
        this._dbIndex[tableName] = new db_idx_1.DatabaseIndex();
        dataModels.forEach(function (d) {
            if (d.props && d.props.indexOf("pk") > -1) {
                _this._pkType[tableName] = d.type;
                _this._pkKey[tableName] = d.key;
                if (d.type === "int" && d.props.indexOf("ai") !== -1) {
                    _this._dbIndex[tableName].doAI = true;
                }
            }
        });
    };
    RedisAdapter.prototype.write = function (table, pk, newData, complete) {
        var _this = this;
        if (!this._dbIndex[table].doAI) {
            pk = pk || utilities_1.generateID(this._pkType[table], 0);
            if (!pk) {
                throw new Error("Can't add a row without a primary key!");
            }
        }
        var pkKey = this._pkKey[table];
        var isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
        var doInsert = function (oldData) {
            if (_this._dbIndex[table].indexOf(pk) === -1) {
                _this._dbIndex[table].add(pk);
                _this._pub.publish("nsql", JSON.stringify({
                    source: _this._clientID,
                    type: "add_idx",
                    event: {
                        table: table,
                        key: pk
                    }
                }));
            }
            _this._getDB(table).zadd(_this._key(table, "_index"), isNum ? pk : 0, String(pk));
            var r = __assign({}, oldData, newData, (_a = {}, _a[_this._pkKey[table]] = pk, _a));
            _this._getDB(table).set(_this._key(table, r[pkKey]), JSON.stringify(r), function (err, reply) {
                if (err)
                    throw err;
                complete(r);
            });
            var _a;
        };
        if (pk) {
            doInsert({});
        }
        else {
            this._getDB(table).incr(this._key(table, "_AI"), function (err, result) {
                pk = result;
                doInsert({});
            });
        }
    };
    RedisAdapter.prototype.delete = function (table, pk, complete) {
        var idx = this._dbIndex[table].indexOf(pk);
        if (idx !== -1) {
            this._dbIndex[table].remove(pk);
            this._pub.publish("nsql", JSON.stringify({
                source: this._clientID,
                type: "rem_idx",
                event: {
                    table: table,
                    key: pk
                }
            }));
        }
        this._getDB(table).zrem(this._key(table, "_index"), String(pk));
        this._getDB(table).del(this._key(table, pk), complete);
    };
    RedisAdapter.prototype.batchRead = function (table, pks, callback) {
        var _this = this;
        var keys = pks.map(function (k) { return _this._key(table, k); });
        var pkKey = this._pkKey[table];
        var rows = [];
        this.mGet(table, keys, function (row, idx, next) {
            rows.push(row);
            next();
        }, function () {
            callback(rows);
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
        var getKeys = [];
        var i = this._dbIndex[table].keys().length;
        if (usefulValues && usePK) {
            while (i--) {
                var key = this._dbIndex[table].keys()[i];
                if (key >= from && key <= to) {
                    getKeys.unshift(key);
                }
            }
            complete(getKeys);
        }
        else if (usefulValues) {
            while (i--) {
                if (i >= from && i <= to) {
                    getKeys.unshift(this._dbIndex[table].keys()[i]);
                }
            }
            complete(getKeys);
        }
        else {
            complete(this._dbIndex[table].keys());
        }
    };
    RedisAdapter.prototype.rangeRead = function (table, rowCallback, complete, from, to, usePK) {
        var _this = this;
        var usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;
        this._getIndexRange(table, function (index) {
            index = index.map(function (k) { return _this._key(table, k); });
            _this.mGet(table, index, rowCallback, complete);
        }, from, to, usePK);
    };
    RedisAdapter.prototype.mGet = function (table, keys, rowCallback, callback) {
        var _this = this;
        var pkKey = this._pkKey[table];
        var getBatch = function (index, done, returnRows) {
            if (!keys.length) {
                done();
                return;
            }
            _this._getDB(table).mget(index, function (err, result) {
                if (err) {
                    throw err;
                }
                var rows = result.map(function (r) { return JSON.parse(r); }).sort(function (a, b) { return a[pkKey] > b[pkKey] ? 1 : -1; });
                if (returnRows) {
                    done(rows);
                    return;
                }
                var i = 0;
                var getRow = function () {
                    if (rows.length > i) {
                        rowCallback(rows[i], i, function () {
                            i++;
                            getRow();
                        });
                    }
                    else {
                        done();
                    }
                };
                getRow();
            });
        };
        if (keys.length < 100) {
            getBatch(keys, callback);
        }
        else {
            var batchKeys = [];
            var batchKeyIdx = 0;
            batchKeys[0] = [];
            for (var i = 0; i < keys.length; i++) {
                if (i > 0 && i % 100 === 0) {
                    batchKeyIdx++;
                    batchKeys[batchKeyIdx] = [];
                }
                batchKeys[batchKeyIdx].push(keys[i]);
            }
            utilities_1.fastALL(batchKeys, function (getKeys, i, done) {
                getBatch(getKeys, done, true);
            }).then(function (rows) {
                var allRows = [].concat.apply([], rows);
                var i = 0;
                var getRow = function () {
                    if (allRows.length > i) {
                        rowCallback(allRows[i], i, function () {
                            i++;
                            i % 100 === 0 ? lie_ts_1.setFast(getRow) : getRow();
                        });
                    }
                    else {
                        callback();
                    }
                };
                getRow();
            });
        }
    };
    RedisAdapter.prototype.drop = function (table, callback) {
        var _this = this;
        this._getDB(table).del(this._key(table, "_index"), function () {
            var newIndex = new db_idx_1.DatabaseIndex();
            newIndex.doAI = _this._dbIndex[table].doAI;
            _this._dbIndex[table] = newIndex;
            _this._pub.publish("nsql", JSON.stringify({
                source: _this._clientID,
                type: "clr_idx",
                event: {
                    table: table
                }
            }));
            utilities_1.fastALL(_this._dbIndex[table].keys(), function (item, i, done) {
                _this._getDB(table).del(item, done);
            }).then(callback);
        });
    };
    RedisAdapter.prototype.getIndex = function (table, getLength, complete) {
        complete(getLength ? this._dbIndex[table].keys().length : this._dbIndex[table].keys());
    };
    RedisAdapter.prototype.destroy = function (complete) {
        var _this = this;
        if (this.multipleDBs) {
            utilities_1.fastALL(Object.keys(this._DBIds), function (table, i, done) {
                _this._getDB(table).flushall(done);
            }).then(complete);
        }
        else {
            this._dbPool[0].flushall(function () {
                complete();
            });
        }
    };
    RedisAdapter.prototype.sub = function (type, callback) {
        really_small_events_1.RSE.on(type, callback);
    };
    RedisAdapter.prototype.emit = function (type, message) {
        really_small_events_1.RSE.trigger(type, message);
        this._pub.publish("nsql", JSON.stringify({
            source: this._clientID,
            type: "pubsub",
            eventType: type,
            event: message
        }));
    };
    RedisAdapter.prototype.setNSQL = function (nsql) {
        var _this = this;
        this._sub.on("message", function (channel, msg) {
            var data = JSON.parse(msg);
            if (data.source !== _this._clientID) {
                switch (data.type) {
                    case "event":
                        nsql.triggerEvent(data.event);
                        break;
                    case "pubsub":
                        really_small_events_1.RSE.trigger(data.eventType, data.event);
                        break;
                    case "add_idx":
                        if (!_this._dbIndex[data.event.table])
                            return;
                        _this._dbIndex[data.event.table].add(data.event.key);
                        break;
                    case "rem_idx":
                        if (!_this._dbIndex[data.event.table])
                            return;
                        _this._dbIndex[data.event.table].remove(data.event.key);
                        break;
                    case "clr_idx":
                        if (!_this._dbIndex[data.event.table])
                            return;
                        var newIndex = new db_idx_1.DatabaseIndex();
                        newIndex.doAI = _this._dbIndex[data.event.table].doAI;
                        _this._dbIndex[data.event.table] = newIndex;
                        break;
                }
            }
        });
        this._sub.subscribe("nsql");
        nsql.table("*").on("*", function (event) {
            if (event.table && event.table.indexOf("_") !== 0) {
                _this._pub.publish("nsql", JSON.stringify({
                    source: _this._clientID,
                    type: "event",
                    event: __assign({}, event, { affectedRows: [] })
                }));
            }
        });
    };
    return RedisAdapter;
}());
exports.RedisAdapter = RedisAdapter;
