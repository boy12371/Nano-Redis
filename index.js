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
var redisAdapter = (function () {
    function redisAdapter(connectArgs) {
        this.connectArgs = connectArgs;
        this._pkKey = {};
        this._pkType = {};
        this._doAI = {};
    }
    redisAdapter.prototype.setID = function (id) {
        this._id = id;
    };
    redisAdapter.prototype.connect = function (complete) {
        this._db = redis.createClient(this.connectArgs);
        this._db.on("ready", complete);
    };
    redisAdapter.prototype._getIndex = function (table, complete) {
        this._db.sort(table + "::index", "ALPHA", function (err, keys) {
            if (err) {
                complete([]);
                return;
            }
            complete(keys);
        });
    };
    redisAdapter.prototype.makeTable = function (tableName, dataModels) {
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
    redisAdapter.prototype.write = function (table, pk, newData, complete, skipReadBeforeWrite) {
        var _this = this;
        if (!this._doAI[table]) {
            pk = pk || utilities_1.generateID(this._pkType[table], 0);
            if (!pk) {
                throw new Error("Can't add a row without a primary key!");
            }
        }
        var pkKey = this._pkKey[table];
        var doInsert = function (oldData) {
            var r = __assign({}, oldData, newData, (_a = {}, _a[_this._pkKey[table]] = pk, _a));
            utilities_1.fastALL([0, 1], function (item, i, done) {
                if (i === 0) {
                    _this._db.sadd(table + "::index", String(pk), done);
                }
                else {
                    _this._db.set(table + "::" + r[pkKey], JSON.stringify(r), function (err, reply) {
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
            this._db.incr(table + "::_AI", function (err, result) {
                pk = result;
                doInsert({});
            });
        }
    };
    redisAdapter.prototype.delete = function (table, pk, complete) {
        var _this = this;
        utilities_1.fastALL([0, 1], function (item, i, done) {
            if (i === 0) {
                _this._db.srem(table + "::index", String(pk), done);
            }
            else {
                _this._db.del(table + "::" + pk, done);
            }
        }).then(function () {
            complete();
        });
    };
    redisAdapter.prototype.batchRead = function (table, pks, callback) {
        var keys = pks.map(function (k) { return table + "::" + k; });
        this._db.mget(keys, function (err, result) {
            callback(result && result.length ? result.map(function (r) { return JSON.parse(r); }) : []);
        });
    };
    redisAdapter.prototype.read = function (table, pk, callback) {
        this._db.get(table + "::" + pk, function (err, result) {
            if (err)
                throw err;
            callback(result ? JSON.parse(result) : undefined);
        });
    };
    redisAdapter.prototype.rangeRead = function (table, rowCallback, complete, from, to, usePK) {
        var _this = this;
        var usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;
        this.getIndex(table, false, function (index) {
            var keys = index;
            if (usefulValues && usePK) {
                keys = index.filter(function (k, i) { return k >= from && k <= to; });
            }
            else if (usefulValues) {
                keys = index.filter(function (k, i) { return i >= from && i <= to; });
            }
            keys = keys.map(function (k) { return table + "::" + k; });
            _this._db.mget(keys, function (err, result) {
                if (err) {
                    complete();
                    return;
                }
                var rows = result.map(function (r) { return JSON.parse(r); });
                var i = 0;
                var getRow = function () {
                    if (rows.length > i) {
                        rowCallback(rows[i], i, function () {
                            i++;
                            i > 1000 ? lie_ts_1.setFast(getRow) : getRow();
                        });
                    }
                    else {
                        complete();
                    }
                };
                getRow();
            });
        });
    };
    redisAdapter.prototype.drop = function (table, callback) {
        var _this = this;
        this._getIndex(table, function (idx) {
            _this._db.del(table + "::index", function () {
                utilities_1.fastALL(idx, function (item, i, done) {
                    _this._db.del(item, done);
                }).then(callback);
            });
        });
    };
    redisAdapter.prototype.getIndex = function (table, getLength, complete) {
        var isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
        this._getIndex(table, function (idx) {
            complete(getLength ? idx.length : (isNum ? idx.map(function (i) { return parseFloat(i); }).sort(function (a, b) { return a > b ? 1 : -1; }) : idx.sort()));
        });
    };
    redisAdapter.prototype.destroy = function (complete) {
        this._db.flushall(function () {
            complete();
        });
    };
    return redisAdapter;
}());
exports.redisAdapter = redisAdapter;
