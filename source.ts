import { NanoSQLStorageAdapter, DBKey, DBRow, _NanoSQLStorage } from "nano-sql/lib/database/storage";
import { DataModel, NanoSQLInstance } from "nano-sql/lib/index";
import { setFast, Promise } from "lie-ts";
import { StdObject, hash, fastALL, fastCHAIN, deepFreeze, uuid, timeid, _assign, generateID, sortedInsert, isAndroid } from "nano-sql/lib/utilities";
import * as redis from "redis";
import { DatabaseIndex } from "nano-sql/lib/database/db-idx";
import { RSE } from "really-small-events";

/**
 * Handles WebSQL persistent storage
 *
 * @export
 * @class _SyncStore
 * @implements {NanoSQLStorageAdapter}
 */
// tslint:disable-next-line
export class RedisAdapter implements NanoSQLStorageAdapter {


    private _pkKey: {
        [tableName: string]: string;
    };

    private _pkType: {
        [tableName: string]: string;
    };

    private _dbIndex: {
        [tableName: string]: DatabaseIndex;
    };


    private _id: string;

    private _poolPtr: number;

    private _dbPool: redis.RedisClient[];

    private _dbClients: {
        [table: string]: redis.RedisClient;
    }

    private _pub: redis.RedisClient;
    private _sub: redis.RedisClient;

    private _filename: string;
    private _mode: any;

    private _clientID: string;

    private _DBIds: {
        [table: string]: number;
    }

    constructor(public connectArgs: redis.ClientOpts, public multipleDBs?: boolean) {

        if (!this.connectArgs.retry_strategy) {
            this.connectArgs.retry_strategy = (options) => {
                if (options.error && options.error.code === 'ECONNREFUSED') {
                    // End reconnecting on a specific error and flush all commands with
                    // a individual error
                    return new Error('The server refused the connection');
                }
                // attempt reconnect forever, every 5 seconds at most
                return Math.min(options.attempt * 100, 5000);
            }
        }

        this._poolPtr = 0;
        this._pkKey = {};
        this._pkType = {};
        this._dbIndex = {};
        this._DBIds = {};
        this._clientID = uuid();
    }

    public setID(id: string) {
        this._id = id;
    }

    private _key(table: string, pk: any) {
        if (this.multipleDBs) {
            return table + "::" + String(pk);
        } else {
            return this._id + "::" + table + "::" + String(pk);
        }

    }

    private _retryCounter: number = 0;

    private _getDB(table: string, increaseRetryCounter?: boolean): redis.RedisClient {
        if (this.multipleDBs) {
            return this._dbClients[table];
        }

        if (!increaseRetryCounter) {
            this._retryCounter = 0;
        }

        const db = this._dbPool[this._poolPtr];

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
    }

    public connect(complete: () => void) {

        this._dbClients = {};
        this._dbPool = [];
        if (this.multipleDBs) {
            this._dbPool.push(redis.createClient(this.connectArgs));
        } else {
            for (let i = 0; i < 30; i++) {
                this._dbPool.push(redis.createClient(this.connectArgs));
            }
        }

        this._pub = redis.createClient(this.connectArgs);
        this._sub = redis.createClient(this.connectArgs);

        const getIndexes = () => {
            this.updateIndexes(true).then(() => {
                complete();

                if (this.multipleDBs) {
                    this._dbPool[0].quit();
                }

                // grab a copy of the table indexs every ten minutes
                // redis pub/sub should gaurantee indexes stay in sync but this is our insurance policy
                // random hash gaurantees all clients don't hit at the same time
                const hash = this._clientID.split("").reduce((prev, cur) => prev + cur.charCodeAt(0), 0) % 10;

                setInterval(() => {
                    if (new Date().getMinutes() % 10 === hash && new Date().getSeconds() === hash * 5) {
                        this.updateIndexes();
                    }
                }, 1000);
            });
        }


        fastALL([this._pub, this._sub].concat(this._dbPool), (item: redis.RedisClient, i, done) => {
            if (item.connected) {
                done();
            } else {
                item.on("ready", done);
            }
            const doCheck = () => {
                if (item.connected) {
                    done();
                } else {
                    setTimeout(doCheck, 50);
                }
            }
            setTimeout(doCheck, 50);
        }).then(() => {
            if (this.multipleDBs) {
                this._dbPool[0].get("_db_idx_", (err, result) => {
                    let dbIDX = result ? JSON.parse(result) : {};
                    let maxID = Object.keys(dbIDX).reduce((prev, cur) => {
                        return Math.max(prev, dbIDX[cur]);
                    }, 0) || 0;
                    let doUpdate = false;
                    Object.keys(this._pkKey).forEach((table) => {
                        const tableKey = this._id + "::" + table;
                        if (dbIDX[tableKey] !== undefined) {
                            this._DBIds[table] = dbIDX[tableKey];
                        } else {
                            doUpdate = true;
                            this._DBIds[table] = maxID;
                            dbIDX[tableKey] = maxID;
                            maxID++;
                        }
                    });
                    const genClients = () => {
                        fastCHAIN(Object.keys(this._pkKey), (item, i, next) => {
                            this._dbClients[item] = redis.createClient(this.connectArgs);
                            this._dbClients[item].on("ready", () => {
                                this._dbClients[item].select(this._DBIds[item], next);
                            });
                        }).then(() => {
                            getIndexes();
                        });
                    }
                    if (doUpdate) {
                        this._dbPool[0].set("_db_idx_", JSON.stringify(dbIDX), genClients);
                    } else {
                        genClients();
                    }
                });
            } else {
                getIndexes();
            }
        });
    }

    public updateIndexes(getThemFast?: boolean): Promise<any> {

        if (getThemFast) {
            // block redis to get the index ASAP
            return fastALL(Object.keys(this._dbIndex), (table, i, next) => {
                this._getDB(table).zrange(this._key(table, "_index"), 0, -1, (err, result) => {
                    if (err) throw err;
                    this._dbIndex[table].set(result.sort((a, b) => a > b ? 1 : -1));
                    next();
                });
            });
        } else {
            // non blocking to get the index whenever redis gets around to it.
            return fastCHAIN(Object.keys(this._dbIndex), (table, i, next) => {

                let ptr = "0";
                let index: any[] = [];
                const getNextPage = () => {
                    this._getDB(table).zscan(this._key(table, "_index"), ptr, (err, result) => {
                        if (err) throw err;

                        if (!result[1].length) {
                            ptr = result[0];
                            getNextPage();
                        }

                        if (result[0] === "0") {
                            this._dbIndex[table].set(index.sort((a, b) => a > b ? 1 : -1));
                            next();
                        } else {
                            ptr = result[0];
                            index = index.concat(result[1].filter((val, i) => i % 2 === 0));
                            getNextPage();
                        }
                    })
                }
                getNextPage();
            });
        }

    }

    public makeTable(tableName: string, dataModels: DataModel[]): void {

        this._dbIndex[tableName] = new DatabaseIndex();

        dataModels.forEach((d) => {
            if (d.props && d.props.indexOf("pk") > -1) {
                this._pkType[tableName] = d.type;
                this._pkKey[tableName] = d.key;
                if (d.type === "int" && d.props.indexOf("ai") !== -1) {
                    //this._doAI[tableName] = true;
                    this._dbIndex[tableName].doAI = true;
                }
            }
        });
    }

    public write(table: string, pk: DBKey | null, newData: DBRow, complete: (row: DBRow) => void): void {

        if (!this._dbIndex[table].doAI) {
            pk = pk || generateID(this._pkType[table], 0) as DBKey;

            if (!pk) {
                throw new Error("Can't add a row without a primary key!");
            }
        }

        const pkKey = this._pkKey[table];
        const isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;

        const doInsert = (oldData: any) => {

            if (this._dbIndex[table].indexOf(pk) === -1) {
                this._dbIndex[table].add(pk);

                this._pub.publish("nsql", JSON.stringify({
                    source: this._clientID,
                    type: "add_idx",
                    event: {
                        table: table,
                        key: pk
                    }
                }));
            }

            this._getDB(table).zadd(this._key(table, "_index"), isNum ? pk as any : 0, String(pk));

            const r = {
                ...oldData,
                ...newData,
                [this._pkKey[table]]: pk
            }

            this._getDB(table).set(this._key(table, r[pkKey]), JSON.stringify(r), (err, reply) => {
                if (err) throw err;
                complete(r);
            })
        }

        if (pk) {
            doInsert({});
        } else { // auto incriment add

            this._getDB(table).incr(this._key(table, "_AI"), (err, result) => {
                pk = result as any;
                doInsert({});
            });
        }


    }

    public delete(table: string, pk: DBKey, complete: () => void): void {

        let idx = this._dbIndex[table].indexOf(pk);
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

    }

    public batchRead(table: string, pks: DBKey[], callback: (rows: any[]) => void) {

        const keys = pks.map(k => this._key(table, k));
        const pkKey = this._pkKey[table];

        let rows: any[] = [];

        this.mGet(table, keys, (row, idx, next) => {
            rows.push(row);
            next();
        }, () => {
            callback(rows);
        });
    }


    public read(table: string, pk: DBKey, callback: (row: DBRow) => void): void {

        this._getDB(table).get(this._key(table, pk), (err, result) => {
            if (err) throw err;
            callback(result ? JSON.parse(result) : undefined);
        });
    }

    public _getIndexRange(table: string, complete: (idx: any[]) => void, from?: any, to?: any, usePK?: boolean) {

        const usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;
        const pkKey = this._pkKey[table];

        let getKeys: any[] = [];


        let i = this._dbIndex[table].keys().length;

        if (usefulValues && usePK) {
            // form pk to pk
            while (i--) {
                const key = this._dbIndex[table].keys()[i];
                if (key >= from && key <= to) {
                    getKeys.unshift(key);
                }
            }
            complete(getKeys);

        } else if (usefulValues) {
            // limit, offset
            while (i--) {
                if (i >= from && i <= to) {
                    getKeys.unshift(this._dbIndex[table].keys()[i]);
                }
            }
            complete(getKeys);
        } else {
            // full table scan
            complete(this._dbIndex[table].keys());
        }
    }

    public rangeRead(table: string, rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, complete: () => void, from?: any, to?: any, usePK?: boolean): void {

        const usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;

        this._getIndexRange(table, (index) => {

            index = index.map(k => this._key(table, k));

            this.mGet(table, index, rowCallback, complete);

        }, from, to, usePK);


    }


    /**
     * Pass in the keys we want to get.
     * If less than 100 keys we'll get them all in one request.
     * Otherwise we break them up into 100 key segments and get them in parallel.
     * 
     * Prevents blocking request from hitting Redis server. 100 row requests normally take > 100ms.
     * 
     * @param {string} table 
     * @param {any[]} keys 
     * @param {(row: DBRow, idx: number, nextRow: () => void) => void} rowCallback 
     * @param {() => void} callback 
     * @memberof RedisAdapter
     */
    public mGet(table: string, keys: any[], rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, callback: () => void) {

        const pkKey = this._pkKey[table];

        const getBatch = (index: any[], done: (rows?: any[]) => void, returnRows?: boolean) => {
            if (!keys.length) {
                done();
                return;
            }
            this._getDB(table).mget(index, (err, result) => {
                if (err) {
                    throw err;
                }
                let rows = result.map(r => JSON.parse(r)).sort((a, b) => a[pkKey] > b[pkKey] ? 1 : -1);
                if (returnRows) {
                    done(rows);
                    return;
                }
                let i = 0;
                const getRow = () => {
                    if (rows.length > i) {
                        rowCallback(rows[i], i, () => {
                            i++;
                            getRow();
                        });
                    } else {
                        done();
                    }
                };
                getRow();
            });
        }

        if (keys.length < 100) {
            getBatch(keys, callback);
        } else {
            let batchKeys: any[][] = [];
            let batchKeyIdx = 0;
            batchKeys[0] = [];

            for (let i = 0; i < keys.length; i++) {
                if (i > 0 && i % 100 === 0) {
                    batchKeyIdx++;
                    batchKeys[batchKeyIdx] = [];
                }
                batchKeys[batchKeyIdx].push(keys[i]);
            }

            fastALL(batchKeys, (getKeys, i, done) => {
                getBatch(getKeys, done, true);
            }).then((rows) => {
                const allRows = [].concat.apply([], rows);
                let i = 0;
                const getRow = () => {
                    if (allRows.length > i) {
                        rowCallback(allRows[i], i, () => {
                            i++;
                            i % 100 === 0 ? setFast(getRow) : getRow(); // break up call stack
                        });
                    } else {
                        callback();
                    }
                };
                getRow();
            });
        }
    }

    public drop(table: string, callback: () => void): void {

        this._getDB(table).del(this._key(table, "_index"), () => {

            let newIndex = new DatabaseIndex();
            newIndex.doAI = this._dbIndex[table].doAI;
            this._dbIndex[table] = newIndex;

            this._pub.publish("nsql", JSON.stringify({
                source: this._clientID,
                type: "clr_idx",
                event: {
                    table: table
                }
            }));

            fastALL(this._dbIndex[table].keys(), (item, i, done) => {
                this._getDB(table).del(item, done);
            }).then(callback);
        });

    }

    public getIndex(table: string, getLength: boolean, complete: (index) => void): void {
        complete(getLength ? this._dbIndex[table].keys().length : this._dbIndex[table].keys());
    }

    public destroy(complete: () => void) {
        if (this.multipleDBs) {
            fastALL(Object.keys(this._DBIds), (table, i, done) => {
                this._getDB(table).flushall(done);
            }).then(complete);
        } else {
            this._dbPool[0].flushall(() => {
                complete();
            })
        }
    }

    public sub(type: string, callback: (eventData: any) => void) {
        RSE.on(type, callback);
    }

    public emit(type: string, message: any) {
        RSE.trigger(type, message);
        this._pub.publish("nsql", JSON.stringify({
            source: this._clientID,
            type: "pubsub",
            eventType: type,
            event: message
        }));
    }

    public setNSQL(nsql: NanoSQLInstance) {
        /**
         * Uses redis pub/sub to maintain event system across clients
         */
        this._sub.on("message", (channel, msg) => {
            const data = JSON.parse(msg);
            if (data.source !== this._clientID) {
                switch (data.type) {
                    case "event":
                        nsql.triggerEvent(data.event);
                        break;
                    case "pubsub":
                        RSE.trigger(data.eventType, data.event);
                    break;
                    case "add_idx":
                        if (!this._dbIndex[data.event.table]) return;
                        this._dbIndex[data.event.table].add(data.event.key);
                        break;
                    case "rem_idx":
                        if (!this._dbIndex[data.event.table]) return;
                        this._dbIndex[data.event.table].remove(data.event.key);
                        break;
                    case "clr_idx":
                        if (!this._dbIndex[data.event.table]) return;
                        let newIndex = new DatabaseIndex();
                        newIndex.doAI = this._dbIndex[data.event.table].doAI;
                        this._dbIndex[data.event.table] = newIndex;
                        break;
                }
            }
        });
        this._sub.subscribe("nsql");

        nsql.table("*").on("*", (event) => {
            if (event.table && event.table.indexOf("_") !== 0) {
                this._pub.publish("nsql", JSON.stringify({
                    source: this._clientID,
                    type: "event",
                    event: {
                        ...event,
                        affectedRows: []
                    }
                }));
            }
        });

    }
}