import { NanoSQLStorageAdapter, DBKey, DBRow, _NanoSQLStorage } from "nano-sql/lib/database/storage";
import { DataModel, NanoSQLInstance } from "nano-sql/lib/index";
import { setFast, Promise } from "lie-ts";
import { StdObject, hash, fastALL, fastCHAIN, deepFreeze, uuid, timeid, _assign, generateID, sortedInsert, isAndroid } from "nano-sql/lib/utilities";
import * as redis from "redis";

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

    private _doAI: {
        [tableName: string]: boolean;
    }


    private _id: string;

    private _db: redis.RedisClient;

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
        this._pkKey = {};
        this._pkType = {};
        this._doAI = {};
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

    private _getDB(table: string): redis.RedisClient {
        if (this.multipleDBs) {
            return this._dbClients[table];
        }
        return this._db;
    }

    public connect(complete: () => void) {

        this._dbClients = {};

        this._db = redis.createClient(this.connectArgs);
        this._pub = redis.createClient(this.connectArgs);
        this._sub = redis.createClient(this.connectArgs);

        fastALL([this._db, this._pub, this._sub], (item, i, done) => {
            item.on("ready", done);
        }).then(() => {
            if (this.multipleDBs) {
                this._db.get("_db_idx_", (err, result) => {
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
                        }).then(complete);
                    }
                    if (doUpdate) {
                        this._db.set("_db_idx_", JSON.stringify(dbIDX), genClients);
                    } else {
                        genClients();
                    }
                });
            } else {
                complete();
            }
        });
    }

    private _getIndex(table: string, complete: (idx: any[]) => void) {

            const isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;

            const indexCallback = (err, keys) => {
                if (err) {
                    complete([]);
                    return;
                }
    
                complete(isNum ? keys.map(k => parseFloat(k)) : keys); 
            }
    
            this._getDB(table).zrange(this._key(table, "_index"), 0, -1, indexCallback);


    }


    public makeTable(tableName: string, dataModels: DataModel[]): void {

        dataModels.forEach((d) => {
            if (d.props && d.props.indexOf("pk") > -1) {
                this._pkType[tableName] = d.type;
                this._pkKey[tableName] = d.key;
                if (d.type === "int" && d.props.indexOf("ai") !== -1) {
                    this._doAI[tableName] = true;
                }
            }
        });
    }

    public write(table: string, pk: DBKey | null, newData: DBRow, complete: (row: DBRow) => void, skipReadBeforeWrite: boolean): void {


            if (!this._doAI[table]) {
                pk = pk || generateID(this._pkType[table], 0) as DBKey;
    
                if (!pk) {
                    throw new Error("Can't add a row without a primary key!");
                }    
            }
    
            const pkKey = this._pkKey[table];
            const isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
    
            const doInsert = (oldData: any) => {
                const r = {
                    ...oldData,
                    ...newData,
                    [this._pkKey[table]]: pk
                }
                fastALL([0, 1], (item, i, done) => {
                    if (i === 0) {
                        this._getDB(table).zadd(this._key(table,  "_index"), isNum ? pk as any : 0 , String(pk), done);
                    } else {
                        this._getDB(table).set(this._key(table,  r[pkKey]), JSON.stringify(r), (err, reply) => {
                            if (err) throw err;
                            done();
                        })
                    }
                }).then(() => {
                    complete(r);
                })
            }
    
            if (pk) {
                if (skipReadBeforeWrite) {
                    doInsert({});
                } else {
                    this.read(table, pk, (row) => {
                        doInsert(row);
                    });
                }
            } else { // auto incriment add
    
                this._getDB(table).incr(this._key(table, "_AI"), (err, result) => {
                    pk = result as any;
                    doInsert({});
                });
            }


    }

    public delete(table: string, pk: DBKey, complete: () => void): void {

            fastALL([0, 1], (item, i, done) => {
                if (i === 0) {
                    this._getDB(table).zrem(this._key(table, "_index"), String(pk), done);
                } else {
                    this._getDB(table).del(this._key(table, pk), done);
                }
            }).then(() => {
                complete();
            })


    }

    public batchRead(table: string, pks: DBKey[], callback: (rows: any[]) => void) {

            const keys = pks.map(k => this._key(table, k));
            const pkKey = this._pkKey[table];
    
            this._getDB(table).mget(keys, (err, result) => {
                callback(result && result.length ? result.map(r => JSON.parse(r)).sort((a, b) => a[pkKey] > b[pkKey] ? 1 : -1) : []);
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
            const isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
    
            const queryCallback = (err, reply) => {
                if (err) {
                    complete([]);
                    return;
                }
                complete(reply);
            }
    
            if (usefulValues && usePK) {
                // form pk to pk
                if (isNum) {
                    this._getDB(table).zrangebyscore(this._key(table, "_index"), from, to, queryCallback);
                } else {
                    this._getDB(table).zrangebylex(this._key(table, "_index"), `[${from}`, `[${to}`, queryCallback);
                }
                
            } else if (usefulValues) {
                // limit, offset
                this._getDB(table).zrange(this._key(table, "_index"), from, to, queryCallback);
            } else {
                // full table scan
                this._getDB(table).zrange(this._key(table, "_index"), 0, -1, queryCallback);
            }
        

    }

    public rangeRead(table: string, rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, complete: () => void, from?: any, to?: any, usePK?: boolean): void {

        const usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;
        const pkKey = this._pkKey[table];


            this._getIndexRange(table, (index) => {

                index = index.map(k => this._key(table, k));
    
                const getBatch = (keys: any[], callback: () => void) => {
                    this._getDB(table).mget(keys, (err, result) => {
                        if (err) {
                            callback();
                            return;
                        }
                        let rows = result.map(r => JSON.parse(r)).sort((a, b) => a[pkKey] > b[pkKey] ? 1 : -1);
                        let i = 0;
                        const getRow = () => {
                            if (rows.length > i) {
                                rowCallback(rows[i], i, () => {
                                    i++;
                                    i > 1000 ? setFast(getRow) : getRow(); // handle maximum call stack error
                                });
                            } else {
                                callback();
                            }
                        };
                        getRow();
                    });
                }
    
                if (index.length < 5000) {
                    getBatch(index, complete);
                } else {
                    let batchKeys: any[][] = [];
                    let batchKeyIdx = 0;
                    for (let i = 0; i < index.length; i++) {
                        if (i > 0 && i % 1000 === 0) {
                            batchKeyIdx++;
                        }
                        if (!batchKeys[batchKeyIdx]) {
                            batchKeys[batchKeyIdx] = [];
                        }
                        batchKeys[batchKeyIdx].push(index[i]);
                    }
                    fastCHAIN(batchKeys, (keys, i, done) => {
                        getBatch(keys, done);
                    }).then(complete);
                }
            }, from, to, usePK);


    }

    public drop(table: string, callback: () => void): void {

            this._getIndex(table, (idx) => {
                this._getDB(table).del(this._key(table, "_index"), () => {
                    fastALL(idx, (item, i , done) => {
                        this._getDB(table).del(item, done);
                    }).then(callback);
                });
            });

    }

    public getIndex(table: string, getLength: boolean, complete: (index) => void): void {
        this._getIndex(table, (idx) => {
            complete(getLength ? idx.length : idx);
        });
    }

    public destroy(complete: () => void) {
        if (this.multipleDBs) {
            fastALL(Object.keys(this._DBIds), (table, i ,done) => {
                this._getDB(table).flushall(done);
            }).then(complete);
        } else {
            this._db.flushall(() => {
                complete();
            })
        }

    }

    public setNSQL(nsql: NanoSQLInstance) {
        /**
         * Uses redis pub/sub to maintain event system across clients
         */
        this._sub.on("message", (channel, msg) => {
            if (channel === "nsql") {
                const data = JSON.parse(msg);
                if (data.source !== this._clientID) {
                    nsql.triggerEvent(data.event);
                }
            }
        });
        nsql.table("*").on("*", (event) => {
            if (event.table && event.table.indexOf("_") !== 0) {
                this._pub.publish("nsql", JSON.stringify({
                    source: this._clientID,
                    event: {
                        ...event,
                        affectedRows: []
                    }
                }));
            }
        });
    }
}