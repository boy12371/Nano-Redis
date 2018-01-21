import { NanoSQLStorageAdapter, DBKey, DBRow, _NanoSQLStorage } from "nano-sql/lib/database/storage";
import { DataModel } from "nano-sql/lib/index";
import { setFast } from "lie-ts";
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
export class redisAdapter implements NanoSQLStorageAdapter {


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

    private _filename: string;
    private _mode: any;

    constructor(public connectArgs: redis.ClientOpts) {
        this._pkKey = {};
        this._pkType = {};
        this._doAI = {};
    }

    public setID(id: string) {
        this._id = id;
    }

    public connect(complete: () => void) {

        this._db = redis.createClient(this.connectArgs);

        this._db.on("ready", complete);
    }

    private _getIndex(table: string, complete: (idx: any[]) => void) {
        this._db.sort(table + "::index", "ALPHA", (err, keys) => {
            if (err) {
                complete([]);
                return;
            }
            complete(keys);
        });
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

        const doInsert = (oldData: any) => {
            const r = {
                ...oldData,
                ...newData,
                [this._pkKey[table]]: pk
            }
            fastALL([0, 1], (item, i, done) => {
                if (i === 0) {
                    this._db.sadd(table + "::index", String(pk), done);
                } else {
                    this._db.set(table + "::" + r[pkKey], JSON.stringify(r), (err, reply) => {
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

            this._db.incr(table + "::_AI", (err, result) => {
                pk = result as any;
                doInsert({});
            });
        }
    }

    public delete(table: string, pk: DBKey, complete: () => void): void {
        fastALL([0, 1], (item, i, done) => {
            if (i === 0) {
                this._db.srem(table + "::index", String(pk), done);
            } else {
                this._db.del(table + "::" + pk, done);
            }
        }).then(() => {
            complete();
        })
    }

    public batchRead(table: string, pks: DBKey[], callback: (rows: any[]) => void) {
        const keys = pks.map(k => table + "::" + k);

        this._db.mget(keys, (err, result) => {
            callback(result && result.length ? result.map(r => JSON.parse(r)) : []);
        });
    }


    public read(table: string, pk: DBKey, callback: (row: DBRow) => void): void {
        this._db.get(table + "::" + pk, (err, result) => {
            if (err) throw err;
            callback(result ? JSON.parse(result) : undefined);
        });
    }

    public rangeRead(table: string, rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, complete: () => void, from?: any, to?: any, usePK?: boolean): void {

        const usefulValues = [typeof from, typeof to].indexOf("undefined") === -1;

        this.getIndex(table, false, (index) => {
            let keys = index

            if (usefulValues && usePK) {
                keys = index.filter((k, i) => k >= from && k <= to);
            } else if (usefulValues) {
                keys = index.filter((k, i) => i >= from && i <= to);
            }

            keys = keys.map(k => table + "::" + k);

            this._db.mget(keys, (err, result) => {
                if (err) {
                    complete();
                    return;
                }
                let rows = result.map(r => JSON.parse(r));
                let i = 0;
                const getRow = () => {
                    if (rows.length > i) {
                        rowCallback(rows[i], i, () => {
                            i++;
                            i > 1000 ? setFast(getRow) : getRow(); // handle maximum call stack error
                        });
                    } else {
                        complete();
                    }
                };
                getRow();
            });
        });
    }

    public drop(table: string, callback: () => void): void {
        this._getIndex(table, (idx) => {
            this._db.del(table + "::index", () => {
                fastALL(idx, (item, i , done) => {
                    this._db.del(item, done);
                }).then(callback);
            });
        });
    }

    public getIndex(table: string, getLength: boolean, complete: (index) => void): void {
        const isNum = ["float", "number", "int"].indexOf(this._pkType[table]) !== -1;
        this._getIndex(table, (idx) => {
            complete(getLength ? idx.length : (isNum ? idx.map(i => parseFloat(i)).sort((a, b) => a > b ? 1 : -1) : idx.sort()));
        });
    }

    public destroy(complete: () => void) {
        this._db.flushall(() => {
            complete();
        })
    }
}