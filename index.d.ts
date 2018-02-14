import { NanoSQLStorageAdapter, DBKey, DBRow } from "nano-sql/lib/database/storage";
import { DataModel, NanoSQLInstance } from "nano-sql/lib/index";
import { Promise } from "lie-ts";
import * as redis from "redis";
export declare class RedisAdapter implements NanoSQLStorageAdapter {
    connectArgs: redis.ClientOpts;
    multipleDBs: boolean | undefined;
    private _pkKey;
    private _pkType;
    private _dbIndex;
    private _id;
    private _poolPtr;
    private _dbPool;
    private _dbClients;
    private _pub;
    private _sub;
    private _filename;
    private _mode;
    private _clientID;
    private _DBIds;
    constructor(connectArgs: redis.ClientOpts, multipleDBs?: boolean | undefined);
    setID(id: string): void;
    private _key(table, pk);
    private _getDB(table, increaseRetryCounter?);
    connect(complete: () => void): void;
    updateIndexes(getThemFast?: boolean): Promise<any>;
    makeTable(tableName: string, dataModels: DataModel[]): void;
    write(table: string, pk: DBKey | null, newData: DBRow, complete: (row: DBRow) => void): void;
    delete(table: string, pk: DBKey, complete: () => void): void;
    batchRead(table: string, pks: DBKey[], callback: (rows: any[]) => void): void;
    read(table: string, pk: DBKey, callback: (row: DBRow) => void): void;
    _getIndexRange(table: string, complete: (idx: any[]) => void, from?: any, to?: any, usePK?: boolean): void;
    rangeRead(table: string, rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, complete: () => void, from?: any, to?: any, usePK?: boolean): void;
    mGet(table: string, keys: any[], rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, callback: () => void): void;
    drop(table: string, callback: () => void): void;
    getIndex(table: string, getLength: boolean, complete: (index) => void): void;
    destroy(complete: () => void): void;
    sub(type: string, callback: (eventData: any) => void): void;
    emit(type: string, message: any): void;
    setNSQL(nsql: NanoSQLInstance): void;
}
