import { NanoSQLStorageAdapter, DBKey, DBRow } from "nano-sql/lib/database/storage";
import { DataModel } from "nano-sql/lib/index";
import * as redis from "redis";
export declare class redisAdapter implements NanoSQLStorageAdapter {
    connectArgs: redis.ClientOpts;
    private _pkKey;
    private _pkType;
    private _doAI;
    private _id;
    private _db;
    private _filename;
    private _mode;
    constructor(connectArgs: redis.ClientOpts);
    setID(id: string): void;
    connect(complete: () => void): void;
    private _getIndex(table, complete);
    makeTable(tableName: string, dataModels: DataModel[]): void;
    write(table: string, pk: DBKey | null, newData: DBRow, complete: (row: DBRow) => void, skipReadBeforeWrite: boolean): void;
    delete(table: string, pk: DBKey, complete: () => void): void;
    batchRead(table: string, pks: DBKey[], callback: (rows: any[]) => void): void;
    read(table: string, pk: DBKey, callback: (row: DBRow) => void): void;
    rangeRead(table: string, rowCallback: (row: DBRow, idx: number, nextRow: () => void) => void, complete: () => void, from?: any, to?: any, usePK?: boolean): void;
    drop(table: string, callback: () => void): void;
    getIndex(table: string, getLength: boolean, complete: (index) => void): void;
    destroy(complete: () => void): void;
}
