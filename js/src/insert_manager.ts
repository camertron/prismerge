import type { Connection } from "./connection.js";

/* The InsertManager is a convenient way to insert records in bulk. Every time
 * a record is inserted, the manager adds it to an internal list. When the length
 * of the list exceeds the given threshold, all the records are inserted at once,
 * in bulk.
 *
 * There are two kinds of records managed by the InsertManager - regular records,
 * and so-called "supporting" records. Supporting records are records that do not
 * contribute to overall merge progress. For prismerge, supporting records are
 * records inserted into ID mapping tables. Other records, i.e. records from
 * input databases, are regular records.
 *
 * Insert regular records using the `insert()` method, `and insert_supporting()`
 * to insert supporting records. The value of the `count` attribute will be
 * incremented for regular records, but not for supporting records. Each insert
 * method returns either 0 or this count value, indicating how many regular
 * records were actually inserted.
 *
 * Call the `flush()` method to force the InsertManager to insert all pending
 * records, regular and otherwise.
 */
export class InsertManager {
  private statements: string[];
  private count: number;

  constructor(
    private connection: Connection,
    private threshold: number
  ) {
    this.statements = [];
    this.count = 0;
  }

  async insert(statement: string): Promise<number> {
    this.statements.push(statement);
    this.count += 1;
    return await this.maybeFlush();
  }

  async insertSupporting(statement: string): Promise<number> {
    this.statements.push(statement);
    return await this.maybeFlush();
  }

  private async maybeFlush(): Promise<number> {
    if (this.statements.length >= this.threshold) {
      return await this.flush();
    }

    return 0;
  }

  async flush(): Promise<number> {
    const batch = `BEGIN TRANSACTION; ${this.statements.join("; ")}; COMMIT;`;
    this.statements.splice(0, this.statements.length);
    const count = this.count;
    this.count = 0;
    await this.connection.execOrError(batch);
    return count;
  }
}
