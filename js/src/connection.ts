import { Statement, type Database } from 'sqlite';
import { Err, Ok, type Result } from "./result.js";
import { None, Some, type Option } from "./option.js";

export class Connection {
  constructor(public db: Database) {}

  async exec(sql: string): Promise<Result<void, Error>> {
    try {
      return Ok(await this.db.exec(sql));
    } catch (e) {
      return Err(e as Error);
    }
  }

  async execOrError(sql: string): Promise<void> {
    (await this.exec(sql)).unwrap();
  }

  async each<T>(query: string, callback: (result: Result<T, Error>) => Promise<void>): Promise<number> {
    const queue: Promise<void>[] = [];
    let finished = false;
    let readyResolve;
    let readyPromise = new Promise(res => {
      readyResolve = res;
    });

    const worker = (async () => {
      while (true) {
        await readyPromise;

        while (queue.length > 0) {
          await queue.pop();
        }

        if (finished) {
          break;
        } else {
          readyPromise = new Promise(res => {
            readyResolve = res;
          });
        }
      }
    })();

    const count = await this.db.each<T>(query, (err: Error | null, row: T) => {
      if (err) {
        queue.unshift(callback(Err(err)));
        readyResolve!();
      } else {
        queue.unshift(callback(Ok(row)));
        readyResolve!();
      }
    });

    finished = true;
    readyResolve!();
    await worker;

    return count;
  }

  async close(): Promise<void> {
    await this.db.close();
  }

  async prepare(sql: string): Promise<Result<Statement, Error>> {
    try {
      return Ok(await this.db.prepare(sql));
    } catch (e) {
      return Err(e as Error);
    }
  }

  async get<T>(sql: string): Promise<Result<Option<T>, Error>> {
    try {
      const result = await this.db.get<T>(sql);

      if (result) {
        return Ok(Some(result));
      } else {
        return Ok(None());
      }
    } catch (e) {
      return Err(e as Error);
    }
  }
}
