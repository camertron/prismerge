use rusqlite::Connection;

/* The InsertManager is a convenient way to insert records in bulk. Every time
 * a record is inserted, the manager adds it to an internal list. When the length
 * of the list exceeds the given threshold, all the records are inserted at once,
 * in bulk.
 *
 * There are two kinds of records managed by the InsertManager - regular reecords,
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
pub struct InsertManager<'a> {
    connection: &'a Connection,
    threshold: u64,
    statements: Vec<String>,
    count: usize
}

impl<'a> InsertManager<'a> {
    pub fn new(connection: &'a Connection, threshold: u64) -> Self {
        InsertManager { connection, threshold, statements: vec![], count: 0 }
    }

    pub fn insert(self: &mut Self, statement: String) -> u64 {
        self.statements.push(statement);
        self.count += 1;
        self.maybe_flush()
    }

    pub fn insert_supporting(self: &mut Self, statement: String) -> u64 {
        self.statements.push(statement);
        self.maybe_flush()
    }

    fn maybe_flush(self: &mut Self) -> u64 {
        if self.statements.len() as u64 >= self.threshold {
            return self.flush();
        }

        0
    }

    pub fn flush(self: &mut Self) -> u64 {
        let batch = format!("BEGIN TRANSACTION; {}; COMMIT;", self.statements.join("; "));
        self.connection.execute_batch(batch.as_str()).unwrap();
        self.statements.clear();
        let count = self.count as u64;
        self.count = 0;
        count
    }
}
