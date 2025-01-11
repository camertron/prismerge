import { Connection } from "../connection";
import { Schema, Model, Column, ColumnType, Relation, Unique } from "../data";
import { None, Some } from "../option";
import crypto from "node:crypto";

const schema = new Schema();

schema.models.set(
  "Owner", new Model(
    "Owner", [
      new Column(
        "id",
        new ColumnType(
          "String",
          false,
          false
        ),
        None(),
        false,
        true
      ),

      new Column(
        "name",
        new ColumnType(
          "String",
          false,
          false
        ),
        None(),
        false,
        false
      )
    ],
    Some(new Unique(["name"]))
  )
);

schema.models.set(
  "TodoList", new Model(
    "TodoList", [
      new Column(
        "id",
        new ColumnType(
          "String",
          false,
          false
        ),
        None(),
        false,
        true
      ),

      new Column(
        "name",
        new ColumnType(
          "String",
          false,
          false
        ),
        None(),
        false,
        false
      ),

      new Column(
        "ownerId",
        new ColumnType(
          "String",
          false,
          false,
        ),
        None(),
        false,
        false
      ),

      new Column(
        "owner",
        new ColumnType(
            "Owner",
            false,
            false
        ),
        Some(
          new Relation(
            ["ownerId"],
            ["id"]
          )
        ),
        false,
        false
      )
    ],
    Some(
      new Unique(
        ["name", "ownerId"]
      )
    )
  )
);

class Owner {
  constructor (public id: string, public name: string) {}

  static async setup(conn: Connection): Promise<void> {
    await conn.execOrError(
      `
        CREATE TABLE IF NOT EXISTS "Owner" (
            "id"    TEXT NOT NULL PRIMARY KEY,
            "name"  TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS "Owner_name_key"
        ON "Owner"("name");
      `
    );
  }

  static async create(conn: Connection, name: string): Promise<Owner> {
    let id = crypto.randomUUID();

    await conn.execOrError(
      `INSERT INTO Owner(\"id\", \"name\") VALUES('${id}', '${name}')`
    );

    return new Owner(id, name);
  }

  static async allByName(conn: Connection): Promise<Map<string, Owner>> {
    const result: Map<string, Owner> = new Map();

    await conn.each<{id: string, name: string}>("SELECT * FROM \"Owner\" WHERE 1", async (rowResult) => {
      rowResult.and(row => {
        result.set(row.name, new Owner(row.id, row.name));
      });
    });

    return result;
  }
}

class TodoList {
  constructor(
    public id: String,
    public name: String,
    public owner_id: String
  ) {}

  static async setup(conn: Connection) {
    await conn.execOrError(
      `
        CREATE TABLE IF NOT EXISTS "TodoList" (
            "id"      TEXT NOT NULL PRIMARY KEY,
            "name"    TEXT NOT NULL,
            "ownerId" TEXT NOT NULL,
            CONSTRAINT "TodoList_ownerId_fkey"
                FOREIGN KEY ("ownerId")
                REFERENCES "Owner" ("id")
                ON DELETE RESTRICT
                ON UPDATE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS "TodoList_name_ownerId_key"
        ON "TodoList"("name", "ownerId");
      `
    );
  }

  static async create(conn: Connection, name: string, owner_id: string): Promise<TodoList> {
    let id = crypto.randomUUID();

    await conn.execOrError(
      `INSERT INTO TodoList(\"id\", \"name\", \"ownerId\") VALUES('${id}', '${name}', '${owner_id}')`
    );

    return new TodoList(id, name, owner_id);
  }

  static async allByName(conn: Connection): Promise<Map<string, TodoList>> {
    const result: Map<string, TodoList> = new Map();

    await conn.each<{id: string, name: string, ownerId: string}>("SELECT * FROM \"TodoList\" WHERE 1", async (rowResult) => {
      rowResult.and(row => {
        result.set(row.name, new TodoList(row.id, row.name, row.ownerId));
      });
    });

    return result;
  }
}

export { schema, Owner, TodoList };
