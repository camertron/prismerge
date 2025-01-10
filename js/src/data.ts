import type { Connection } from "./connection";
import { Some, None, type Option } from "./option";
import * as toposort from 'toposort';
import { Err, Ok, type Result } from "./result";

export class Relation {
  constructor(
    public fields: string[],
    public references: string[]
  ) {}
}

export class ColumnType {
  constructor(
    public name: string,
    public collection: boolean,
    public nullable: boolean
  ) {}
}

export class Column {
  constructor(
    public name: string,
    public ty: ColumnType,
    public relation: Option<Relation>,
    public unique: boolean,
    public primary_key: boolean
  ) {}

  hasRelation(): boolean {
    return this.relation.isSome();
  }

  getRelatedColumn(model: Model): Option<Column> {
    for (const column of model.columns) {
      const relation = column.relation;

      if (relation.isSome()) {
        if (relation.unwrap().fields.includes(this.name)) {
          return Some(column);
        }
      }
    }

    return None()
  }

  quoted(modelName: string): string {
    return `quote("${modelName}"."${this.name}")`;
  }

  isRegular(schema: Schema): boolean {
    return (
      !this.primary_key &&
      !this.ty.collection &&
      !this.hasRelation() &&
      !schema.models.has(this.ty.name)
    );
  }
}

export class Unique {
  constructor(public columnNames: string[]) {}
}

export class Model {
  public name: string;
  public columns: Column[];
  public unique: Option<Unique>;
  public mapTable: MapTable;
  public primaryKeyIndex: Option<number>;

  constructor(name: string, columns: Column[], unique: Option<Unique>) {
    let primaryKeyIndex: Option<number> = None();

    for (let idx = 0; idx < columns.length; idx ++) {
      const column = columns[idx]!;

      if (column.primary_key) {
        primaryKeyIndex = Some(idx);
      }
    }

    this.name = name;
    this.columns = columns;
    this.unique = unique;
    this.mapTable = new MapTable(name);
    this.primaryKeyIndex = primaryKeyIndex;
  }

  get primaryKey(): Option<Column> {
    if (this.primaryKeyIndex.isSome()) {
      return Some(this.columns[this.primaryKeyIndex.unwrap()]!);
    }

    return None();
  }

  // Return the column with the given name.
  getCol(name: string): Option<Column> {
    for (const column of this.columns) {
      if (column.name == name) {
        return Some(column);
      }
    }

    return None();
  }

  // Check that all foreign keys point to existing records. Returns the count of
  // rows that have bad/missing foreign keys.
  async verifyIntegrity(conn: Connection): Promise<Result<void, number>> {
    const check = await conn.get<{count: number}>(`SELECT COUNT(*) FROM pragma_foreign_key_check('${this.name}');`);
    if (check.isOk()) {
      const count = check.unwrap().unwrap().count;

      if (count > 0) {
        return Err(count);
      }
    }

    return Ok();
  }
}

export class MapTable {
  public name: string;

  constructor(modelName: string) {
    this.name = `${modelName}_id_map`;
  }

  async createInto(conn: Connection): Promise<void> {
    await conn.execOrError(
      `
        CREATE TABLE ${this.name} (
            old_id TEXT NOT NULL,
            new_id TEXT NOT NULL
        )
      `
    );
  }

  async dropFrom(conn: Connection) {
    await conn.execOrError(
      `
        DROP INDEX IF EXISTS "${this.name}_old_id";
        DROP INDEX IF EXISTS "${this.name}_new_id";
        DROP INDEX IF EXISTS "${this.name}_new_id_old_id";
        DROP TABLE IF EXISTS "${this.name}";
      `
    );
  }

  async createIndices(conn: Connection) {
    await conn.execOrError(
      `
        CREATE INDEX "${this.name}_old_id" ON "${this.name}"("old_id");
        CREATE INDEX "${this.name}_new_id" ON "${this.name}"("new_id");
        CREATE INDEX "${this.name}_new_id_old_id" ON "${this.name}"("new_id", "old_id");
      `
    );
  }
}

export class Schema {
  public models: Map<String, Model>;

  constructor() {
    this.models = new Map();
  }

  sortedModels(): Model[] {
    const edges: [string, string | undefined][] = [];

    for (const [_, model] of this.models) {
      for (const column of model.columns) {
        if (column.relation.isSome()) {
          const childModel = this.models.get(column.ty.name);

          if (childModel) {
            edges.push([model.name, childModel.name]);
          }
        }
      }
    }

    const modelNames = toposort.array<string>(
      Array.from(this.models.values()).map(model => model.name),
      edges
    );

    return modelNames.map(modelName => this.models.get(modelName)!).reverse();
  }
}
