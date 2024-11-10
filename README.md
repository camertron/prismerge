[![Tests](https://github.com/camertron/prismerge/actions/workflows/test.yml/badge.svg)](https://github.com/camertron/prismerge/actions/workflows/test.yml)

## prismerge

Merge SQLite databases together using their shared Prisma schema.

## Preamble

Prismerge is a tool written specifically for merging SQLite databases together for an internal tool at GitHub that allows engineers working on the Primer design system (https://primer.style) to easily identify React and Rails component usages across various repositories and package versions.

**NOTE**: Prismerge may or may not work for your use-case. Please read this entire document carefully to understand how it works and what it assumes about your schema and data model.

## Introduction

Prismerge is capable of merging n distinct SQLite databases into a single database, where each database has the same schema defined in a Prisma schema file. Prisma is a JavaScript ORM akin to ActiveRecord in Rails, Ecto in Phoenix, or Hibernate in Java. The schema file enumerates the columns present in each table as well as the
relationships between tables. The information in the schema file is more accurate than what could be gathered by dumping the database's native schema, and also easier to parse. Prismerge copies data from each input database into a final merged database, ensuring foreign key data integrity along the way.

The idea is fairly straightforward: each time a row is inserted into the database, record its old primary key (eg. its "ID") and its new primary key in a separate mapping table. When inserting rows that reference this table via a foreign key, translate the old ID to the new ID before insertion. This way, all copied rows will correctly reference their parent rows in the new database.

## Assumptions

Prismerge assumes several major things about your schema and data model:

1. All tables have a primary key.

2. All primary keys are UUID strings. When inserting new rows, prismerge generates new UUIDs and inserts them into both the merged table and the mapping table. Prismerge is not designed to accommodate non-string, non-UUID primary keys.

3. Primary keys are strictly IDs and not data. For example, a table cannot use a git SHA as a primary key because the merging process involves generating new primary keys for inserted rows. This limitation could perhaps be relaxed in the future.

3. Tables have unique indices to prevent duplicate rows. Prismerge detects the presence of unique indices defined in the Prisma schema and uses them to prevent inserting duplicate rows. For each row in each of the input databases, Prismerge checks the merged database for a row that contains the same data as the current one. If such a row exists, prismerge skips inserting a new row and instead only inserts a mapping table row where the old ID is the ID of the original row and the new ID is the ID of the already inserted row.

4. No cycles. Prismerge assumes no cycles in the graph of relationships between tables, i.e. A depends on B which depends on A again.

## Getting Started

Clone this repository and install a Rust toolchain (I used 1.81.0 to develop prismerge). Run `cargo build --release` in the repository root to build the project. Cargo should put the prismerge executable at target/release/prismerge.

See: https://www.rust-lang.org/tools/install

Once the project has been compiled, you can run the target/release/prismerge executable or use `cargo` to print usage information:

```bash
target/release/prismerge --help
# OR:
cargo run -- --help
```

Refer to the output to construct a command that will do what you want. Here's an example:

```bash
cargo run -- --schema-path path/to/schema.prisma path/to/databases/*.db
```

This will combine the input databases into a single database called merged.db in the current directory.

## License

Licensed under the MIT license. See LICENSE for details.

## Authors

* Cameron C. Dutro: http://github.com/camertron
