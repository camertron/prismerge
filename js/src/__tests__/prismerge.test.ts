import { it, expect } from "@jest/globals"
import { Owner, schema, TodoList } from "./schema";
import { Connection } from "../connection";
import { prismerge } from "../merge";
import sqlite3 from 'sqlite3'
import { open } from 'sqlite';

async function applySchema(conn: Connection) {
  await Owner.setup(conn);
  await TodoList.setup(conn);
}

async function createConnection(): Promise<Connection> {
  return new Connection(await open({filename: ":memory:", driver: sqlite3.Database}))
}

async function createConnections(): Promise<[Connection, Connection, Connection]> {
  const first = await createConnection();
  const second = await createConnection();
  const merged = await createConnection();

  await applySchema(first);
  await applySchema(second);

  return [first, second, merged];
}

it("merges tables with no foreign keys", async () => {
  const [first, second, merged] = await createConnections();

  const woody = await Owner.create(first, "Woody");
  const jessie = await Owner.create(second, "Jessie");
  const bo = await Owner.create(second, "Bo");

  await prismerge(
    schema,
    [first, second],
    merged,
    1,
    false
  );

  const records = await Owner.allByName(merged);
  expect(records.size).toEqual(3);

  // Jessie and Bo are part of the primary because there are more records in
  // that db (2 vs 1). Because they're in the primary, they retain their old
  // IDs.
  expect(records.get("Jessie")!.name).toEqual("Jessie");
  expect(records.get("Jessie")!.id).toEqual(jessie.id);

  expect(records.get("Bo")!.name).toEqual("Bo");
  expect(records.get("Bo")!.id).toEqual(bo.id);

  // Woody is in the secondary DB and therefore gets a new ID.
  expect(records.get("Woody")!.name).toEqual("Woody");
  expect(records.get("Woody")!.id).not.toEqual(woody.id);
});

it("merges tables with foreign keys", async () => {
  const [first, second, merged] = await createConnections();

  const woody = await Owner.create(first, "Woody");
  const jessie = await Owner.create(second, "Jessie");
  const bo = await Owner.create(second, "Bo");

  await TodoList.create(first, "Groceries", woody.id);
  await TodoList.create(second, "Chores", jessie.id);
  await TodoList.create(second, "Errands", bo.id);

  await prismerge(
    schema,
    [first, second],
    merged,
    1,
    false
  );

  const owners = await Owner.allByName(merged);
  const todoLists = await TodoList.allByName(merged);

  expect(owners.size).toEqual(3);
  expect(todoLists.size).toEqual(3);

  const woodysGroceries = todoLists.get("Groceries")!;
  expect(woodysGroceries.name).toEqual("Groceries");
  expect(woodysGroceries.owner_id).toEqual(owners.get("Woody")!.id);

  const jessiesChores = todoLists.get("Chores")!;
  expect(jessiesChores.name).toEqual("Chores");
  expect(jessiesChores.owner_id).toEqual(owners.get("Jessie")!.id);

  let bosErrands = todoLists.get("Errands")!;
  expect(bosErrands.name).toEqual("Errands");
  expect(bosErrands.owner_id).toEqual(owners.get("Bo")!.id);
});

it("merges duplicate records", async () => {
  const [first, second, merged] = await createConnections();
  const firstWoody = await Owner.create(first, "Woody");
  const secondWoody = await Owner.create(second, "Woody");

  await TodoList.create(first, "Chores", firstWoody.id);
  await TodoList.create(second, "Errands", secondWoody.id);

  await prismerge(
    schema,
    [first, second],
    merged,
    1,
    false
  );

  const owners = await Owner.allByName(merged);
  const todoLists = await TodoList.allByName(merged);

  expect(owners.size).toEqual(1);
  expect(todoLists.size).toEqual(2);

  let mergedWoody = owners.get("Woody")!;
  expect([firstWoody.id, secondWoody.id].includes(mergedWoody.id)).toBeTruthy();

  for (const [_, todo_list] of todoLists) {
    expect(todo_list.owner_id).toEqual(mergedWoody.id);
  }
});
