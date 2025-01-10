import { Prisma } from "@prisma/client";
import { Column, ColumnType, Model, Relation, Schema, Unique } from "./data.js"
import type { DMMF } from "@prisma/client/runtime/library";
import { Some, None, type Option } from "./option.js";

export function getSchema(): Schema {
  const schema = new Schema();

  Prisma.dmmf.datamodel.models.forEach((model: DMMF.Model) => {
    schema.models.set(model.name, handleModel(model));
  });

  return schema;
}

function handleModel(model: DMMF.Model): Model {
  const columns = [];

  for (const column of model.fields) {
    columns.push(handleColumn(column));
  }

  let unique: Option<Unique> = None();

  if (model.uniqueFields[0]) {
    unique = Some(new Unique(Array.from(model.uniqueFields[0])));
  } else {
    const uniqueCol = columns.find(col => col.unique);

    if (uniqueCol) {
      unique = Some(new Unique([uniqueCol.name]));
    }
  }

  return new Model(model.name, columns, unique);
}

function handleColumn(column: DMMF.Field) {
  return new Column(
    column.name,
    columnTypeFromColumn(column),
    relationFromColumn(column),
    column.isUnique,
    column.isId
  );
}

function columnTypeFromColumn(column: DMMF.Field) {
  return new ColumnType(column.type, column.isList, !column.isReadOnly);
}

function relationFromColumn(column: DMMF.Field): Option<Relation> {
  if (column.relationToFields && column.relationToFields.length > 0 && column.relationFromFields && column.relationFromFields.length > 0) {
    return Some(
      new Relation(
        Array.from(column.relationFromFields),
        Array.from(column.relationToFields)
      )
    );
  }

  return None();
}
