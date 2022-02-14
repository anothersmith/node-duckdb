/* eslint-disable no-console, no-loops/no-loops, no-restricted-properties, max-lines-per-function */
import { constants, promises as fsPromises } from "fs";

// eslint-disable-next-line node/no-unpublished-import
import { random, times } from "lodash";
// eslint-disable-next-line node/no-unpublished-import
import parquet from "parquetjs";

const { unlink, access } = fsPromises;

const possibleTypes = ["INT32", "INT64", "DOUBLE", "UTF8", "BOOLEAN"];
const numberOfColumnsWithRandomTypes = 250;

// first column types are predetermined, the rest random
const columnTypes = [
  ...possibleTypes,
  ...times(10, () => "BOOLEAN"),
  ...Array.from(
    new Array(numberOfColumnsWithRandomTypes),
    () => possibleTypes[random(0, possibleTypes.length - 1, false)],
  ),
];

// Commented out compression because there seems to be a case-sensitivity incompatibility between
// parquetjs and latest DuckDb: parquetjs only understands all caps ('GZIP'), but DuckDb now requires
// lower case ('gzip')
const schema = Object.assign(
  {},
  ...columnTypes.map((columnType, index) => ({ [`col${index}`]: { type: columnType /* , compression: "GZIP" */ } })),
);

function getRandomValue(columnType: string): number | string | boolean {
  switch (columnType) {
    case "INT32":
      return random(0, 2 ^ 31, false);
    case "INT64":
      return random(0, 2 ^ 63, false);
    case "DOUBLE":
      return random(0, Number.MAX_SAFE_INTEGER, true);
    case "UTF8":
      return String(random(0, Number.MAX_SAFE_INTEGER, true));
    case "BOOLEAN":
      return !!random(0, 1);
    default:
      throw Error(`${columnType} not supported`);
  }
}

export async function writeSyntheticParquetFile(
  path: string,
  numberOfRows: number,
  forceRewrite: boolean,
): Promise<void> {
  let doesFileExist = true;
  try {
    await access(path, constants.F_OK);
  } catch (e) {
    doesFileExist = false;
  }

  if (!forceRewrite && doesFileExist) {
    console.log(`${path} exists`);
    return;
  }

  if (forceRewrite && doesFileExist) {
    await unlink(path);
  }
  console.log("writing synthetic file");
  const writer = await parquet.ParquetWriter.openFile(new parquet.ParquetSchema(schema), path);
  for (let i = 0; i < numberOfRows; i++) {
    const row = Object.assign(
      {},
      ...columnTypes.map((columnType, index) => ({ [`col${index}`]: getRandomValue(columnType) })),
    );
    await writer.appendRow(row);
  }
  await writer.close();
  console.log("finished writing synthetic file");
}
