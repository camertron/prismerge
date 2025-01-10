import { merge } from "./merge"
import { Command } from "commander"
import fs from "fs";
import readline from "readline";

const program = new Command();

program
  .name("prismerge")
  .description("Merge SQLite databases together using their shared Prisma schema.")
  .version("1.0.0")
  .option("--output-path <path>", "The path of the merged database file.", "./merged.db")
  .option("--remove", "If it exists, delete the database specified by --output-path before merging.", false)
  .option("--min-inserts <number>", "The path of the merged database file.", val => parseInt(val), 1000)
  .option("--keep-id-maps", "After merging is complete, don't drop the temporary tables prismerge creates to keep track of old -> new foreign key mappings.", false)
  .argument("<input paths...>", "Paths to the SQLite database files to merge.");

type CLIOptions = {
  outputPath: string
  remove: boolean,
  minInserts: number
  keepIdMaps: boolean
}

program.parse(process.argv);
const options = program.opts<CLIOptions>();
const inputPaths = program.args;

if (options.remove && fs.existsSync(options.outputPath)) {
  fs.unlinkSync(options.outputPath);
}

if (fs.existsSync(options.outputPath)) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  await new Promise<void>(resolve => {
    rl.question(`Output path '${options.outputPath}' already exists, overwrite it (y/n)? `, answer => {
      if (answer.match(/[yY]/)) {
        fs.unlinkSync(options.outputPath);
        console.log(`Deleted '${options.outputPath}'`);
        resolve();
      } else {
        console.log(`Refusing to write to existing database '${options.outputPath}'`);
        process.exit(1);
      }

      rl.close();
    });
  });
}

await merge(inputPaths, options.outputPath, options.minInserts);
