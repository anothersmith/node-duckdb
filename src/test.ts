import { writeSyntheticParquetFile } from "./tests/synthetic-test-data-generator";
import { join } from "path";

const filePath = join(__dirname, "./large-synth");
console.log(filePath);

async function test() {
    // 5000000
    await writeSyntheticParquetFile(filePath, 2000, true);

}

test();
