// Generate ts/types.ts from the JSON Schemas published by fault-model.
//
//   node scripts/generate-types.mjs          rewrite ts/types.ts
//   node scripts/generate-types.mjs --check  fail when ts/types.ts is stale

import { readFile, readdir, writeFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { compile } from "json-schema-to-typescript";

const schemas = new URL("../../docs/schemas/", import.meta.url);
const output = new URL("../ts/types.ts", import.meta.url);
const root = "FaultSchemas";

const definitions = {};
const titles = [];

function define(name, schema, source) {
  if (name in definitions && !isDeepStrictEqual(definitions[name], schema)) {
    throw new Error(`${source} redefines ${name} differently`);
  }
  definitions[name] = schema;
}

for (const file of (await readdir(schemas)).filter((f) => f.endsWith(".json")).sort()) {
  const { $schema, $defs = {}, title, ...schema } = JSON.parse(
    await readFile(new URL(file, schemas), "utf8"),
  );
  for (const [name, definition] of Object.entries($defs)) {
    define(name, definition, file);
  }
  define(title, schema, file);
  titles.push(title);
}

// schemars documents a referencing property beside its `$ref`; the generator
// would turn each such site into a duplicate named type, so keep the bare
// reference and let the referenced definition carry the documentation.
function bareReferences(value) {
  if (Array.isArray(value)) return value.map(bareReferences);
  if (value === null || typeof value !== "object") return value;
  if (typeof value.$ref === "string") return { $ref: value.$ref };
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, bareReferences(item)]),
  );
}

const merged = {
  title: root,
  type: "object",
  additionalProperties: false,
  properties: Object.fromEntries(
    titles.map((title) => [title, { $ref: `#/$defs/${title}` }]),
  ),
  $defs: bareReferences(definitions),
};

const banner = `// Generated from docs/schemas by scripts/generate-types.mjs.
// Do not edit by hand; run \`npm run generate:types\` instead.
`;

const generated = (
  await compile(merged, root, {
    bannerComment: banner,
    additionalProperties: false,
    strictIndexSignatures: true,
    format: true,
    style: { singleQuote: false, semi: true, trailingComma: "all" },
  })
).replace(new RegExp(`export interface ${root} \\{[\\s\\S]*?\\n\\}\\n\\n?`), "");

if (process.argv.includes("--check")) {
  const committed = await readFile(output, "utf8").catch(() => "");
  if (committed !== generated) {
    console.error("ts/types.ts is stale; run `npm run generate:types`");
    process.exit(1);
  }
} else {
  await writeFile(output, generated);
}
