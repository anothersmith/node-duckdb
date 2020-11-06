const path = require("path");

module.exports = {
  env: {
    node: true,
  },
  extends: ["eslint-config-deepcrawl"],
  rules: {
    "@typescript-eslint/no-explicit-any": "off",
    "clean-code/feature-envy": "off",
  },
  overrides: [
    {
      files: ["**/*.test.ts"],
      rules: {
        "max-lines-per-function": "off",
        "@typescript-eslint/naming-convention": "off",
      },
    },
  ],
  parserOptions: {
    ecmaVersion: 2019,
    project: [path.resolve(__dirname, "./tsconfig.json")],
    sourceType: "module",
  },
  root: true,
};
