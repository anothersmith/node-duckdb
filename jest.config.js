const path = require("path");

module.exports = {
  collectCoverage: true,
  coverageReporters: ["lcov", "text"],
  roots: ["<rootDir>"],
  testMatch: ["<rootDir>/dist/**/__tests__/**/*.[jt]s?(x)",
  "<rootDir>/dist/**/*(*.)@(spec|test).[tj]s?(x)"
  ],
  setupFilesAfterEnv: ["jest-extended"],
  testEnvironment: "node",
};
