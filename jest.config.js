const path = require("path");

module.exports = {
  collectCoverage: true,
  coverageReporters: ["lcov", "text"],
  roots: ["<rootDir>"],
  testMatch: ["<rootDir>/dist/**/?(*.)+(test).js"],
  setupFilesAfterEnv: ["jest-extended"],
};
