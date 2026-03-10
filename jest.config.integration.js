module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/*.integration.test.ts'],
  testTimeout: 30000,
  setupFilesAfterEnv: ['<rootDir>/src/test/setup.integration.ts'],
};
