import type {Config} from 'jest';
import { createDefaultEsmPreset } from 'ts-jest'

const config: Config = {
  verbose: true,
  testMatch: ['**/__tests__/**/*.test.ts'],
  ...createDefaultEsmPreset(),
  transform: {
    "\\.[jt]s?$": [
      "ts-jest",
      {
        useESM: true
      }
    ]
  },
  moduleNameMapper: {
    "(.+)\\.js": "$1"
  },
  extensionsToTreatAsEsm: [
    ".ts"
  ],
};

export default config;
