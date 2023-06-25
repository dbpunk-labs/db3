export default {
    roots: ['<rootDir>'],
    preset: 'ts-jest',
    testEnvironment: 'node',
    transform: {
        '\\.[jt]sx?$': 'babel-jest',
    },
    testPathIgnorePatterns: ['/node_modules/', '/thirdparty/', '/src/'],
    setupFilesAfterEnv: ['<rootDir>/jest.setup.ts'],
    testTimeout: 10000000,
}
