export default {
    preset: 'ts-jest',
    testEnvironment: 'jsdom',
    transform: {
        '\\.[jt]sx?$': 'babel-jest',
    },
}
