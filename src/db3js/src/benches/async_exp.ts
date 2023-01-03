import b from 'benny'
const delay = (seconds: number) =>
    new Promise((resolve) => setTimeout(resolve, seconds * 1000))

b.suite(
    'Example',

    b.add('Async example 1a', async () => {
        await delay(0.2)
    }),

    b.add('Async example 1b', async () => {
        await delay(0.05)
    }),

    b.cycle(),
    b.complete(),
    b.save({ file: 'async_bench_example', version: '1.0.0' }),
    b.save({ file: 'async_bench_example', format: 'chart.html' })
)
