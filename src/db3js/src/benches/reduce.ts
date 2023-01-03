import b from 'benny'
b.suite(
    'Batch Get Query',

    b.add('Reduce two elements', () => {
        ;[1, 2].reduce((a, b) => a + b)
    }),

    b.add('Reduce five elements', () => {
        ;[1, 2, 3, 4, 5].reduce((a, b) => a + b)
    }),

    b.cycle(),
    b.complete(),
    b.save({ file: 'reduce', version: '1.0.0' }),
    b.save({ file: 'reduce', format: 'chart.html' })
)
