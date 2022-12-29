use criterion::{black_box, criterion_group, criterion_main, Criterion};


#[inline]
fn fibonacci(n: u64) -> u64 {
    let mut a = 0;
    let mut b = 1;

    match n {
        0 => b,
        _ => {
            for _ in 0..n {
                let c = a + b;
                a = b;
                b = c;
            }
            b
        }
    }
}
pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib_20", |b| b.iter(|| fibonacci(black_box(20))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
