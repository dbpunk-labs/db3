// @ts-ignore
import { DB3} from '../lib/db3'
import { sign, getATestStaticKeypair, getAddress } from '../lib/keys'
import b from 'benny';
import {TextDecoder} from "util";

const delay = (seconds: number) =>
    new Promise((resolve) => setTimeout(resolve, seconds * 1000))
async function getSign() {
    const [sk, public_key] = await getATestStaticKeypair()
    async function _sign(
        data: Uint8Array
    ): Promise<[Uint8Array, Uint8Array]> {
        return [await sign(data, sk), public_key]
    }
    return _sign
}

async function run_1000_get_key(ns: string, keyList: string[]) {
    const db3_instance = new DB3('http://127.0.0.1:26659')
    const _sign = await getSign()
    try {
        await db3_instance.openQuerySession(_sign)
        for (let i = 0; i < 1000; i++) {
            // console.log("process %d", i);
            const queryRes = await db3_instance.getKey({
                ns: ns,
                keyList: keyList,
            })
            const value_size = queryRes.batchGetValues!.values.length;
            // expect(value_size).toBe(keyList.length);
        }
        const closeRes = await db3_instance.closeQuerySession(_sign)
        // expect(closeRes).toBeDefined()
    } catch (error) {
        console.error(error)
        throw error
    }
}
function benchmark_data(size: number) : Record<string, string> {
    let kvs : Record<string, string> = {};
    for (let i = 0; i < 10; i++) {
        kvs["bm_key_" + i] = "bm_value_" + i;
    }
    return kvs;
}
async function submit_mutation(ns: string, kvs: Record<string, string>) {
    const db3_instance = new DB3('http://127.0.0.1:26659')
    const _sign = await getSign()
    console.log("submit mutation start");
    await db3_instance.submitMutaition(
        {
            ns: ns,
            gasLimit: 10,
            data: kvs,
        },
        _sign
    );
    // await new Promise((r) => setTimeout(r, 2000))
    await delay(2);
    console.log("submit mutation done");
}
b.suite(
    'DB3 JS SDK Benchmark',
    b.add(`batch get key 1000 requests per session/keys size/1`, async () => {
        const ns:string = "bm_ns_test";
        let kvs : Record<string, string> = benchmark_data(1);

        await submit_mutation(ns, kvs);

        const keyList:string[] = Object.keys(kvs) as Array<string>

        console.log("Warming up start...")
        // warm up
        for (let i = 0; i < 3; i++) {
            await run_1000_get_key(ns, keyList);
        }
        console.log("Warming up done...")

        return async () => {
            await run_1000_get_key(ns, keyList);
        }
    }),
    b.add(`batch get key 1000 requests per session/keys size/10`, async () => {
        const ns:string = "bm_ns_test";
        let kvs : Record<string, string> = benchmark_data(10);

        await submit_mutation(ns, kvs);

        const keyList:string[] = Object.keys(kvs) as Array<string>

        console.log("Warming up start...")
        // warm up
        for (let i = 0; i < 3; i++) {
            await run_1000_get_key(ns, keyList);
        }
        console.log("Warming up done...")

        return async () => {
            await run_1000_get_key(ns, keyList);
        }
    }),
    b.add(`batch get key 1000 requests per session/keys size/100`, async () => {
        const ns:string = "bm_ns_test";
        let kvs : Record<string, string> = benchmark_data(100);

        await submit_mutation(ns, kvs);

        const keyList:string[] = Object.keys(kvs) as Array<string>

        console.log("Warming up start...")
        // warm up
        for (let i = 0; i < 3; i++) {
            await run_1000_get_key(ns, keyList);
        }
        console.log("Warming up done...")

        return async () => {
            await run_1000_get_key(ns, keyList);
        }
    }),

    b.cycle(),
    b.complete(),
    b.configure({
        minDisplayPrecision: 3,
    }),
    b.save({ file: 'js_sdk_benchmark', details: true, version: '1.0.0' }),
    b.save({ file: 'js_sdk_benchmark', details: true, format: 'chart.html' }),
)