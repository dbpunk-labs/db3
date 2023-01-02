// @ts-nocheck
import { DB3 } from '../lib/db3'
import { sign, getATestStaticKeypair, getAddress } from '../lib/keys'
import {b} from 'benny';
import {TextDecoder} from "util";
import {expect} from "@jest/globals";

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
b.suite(
    'Example',


    b.add('Async example 1a', async () => {
        await delay(0.2)
    }),

    b.add('DB3 getKey 100 request per session/key size/1', async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        try {
            await db3_instance.submitMutaition(
                {
                    ns: 'my_twitter',
                    gasLimit: 10,
                    data: { test2: 'value123' },
                },
                _sign
            )
            await new Promise((r) => setTimeout(r, 20000))
            await db3_instance.openQuerySession(_sign)

            for (let i = 0; i < 100; i++)
            {

                const queryRes = await db3_instance.getKey({
                    ns: 'my_twitter',
                    keyList: ['test2'],
                })
                const value = new TextDecoder('utf-8').decode(
                    queryRes.batchGetValues!.values[0].value
                )
                expect(value).toBe('value123')
            }
            const closeRes = await db3_instance.closeQuerySession(_sign)
            expect(closeRes).toBeDefined()
        } catch (error) {
            console.error(error)
            throw error
        }
    }),



    b.cycle(),
    b.complete(),
    b.save({ file: 'async_bench_example', version: '1.0.0' }),
    b.save({ file: 'async_bench_example', format: 'chart.html' }),
)