// @ts-ignore
import { DB3 } from '../lib/db3'
import { sign, getATestStaticKeypair, getAddress } from '../lib/keys'
import b from 'benny'
import { TextDecoder } from 'util'

const delay = (seconds: number) =>
    new Promise((resolve) => setTimeout(resolve, seconds * 1000))
async function getSign() {
    const [sk, public_key] = await getATestStaticKeypair()
    async function _sign(data: Uint8Array): Promise<[Uint8Array, Uint8Array]> {
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
            // const value = new TextDecoder('utf-8').decode(
            //     queryRes.batchGetValues!.values[0].value
            // )
            // expect(value).toBe('value123')
        }
        const closeRes = await db3_instance.closeQuerySession(_sign)
        // expect(closeRes).toBeDefined()
    } catch (error) {
        console.error(error)
        throw error
    }
}

b.suite(
    'DB3 JS SDK Benchmark',
    b.add(`batch get key 1000 requests per session/keys size/1`, async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        console.log('submit mutation start')
        await db3_instance.submitMutaition(
            {
                ns: 'my_twitter',
                gasLimit: 10,
                data: {
                    bm_key1: 'bm_value1',
                },
            },
            _sign
        )
        const keyList = ['bm_key1']
        await new Promise((r) => setTimeout(r, 2000))
        console.log('submit mutation done')
        return async () => {
            await run_1000_get_key('my_twitter', keyList)
        }
    }),

    b.add(`batch get key 1000 requests per session/keys size/10`, async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        console.log('submit mutation start')
        await db3_instance.submitMutaition(
            {
                ns: 'my_twitter',
                gasLimit: 10,
                data: {
                    bm_key1: 'bm_value1',
                    bm_key2: 'bm_value2',
                    bm_key3: 'bm_value3',
                    bm_key4: 'bm_value4',
                    bm_key5: 'bm_value5',
                    bm_key6: 'bm_value6',
                    bm_key7: 'bm_value7',
                    bm_key8: 'bm_value8',
                    bm_key9: 'bm_value9',
                    bm_key10: 'bm_value10',
                },
            },
            _sign
        )
        const keyList = [
            'bm_key1',
            'bm_key2',
            'bm_key3',
            'bm_key4',
            'bm_key5',
            'bm_key6',
            'bm_key7',
            'bm_key8',
            'bm_key9',
            'bm_key10',
        ]
        await new Promise((r) => setTimeout(r, 2000))
        console.log('submit mutation done')
        return async () => {
            await run_1000_get_key('my_twitter', keyList)
        }
    }),

    b.cycle(),
    b.complete(),
    b.configure({
        minDisplayPrecision: 3,
    }),
    b.save({ file: 'js_sdk_benchmark', details: true, version: '1.0.0' }),
    b.save({ file: 'js_sdk_benchmark', details: true, format: 'chart.html' })
)
