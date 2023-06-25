/**
 * Copyright 2023 db3 network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// @ts-ignore
import { DB3 } from '../lib/db3'
import { sign, getATestStaticKeypair, getAddress } from '../lib/keys'
import b from 'benny'
const options = {
    minSamples: 1000,
    maxTime: 2,
}
const delay = (mills: number) =>
    new Promise((resolve) => setTimeout(resolve, mills))
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
function benchmark_data(suffix: string, size: number): Record<string, string> {
    let kvs: Record<string, string> = {}
    for (let i = 0; i < size; i++) {
        kvs['bm_key_' + i + suffix] = 'bm_value_' + i + suffix
    }
    return kvs
}

async function send_submit_mutation(suffix: string, qps: number) {
    console.log('send_submit_mutation...')
    const db3_instance = new DB3('http://127.0.0.1:26659')
    const _sign = await getSign()
    const tc = new Date().getMilliseconds()
    for (let i = 0; i < qps; i++) {
        await db3_instance.submitMutaition(
            {
                ns: 'my_twitter',
                gasLimit: 10,
                data: benchmark_data(suffix + tc + '_mutation_', 1),
            },
            _sign
        )
    }
    console.log('send_submit_mutation...end')
}

b.suite(
    'DB3 JS SDK Benchmark',
    b.add(
        `submit mutation 100 requests per session/keys size/1`,
        async () => {
            console.log('submit mutation start')
            return async () => {
                console.log('submit mutation')
                await send_submit_mutation('group1_', 100)
            }
        },
        options
    ),
    b.cycle(),
    b.complete(),
    b.configure({
        minDisplayPrecision: 3,
    }),
    b.save({ file: 'js_sdk_benchmark', details: true, version: '1.0.0' }),
    b.save({ file: 'js_sdk_benchmark', details: true, format: 'chart.html' })
)
