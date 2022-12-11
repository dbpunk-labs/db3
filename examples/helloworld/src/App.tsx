import { useEffect, useMemo, useState } from "react";
import { Input, Button } from "antd";
import { useAsyncFn } from "react-use";
import { DB3, DocStore, DocKeyType,  generateKey, sign, getATestStaticKeypair} from "db3js";
import "./App.css";
import { Buffer as BufferPolyfill } from 'buffer'
declare var Buffer: typeof BufferPolyfill;
globalThis.Buffer = BufferPolyfill;

const db3_instance = new DB3("http://127.0.0.1:26659");
const doc_store = new DocStore(db3_instance);

function App() {
	const [res, insertDoc] = useAsyncFn(async () => {
		async function getSign() {
			const [sk, public_key] = await getATestStaticKeypair();
			async function _sign(data: Uint8Array): Promise<[Uint8Array, Uint8Array]> {
				return [await sign(data, sk), public_key];
			} 
			return _sign;
		};
        const _sign = await getSign();
        const doc_index = {
            keys: [
                {
                    name: 'address',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: 'ns1',
            docName: 'transaction',
        };
        const transacion = {
            address: '0x11111',
            ts: 9527,
            amount: 10,
        };
        try {
            const result = await doc_store.insertDocs(doc_index, [transacion], _sign, 1);
            console.log(result);
        }catch(error) {
            console.log(error);
        }
	}, []);

    const [docs, queryDoc] = useAsyncFn(async ()=>  {
        async function getSign() {
			const [sk, public_key] = await getATestStaticKeypair();
			async function _sign(data: Uint8Array): Promise<[Uint8Array, Uint8Array]> {
				return [await sign(data, sk), public_key];
			} 
			return _sign;
		};
        const _sign = await getSign();

        const doc_index = {
            keys: [
                {
                    name: 'address',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: 'ns1',
            docName: 'transaction',
        };
        const query = {
            address: '0x11111',
            ts: 9527,
        };
        try {
            const docs = await doc_store.getDocs(doc_index, [query], _sign);
            console.log(docs);
        } catch(error) {
            console.log(error);
        }
    }, []);

	return (
		<div className='App'>
			<Button onClick={() => insertDoc()}>insertDoc</Button>
			<Button onClick={() => queryDoc()}>queryDoc</Button>
		</div>
	);
}

export default App;
