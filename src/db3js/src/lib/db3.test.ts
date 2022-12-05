import { describe, expect, test } from "@jest/globals";
import { DB3 } from "./db3";
import { DocStore, DocIndex, DocKey, DocKeyType, genPrimaryKey, object2Buffer } from "./doc_store";
import { sign, getATestStaticKeypair } from "./keys";
import { TextEncoder, TextDecoder } from "util";
global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;

describe("test db3js api", () => {
	test("test submitMutation", async () => {
		const [sk, public_key] = await getATestStaticKeypair();
		const db3_instance = new DB3("http://127.0.0.1:26659");
		async function _sign(data: Uint8Array) {
			return [await sign(data, sk), public_key];
		}
        const result = await db3_instance.submitMutaition(
            {
                ns: "my_twitter",
                gasLimit: 10,
                data: { key123: "value123" },
            },
            _sign,
            1
        );
        expect(result.hash).toBe("ZgvV60A4yTiUGTg8f1YJInyPUvOrfwsIHE4HMgruhX8=");
	});
    test("gen primary key", async () => {
        const doc_index = {
            keys:[{
               name:"address",
               keyType: DocKeyType.STRING
            },{
                name:"ts",
                keyType: DocKeyType.NUMBER,
            }],
            ns:"ns1",
            docName: "transaction"
        }
        const transacion = {
            address:"0x11111",
            ts:9527,
        };
        const pk = genPrimaryKey(doc_index, transacion);
        const uint8ToBase64 = (arr: Uint8Array): string =>
    btoa(
        Array(arr.length)
            .fill('')
            .map((_, i) => String.fromCharCode(arr[i]))
            .join('')
    );
        expect(uint8ToBase64(pk)).toBe("dHJhbnNhY3Rpb24weDExMTExAAAAAAAAJTc=");
    });
    test("test submitMutation", async () => {
		const [sk, public_key] = await getATestStaticKeypair();
		const db3_instance = new DB3("http://127.0.0.1:26659");
		async function _sign(data: Uint8Array) {
			return [await sign(data, sk), public_key];
		}
        const doc_store = new DocStore(db3_instance);
        const doc_index = {
            keys:[{
               name:"address",
               keyType: DocKeyType.STRING
            },{
                name:"ts",
                keyType: DocKeyType.NUMBER,
            }],
            ns:"ns1",
            docName: "transaction"
        }
        const transacion = {
            address:"0x11111",
            ts:9527,
        };
        const result = await doc_store.insertDocs(doc_index, [transacion], _sign, 1);
        expect(result.hash).toBe("loC+lmjceq5tQCTBcHDe5/+OiNxgqZLmJR9daudVtH8=");
	});


});
