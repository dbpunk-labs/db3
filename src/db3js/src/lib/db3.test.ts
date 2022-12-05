import { describe, expect, test } from "@jest/globals";
import { DB3 } from "./db3";
import { DocStore, DocIndex, DocKey, DocKeyType } from "./doc_store";
import { getATestKeyPair, sign, generateKey } from "./keys";
import { TextEncoder, TextDecoder } from "util";
global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;

describe("test db3js api", () => {
	test("test submitMutation", async () => {
		const [sk, public_key] = await generateKey();
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
        );
        expect(result).toBe("xxx")
	});

    test("test submitMutation", async () => {
		const [sk, public_key] = await generateKey();
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
        const result = await doc_store.insertDocs(doc_index, [transacion], _sign);
        expect(result).toBe("xxx")
	});


});
