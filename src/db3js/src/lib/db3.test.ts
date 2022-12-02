import { describe, expect, test } from "@jest/globals";
import { DB3 } from "./db3";
import { generateKey, sign } from "./keys";
import { TextEncoder, TextDecoder } from "util";
global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;

describe("test db3js api", () => {
	test("test submitMutation", async () => {
		const [sk] = await generateKey();
		const db3_instance = new DB3("http://127.0.0.1:26659");
		async function _sign(data: Uint8Array) {
			return await sign(data, sk);
		}
        const result = await db3_instance.submitMutaition(
            {
                ns: "my_twitter",
                gasLimit: 10,
                data: { key123: "value123" },
            },
            _sign,
        );
        expect(result).toBeDefined();
	});
});
