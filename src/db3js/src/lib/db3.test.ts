import { describe, expect, test } from "@jest/globals";
import { DB3 } from "./db3";
import { generateKey, sign } from "./keys";
import { TextEncoder, TextDecoder } from "util";
global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;

describe("test db3js api", () => {
	const db3_instance = new DB3("http://127.0.0.1:26659");
	async function getSign() {
		const [sk, public_key] = await generateKey();

		async function _sign(
			data: Uint8Array,
		): Promise<[Uint8Array, Uint8Array]> {
			return [await sign(data, sk), public_key];
		}
		return _sign;
	}

	test("test submitMutation", async () => {
		const _sign = await getSign();
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
	test("test openQuerySession", async () => {
		const _sign = await getSign();
		try {
			const res = await db3_instance.openQuerySession(_sign);
			console.log(res);
			expect(res).toBeDefined();
		} catch (error) {
			console.error(error);
		}
	});
});
