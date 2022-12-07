import { describe, expect, test } from "@jest/globals";
import { DB3 } from "./db3";
import { generateKey, sign } from "./keys";
import { TextEncoder, TextDecoder } from "util";
global.TextEncoder = TextEncoder;
global.TextDecoder = TextDecoder;

describe("test db3js api", () => {
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
		const db3_instance = new DB3("http://127.0.0.1:26659");
		const _sign = await getSign();
		const result = await db3_instance.submitMutaition(
			{
				ns: "my_twitter",
				gasLimit: 10,
				data: { test1: "value123" },
			},
			_sign,
		);
		expect(result).toBeDefined();
	});
	test("test openQuerySession", async () => {
		const db3_instance = new DB3("http://127.0.0.1:26659");
		const _sign = await getSign();
		try {
			const { sessionToken } = await db3_instance.openQuerySession(_sign);
			expect(typeof sessionToken).toBe("string");
		} catch (error) {
			throw error;
		}
	});
	test("test getKey", async () => {
		const db3_instance = new DB3("http://127.0.0.1:26659");
		const _sign = await getSign();
		try {
			await db3_instance.submitMutaition(
				{
					ns: "my_twitter",
					gasLimit: 10,
					data: { key123: "value123" },
				},
				_sign,
			);
			const { sessionToken } = await db3_instance.openQuerySession(_sign);
			await new Promise((r) => setTimeout(r, 2000));
			const queryRes = await db3_instance.getKey({
				ns: "my_twitter",
				keyList: ["key123"],
				sessionToken,
			});
			expect(queryRes.batchGetValues?.valuesList[0].value).toBe(
				"value123",
			);
		} catch (error) {
			throw error;
		}
	});
	test("test db3 submit data and query data", async () => {
		const db3_instance = new DB3("http://127.0.0.1:26659");
		const _sign = await getSign();
		try {
			await db3_instance.submitMutaition(
				{
					ns: "my_twitter",
					gasLimit: 10,
					data: { test2: "value123" },
				},
				_sign,
			);
			await new Promise((r) => setTimeout(r, 2000));
			const { sessionToken } = await db3_instance.openQuerySession(_sign);
			const queryRes = await db3_instance.getKey({
				ns: "my_twitter",
				keyList: ["test2"],
				sessionToken,
			});
			expect(queryRes.batchGetValues?.valuesList[0].value).toBe(
				"value123",
			);
			const closeRes = await db3_instance.closeQuerySession(_sign);
			expect(closeRes).toBeDefined();
		} catch (error) {
			console.error(error);
			throw error;
		}
	});

	test("test db3 queryBill", async () => {
		const db3_instance = new DB3("http://127.0.0.1:26659");
		const _sign = await getSign();
		try {
			await db3_instance.openQuerySession(_sign);
			const res = db3_instance.queryBill(1, 0, 10);
			expect(res).toBeDefined();
		} catch (error) {
			throw error;
		}
	});
});
