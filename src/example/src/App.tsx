import { useEffect, useMemo, useState } from "react";
import { Input, Button } from "antd";
import { useAsyncFn } from "react-use";
import { DB3, generateKey, sign } from "db3js";
import "./App.css";

const db3_instance = new DB3("https://grpc.devnet.db3.network/");

function App() {
	const [res, submitMutation] = useAsyncFn(async () => {
		const [sk, pk] = await generateKey();
		function _sign(data: Uint8Array) {
			return sign(data, sk);
		}

		try {
			const result = await db3_instance.submitMutaition(
				{
					ns: "my_twitter",
					gasLimit: 10,
					data: { key123: "value123" },
				},
				_sign,
			);
			console.log(result);
		} catch (error) {
			console.error(error);
		}
	}, []);

	return (
		<div className='App'>
			{/* <Input.TextArea
				value={mutationData}
				onChange={(e) => setMutationData(e.target.value)}
			/> */}
			<Button onClick={() => submitMutation()}>Submit mutation</Button>
			{/* <div>{res.value && JSON.stringify(res.value)}</div> */}
		</div>
	);
}

export default App;
