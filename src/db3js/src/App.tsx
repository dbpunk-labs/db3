import { useEffect, useMemo, useState } from "react";
import { DB3, generateMnemonic, generateKey } from "./lib/db3";
import { Input, Button } from "antd";
import { useAsyncFn } from "react-use";
import "./App.css";

const db3 = new DB3("https://grpc.devnet.db3.network/");

function App() {
	const [sk, setSk] = useState();
	useEffect(() => {
		const mn = generateMnemonic();
		generateKey(mn, "password").then(([pk, sk]) => {
			setSk(sk);
		});
	}, []);

	const [mutationData, setMutationData] = useState('{"key123": "value123"}');

	const [res, submitMutation] = useAsyncFn(
		async (data) => {
			if (!sk) return;
			try {
				const res = await db3.submitMutaition(
					{
						ns: "my_twitter",
						gasLimit: 10,
						data: JSON.parse(data),
					},
					sk,
				);
				return res;
			} catch (error) {
				console.error(error);
			}
		},
		[sk],
	);

	return (
		<div className='App'>
			<Input.TextArea
				value={mutationData}
				onChange={(e) => setMutationData(e.target.value)}
			/>
			<Button onClick={() => submitMutation(mutationData)}>
				Submit mutation
			</Button>
			<div>{res.value && JSON.stringify(res.value)}</div>
		</div>
	);
}

export default App;
