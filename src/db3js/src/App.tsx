import { useEffect, useState } from "react";
import reactLogo from "./assets/react.svg";
import * as bip39 from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english";
import { gen_key, sign } from "./pkg/db3_jsdk";
import "./App.css";

function App() {
	useEffect(() => {
		const mn = bip39.generateMnemonic(wordlist);
		bip39.mnemonicToSeed(mn, "password").then((seed: Uint8Array) => {
			console.log(seed.toString());
			const [pk, sk] = gen_key(seed);
			const signature = sign(new TextEncoder().encode("test"), sk);
			console.log(signature);
		});
	}, []);

	return (
		<div className='App'>
			<div>
				<a href='https://vitejs.dev' target='_blank'>
					<img src='/vite.svg' className='logo' alt='Vite logo' />
				</a>
				<a href='https://reactjs.org' target='_blank'>
					<img
						src={reactLogo}
						className='logo react'
						alt='React logo'
					/>
				</a>
			</div>
			<h1>Vite + React</h1>
		</div>
	);
}

export default App;
