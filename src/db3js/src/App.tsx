import { useEffect, useState } from "react";
import reactLogo from "./assets/react.svg";
import { gen_key } from "./pkg/db3_jsdk_bg.wasm";
import * as bip39 from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english";
import "./App.css";

function App() {
	useEffect(() => {
		const mn = bip39.generateMnemonic(wordlist);
		console.log(mn);
		bip39.mnemonicToSeed(mn, "password").then((seed: Uint8Array) => {
			console.log(seed.toString());
			const p = gen_key(seed);
			console.log(p);
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
