
## **Build a dapp with JS**

Start with `npm install db3.js`

**1. Build db3 client**

```typescript
// the key seed
const mnemonic = "...";
// create a wallet
const wallet = DB3BrowserWallet.createNew(mnemonic, "DB3_SECP256K1");
// build db3 client
const client = new DB3Client("http://127.0.0.1:26659", wallet);
```

**2. Init databases and collection instance**

```typescript
// The database address
const databaseAddr = "0x14c4eacfcb43d09b09139a0323d49fbe4ea0d5c9";
// The collection that store data
const collectionName = "message";
const db = new DB3Store(databaseAddr, client);
const collectionIns = await collection(db, collectionName);
```

**3. CRUD data by collection**

```typescript
// add a doc to collection
const result = await addDoc(collectionIns, {
  msg: "hello",
  time: new Date(),
  address: "north",
});
// get all docs from collection
const docs = await getDocs(collectionIns);

// get docs by condition
const re = await getDocs(query(collectionIns, where("docId", "==", "xxxxx")));
```

For a full demo build with `npm install db3.js` go to the repository **[TODO MVC Demo ](https://github.com/dbpunk-labs/db3.js/tree/main/examples)**  
For more info please go to **[db3.js](https://github.com/dbpunk-labs/db3.js)** repository

