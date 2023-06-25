# What is db3.js
![GitHub release (latest by date)](https://img.shields.io/github/v/release/dbpunk-labs/db3.js?color=green&display_name=tag&label=db3.js&logo=db3&logoColor=https%3A%2F%2Favatars.githubusercontent.com%2Fu%2F102341693%3Fs%3D96%26v%3D4&style=flat-square)
![npm](https://img.shields.io/npm/dw/db3.js?style=flat-square)
[![Coveralls branch](https://img.shields.io/coverallsCoverage/github/dbpunk-labs/db3.js?style=flat-square)](https://coveralls.io/github/dbpunk-labs/db3.js)

db3.js is the [db3 network](https://github.com/dbpunk-labs/db3) javascript API and you can use it to write and query JSON documents against the db3 network.
and you can build fully decentralized applications combining [web3.js](https://github.com/web3/web3.js) and db3.js

# Play with db3.js

## Install db3.js

```
yarn add db3.js
```

## Use db3.js in action

```typescript
// create a account
const account = createRandomAccount()
// create the client
const client = createClient('http://127.0.0.1:26619',
                            'http://127.0.0.1:26639', 
                             account)

// get the collection
const collection = await getCollection("0xF7..79", "book", client)

// add a document
const {id} = await addDoc(collection, {
                name:"The Three-Body Problem"，
                author:"Cixin-Liu",
                rate:"4.8"} as Book)
// query the document
const resultSet = await queryDoc<Book>(collection, "/[author=Cixin-Liu]")
```
