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

```ts

const account = createRandomAccount()
const client = createClient('http://127.0.0.1:26619', '', account)
const nonce = await syncAccountNonce(client)
// create a database
const { db, result } = await createDocumentDatabase(client, 'desc')
const index: Index = {
      path: '/name',
      indexType: IndexType.StringKey,
}
// create a collection
const { collection, result } = await createCollection(
       db,
      'col',
      [index]
)
// add a document
const [txId, block, order] = await addDoc(collection, {
    name: 'book1',
    author: 'db3 developers',
    tag:'web3',
    time: 1686285013,
})
// query document
const query = '/[name = book1]'
const resultSet = await queryDoc<Book>(
                      collection,
                      query)   
```

## Show Your Support
Please ⭐️ this repository if this project helped you!


# Contribution

## 1. Checkout

```shell
git clone https://github.com/dbpunk-labs/db3.js.git
git submodule update --recursive
```

## 2. Run DB3 Localnet

```shell
cd tools && bash start_localnet.sh
```

## 3. Run Testcase

```shell
git submodule update
# install the dependency
yarn
# generate the protobuf
make
# run test
yarn test
# format the code
yarn prettier --write src
# run benchmark
yarn benny-sdk
```
