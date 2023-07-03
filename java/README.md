# Quick Started

the java sdk is under active development, please feel free to ask for help if you have some problems with it.

## DB3 SDK

```xml
<dependency>
    <groupId>network.db3</groupId>
    <artifactId>sdk</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## How to use

```java
// from web3j
ECKeyPair keyPair = Keys.createEcKeyPair();
Client client = new Client("https://rollup.cloud.db3.network", "https://index.cloud.db3.network", keyPair);
// update the nonce for the first time
client.updateNonce();
String db = "0x6ef32f0d8fc6bc872ffa977eb80920a0a75d0206";
String col = "book";
String doc = """{
"name":"The Three-Body Problem",
"author":"Cixin-Liu",
"rate":"4.8"
}""";
AddDocResult addDocResult = client.addDoc(db, col, doc);
ResultSet resultSet = client.runQuery(db, col, """/[author=Cixin-Liu]""");
```
you can the the db3 [console](https://console.cloud.db3.network/console/database/) to create a database




