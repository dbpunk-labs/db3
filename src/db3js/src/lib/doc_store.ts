import { DB3} from "./db3"
import db3_mutation_pb from "../pkg/db3_mutation_pb";
import { SmartBuffer, SmartBufferOptions} from "smart-buffer";
import * as jspb from 'google-protobuf';

// the type of doc key 
// the string and number are supported
export enum DocKeyType {
    STRING = 0,
    NUMBER,
}

export interface DocKey {
    name: string;
    keyType: DocKeyType;    
}

export interface DocIndex {
    keys: DocKey[];
    ns:string,
    docName:string,
}

function genPrimaryKey(index:DocIndex, doc:Object) {
    const buff = new SmartBuffer();
    type ObjectKey = keyof typeof doc;
    // write the doc name to the key
    buff.writeString(index.docName);
    index.keys.forEach((key: DocKey)=> {
        switch (key.keyType) {
            case DocKeyType.STRING: {
                const objectKey = key.name as ObjectKey;
                let value = doc[objectKey]
                buff.writeString(value as unknown as string);
                break;
            }
            case DocKeyType.NUMBER: {
                const objectKey = key.name as ObjectKey;
                let value = doc[objectKey];
                buff.writeBigInt64BE(BigInt(value as unknown as number));
                break;
            }
        }
    });
    return buff.toBuffer();
}

export class DocStore {
    private db3: DB3;
    constructor(db3: DB3) {
		this.db3 = db3;
	}

    async insertDocs(index:DocIndex, docs:Object[], sign: (target: Uint8Array) => [Uint8Array, Uint8Array]) {
        const kvPairs: db3_mutation_pb.KVPair[] = [];
        docs.forEach((doc:Object)=>{
            const key = genPrimaryKey(index, doc);
			const kvPair = new db3_mutation_pb.KVPair();
            kvPair.setKey(key);
			kvPair.setValue(JSON.stringify(doc));
			kvPair.setAction(db3_mutation_pb.MutationAction.INSERTKV);
            kvPairs.push(kvPair);
        });
        return await this.db3.submitRawMutation(index.ns, kvPairs, sign);
    }
}
