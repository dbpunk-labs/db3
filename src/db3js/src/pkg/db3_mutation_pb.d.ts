import * as jspb from 'google-protobuf'

import * as db3_base_pb from './db3_base_pb';


export class KVPair extends jspb.Message {
  getKey(): Uint8Array | string;
  getKey_asU8(): Uint8Array;
  getKey_asB64(): string;
  setKey(value: Uint8Array | string): KVPair;

  getValue(): Uint8Array | string;
  getValue_asU8(): Uint8Array;
  getValue_asB64(): string;
  setValue(value: Uint8Array | string): KVPair;

  getAction(): MutationAction;
  setAction(value: MutationAction): KVPair;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): KVPair.AsObject;
  static toObject(includeInstance: boolean, msg: KVPair): KVPair.AsObject;
  static serializeBinaryToWriter(message: KVPair, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): KVPair;
  static deserializeBinaryFromReader(message: KVPair, reader: jspb.BinaryReader): KVPair;
}

export namespace KVPair {
  export type AsObject = {
    key: Uint8Array | string,
    value: Uint8Array | string,
    action: MutationAction,
  }
}

export class Mutation extends jspb.Message {
  getNs(): Uint8Array | string;
  getNs_asU8(): Uint8Array;
  getNs_asB64(): string;
  setNs(value: Uint8Array | string): Mutation;

  getKvPairsList(): Array<KVPair>;
  setKvPairsList(value: Array<KVPair>): Mutation;
  clearKvPairsList(): Mutation;
  addKvPairs(value?: KVPair, index?: number): KVPair;

  getNonce(): number;
  setNonce(value: number): Mutation;

  getChainId(): db3_base_pb.ChainId;
  setChainId(value: db3_base_pb.ChainId): Mutation;

  getChainRole(): db3_base_pb.ChainRole;
  setChainRole(value: db3_base_pb.ChainRole): Mutation;

  getGasPrice(): db3_base_pb.Units | undefined;
  setGasPrice(value?: db3_base_pb.Units): Mutation;
  hasGasPrice(): boolean;
  clearGasPrice(): Mutation;

  getGas(): number;
  setGas(value: number): Mutation;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Mutation.AsObject;
  static toObject(includeInstance: boolean, msg: Mutation): Mutation.AsObject;
  static serializeBinaryToWriter(message: Mutation, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Mutation;
  static deserializeBinaryFromReader(message: Mutation, reader: jspb.BinaryReader): Mutation;
}

export namespace Mutation {
  export type AsObject = {
    ns: Uint8Array | string,
    kvPairsList: Array<KVPair.AsObject>,
    nonce: number,
    chainId: db3_base_pb.ChainId,
    chainRole: db3_base_pb.ChainRole,
    gasPrice?: db3_base_pb.Units.AsObject,
    gas: number,
  }
}

export class WriteRequest extends jspb.Message {
  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): WriteRequest;

  getPayload(): Uint8Array | string;
  getPayload_asU8(): Uint8Array;
  getPayload_asB64(): string;
  setPayload(value: Uint8Array | string): WriteRequest;

  getPublicKey(): Uint8Array | string;
  getPublicKey_asU8(): Uint8Array;
  getPublicKey_asB64(): string;
  setPublicKey(value: Uint8Array | string): WriteRequest;

  getPayloadType(): PayloadType;
  setPayloadType(value: PayloadType): WriteRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): WriteRequest.AsObject;
  static toObject(includeInstance: boolean, msg: WriteRequest): WriteRequest.AsObject;
  static serializeBinaryToWriter(message: WriteRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): WriteRequest;
  static deserializeBinaryFromReader(message: WriteRequest, reader: jspb.BinaryReader): WriteRequest;
}

export namespace WriteRequest {
  export type AsObject = {
    signature: Uint8Array | string,
    payload: Uint8Array | string,
    publicKey: Uint8Array | string,
    payloadType: PayloadType,
  }
}

export enum MutationAction { 
  INSERTKV = 0,
  DELETEKV = 1,
  NONCE = 2,
}
export enum PayloadType { 
  MUTATIONPAYLOAD = 0,
  QUERYSESSIONPAYLOAD = 1,
  NAMESPACEPAYLOAD = 2,
}
