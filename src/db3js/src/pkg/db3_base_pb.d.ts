import * as jspb from 'google-protobuf'



export class Units extends jspb.Message {
  getUtype(): UnitType;
  setUtype(value: UnitType): Units;

  getAmount(): number;
  setAmount(value: number): Units;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Units.AsObject;
  static toObject(includeInstance: boolean, msg: Units): Units.AsObject;
  static serializeBinaryToWriter(message: Units, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Units;
  static deserializeBinaryFromReader(message: Units, reader: jspb.BinaryReader): Units;
}

export namespace Units {
  export type AsObject = {
    utype: UnitType,
    amount: number,
  }
}

export class Erc20Token extends jspb.Message {
  getSymbal(): string;
  setSymbal(value: string): Erc20Token;

  getUnitsList(): Array<string>;
  setUnitsList(value: Array<string>): Erc20Token;
  clearUnitsList(): Erc20Token;
  addUnits(value: string, index?: number): Erc20Token;

  getScalarList(): Array<number>;
  setScalarList(value: Array<number>): Erc20Token;
  clearScalarList(): Erc20Token;
  addScalar(value: number, index?: number): Erc20Token;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Erc20Token.AsObject;
  static toObject(includeInstance: boolean, msg: Erc20Token): Erc20Token.AsObject;
  static serializeBinaryToWriter(message: Erc20Token, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Erc20Token;
  static deserializeBinaryFromReader(message: Erc20Token, reader: jspb.BinaryReader): Erc20Token;
}

export namespace Erc20Token {
  export type AsObject = {
    symbal: string,
    unitsList: Array<string>,
    scalarList: Array<number>,
  }
}

export class Price extends jspb.Message {
  getAmount(): number;
  setAmount(value: number): Price;

  getUnit(): string;
  setUnit(value: string): Price;

  getToken(): Erc20Token | undefined;
  setToken(value?: Erc20Token): Price;
  hasToken(): boolean;
  clearToken(): Price;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Price.AsObject;
  static toObject(includeInstance: boolean, msg: Price): Price.AsObject;
  static serializeBinaryToWriter(message: Price, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Price;
  static deserializeBinaryFromReader(message: Price, reader: jspb.BinaryReader): Price;
}

export namespace Price {
  export type AsObject = {
    amount: number,
    unit: string,
    token?: Erc20Token.AsObject,
  }
}

export class BroadcastMeta extends jspb.Message {
  getNonce(): number;
  setNonce(value: number): BroadcastMeta;

  getChainId(): ChainId;
  setChainId(value: ChainId): BroadcastMeta;

  getChainRole(): ChainRole;
  setChainRole(value: ChainRole): BroadcastMeta;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BroadcastMeta.AsObject;
  static toObject(includeInstance: boolean, msg: BroadcastMeta): BroadcastMeta.AsObject;
  static serializeBinaryToWriter(message: BroadcastMeta, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BroadcastMeta;
  static deserializeBinaryFromReader(message: BroadcastMeta, reader: jspb.BinaryReader): BroadcastMeta;
}

export namespace BroadcastMeta {
  export type AsObject = {
    nonce: number,
    chainId: ChainId,
    chainRole: ChainRole,
  }
}

export enum UnitType { 
  DB3 = 0,
  TAI = 1,
}
export enum ChainRole { 
  SETTLEMENTCHAIN = 0,
  STORAGESHARDCHAIN = 10,
  DVMCOMPUTINGCHAIN = 20,
}
export enum ChainId { 
  MAINNET = 0,
  TESTNET = 10,
  DEVNET = 20,
}
