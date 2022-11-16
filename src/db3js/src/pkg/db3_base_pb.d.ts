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
