import * as jspb from 'google-protobuf'

import * as db3_base_pb from './db3_base_pb';


export class Bill extends jspb.Message {
  getGasFee(): db3_base_pb.Units | undefined;
  setGasFee(value?: db3_base_pb.Units): Bill;
  hasGasFee(): boolean;
  clearGasFee(): Bill;

  getBlockHeight(): number;
  setBlockHeight(value: number): Bill;

  getBillId(): number;
  setBillId(value: number): Bill;

  getBillType(): BillType;
  setBillType(value: BillType): Bill;

  getTime(): number;
  setTime(value: number): Bill;

  getBillTargetId(): Uint8Array | string;
  getBillTargetId_asU8(): Uint8Array;
  getBillTargetId_asB64(): string;
  setBillTargetId(value: Uint8Array | string): Bill;

  getOwner(): Uint8Array | string;
  getOwner_asU8(): Uint8Array;
  getOwner_asB64(): string;
  setOwner(value: Uint8Array | string): Bill;

  getQueryAddr(): Uint8Array | string;
  getQueryAddr_asU8(): Uint8Array;
  getQueryAddr_asB64(): string;
  setQueryAddr(value: Uint8Array | string): Bill;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Bill.AsObject;
  static toObject(includeInstance: boolean, msg: Bill): Bill.AsObject;
  static serializeBinaryToWriter(message: Bill, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Bill;
  static deserializeBinaryFromReader(message: Bill, reader: jspb.BinaryReader): Bill;
}

export namespace Bill {
  export type AsObject = {
    gasFee?: db3_base_pb.Units.AsObject,
    blockHeight: number,
    billId: number,
    billType: BillType,
    time: number,
    billTargetId: Uint8Array | string,
    owner: Uint8Array | string,
    queryAddr: Uint8Array | string,
  }
}

export class BillQueryRequest extends jspb.Message {
  getBlockHeight(): number;
  setBlockHeight(value: number): BillQueryRequest;

  getStartId(): number;
  setStartId(value: number): BillQueryRequest;

  getEndId(): number;
  setEndId(value: number): BillQueryRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BillQueryRequest.AsObject;
  static toObject(includeInstance: boolean, msg: BillQueryRequest): BillQueryRequest.AsObject;
  static serializeBinaryToWriter(message: BillQueryRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BillQueryRequest;
  static deserializeBinaryFromReader(message: BillQueryRequest, reader: jspb.BinaryReader): BillQueryRequest;
}

export namespace BillQueryRequest {
  export type AsObject = {
    blockHeight: number,
    startId: number,
    endId: number,
  }
}

export enum BillType { 
  BILLFORMUTATION = 0,
  BILLFORQUERY = 1,
}
