import * as jspb from 'google-protobuf'

import * as db3_bill_pb from './db3_bill_pb';
import * as db3_mutation_pb from './db3_mutation_pb';
import * as db3_account_pb from './db3_account_pb';


export class QueryBillRequest extends jspb.Message {
  getHeight(): number;
  setHeight(value: number): QueryBillRequest;

  getStartId(): number;
  setStartId(value: number): QueryBillRequest;

  getEndId(): number;
  setEndId(value: number): QueryBillRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryBillRequest.AsObject;
  static toObject(includeInstance: boolean, msg: QueryBillRequest): QueryBillRequest.AsObject;
  static serializeBinaryToWriter(message: QueryBillRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryBillRequest;
  static deserializeBinaryFromReader(message: QueryBillRequest, reader: jspb.BinaryReader): QueryBillRequest;
}

export namespace QueryBillRequest {
  export type AsObject = {
    height: number,
    startId: number,
    endId: number,
  }
}

export class QueryBillResponse extends jspb.Message {
  getBillsList(): Array<db3_bill_pb.Bill>;
  setBillsList(value: Array<db3_bill_pb.Bill>): QueryBillResponse;
  clearBillsList(): QueryBillResponse;
  addBills(value?: db3_bill_pb.Bill, index?: number): db3_bill_pb.Bill;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryBillResponse.AsObject;
  static toObject(includeInstance: boolean, msg: QueryBillResponse): QueryBillResponse.AsObject;
  static serializeBinaryToWriter(message: QueryBillResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryBillResponse;
  static deserializeBinaryFromReader(message: QueryBillResponse, reader: jspb.BinaryReader): QueryBillResponse;
}

export namespace QueryBillResponse {
  export type AsObject = {
    billsList: Array<db3_bill_pb.Bill.AsObject>,
  }
}

export class Range extends jspb.Message {
  getStart(): Uint8Array | string;
  getStart_asU8(): Uint8Array;
  getStart_asB64(): string;
  setStart(value: Uint8Array | string): Range;

  getEnd(): Uint8Array | string;
  getEnd_asU8(): Uint8Array;
  getEnd_asB64(): string;
  setEnd(value: Uint8Array | string): Range;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Range.AsObject;
  static toObject(includeInstance: boolean, msg: Range): Range.AsObject;
  static serializeBinaryToWriter(message: Range, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Range;
  static deserializeBinaryFromReader(message: Range, reader: jspb.BinaryReader): Range;
}

export namespace Range {
  export type AsObject = {
    start: Uint8Array | string,
    end: Uint8Array | string,
  }
}

export class BatchRangeKey extends jspb.Message {
  getNs(): Uint8Array | string;
  getNs_asU8(): Uint8Array;
  getNs_asB64(): string;
  setNs(value: Uint8Array | string): BatchRangeKey;

  getRangeList(): Array<Range>;
  setRangeList(value: Array<Range>): BatchRangeKey;
  clearRangeList(): BatchRangeKey;
  addRange(value?: Range, index?: number): Range;

  getSession(): number;
  setSession(value: number): BatchRangeKey;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BatchRangeKey.AsObject;
  static toObject(includeInstance: boolean, msg: BatchRangeKey): BatchRangeKey.AsObject;
  static serializeBinaryToWriter(message: BatchRangeKey, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BatchRangeKey;
  static deserializeBinaryFromReader(message: BatchRangeKey, reader: jspb.BinaryReader): BatchRangeKey;
}

export namespace BatchRangeKey {
  export type AsObject = {
    ns: Uint8Array | string,
    rangeList: Array<Range.AsObject>,
    session: number,
  }
}

export class BatchGetKey extends jspb.Message {
  getNs(): Uint8Array | string;
  getNs_asU8(): Uint8Array;
  getNs_asB64(): string;
  setNs(value: Uint8Array | string): BatchGetKey;

  getKeysList(): Array<Uint8Array | string>;
  setKeysList(value: Array<Uint8Array | string>): BatchGetKey;
  clearKeysList(): BatchGetKey;
  addKeys(value: Uint8Array | string, index?: number): BatchGetKey;

  getSession(): number;
  setSession(value: number): BatchGetKey;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BatchGetKey.AsObject;
  static toObject(includeInstance: boolean, msg: BatchGetKey): BatchGetKey.AsObject;
  static serializeBinaryToWriter(message: BatchGetKey, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BatchGetKey;
  static deserializeBinaryFromReader(message: BatchGetKey, reader: jspb.BinaryReader): BatchGetKey;
}

export namespace BatchGetKey {
  export type AsObject = {
    ns: Uint8Array | string,
    keysList: Array<Uint8Array | string>,
    session: number,
  }
}

export class BatchGetValue extends jspb.Message {
  getValuesList(): Array<db3_mutation_pb.KVPair>;
  setValuesList(value: Array<db3_mutation_pb.KVPair>): BatchGetValue;
  clearValuesList(): BatchGetValue;
  addValues(value?: db3_mutation_pb.KVPair, index?: number): db3_mutation_pb.KVPair;

  getSession(): number;
  setSession(value: number): BatchGetValue;

  getNs(): Uint8Array | string;
  getNs_asU8(): Uint8Array;
  getNs_asB64(): string;
  setNs(value: Uint8Array | string): BatchGetValue;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BatchGetValue.AsObject;
  static toObject(includeInstance: boolean, msg: BatchGetValue): BatchGetValue.AsObject;
  static serializeBinaryToWriter(message: BatchGetValue, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BatchGetValue;
  static deserializeBinaryFromReader(message: BatchGetValue, reader: jspb.BinaryReader): BatchGetValue;
}

export namespace BatchGetValue {
  export type AsObject = {
    valuesList: Array<db3_mutation_pb.KVPair.AsObject>,
    session: number,
    ns: Uint8Array | string,
  }
}

export class GetKeyRequest extends jspb.Message {
  getBatchGet(): Uint8Array | string;
  getBatchGet_asU8(): Uint8Array;
  getBatchGet_asB64(): string;
  setBatchGet(value: Uint8Array | string): GetKeyRequest;

  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): GetKeyRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetKeyRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetKeyRequest): GetKeyRequest.AsObject;
  static serializeBinaryToWriter(message: GetKeyRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetKeyRequest;
  static deserializeBinaryFromReader(message: GetKeyRequest, reader: jspb.BinaryReader): GetKeyRequest;
}

export namespace GetKeyRequest {
  export type AsObject = {
    batchGet: Uint8Array | string,
    signature: Uint8Array | string,
  }
}

export class GetKeyResponse extends jspb.Message {
  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): GetKeyResponse;

  getBatchGetValues(): BatchGetValue | undefined;
  setBatchGetValues(value?: BatchGetValue): GetKeyResponse;
  hasBatchGetValues(): boolean;
  clearBatchGetValues(): GetKeyResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetKeyResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetKeyResponse): GetKeyResponse.AsObject;
  static serializeBinaryToWriter(message: GetKeyResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetKeyResponse;
  static deserializeBinaryFromReader(message: GetKeyResponse, reader: jspb.BinaryReader): GetKeyResponse;
}

export namespace GetKeyResponse {
  export type AsObject = {
    signature: Uint8Array | string,
    batchGetValues?: BatchGetValue.AsObject,
  }
}

export class RestartSessionRequest extends jspb.Message {
  getQuerySessionInfo(): Uint8Array | string;
  getQuerySessionInfo_asU8(): Uint8Array;
  getQuerySessionInfo_asB64(): string;
  setQuerySessionInfo(value: Uint8Array | string): RestartSessionRequest;

  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): RestartSessionRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RestartSessionRequest.AsObject;
  static toObject(includeInstance: boolean, msg: RestartSessionRequest): RestartSessionRequest.AsObject;
  static serializeBinaryToWriter(message: RestartSessionRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RestartSessionRequest;
  static deserializeBinaryFromReader(message: RestartSessionRequest, reader: jspb.BinaryReader): RestartSessionRequest;
}

export namespace RestartSessionRequest {
  export type AsObject = {
    querySessionInfo: Uint8Array | string,
    signature: Uint8Array | string,
  }
}

export class RestartSessionResponse extends jspb.Message {
  getSession(): number;
  setSession(value: number): RestartSessionResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RestartSessionResponse.AsObject;
  static toObject(includeInstance: boolean, msg: RestartSessionResponse): RestartSessionResponse.AsObject;
  static serializeBinaryToWriter(message: RestartSessionResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RestartSessionResponse;
  static deserializeBinaryFromReader(message: RestartSessionResponse, reader: jspb.BinaryReader): RestartSessionResponse;
}

export namespace RestartSessionResponse {
  export type AsObject = {
    session: number,
  }
}

export class GetAccountRequest extends jspb.Message {
  getAddr(): string;
  setAddr(value: string): GetAccountRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetAccountRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetAccountRequest): GetAccountRequest.AsObject;
  static serializeBinaryToWriter(message: GetAccountRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetAccountRequest;
  static deserializeBinaryFromReader(message: GetAccountRequest, reader: jspb.BinaryReader): GetAccountRequest;
}

export namespace GetAccountRequest {
  export type AsObject = {
    addr: string,
  }
}

export class GetSessionInfoRequest extends jspb.Message {
  getAddr(): string;
  setAddr(value: string): GetSessionInfoRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetSessionInfoRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetSessionInfoRequest): GetSessionInfoRequest.AsObject;
  static serializeBinaryToWriter(message: GetSessionInfoRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetSessionInfoRequest;
  static deserializeBinaryFromReader(message: GetSessionInfoRequest, reader: jspb.BinaryReader): GetSessionInfoRequest;
}

export namespace GetSessionInfoRequest {
  export type AsObject = {
    addr: string,
  }
}

export class QuerySessionInfo extends jspb.Message {
  getId(): number;
  setId(value: number): QuerySessionInfo;

  getStartTime(): number;
  setStartTime(value: number): QuerySessionInfo;

  getStatus(): SessionStatus;
  setStatus(value: SessionStatus): QuerySessionInfo;

  getQueryCount(): number;
  setQueryCount(value: number): QuerySessionInfo;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QuerySessionInfo.AsObject;
  static toObject(includeInstance: boolean, msg: QuerySessionInfo): QuerySessionInfo.AsObject;
  static serializeBinaryToWriter(message: QuerySessionInfo, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QuerySessionInfo;
  static deserializeBinaryFromReader(message: QuerySessionInfo, reader: jspb.BinaryReader): QuerySessionInfo;
}

export namespace QuerySessionInfo {
  export type AsObject = {
    id: number,
    startTime: number,
    status: SessionStatus,
    queryCount: number,
  }
}

export class GetSessionInfoResponse extends jspb.Message {
  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): GetSessionInfoResponse;

  getSessionInfo(): QuerySessionInfo | undefined;
  setSessionInfo(value?: QuerySessionInfo): GetSessionInfoResponse;
  hasSessionInfo(): boolean;
  clearSessionInfo(): GetSessionInfoResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetSessionInfoResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetSessionInfoResponse): GetSessionInfoResponse.AsObject;
  static serializeBinaryToWriter(message: GetSessionInfoResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetSessionInfoResponse;
  static deserializeBinaryFromReader(message: GetSessionInfoResponse, reader: jspb.BinaryReader): GetSessionInfoResponse;
}

export namespace GetSessionInfoResponse {
  export type AsObject = {
    signature: Uint8Array | string,
    sessionInfo?: QuerySessionInfo.AsObject,
  }
}

export class BroadcastRequest extends jspb.Message {
  getBody(): Uint8Array | string;
  getBody_asU8(): Uint8Array;
  getBody_asB64(): string;
  setBody(value: Uint8Array | string): BroadcastRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BroadcastRequest.AsObject;
  static toObject(includeInstance: boolean, msg: BroadcastRequest): BroadcastRequest.AsObject;
  static serializeBinaryToWriter(message: BroadcastRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BroadcastRequest;
  static deserializeBinaryFromReader(message: BroadcastRequest, reader: jspb.BinaryReader): BroadcastRequest;
}

export namespace BroadcastRequest {
  export type AsObject = {
    body: Uint8Array | string,
  }
}

export class BroadcastResponse extends jspb.Message {
  getHash(): Uint8Array | string;
  getHash_asU8(): Uint8Array;
  getHash_asB64(): string;
  setHash(value: Uint8Array | string): BroadcastResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): BroadcastResponse.AsObject;
  static toObject(includeInstance: boolean, msg: BroadcastResponse): BroadcastResponse.AsObject;
  static serializeBinaryToWriter(message: BroadcastResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): BroadcastResponse;
  static deserializeBinaryFromReader(message: BroadcastResponse, reader: jspb.BinaryReader): BroadcastResponse;
}

export namespace BroadcastResponse {
  export type AsObject = {
    hash: Uint8Array | string,
  }
}

export enum SessionStatus { 
  RUNNING = 0,
  BLOCKED = 1,
}
