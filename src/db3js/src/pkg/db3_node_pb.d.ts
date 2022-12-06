import * as jspb from 'google-protobuf'

import * as db3_bill_pb from './db3_bill_pb';
import * as db3_mutation_pb from './db3_mutation_pb';
import * as db3_account_pb from './db3_account_pb';


export class QueryBillKey extends jspb.Message {
  getHeight(): number;
  setHeight(value: number): QueryBillKey;

  getStartId(): number;
  setStartId(value: number): QueryBillKey;

  getEndId(): number;
  setEndId(value: number): QueryBillKey;

  getSessionToken(): string;
  setSessionToken(value: string): QueryBillKey;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryBillKey.AsObject;
  static toObject(includeInstance: boolean, msg: QueryBillKey): QueryBillKey.AsObject;
  static serializeBinaryToWriter(message: QueryBillKey, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryBillKey;
  static deserializeBinaryFromReader(message: QueryBillKey, reader: jspb.BinaryReader): QueryBillKey;
}

export namespace QueryBillKey {
  export type AsObject = {
    height: number,
    startId: number,
    endId: number,
    sessionToken: string,
  }
}

export class QueryBillRequest extends jspb.Message {
  getQueryBillKey(): QueryBillKey | undefined;
  setQueryBillKey(value?: QueryBillKey): QueryBillRequest;
  hasQueryBillKey(): boolean;
  clearQueryBillKey(): QueryBillRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryBillRequest.AsObject;
  static toObject(includeInstance: boolean, msg: QueryBillRequest): QueryBillRequest.AsObject;
  static serializeBinaryToWriter(message: QueryBillRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryBillRequest;
  static deserializeBinaryFromReader(message: QueryBillRequest, reader: jspb.BinaryReader): QueryBillRequest;
}

export namespace QueryBillRequest {
  export type AsObject = {
    queryBillKey?: QueryBillKey.AsObject,
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

export class RangeKey extends jspb.Message {
  getNs(): Uint8Array | string;
  getNs_asU8(): Uint8Array;
  getNs_asB64(): string;
  setNs(value: Uint8Array | string): RangeKey;

  getRange(): Range | undefined;
  setRange(value?: Range): RangeKey;
  hasRange(): boolean;
  clearRange(): RangeKey;

  getSessionToken(): string;
  setSessionToken(value: string): RangeKey;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RangeKey.AsObject;
  static toObject(includeInstance: boolean, msg: RangeKey): RangeKey.AsObject;
  static serializeBinaryToWriter(message: RangeKey, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RangeKey;
  static deserializeBinaryFromReader(message: RangeKey, reader: jspb.BinaryReader): RangeKey;
}

export namespace RangeKey {
  export type AsObject = {
    ns: Uint8Array | string,
    range?: Range.AsObject,
    sessionToken: string,
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

  getSessionToken(): string;
  setSessionToken(value: string): BatchGetKey;

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
    sessionToken: string,
  }
}

export class RangeValue extends jspb.Message {
  getValuesList(): Array<db3_mutation_pb.KVPair>;
  setValuesList(value: Array<db3_mutation_pb.KVPair>): RangeValue;
  clearValuesList(): RangeValue;
  addValues(value?: db3_mutation_pb.KVPair, index?: number): db3_mutation_pb.KVPair;

  getSessionToken(): string;
  setSessionToken(value: string): RangeValue;

  getNs(): Uint8Array | string;
  getNs_asU8(): Uint8Array;
  getNs_asB64(): string;
  setNs(value: Uint8Array | string): RangeValue;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RangeValue.AsObject;
  static toObject(includeInstance: boolean, msg: RangeValue): RangeValue.AsObject;
  static serializeBinaryToWriter(message: RangeValue, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RangeValue;
  static deserializeBinaryFromReader(message: RangeValue, reader: jspb.BinaryReader): RangeValue;
}

export namespace RangeValue {
  export type AsObject = {
    valuesList: Array<db3_mutation_pb.KVPair.AsObject>,
    sessionToken: string,
    ns: Uint8Array | string,
  }
}

export class BatchGetValue extends jspb.Message {
  getValuesList(): Array<db3_mutation_pb.KVPair>;
  setValuesList(value: Array<db3_mutation_pb.KVPair>): BatchGetValue;
  clearValuesList(): BatchGetValue;
  addValues(value?: db3_mutation_pb.KVPair, index?: number): db3_mutation_pb.KVPair;

  getSessionToken(): string;
  setSessionToken(value: string): BatchGetValue;

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
    sessionToken: string,
    ns: Uint8Array | string,
  }
}

export class SessionIdentifier extends jspb.Message {
  getSessionToken(): string;
  setSessionToken(value: string): SessionIdentifier;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): SessionIdentifier.AsObject;
  static toObject(includeInstance: boolean, msg: SessionIdentifier): SessionIdentifier.AsObject;
  static serializeBinaryToWriter(message: SessionIdentifier, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): SessionIdentifier;
  static deserializeBinaryFromReader(message: SessionIdentifier, reader: jspb.BinaryReader): SessionIdentifier;
}

export namespace SessionIdentifier {
  export type AsObject = {
    sessionToken: string,
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

export class GetKeyRequest extends jspb.Message {
  getBatchGet(): BatchGetKey | undefined;
  setBatchGet(value?: BatchGetKey): GetKeyRequest;
  hasBatchGet(): boolean;
  clearBatchGet(): GetKeyRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetKeyRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetKeyRequest): GetKeyRequest.AsObject;
  static serializeBinaryToWriter(message: GetKeyRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetKeyRequest;
  static deserializeBinaryFromReader(message: GetKeyRequest, reader: jspb.BinaryReader): GetKeyRequest;
}

export namespace GetKeyRequest {
  export type AsObject = {
    batchGet?: BatchGetKey.AsObject,
  }
}

export class GetKeyResponse extends jspb.Message {
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
    batchGetValues?: BatchGetValue.AsObject,
  }
}

export class GetRangeRequest extends jspb.Message {
  getRangeKeys(): RangeKey | undefined;
  setRangeKeys(value?: RangeKey): GetRangeRequest;
  hasRangeKeys(): boolean;
  clearRangeKeys(): GetRangeRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetRangeRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetRangeRequest): GetRangeRequest.AsObject;
  static serializeBinaryToWriter(message: GetRangeRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetRangeRequest;
  static deserializeBinaryFromReader(message: GetRangeRequest, reader: jspb.BinaryReader): GetRangeRequest;
}

export namespace GetRangeRequest {
  export type AsObject = {
    rangeKeys?: RangeKey.AsObject,
  }
}

export class GetRangeResponse extends jspb.Message {
  getRangeValue(): RangeValue | undefined;
  setRangeValue(value?: RangeValue): GetRangeResponse;
  hasRangeValue(): boolean;
  clearRangeValue(): GetRangeResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetRangeResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetRangeResponse): GetRangeResponse.AsObject;
  static serializeBinaryToWriter(message: GetRangeResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetRangeResponse;
  static deserializeBinaryFromReader(message: GetRangeResponse, reader: jspb.BinaryReader): GetRangeResponse;
}

export namespace GetRangeResponse {
  export type AsObject = {
    rangeValue?: RangeValue.AsObject,
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
  getSessionIdentifier(): SessionIdentifier | undefined;
  setSessionIdentifier(value?: SessionIdentifier): GetSessionInfoRequest;
  hasSessionIdentifier(): boolean;
  clearSessionIdentifier(): GetSessionInfoRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetSessionInfoRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetSessionInfoRequest): GetSessionInfoRequest.AsObject;
  static serializeBinaryToWriter(message: GetSessionInfoRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetSessionInfoRequest;
  static deserializeBinaryFromReader(message: GetSessionInfoRequest, reader: jspb.BinaryReader): GetSessionInfoRequest;
}

export namespace GetSessionInfoRequest {
  export type AsObject = {
    sessionIdentifier?: SessionIdentifier.AsObject,
  }
}

export class OpenSessionRequest extends jspb.Message {
  getHeader(): Uint8Array | string;
  getHeader_asU8(): Uint8Array;
  getHeader_asB64(): string;
  setHeader(value: Uint8Array | string): OpenSessionRequest;

  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): OpenSessionRequest;

  getPublicKey(): Uint8Array | string;
  getPublicKey_asU8(): Uint8Array;
  getPublicKey_asB64(): string;
  setPublicKey(value: Uint8Array | string): OpenSessionRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): OpenSessionRequest.AsObject;
  static toObject(includeInstance: boolean, msg: OpenSessionRequest): OpenSessionRequest.AsObject;
  static serializeBinaryToWriter(message: OpenSessionRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): OpenSessionRequest;
  static deserializeBinaryFromReader(message: OpenSessionRequest, reader: jspb.BinaryReader): OpenSessionRequest;
}

export namespace OpenSessionRequest {
  export type AsObject = {
    header: Uint8Array | string,
    signature: Uint8Array | string,
    publicKey: Uint8Array | string,
  }
}

export class OpenSessionResponse extends jspb.Message {
  getQuerySessionInfo(): QuerySessionInfo | undefined;
  setQuerySessionInfo(value?: QuerySessionInfo): OpenSessionResponse;
  hasQuerySessionInfo(): boolean;
  clearQuerySessionInfo(): OpenSessionResponse;

  getSessionTimeoutSecond(): number;
  setSessionTimeoutSecond(value: number): OpenSessionResponse;

  getMaxQueryLimit(): number;
  setMaxQueryLimit(value: number): OpenSessionResponse;

  getSessionToken(): string;
  setSessionToken(value: string): OpenSessionResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): OpenSessionResponse.AsObject;
  static toObject(includeInstance: boolean, msg: OpenSessionResponse): OpenSessionResponse.AsObject;
  static serializeBinaryToWriter(message: OpenSessionResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): OpenSessionResponse;
  static deserializeBinaryFromReader(message: OpenSessionResponse, reader: jspb.BinaryReader): OpenSessionResponse;
}

export namespace OpenSessionResponse {
  export type AsObject = {
    querySessionInfo?: QuerySessionInfo.AsObject,
    sessionTimeoutSecond: number,
    maxQueryLimit: number,
    sessionToken: string,
  }
}

export class CloseSessionPayload extends jspb.Message {
  getSessionInfo(): QuerySessionInfo | undefined;
  setSessionInfo(value?: QuerySessionInfo): CloseSessionPayload;
  hasSessionInfo(): boolean;
  clearSessionInfo(): CloseSessionPayload;

  getSessionToken(): string;
  setSessionToken(value: string): CloseSessionPayload;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CloseSessionPayload.AsObject;
  static toObject(includeInstance: boolean, msg: CloseSessionPayload): CloseSessionPayload.AsObject;
  static serializeBinaryToWriter(message: CloseSessionPayload, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CloseSessionPayload;
  static deserializeBinaryFromReader(message: CloseSessionPayload, reader: jspb.BinaryReader): CloseSessionPayload;
}

export namespace CloseSessionPayload {
  export type AsObject = {
    sessionInfo?: QuerySessionInfo.AsObject,
    sessionToken: string,
  }
}

export class CloseSessionRequest extends jspb.Message {
  getPayload(): Uint8Array | string;
  getPayload_asU8(): Uint8Array;
  getPayload_asB64(): string;
  setPayload(value: Uint8Array | string): CloseSessionRequest;

  getSignature(): Uint8Array | string;
  getSignature_asU8(): Uint8Array;
  getSignature_asB64(): string;
  setSignature(value: Uint8Array | string): CloseSessionRequest;

  getPublicKey(): Uint8Array | string;
  getPublicKey_asU8(): Uint8Array;
  getPublicKey_asB64(): string;
  setPublicKey(value: Uint8Array | string): CloseSessionRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CloseSessionRequest.AsObject;
  static toObject(includeInstance: boolean, msg: CloseSessionRequest): CloseSessionRequest.AsObject;
  static serializeBinaryToWriter(message: CloseSessionRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CloseSessionRequest;
  static deserializeBinaryFromReader(message: CloseSessionRequest, reader: jspb.BinaryReader): CloseSessionRequest;
}

export namespace CloseSessionRequest {
  export type AsObject = {
    payload: Uint8Array | string,
    signature: Uint8Array | string,
    publicKey: Uint8Array | string,
  }
}

export class CloseSessionResponse extends jspb.Message {
  getQuerySessionInfo(): QuerySessionInfo | undefined;
  setQuerySessionInfo(value?: QuerySessionInfo): CloseSessionResponse;
  hasQuerySessionInfo(): boolean;
  clearQuerySessionInfo(): CloseSessionResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CloseSessionResponse.AsObject;
  static toObject(includeInstance: boolean, msg: CloseSessionResponse): CloseSessionResponse.AsObject;
  static serializeBinaryToWriter(message: CloseSessionResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CloseSessionResponse;
  static deserializeBinaryFromReader(message: CloseSessionResponse, reader: jspb.BinaryReader): CloseSessionResponse;
}

export namespace CloseSessionResponse {
  export type AsObject = {
    querySessionInfo?: QuerySessionInfo.AsObject,
  }
}

export class GetSessionInfoResponse extends jspb.Message {
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
  STOP = 2,
}
