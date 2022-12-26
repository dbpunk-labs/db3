import * as jspb from 'google-protobuf'

import * as db3_base_pb from './db3_base_pb';


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

export class QuerySession extends jspb.Message {
  getNonce(): number;
  setNonce(value: number): QuerySession;

  getChainId(): db3_base_pb.ChainId;
  setChainId(value: db3_base_pb.ChainId): QuerySession;

  getChainRole(): db3_base_pb.ChainRole;
  setChainRole(value: db3_base_pb.ChainRole): QuerySession;

  getNodeQuerySessionInfo(): QuerySessionInfo | undefined;
  setNodeQuerySessionInfo(value?: QuerySessionInfo): QuerySession;
  hasNodeQuerySessionInfo(): boolean;
  clearNodeQuerySessionInfo(): QuerySession;

  getClientQuerySession(): Uint8Array | string;
  getClientQuerySession_asU8(): Uint8Array;
  getClientQuerySession_asB64(): string;
  setClientQuerySession(value: Uint8Array | string): QuerySession;

  getClientSignature(): Uint8Array | string;
  getClientSignature_asU8(): Uint8Array;
  getClientSignature_asB64(): string;
  setClientSignature(value: Uint8Array | string): QuerySession;

  getClientPublicKey(): Uint8Array | string;
  getClientPublicKey_asU8(): Uint8Array;
  getClientPublicKey_asB64(): string;
  setClientPublicKey(value: Uint8Array | string): QuerySession;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QuerySession.AsObject;
  static toObject(includeInstance: boolean, msg: QuerySession): QuerySession.AsObject;
  static serializeBinaryToWriter(message: QuerySession, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QuerySession;
  static deserializeBinaryFromReader(message: QuerySession, reader: jspb.BinaryReader): QuerySession;
}

export namespace QuerySession {
  export type AsObject = {
    nonce: number,
    chainId: db3_base_pb.ChainId,
    chainRole: db3_base_pb.ChainRole,
    nodeQuerySessionInfo?: QuerySessionInfo.AsObject,
    clientQuerySession: Uint8Array | string,
    clientSignature: Uint8Array | string,
    clientPublicKey: Uint8Array | string,
  }
}

export enum SessionStatus { 
  RUNNING = 0,
  BLOCKED = 1,
  STOP = 2,
}
