import * as jspb from 'google-protobuf'

import * as db3_base_pb from './db3_base_pb';


export class QueryPrice extends jspb.Message {
  getPrice(): db3_base_pb.Price | undefined;
  setPrice(value?: db3_base_pb.Price): QueryPrice;
  hasPrice(): boolean;
  clearPrice(): QueryPrice;

  getQueryCount(): number;
  setQueryCount(value: number): QueryPrice;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryPrice.AsObject;
  static toObject(includeInstance: boolean, msg: QueryPrice): QueryPrice.AsObject;
  static serializeBinaryToWriter(message: QueryPrice, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryPrice;
  static deserializeBinaryFromReader(message: QueryPrice, reader: jspb.BinaryReader): QueryPrice;
}

export namespace QueryPrice {
  export type AsObject = {
    price?: db3_base_pb.Price.AsObject,
    queryCount: number,
  }
}

export class Namespace extends jspb.Message {
  getName(): string;
  setName(value: string): Namespace;

  getPrice(): QueryPrice | undefined;
  setPrice(value?: QueryPrice): Namespace;
  hasPrice(): boolean;
  clearPrice(): Namespace;

  getTs(): number;
  setTs(value: number): Namespace;

  getDescription(): string;
  setDescription(value: string): Namespace;

  getMeta(): db3_base_pb.BroadcastMeta | undefined;
  setMeta(value?: db3_base_pb.BroadcastMeta): Namespace;
  hasMeta(): boolean;
  clearMeta(): Namespace;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Namespace.AsObject;
  static toObject(includeInstance: boolean, msg: Namespace): Namespace.AsObject;
  static serializeBinaryToWriter(message: Namespace, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Namespace;
  static deserializeBinaryFromReader(message: Namespace, reader: jspb.BinaryReader): Namespace;
}

export namespace Namespace {
  export type AsObject = {
    name: string,
    price?: QueryPrice.AsObject,
    ts: number,
    description: string,
    meta?: db3_base_pb.BroadcastMeta.AsObject,
  }
}

