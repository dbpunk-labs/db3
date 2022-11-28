/**
 * @fileoverview gRPC-Web generated client stub for db3_node_proto
 * @enhanceable
 * @public
 */

// Code generated by protoc-gen-grpc-web. DO NOT EDIT.
// versions:
// 	protoc-gen-grpc-web v1.4.2
// 	protoc              v3.20.3
// source: db3_node.proto

/* eslint-disable */
// @ts-nocheck

import * as grpcWeb from "grpc-web";

import db3_account_pb from "./db3_account_pb";
import db3_node_pb from "./db3_node_pb";

export class StorageNodeClient {
	client_: grpcWeb.AbstractClientBase;
	hostname_: string;
	credentials_: null | { [index: string]: string };
	options_: null | { [index: string]: any };

	constructor(
		hostname: string,
		credentials?: null | { [index: string]: string },
		options?: null | { [index: string]: any },
	) {
		if (!options) options = {};
		if (!credentials) credentials = {};
		options["format"] = "text";

		this.client_ = new grpcWeb.GrpcWebClientBase(options);
		this.hostname_ = hostname.replace(/\/+$/, "");
		this.credentials_ = credentials;
		this.options_ = options;
	}

	methodDescriptorQueryBill = new grpcWeb.MethodDescriptor(
		"/db3_node_proto.StorageNode/QueryBill",
		grpcWeb.MethodType.UNARY,
		db3_node_pb.QueryBillRequest,
		db3_node_pb.QueryBillResponse,
		(request: db3_node_pb.QueryBillRequest) => {
			return request.serializeBinary();
		},
		db3_node_pb.QueryBillResponse.deserializeBinary,
	);

	queryBill(
		request: db3_node_pb.QueryBillRequest,
		metadata: grpcWeb.Metadata | null,
	): Promise<db3_node_pb.QueryBillResponse>;

	queryBill(
		request: db3_node_pb.QueryBillRequest,
		metadata: grpcWeb.Metadata | null,
		callback: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.QueryBillResponse,
		) => void,
	): grpcWeb.ClientReadableStream<db3_node_pb.QueryBillResponse>;

	queryBill(
		request: db3_node_pb.QueryBillRequest,
		metadata: grpcWeb.Metadata | null,
		callback?: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.QueryBillResponse,
		) => void,
	) {
		if (callback !== undefined) {
			return this.client_.rpcCall(
				this.hostname_ + "/db3_node_proto.StorageNode/QueryBill",
				request,
				metadata || {},
				this.methodDescriptorQueryBill,
				callback,
			);
		}
		return this.client_.unaryCall(
			this.hostname_ + "/db3_node_proto.StorageNode/QueryBill",
			request,
			metadata || {},
			this.methodDescriptorQueryBill,
		);
	}

	methodDescriptorGetKey = new grpcWeb.MethodDescriptor(
		"/db3_node_proto.StorageNode/GetKey",
		grpcWeb.MethodType.UNARY,
		db3_node_pb.GetKeyRequest,
		db3_node_pb.GetKeyResponse,
		(request: db3_node_pb.GetKeyRequest) => {
			return request.serializeBinary();
		},
		db3_node_pb.GetKeyResponse.deserializeBinary,
	);

	getKey(
		request: db3_node_pb.GetKeyRequest,
		metadata: grpcWeb.Metadata | null,
	): Promise<db3_node_pb.GetKeyResponse>;

	getKey(
		request: db3_node_pb.GetKeyRequest,
		metadata: grpcWeb.Metadata | null,
		callback: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.GetKeyResponse,
		) => void,
	): grpcWeb.ClientReadableStream<db3_node_pb.GetKeyResponse>;

	getKey(
		request: db3_node_pb.GetKeyRequest,
		metadata: grpcWeb.Metadata | null,
		callback?: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.GetKeyResponse,
		) => void,
	) {
		if (callback !== undefined) {
			return this.client_.rpcCall(
				this.hostname_ + "/db3_node_proto.StorageNode/GetKey",
				request,
				metadata || {},
				this.methodDescriptorGetKey,
				callback,
			);
		}
		return this.client_.unaryCall(
			this.hostname_ + "/db3_node_proto.StorageNode/GetKey",
			request,
			metadata || {},
			this.methodDescriptorGetKey,
		);
	}

	methodDescriptorRestartQuerySession = new grpcWeb.MethodDescriptor(
		"/db3_node_proto.StorageNode/RestartQuerySession",
		grpcWeb.MethodType.UNARY,
		db3_node_pb.RestartSessionRequest,
		db3_node_pb.RestartSessionResponse,
		(request: db3_node_pb.RestartSessionRequest) => {
			return request.serializeBinary();
		},
		db3_node_pb.RestartSessionResponse.deserializeBinary,
	);

	restartQuerySession(
		request: db3_node_pb.RestartSessionRequest,
		metadata: grpcWeb.Metadata | null,
	): Promise<db3_node_pb.RestartSessionResponse>;

	restartQuerySession(
		request: db3_node_pb.RestartSessionRequest,
		metadata: grpcWeb.Metadata | null,
		callback: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.RestartSessionResponse,
		) => void,
	): grpcWeb.ClientReadableStream<db3_node_pb.RestartSessionResponse>;

	restartQuerySession(
		request: db3_node_pb.RestartSessionRequest,
		metadata: grpcWeb.Metadata | null,
		callback?: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.RestartSessionResponse,
		) => void,
	) {
		if (callback !== undefined) {
			return this.client_.rpcCall(
				this.hostname_ +
					"/db3_node_proto.StorageNode/RestartQuerySession",
				request,
				metadata || {},
				this.methodDescriptorRestartQuerySession,
				callback,
			);
		}
		return this.client_.unaryCall(
			this.hostname_ + "/db3_node_proto.StorageNode/RestartQuerySession",
			request,
			metadata || {},
			this.methodDescriptorRestartQuerySession,
		);
	}

	methodDescriptorGetAccount = new grpcWeb.MethodDescriptor(
		"/db3_node_proto.StorageNode/GetAccount",
		grpcWeb.MethodType.UNARY,
		db3_node_pb.GetAccountRequest,
		db3_account_pb.Account,
		(request: db3_node_pb.GetAccountRequest) => {
			return request.serializeBinary();
		},
		db3_account_pb.Account.deserializeBinary,
	);

	getAccount(
		request: db3_node_pb.GetAccountRequest,
		metadata: grpcWeb.Metadata | null,
	): Promise<db3_account_pb.Account>;

	getAccount(
		request: db3_node_pb.GetAccountRequest,
		metadata: grpcWeb.Metadata | null,
		callback: (
			err: grpcWeb.RpcError,
			response: db3_account_pb.Account,
		) => void,
	): grpcWeb.ClientReadableStream<db3_account_pb.Account>;

	getAccount(
		request: db3_node_pb.GetAccountRequest,
		metadata: grpcWeb.Metadata | null,
		callback?: (
			err: grpcWeb.RpcError,
			response: db3_account_pb.Account,
		) => void,
	) {
		if (callback !== undefined) {
			return this.client_.rpcCall(
				this.hostname_ + "/db3_node_proto.StorageNode/GetAccount",
				request,
				metadata || {},
				this.methodDescriptorGetAccount,
				callback,
			);
		}
		return this.client_.unaryCall(
			this.hostname_ + "/db3_node_proto.StorageNode/GetAccount",
			request,
			metadata || {},
			this.methodDescriptorGetAccount,
		);
	}

	methodDescriptorGetSessionInfo = new grpcWeb.MethodDescriptor(
		"/db3_node_proto.StorageNode/GetSessionInfo",
		grpcWeb.MethodType.UNARY,
		db3_node_pb.GetSessionInfoRequest,
		db3_node_pb.GetSessionInfoResponse,
		(request: db3_node_pb.GetSessionInfoRequest) => {
			return request.serializeBinary();
		},
		db3_node_pb.GetSessionInfoResponse.deserializeBinary,
	);

	getSessionInfo(
		request: db3_node_pb.GetSessionInfoRequest,
		metadata: grpcWeb.Metadata | null,
	): Promise<db3_node_pb.GetSessionInfoResponse>;

	getSessionInfo(
		request: db3_node_pb.GetSessionInfoRequest,
		metadata: grpcWeb.Metadata | null,
		callback: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.GetSessionInfoResponse,
		) => void,
	): grpcWeb.ClientReadableStream<db3_node_pb.GetSessionInfoResponse>;

	getSessionInfo(
		request: db3_node_pb.GetSessionInfoRequest,
		metadata: grpcWeb.Metadata | null,
		callback?: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.GetSessionInfoResponse,
		) => void,
	) {
		if (callback !== undefined) {
			return this.client_.rpcCall(
				this.hostname_ + "/db3_node_proto.StorageNode/GetSessionInfo",
				request,
				metadata || {},
				this.methodDescriptorGetSessionInfo,
				callback,
			);
		}
		return this.client_.unaryCall(
			this.hostname_ + "/db3_node_proto.StorageNode/GetSessionInfo",
			request,
			metadata || {},
			this.methodDescriptorGetSessionInfo,
		);
	}

	methodDescriptorBroadcast = new grpcWeb.MethodDescriptor(
		"/db3_node_proto.StorageNode/Broadcast",
		grpcWeb.MethodType.UNARY,
		db3_node_pb.BroadcastRequest,
		db3_node_pb.BroadcastResponse,
		(request: db3_node_pb.BroadcastRequest) => {
			return request.serializeBinary();
		},
		db3_node_pb.BroadcastResponse.deserializeBinary,
	);

	broadcast(
		request: db3_node_pb.BroadcastRequest,
		metadata: grpcWeb.Metadata | null,
	): Promise<db3_node_pb.BroadcastResponse>;

	broadcast(
		request: db3_node_pb.BroadcastRequest,
		metadata: grpcWeb.Metadata | null,
		callback: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.BroadcastResponse,
		) => void,
	): grpcWeb.ClientReadableStream<db3_node_pb.BroadcastResponse>;

	broadcast(
		request: db3_node_pb.BroadcastRequest,
		metadata: grpcWeb.Metadata | null,
		callback?: (
			err: grpcWeb.RpcError,
			response: db3_node_pb.BroadcastResponse,
		) => void,
	) {
		if (callback !== undefined) {
			return this.client_.rpcCall(
				this.hostname_ + "/db3_node_proto.StorageNode/Broadcast",
				request,
				metadata || {},
				this.methodDescriptorBroadcast,
				callback,
			);
		}
		return this.client_.unaryCall(
			this.hostname_ + "/db3_node_proto.StorageNode/Broadcast",
			request,
			metadata || {},
			this.methodDescriptorBroadcast,
		);
	}
}
