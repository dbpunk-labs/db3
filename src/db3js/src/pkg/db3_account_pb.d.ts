import * as jspb from "google-protobuf";

import * as db3_base_pb from "./db3_base_pb";

export class Account extends jspb.Message {
	getTotalBills(): db3_base_pb.Units | undefined;
	setTotalBills(value?: db3_base_pb.Units): Account;
	hasTotalBills(): boolean;
	clearTotalBills(): Account;

	getTotalStorageInBytes(): number;
	setTotalStorageInBytes(value: number): Account;

	getTotalMutationCount(): number;
	setTotalMutationCount(value: number): Account;

	getTotalQuerySessionCount(): number;
	setTotalQuerySessionCount(value: number): Account;

	getCredits(): db3_base_pb.Units | undefined;
	setCredits(value?: db3_base_pb.Units): Account;
	hasCredits(): boolean;
	clearCredits(): Account;

	getNonce(): number;
	setNonce(value: number): Account;

	getBillNextId(): number;
	setBillNextId(value: number): Account;

	serializeBinary(): Uint8Array;
	toObject(includeInstance?: boolean): Account.AsObject;
	static toObject(includeInstance: boolean, msg: Account): Account.AsObject;
	static serializeBinaryToWriter(
		message: Account,
		writer: jspb.BinaryWriter,
	): void;
	static deserializeBinary(bytes: Uint8Array): Account;
	static deserializeBinaryFromReader(
		message: Account,
		reader: jspb.BinaryReader,
	): Account;
}

export namespace Account {
	export type AsObject = {
		totalBills?: db3_base_pb.Units.AsObject;
		totalStorageInBytes: number;
		totalMutationCount: number;
		totalQuerySessionCount: number;
		credits?: db3_base_pb.Units.AsObject;
		nonce: number;
		billNextId: number;
	};
}
