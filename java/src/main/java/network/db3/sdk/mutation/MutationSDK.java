package network.db3.sdk.mutation;

import cafe.cryptography.ed25519.Ed25519Signature;
import com.google.protobuf.ByteString;
import db3_mutation_proto.Db3Mutation;
import db3_node_proto.Db3Node;
import db3_node_proto.StorageNodeGrpc;
import network.db3.crypto.Ed25519Signer;

import java.util.Base64;

public class MutationSDK {
    private final StorageNodeGrpc.StorageNodeBlockingStub stub;
    private final Ed25519Signer signer;

    public MutationSDK(StorageNodeGrpc.StorageNodeBlockingStub stub, Ed25519Signer signer) {
        this.stub = stub;
        this.signer = signer;
    }

    public String submit(Db3Mutation.Mutation mutation) {
        byte[] message = mutation.toByteArray();
        Ed25519Signature signature = signer.sign(message);
        Db3Mutation.WriteRequest.Builder builder = Db3Mutation.WriteRequest.newBuilder();
        builder.setPayload(ByteString.copyFrom(message));
        builder.setPayloadType(Db3Mutation.PayloadType.MutationPayload);
        builder.setSignature(ByteString.copyFrom(signature.toByteArray()));
        builder.setPublicKey(ByteString.copyFrom(signer.getPublicKey().toByteArray()));
        Db3Mutation.WriteRequest request = builder.build();
        Db3Node.BroadcastRequest.Builder broadcastBuilder = Db3Node.BroadcastRequest.newBuilder();
        broadcastBuilder.setBody(request.toByteString());
        Db3Node.BroadcastResponse response = stub.broadcast(broadcastBuilder.build());
        return Base64.getEncoder().encodeToString(response.getHash().toByteArray());
    }

}
