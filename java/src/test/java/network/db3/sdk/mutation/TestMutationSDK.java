package network.db3.sdk.mutation;

import cafe.cryptography.ed25519.Ed25519PrivateKey;
import com.google.protobuf.ByteString;
import db3_base_proto.Db3Base;
import db3_mutation_proto.Db3Mutation;
import db3_node_proto.StorageNodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import network.db3.common.Utils;
import network.db3.crypto.Ed25519Signer;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.security.SecureRandom;

public class TestMutationSDK {

    @Test
    public void smokeTest() {
        ManagedChannel mchannel = ManagedChannelBuilder.forTarget("127.0.0.1:26659").usePlaintext().build();
        StorageNodeGrpc.StorageNodeBlockingStub stub = StorageNodeGrpc.newBlockingStub(mchannel);
        SecureRandom random = new SecureRandom();
        byte[] bytes = Utils.hexToBytes("833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42");
        Ed25519PrivateKey privateKey = Ed25519PrivateKey.fromByteArray(bytes);
        Ed25519Signer signer = new Ed25519Signer(privateKey);
        MutationSDK sdk = new MutationSDK(stub, signer);
        Db3Mutation.KVPair.Builder kvBuilder = Db3Mutation.KVPair.newBuilder();
        kvBuilder.setAction(Db3Mutation.MutationAction.InsertKv);
        kvBuilder.setKey(ByteString.copyFromUtf8("test_key"));
        kvBuilder.setValue(ByteString.copyFromUtf8("test_value"));
        Db3Mutation.Mutation.Builder mBuilder = Db3Mutation.Mutation.newBuilder();
        mBuilder.setChainId(Db3Base.ChainId.DevNet);
        mBuilder.setChainRole(Db3Base.ChainRole.StorageShardChain);
        mBuilder.setNs(ByteString.copyFromUtf8("ns2"));
        mBuilder.setGas(1000);
        mBuilder.setNonce(111);
        mBuilder.addKvPairs(kvBuilder.build());
        Db3Mutation.Mutation mutation = mBuilder.build();
        String id = sdk.submit(mutation);
        Assert.assertEquals("EFU0X3Xdzb8hnspM086ay+KrGCDGb+n0fbnwd8qbb98=", id);
    }
}
