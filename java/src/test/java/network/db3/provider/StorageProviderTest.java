package network.db3.provider;

import com.google.protobuf.ByteString;
import db3_mutation_v2_proto.Db3MutationV2;
import db3_storage_proto.Db3Storage;
import db3_storage_proto.StorageNodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.Test;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Keys;

import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;


public class StorageProviderTest {

    @Test
    public void testSendMutation() throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        ManagedChannel mchannel = ManagedChannelBuilder.forTarget("127.0.0.1:26619").usePlaintext().build();
        ECKeyPair keyPair = Keys.createEcKeyPair();
        byte[] privateKey = Hex.decode("ad689d9b7751da07b0fb39c5091672cbfe50f59131db015f8a0e76c9790a6fcc");
        StorageNodeGrpc.StorageNodeBlockingStub stub = StorageNodeGrpc.newBlockingStub(mchannel);
        StorageProvider provider = new StorageProvider(stub, keyPair);
        Db3MutationV2.DocumentDatabaseMutation docMutation = Db3MutationV2.DocumentDatabaseMutation.newBuilder().setDbDesc("desc").build();
        Db3MutationV2.Mutation.BodyWrapper body = Db3MutationV2.Mutation.BodyWrapper.newBuilder().setDocDatabaseMutation(docMutation).setDbAddress(ByteString.copyFromUtf8("")).build();
        Db3MutationV2.Mutation mutation = Db3MutationV2.Mutation.newBuilder().setAction(Db3MutationV2.MutationAction.CreateDocumentDB).addBodies(body).build();
        byte[] data = mutation.toByteArray();
        try {
            long nonce = provider.getNonce(Keys.getAddress(keyPair)) + 1;
            Db3Storage.SendMutationResponse response = provider.sendMutation(data, nonce);
            Assert.assertNotNull(response.getId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
