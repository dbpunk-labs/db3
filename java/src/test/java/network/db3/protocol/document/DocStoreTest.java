package network.db3.protocol.document;

import cafe.cryptography.ed25519.Ed25519PrivateKey;
import com.google.gson.JsonObject;
import db3_node_proto.StorageNodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import network.db3.common.Utils;
import network.db3.crypto.Ed25519Signer;
import network.db3.sdk.mutation.MutationSDK;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;
public class DocStoreTest {

    @Test
    public void testInsert() {
        ManagedChannel mchannel = ManagedChannelBuilder.forTarget("127.0.0.1:26659").usePlaintext().build();
        StorageNodeGrpc.StorageNodeBlockingStub stub = StorageNodeGrpc.newBlockingStub(mchannel);
        SecureRandom random = new SecureRandom();
        byte[] bytes = Utils.hexToBytes("833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42");
        Ed25519PrivateKey privateKey = Ed25519PrivateKey.fromByteArray(bytes);
        Ed25519Signer signer = new Ed25519Signer(privateKey);
        MutationSDK sdk = new MutationSDK(stub, signer);
        Key k1 = new Key();
        k1.setName("k1");
        k1.setType(Key.KeyType.DocString);
        Key k2 = new Key();
        k2.setName("k2");
        k2.setType(Key.KeyType.DocNumber);
        List<Key> keys = Arrays.asList(k1, k2);
        DocIndex index = new DocIndex(keys, "transaction");
        DocStore store = new DocStore(sdk, index);
        JsonObject object1 = new JsonObject();
        object1.addProperty("k1", "k");
        object1.addProperty("k2", 10);
        String id = store.insertDocs("ns1", Arrays.asList(object1));
        Assert.assertEquals("UNx6mqfvb8lsu+//neQ9gX5jYucIvzi1nzHQULu+/QE=", id);
    }
}
