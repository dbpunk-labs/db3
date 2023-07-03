package network.db3.provider;

import db3_database_v2_proto.Db3DatabaseV2;
import network.db3.client.AddDocResult;
import network.db3.client.Client;
import network.db3.client.CreateCollectonResult;
import network.db3.client.CreateDBResult;
import network.db3.store.ResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Keys;
import org.web3j.utils.Numeric;

import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Optional;

public class ClientTest {

    private Client buildRandClient() throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException {
        ECKeyPair keyPair = Keys.createEcKeyPair();
        System.out.println(Keys.getAddress(keyPair));
        return new Client("127.0.0.1:26619", "127.0.0.1:26639", keyPair);
    }

    private Client buildFromPrivateKey() {
        ECKeyPair keyPair = ECKeyPair.create(Numeric.hexStringToByteArray("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"));
        System.out.println(Keys.getAddress(keyPair));
        return new Client("127.0.0.1:26619", "127.0.0.1:26639", keyPair);
    }

    @Test
    public void testCreateDB() {
        try {
            Client client = buildFromPrivateKey();
            client.updateNonce();
            CreateDBResult result = client.createDocDatabase("desc");
            Db3DatabaseV2.DatabaseMessage database = client.getDatabase(result.getDb());
            String address = Numeric.toHexString(database.getDocDb().getAddress().toByteArray());
            Assert.assertEquals(result.getDb(), address);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testDBNotExist() {
        try {
            String address = "0x0";
            Client client = buildRandClient();
            Db3DatabaseV2.DatabaseMessage database = client.getDatabase(address);
            Assert.fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void testAddCollection() {
        try {
            Client client = buildRandClient();
            client.updateNonce();
            CreateDBResult result = client.createDocDatabase("desc");
            Db3DatabaseV2.DatabaseMessage database = client.getDatabase(result.getDb());
            String address = Numeric.toHexString(database.getDocDb().getAddress().toByteArray());
            Assert.assertEquals(result.getDb(), address);
            CreateCollectonResult result1 = client.createCollection(result.getDb(), "col1");
            Assert.assertNotNull(result1.getMutationId());
            Optional<Db3DatabaseV2.Collection> collection = client.getCollection(result.getDb(), "col1");
            Assert.assertEquals(collection.isPresent(), true);
            Assert.assertEquals(collection.get().getName(), "col1");
            String doc = "{\"name\":\"a\"}";
            AddDocResult addDocResult = client.addDoc(result.getDb(), "col1", doc);
            Assert.assertNotNull(addDocResult.getMutationId());
            Thread.sleep(1000 * 2);
            ResultSet resultSet = client.runQuery(result.getDb(), "col1", "/[name=\"a\"]");
            Assert.assertEquals(1, resultSet.getCount());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
