package network.db3.protocol.document;

import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class DocKeyBuilderTest {
    @Test
    public void testOrder() {
        Key k1 = new Key();
        k1.setName("k1");
        k1.setType(Key.KeyType.DocString);
        Key k2 = new Key();
        k2.setName("k2");
        k2.setType(Key.KeyType.DocNumber);
        List<Key> keys = Arrays.asList(k1, k2);
        DocIndex index = new DocIndex(keys, "transaction");
        JsonObject object1 = new JsonObject();
        object1.addProperty("k1", "k");
        object1.addProperty("k2", 10);
        ByteBuffer bs = DocKeyBuilder.gen(index, object1);
        JsonObject object2 = new JsonObject();
        object2.addProperty("k1", "k");
        object2.addProperty("k2", 11);
        ByteBuffer bs2 = DocKeyBuilder.gen(index, object2);
        Assert.assertTrue(bs.compareTo(bs2) < 0 );
    }

    @Test
    public void testCrossLanguage() {
        Key k1 = new Key();
        k1.setName("k1");
        k1.setType(Key.KeyType.DocString);
        Key k2 = new Key();
        k2.setName("k2");
        k2.setType(Key.KeyType.DocNumber);
        List<Key> keys = Arrays.asList(k1, k2);
        DocIndex index = new DocIndex(keys, "transaction");
        JsonObject object1 = new JsonObject();
        object1.addProperty("k1", "0x11111");
        object1.addProperty("k2", 9527);
        ByteBuffer bs = DocKeyBuilder.gen(index, object1);
        ByteBuffer bs2 = Base64.getEncoder().encode(bs);
        byte[] bytes = new byte[bs2.remaining()];
        bs2.get(bytes);
        Assert.assertEquals("dHJhbnNhY3Rpb24weDExMTExAAAAAAAAJTc=", new String(bytes, Charset.forName("UTF-8")));
    }
}
