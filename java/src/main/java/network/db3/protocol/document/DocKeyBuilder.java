package network.db3.protocol.document;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class DocKeyBuilder {

    public static ByteBuffer gen(DocIndex index, JsonObject object) {
        // the max key size  is 1kb
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN);
        buffer.put(index.getDocName().getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < index.getKeys().size(); i++) {
            JsonElement element = object.get(index.getKeys().get(i).getName());
            switch (index.getKeys().get(i).getType()) {
                case DocNumber: {
                    long number = element.getAsLong();
                    buffer.putLong(number);
                    break;
                }
                case DocString: {
                    String value = element.getAsString();
                    buffer.put(value.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        buffer.flip();
        return buffer;
    }
}
