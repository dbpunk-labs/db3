package network.db3.provider;

import org.web3j.crypto.StructuredData;

import java.util.*;

public class EIP712TypedMessage {
    private final HashMap<String, List<StructuredData.Entry>> types;
    private final String primaryType;
    private final TypedMessage message;
    private final Map<String, String> domain = new HashMap<>();

    public EIP712TypedMessage(TypedMessage message) {
        this.types = new LinkedHashMap<>();
        this.types.put("EIP712Domain",
                Collections.emptyList()
        );
        this.types.put("Message",
                Arrays.asList(
                        new StructuredData.Entry("payload", "bytes"),
                        new StructuredData.Entry("nonce", "string")
                )
        );
        this.primaryType = "Message";
        this.message = message;
    }

    public HashMap<String, List<StructuredData.Entry>> getTypes() {
        return types;
    }

    public String getPrimaryType() {
        return primaryType;
    }

    public TypedMessage getMessage() {
        return message;
    }

    public Map<String, String> getDomain() {
        return domain;
    }
}