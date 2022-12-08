package network.db3.protocol.document;

public class Key {
    public enum KeyType {
        DocNumber, DocString
    }

    private String name;
    private KeyType type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public KeyType getType() {
        return type;
    }

    public void setType(KeyType type) {
        this.type = type;
    }
}
