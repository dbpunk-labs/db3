package network.db3.protocol.document;

import java.util.List;

public class DocIndex {
    // {"keys":[{"name":"k1", "type":"int"}, {"name":"k2", "type":"string"}]}
    public final static String DOC_KEY = "_doc_decriptor";
    private final List<Key> keys;
    private final String docName;
    public DocIndex(List<Key> keys, String docName) {
        this.keys = keys;
        this.docName = docName;
    }
    public String getDocName() {
        return docName;
    }
    public List<Key> getKeys() {
        return keys;
    }

}
