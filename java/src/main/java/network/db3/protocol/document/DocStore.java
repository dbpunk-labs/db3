package network.db3.protocol.document;

import com.google.gson.JsonObject;
import network.db3.sdk.mutation.MutationSDK;

import java.util.List;

public class DocStore {
    private final MutationSDK sdk;
    private final DocIndex index;

    public DocStore(MutationSDK sdk, DocIndex index) {
        this.sdk = sdk;
        this.index = index;
    }

    public void init() {
        // fetch doc index descriptor
    }

    public void insertDoc(String ns,  String table,  List<JsonObject> object) {

    }

}
