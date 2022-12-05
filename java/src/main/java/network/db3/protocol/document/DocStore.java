package network.db3.protocol.document;

import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import db3_base_proto.Db3Base;
import db3_mutation_proto.Db3Mutation;
import network.db3.sdk.mutation.MutationSDK;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

    public String insertDocs(String ns, String table, List<JsonObject> objects) {
        List<Db3Mutation.KVPair> kvPairs = new ArrayList<>();
        for (int i = 0; i < objects.size(); i++) {
            ByteBuffer bb = DocKeyBuilder.gen(index, objects.get(i));
            Db3Mutation.KVPair.Builder kvPairBuilder = Db3Mutation.KVPair.newBuilder();
            kvPairBuilder.setActionValue(Db3Mutation.MutationAction.InsertKv.getNumber());
            kvPairBuilder.setKey(ByteString.copyFrom(bb));
            kvPairBuilder.setValue(ByteString.copyFrom(objects.get(i).toString().getBytes(StandardCharsets.UTF_8)));
            kvPairs.add(kvPairBuilder.build());
        }
        Db3Mutation.Mutation.Builder mbuilder = Db3Mutation.Mutation.newBuilder();
        mbuilder.addAllKvPairs(kvPairs);
        mbuilder.setNs(ByteString.copyFrom(ns.getBytes(StandardCharsets.UTF_8)));
        //TODO update nonce
        mbuilder.setNonce(1);
        mbuilder.setGas(100);
        mbuilder.setChainRole(Db3Base.ChainRole.StorageShardChain);
        mbuilder.setChainId(Db3Base.ChainId.DevNet);
        Db3Mutation.Mutation mutation = mbuilder.build();
        return sdk.submit(mutation);
    }
}
