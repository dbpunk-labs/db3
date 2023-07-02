package network.db3.provider;

import db3_database_v2_proto.Db3DatabaseV2;
import db3_indexer_proto.Db3Indexer;
import db3_indexer_proto.IndexerNodeGrpc;

public class IndexProvider {
    private final IndexerNodeGrpc.IndexerNodeBlockingStub stub;

    public IndexProvider(IndexerNodeGrpc.IndexerNodeBlockingStub stub) {
        this.stub = stub;
    }

    public Db3Indexer.RunQueryResponse runQuery(String db, String col, String query) {
        Db3DatabaseV2.Query db3Query = Db3DatabaseV2.Query.newBuilder().setQueryStr(query).build();
        Db3Indexer.RunQueryRequest request = Db3Indexer.RunQueryRequest.newBuilder().setDb(db).setColName(col).setQuery(db3Query).build();
        return stub.runQuery(request);
    }

}
