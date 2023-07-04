package network.db3.store;

import db3_database_v2_proto.Db3DatabaseV2;

import java.util.List;

public class ResultSet {
    private List<Db3DatabaseV2.Document> docs;
    private long count;

    public List<Db3DatabaseV2.Document> getDocs() {
        return docs;
    }

    public void setDocs(List<Db3DatabaseV2.Document> docs) {
        this.docs = docs;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
