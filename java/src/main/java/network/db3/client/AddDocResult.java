package network.db3.client;

public class AddDocResult {
    private String mutationId;
    private long docId;

    public AddDocResult(String mutationId, long docId) {
        this.mutationId = mutationId;
        this.docId = docId;
    }

    public long getDocId() {
        return docId;
    }

    public String getMutationId() {
        return mutationId;
    }
}
