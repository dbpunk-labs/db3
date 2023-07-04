package network.db3.client;

public class CreateDBResult {
    private String mutationId;
    private String db;

    public CreateDBResult(String mutationId, String db) {
        this.mutationId = mutationId;
        this.db = db;
    }

    public String getMutationId() {
        return mutationId;
    }

    public String getDb() {
        return db;
    }
}
