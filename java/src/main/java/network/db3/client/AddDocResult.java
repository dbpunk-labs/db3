package network.db3.client;

public class AddDocResult {
    private String mutationId;

    public AddDocResult(String mutationId) {
        this.mutationId = mutationId;
    }

    public String getMutationId() {
        return mutationId;
    }
}
