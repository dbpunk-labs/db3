package network.db3.client;

public class CreateCollectonResult {
    private String mutationId;

    public CreateCollectonResult(String mutationId) {
        this.mutationId = mutationId;
    }

    public String getMutationId() {
        return mutationId;
    }
}
