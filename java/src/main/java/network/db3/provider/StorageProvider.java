package network.db3.provider;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import db3_storage_proto.Db3Storage;
import db3_storage_proto.StorageNodeGrpc;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Sign;
import org.web3j.utils.Numeric;

import java.io.IOException;

public class StorageProvider {
    private final StorageNodeGrpc.StorageNodeBlockingStub stub;
    private final ECKeyPair keyPair;
    private final Gson gson = new Gson();

    public StorageProvider(StorageNodeGrpc.StorageNodeBlockingStub stub,
                           ECKeyPair keyPair) {
        this.stub = stub;
        this.keyPair = keyPair;
    }

    public Db3Storage.SendMutationResponse sendMutation(byte[] mutation, long nonce) throws IOException {
        EIP712TypedMessage message = wrapTypedRequest(mutation, nonce);
        String strMessage = gson.toJson(message);
        Sign.SignatureData sig = Sign.signTypedData(strMessage, keyPair);
        byte[] retrieval = new byte[65];
        System.arraycopy(sig.getR(), 0, retrieval, 0, 32);
        System.arraycopy(sig.getS(), 0, retrieval, 32, 32);
        System.arraycopy(sig.getV(), 0, retrieval, 64, 1);
        String signedMessage = Numeric.toHexString(retrieval);
        Db3Storage.SendMutationRequest.Builder requestBuilder = Db3Storage.SendMutationRequest.newBuilder();
        requestBuilder.setPayload(ByteString.copyFromUtf8(strMessage));
        requestBuilder.setSignature(signedMessage);
        Db3Storage.SendMutationRequest request = requestBuilder.build();
        Db3Storage.SendMutationResponse response = stub.sendMutation(request);
        return response;
    }

    public long getNonce(String addr) {
        Db3Storage.GetNonceRequest request = Db3Storage.GetNonceRequest.newBuilder().setAddress(addr).build();
        Db3Storage.GetNonceResponse response = stub.getNonce(request);
        return response.getNonce();
    }

    private EIP712TypedMessage wrapTypedRequest(byte[] mutation, long nonce) {
        String payload = Numeric.toHexString(mutation);
        TypedMessage message = new TypedMessage();
        message.setPayload(payload);
        message.setNonce(String.valueOf(nonce));
        return new EIP712TypedMessage(message);
    }
}
