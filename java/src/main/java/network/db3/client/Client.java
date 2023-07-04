package network.db3.client;

import com.google.protobuf.ByteString;
import db3_database_v2_proto.Db3DatabaseV2;
import db3_indexer_proto.Db3Indexer;
import db3_indexer_proto.IndexerNodeGrpc;
import db3_mutation_v2_proto.Db3MutationV2;
import db3_storage_proto.Db3Storage;
import db3_storage_proto.StorageNodeGrpc;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.TlsChannelCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import network.db3.provider.IndexProvider;
import network.db3.provider.StorageProvider;
import network.db3.store.ResultSet;
import org.bson.ByteBuf;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Keys;
import org.web3j.utils.Numeric;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final ECKeyPair keyPair;
    private final StorageProvider storageProvider;
    private final IndexProvider indexProvider;
    private final AtomicLong nonce;

    public Client(String rollupUrl, String indexUrl,
                  ECKeyPair keyPair) throws MalformedURLException {
        ManagedChannel rollupChannel = Client.buildFrom(rollupUrl);
        ManagedChannel indexChannel = Client.buildFrom(indexUrl);
        StorageNodeGrpc.StorageNodeBlockingStub rollupStub = StorageNodeGrpc.newBlockingStub(rollupChannel);
        IndexerNodeGrpc.IndexerNodeBlockingStub indexStub = IndexerNodeGrpc.newBlockingStub(indexChannel);
        this.storageProvider = new StorageProvider(rollupStub, keyPair);
        this.indexProvider = new IndexProvider(indexStub);
        this.keyPair = keyPair;
        this.nonce = new AtomicLong(0);
    }

    private static ManagedChannel buildFrom(String url) throws MalformedURLException {
        URL uri = new URL(url);
        if (uri.getProtocol().equals("https")){
            TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder();
            return Grpc.newChannelBuilderForAddress(uri.getHost(), uri.getPort() == 0 ? 443: uri.getPort(),
                    tlsBuilder.build()).build();
        } else {
            return ManagedChannelBuilder.forTarget(uri.getHost() + ":" + uri.getPort()).usePlaintext().build();
        }
    }

    public void updateNonce() {
        long nonce = this.storageProvider.getNonce(Keys.getAddress(keyPair));
        this.nonce.set(nonce);
        logger.info("the new nonce {} for address {}", nonce, Keys.getAddress(keyPair));
    }

    public CreateDBResult createDocDatabase(String desc) throws IOException {
        Db3MutationV2.DocumentDatabaseMutation docMutation = Db3MutationV2.DocumentDatabaseMutation.newBuilder().setDbDesc(desc).build();
        Db3MutationV2.Mutation.BodyWrapper body = Db3MutationV2.Mutation.BodyWrapper.newBuilder().setDocDatabaseMutation(docMutation).setDbAddress(ByteString.copyFromUtf8("")).build();
        Db3MutationV2.Mutation mutation = Db3MutationV2.Mutation.newBuilder().setAction(Db3MutationV2.MutationAction.CreateDocumentDB).addBodies(body).build();
        byte[] data = mutation.toByteArray();
        long nonce = this.nonce.getAndIncrement();
        Db3Storage.SendMutationResponse response = this.storageProvider.sendMutation(data, nonce);
        return new CreateDBResult(response.getId(), response.getItems(0).getValue());
    }

    public Db3DatabaseV2.DatabaseMessage getDatabase(String db) {
        Db3Storage.GetDatabaseResponse response = this.storageProvider.getDatabase(db);
        return response.getDatabase();
    }

    public CreateCollectonResult createCollection(String db, String col) throws IOException {
        byte[] address = Numeric.hexStringToByteArray(db);
        Db3MutationV2.CollectionMutation collectionMutation = Db3MutationV2.CollectionMutation.newBuilder().setCollectionName(col).build();
        Db3MutationV2.Mutation.BodyWrapper body = Db3MutationV2.Mutation.BodyWrapper.newBuilder().setCollectionMutation(collectionMutation).setDbAddress(ByteString.copyFrom(address)).build();
        Db3MutationV2.Mutation mutation = Db3MutationV2.Mutation.newBuilder().setAction(Db3MutationV2.MutationAction.AddCollection).addBodies(body).build();
        byte[] data = mutation.toByteArray();
        long nonce = this.nonce.getAndIncrement();
        Db3Storage.SendMutationResponse response = this.storageProvider.sendMutation(data, nonce);
        return new CreateCollectonResult(response.getId());
    }

    public Optional<Db3DatabaseV2.Collection> getCollection(String db, String col) {
        return this.storageProvider.getCollection(db, col);
    }

    public AddDocResult addDoc(String db, String col, String json) throws IOException {
        RawBsonDocument rawBsonDocument = RawBsonDocument.parse(json);
        ByteBuf buf = rawBsonDocument.getByteBuffer();
        byte[] data = new byte[buf.remaining()];
        buf.get(data);
        Db3MutationV2.DocumentMutation documentMutation = Db3MutationV2.DocumentMutation.newBuilder().addDocuments(ByteString.copyFrom(data)).setCollectionName(col).build();
        Db3MutationV2.Mutation.BodyWrapper body = Db3MutationV2.Mutation.BodyWrapper.newBuilder().setDbAddress(ByteString.copyFrom(Numeric.hexStringToByteArray(db))).setDocumentMutation(documentMutation).build();
        Db3MutationV2.Mutation mutation = Db3MutationV2.Mutation.newBuilder().setAction(Db3MutationV2.MutationAction.AddDocument).addBodies(body).build();
        byte[] buffer = mutation.toByteArray();
        long nonce = this.nonce.getAndIncrement();
        Db3Storage.SendMutationResponse response = this.storageProvider.sendMutation(buffer, nonce);
        return new AddDocResult(response.getId(), Long.parseLong(response.getItems(0).getValue()));
    }

    public ResultSet runQuery(String db, String col, String query) {
        Db3Indexer.RunQueryResponse response = this.indexProvider.runQuery(db, col, query);
        ResultSet resultSet = new ResultSet();
        resultSet.setDocs(response.getDocumentsList());
        resultSet.setCount(response.getCount());
        return resultSet;
    }
}
