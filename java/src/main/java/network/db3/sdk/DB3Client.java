package network.db3.sdk;

import db3_node_proto.Db3Node;
import db3_node_proto.StorageNodeGrpc;
import io.grpc.stub.StreamObserver;

public class DB3Client extends StorageNodeGrpc.StorageNodeImplBase {

    @Override
    public void broadcast(Db3Node.BroadcastRequest request, StreamObserver<Db3Node.BroadcastResponse> responseObserver) {
        super.broadcast(request, responseObserver);
    }
}
