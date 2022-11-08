const {QueryBillRequest, QueryBillResponse} = require('./db3_node_pb.js');
const {StorageNodeClient} = require('./db3_node_grpc_web_pb.js');

var client = new StorageNodeClient('http://' + window.location.hostname + ':8080',
                               null, null);
// simple unary call
var request = new QueryBillRequest();
request.setHeight(10);
client.queryBill(request, {}, (err, response) => {
  if (err) {
    console.log(`Unexpected error for sayHello: code = ${err.code}` +
                `, message = "${err.message}"`);
  } else {
    console.log(response.getMessage());
  }
});
