// source: db3_base.proto
/**
 * @fileoverview
 * @enhanceable
 * @suppress {missingRequire} reports error on implicit type usages.
 * @suppress {messageConventions} JS Compiler reports an error if a variable or
 *     field starts with 'MSG_' and isn't a translatable message.
 * @public
 */
// GENERATED CODE -- DO NOT EDIT!
/* eslint-disable */
// @ts-nocheck
import * as jspb from 'google-protobuf';
var goog = jspb;
var global = (function() { return this || window || global || self || Function('return this')(); }).call(null);

goog.exportSymbol('proto.db3_base_proto.ChainId', null, global);
goog.exportSymbol('proto.db3_base_proto.ChainRole', null, global);
goog.exportSymbol('proto.db3_base_proto.UnitType', null, global);
goog.exportSymbol('proto.db3_base_proto.Units', null, global);
/**
 * Generated by JsPbCodeGenerator.
 * @param {Array=} opt_data Optional initial data array, typically from a
 * server response, or constructed directly in Javascript. The array is used
 * in place and becomes part of the constructed object. It is not cloned.
 * If no data is provided, the constructed object will be empty, but still
 * valid.
 * @extends {jspb.Message}
 * @constructor
 */
proto.db3_base_proto.Units = function(opt_data) {
  jspb.Message.initialize(this, opt_data, 0, -1, null, null);
};
goog.inherits(proto.db3_base_proto.Units, jspb.Message);
if (goog.DEBUG && !COMPILED) {
  /**
   * @public
   * @override
   */
  proto.db3_base_proto.Units.displayName = 'proto.db3_base_proto.Units';
}



if (jspb.Message.GENERATE_TO_OBJECT) {
/**
 * Creates an object representation of this proto.
 * Field names that are reserved in JavaScript and will be renamed to pb_name.
 * Optional fields that are not set will be set to undefined.
 * To access a reserved field use, foo.pb_<name>, eg, foo.pb_default.
 * For the list of reserved names please see:
 *     net/proto2/compiler/js/internal/generator.cc#kKeyword.
 * @param {boolean=} opt_includeInstance Deprecated. whether to include the
 *     JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @return {!Object}
 */
proto.db3_base_proto.Units.prototype.toObject = function(opt_includeInstance) {
  return proto.db3_base_proto.Units.toObject(opt_includeInstance, this);
};


/**
 * Static version of the {@see toObject} method.
 * @param {boolean|undefined} includeInstance Deprecated. Whether to include
 *     the JSPB instance for transitional soy proto support:
 *     http://goto/soy-param-migration
 * @param {!proto.db3_base_proto.Units} msg The msg instance to transform.
 * @return {!Object}
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.db3_base_proto.Units.toObject = function(includeInstance, msg) {
  var f, obj = {
    utype: jspb.Message.getFieldWithDefault(msg, 1, 0),
    amount: jspb.Message.getFieldWithDefault(msg, 2, 0)
  };

  if (includeInstance) {
    obj.$jspbMessageInstance = msg;
  }
  return obj;
};
}


/**
 * Deserializes binary data (in protobuf wire format).
 * @param {jspb.ByteSource} bytes The bytes to deserialize.
 * @return {!proto.db3_base_proto.Units}
 */
proto.db3_base_proto.Units.deserializeBinary = function(bytes) {
  var reader = new jspb.BinaryReader(bytes);
  var msg = new proto.db3_base_proto.Units;
  return proto.db3_base_proto.Units.deserializeBinaryFromReader(msg, reader);
};


/**
 * Deserializes binary data (in protobuf wire format) from the
 * given reader into the given message object.
 * @param {!proto.db3_base_proto.Units} msg The message object to deserialize into.
 * @param {!jspb.BinaryReader} reader The BinaryReader to use.
 * @return {!proto.db3_base_proto.Units}
 */
proto.db3_base_proto.Units.deserializeBinaryFromReader = function(msg, reader) {
  while (reader.nextField()) {
    if (reader.isEndGroup()) {
      break;
    }
    var field = reader.getFieldNumber();
    switch (field) {
    case 1:
      var value = /** @type {!proto.db3_base_proto.UnitType} */ (reader.readEnum());
      msg.setUtype(value);
      break;
    case 2:
      var value = /** @type {number} */ (reader.readInt64());
      msg.setAmount(value);
      break;
    default:
      reader.skipField();
      break;
    }
  }
  return msg;
};


/**
 * Serializes the message to binary data (in protobuf wire format).
 * @return {!Uint8Array}
 */
proto.db3_base_proto.Units.prototype.serializeBinary = function() {
  var writer = new jspb.BinaryWriter();
  proto.db3_base_proto.Units.serializeBinaryToWriter(this, writer);
  return writer.getResultBuffer();
};


/**
 * Serializes the given message to binary data (in protobuf wire
 * format), writing to the given BinaryWriter.
 * @param {!proto.db3_base_proto.Units} message
 * @param {!jspb.BinaryWriter} writer
 * @suppress {unusedLocalVariables} f is only used for nested messages
 */
proto.db3_base_proto.Units.serializeBinaryToWriter = function(message, writer) {
  var f = undefined;
  f = message.getUtype();
  if (f !== 0.0) {
    writer.writeEnum(
      1,
      f
    );
  }
  f = message.getAmount();
  if (f !== 0) {
    writer.writeInt64(
      2,
      f
    );
  }
};


/**
 * optional UnitType utype = 1;
 * @return {!proto.db3_base_proto.UnitType}
 */
proto.db3_base_proto.Units.prototype.getUtype = function() {
  return /** @type {!proto.db3_base_proto.UnitType} */ (jspb.Message.getFieldWithDefault(this, 1, 0));
};


/**
 * @param {!proto.db3_base_proto.UnitType} value
 * @return {!proto.db3_base_proto.Units} returns this
 */
proto.db3_base_proto.Units.prototype.setUtype = function(value) {
  return jspb.Message.setProto3EnumField(this, 1, value);
};


/**
 * optional int64 amount = 2;
 * @return {number}
 */
proto.db3_base_proto.Units.prototype.getAmount = function() {
  return /** @type {number} */ (jspb.Message.getFieldWithDefault(this, 2, 0));
};


/**
 * @param {number} value
 * @return {!proto.db3_base_proto.Units} returns this
 */
proto.db3_base_proto.Units.prototype.setAmount = function(value) {
  return jspb.Message.setProto3IntField(this, 2, value);
};


/**
 * @enum {number}
 */
proto.db3_base_proto.UnitType = {
  DB3: 0,
  TAI: 1
};

/**
 * @enum {number}
 */
proto.db3_base_proto.ChainRole = {
  SETTLEMENTCHAIN: 0,
  STORAGESHARDCHAIN: 10,
  DVMCOMPUTINGCHAIN: 20
};

/**
 * @enum {number}
 */
proto.db3_base_proto.ChainId = {
  MAINNET: 0,
  TESTNET: 10,
  DEVNET: 20
};
export default proto.db3_base_proto;
