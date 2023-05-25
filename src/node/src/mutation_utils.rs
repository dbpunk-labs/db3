use bytes::Bytes;
use db3_crypto::db3_verifier;
use db3_crypto::id::AccountId;
use db3_error::DB3Error;
use db3_proto::db3_mutation_proto::{
    DatabaseMutation, MintCreditsMutation, PayloadType, WriteRequest,
};
use db3_proto::db3_session_proto::QuerySession;
use ethers::core::types::Bytes as EthersBytes;
use ethers::types::transaction::eip712::{Eip712, TypedData};
use prost::Message;
use std::str::FromStr;
use tracing::warn;
/// parse mutation
macro_rules! parse_mutation {
    ($func:ident, $type:ident) => {
        pub fn $func(payload: &[u8]) -> Result<$type, DB3Error> {
            match $type::decode(payload) {
                Ok(dm) => match &dm.meta {
                    Some(_) => Ok(dm),
                    None => {
                        warn!("no meta for mutation");
                        Err(DB3Error::ApplyMutationError("meta is none".to_string()))
                    }
                },
                Err(e) => {
                    //TODO add event ?
                    warn!("invalid mutation data {e}");
                    Err(DB3Error::ApplyMutationError(
                        "invalid mutation data".to_string(),
                    ))
                }
            }
        }
    };
}
pub struct MutationUtil {}

impl MutationUtil {
    parse_mutation!(parse_database_mutation, DatabaseMutation);
    parse_mutation!(parse_mint_credits_mutation, MintCreditsMutation);
    parse_mutation!(parse_query_session, QuerySession);
    /// unwrap and verify write request
    pub fn unwrap_and_verify(
        req: WriteRequest,
    ) -> Result<(EthersBytes, PayloadType, AccountId), DB3Error> {
        if req.payload_type == 3 {
            // typed data
            match serde_json::from_slice::<TypedData>(req.payload.as_ref()) {
                Ok(data) => {
                    let hashed_message = data.encode_eip712().map_err(|e| {
                        DB3Error::ApplyMutationError(format!("invalid payload type for err {e}"))
                    })?;
                    let account_id = db3_verifier::DB3Verifier::verify_hashed(
                        &hashed_message,
                        req.signature.as_ref(),
                    )?;
                    if let (Some(payload), Some(payload_type)) =
                        (data.message.get("payload"), data.message.get("payloadType"))
                    {
                        //TODO advoid data copy
                        let data: EthersBytes =
                            serde_json::from_value(payload.clone()).map_err(|e| {
                                DB3Error::ApplyMutationError(format!(
                                    "invalid payload type for err {e}"
                                ))
                            })?;
                        let internal_data_type = i32::from_str(payload_type.as_str().ok_or(
                            DB3Error::QuerySessionVerifyError("invalid payload type".to_string()),
                        )?)
                        .map_err(|e| {
                            DB3Error::QuerySessionVerifyError(format!(
                                "fail to convert payload type to i32 {e}"
                            ))
                        })?;
                        let data_type: PayloadType = PayloadType::from_i32(internal_data_type)
                            .ok_or(DB3Error::ApplyMutationError(
                                "invalid payload type".to_string(),
                            ))?;
                        Ok((data, data_type, account_id))
                    } else {
                        Err(DB3Error::ApplyMutationError("bad typed data".to_string()))
                    }
                }
                Err(e) => Err(DB3Error::ApplyMutationError(format!(
                    "bad typed data for err {e}"
                ))),
            }
        } else {
            let account_id =
                db3_verifier::DB3Verifier::verify(req.payload.as_ref(), req.signature.as_ref())?;
            let data_type: PayloadType = PayloadType::from_i32(req.payload_type).ok_or(
                DB3Error::ApplyMutationError("invalid payload type".to_string()),
            )?;
            let data = Bytes::from(req.payload);
            Ok((EthersBytes(data), data_type, account_id))
        }
    }
}
