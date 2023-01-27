use db3_crypto::account_id::AccountId;
use db3_crypto::db3_verifier;
use db3_error::{DB3Error, Result};
use db3_proto::db3_session_proto::{CloseSessionPayload, QuerySession, QuerySessionInfo};
use prost::Message;

pub fn verify_query_session(query_session: &QuerySession) -> Result<(AccountId, QuerySessionInfo)> {
    match query_session.node_query_session_info.as_ref() {
        Some(node_query_session_info) => match db3_verifier::DB3Verifier::verify(
            query_session.client_query_session.as_ref(),
            query_session.client_signature.as_ref(),
        ) {
            Ok(client_account) => {
                match CloseSessionPayload::decode(query_session.client_query_session.as_ref()) {
                    Ok(client_query_session) => {
                        if check_query_session_info(
                            &node_query_session_info,
                            &client_query_session.session_info.as_ref().unwrap(),
                        ) {
                            Ok((client_account, node_query_session_info.clone()))
                        } else {
                            Err(DB3Error::QuerySessionVerifyError(format!(
                                "node query count and client query count inconsistent"
                            )))
                        }
                    }
                    Err(e) => Err(DB3Error::VerifyFailed(format!(
                        "invalid client query session info {}",
                        e
                    ))),
                }
            }
            Err(e) => Err(e),
        },
        None => Err(DB3Error::QuerySessionVerifyError(format!(
            "node query session info is none"
        ))),
    }
}

pub fn check_query_session_info(
    node_query_session: &QuerySessionInfo,
    client_query_session: &QuerySessionInfo,
) -> bool {
    node_query_session.query_count == client_query_session.query_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use chrono::Utc;
    use db3_base::get_a_static_keypair;
    use db3_crypto::signer::Db3Signer;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_session_proto::SessionStatus;

    #[test]
    fn test_verify_happy_path() -> Result<()> {
        let client_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
            status: SessionStatus::Stop.into(),
        };
        let client_query_session = CloseSessionPayload {
            session_info: Some(client_query_session_info),
            session_token: "DummyToken".to_string(),
        };
        let node_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
            status: SessionStatus::Stop.into(),
        };
        // encode and sign client_query_session_info
        let kp = get_a_static_keypair();
        let mut buf = BytesMut::with_capacity(1024 * 8);
        client_query_session.encode(&mut buf).unwrap();
        let buf = buf.freeze();
        let signer = Db3Signer::new(kp);
        let (signature_raw, public_key_raw) = signer.sign(buf.as_ref())?;
        let query_session = QuerySession {
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            node_query_session_info: Some(node_query_session_info),
            client_query_session: buf.as_ref().to_vec().to_owned(),
            client_signature: signature_raw.as_ref().to_vec().to_owned(),
        };
        let res = verify_query_session(&query_session);
        assert!(res.is_ok());
        Ok(())
    }

    #[test]
    fn test_verify_fail() -> Result<()> {
        let client_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 100,
            status: SessionStatus::Stop.into(),
        };
        let node_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
            status: SessionStatus::Stop.into(),
        };
        // encode and sign client_query_session_info
        let kp = get_a_static_keypair();
        let mut buf = BytesMut::with_capacity(1024 * 8);
        client_query_session_info.encode(&mut buf).unwrap();
        let buf = buf.freeze();
        let signer = Db3Signer::new(kp);
        let (signature_raw, public_key_raw) = signer.sign(buf.as_ref())?;
        let query_session = QuerySession {
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            node_query_session_info: Some(node_query_session_info),
            client_query_session: buf.as_ref().to_vec().to_owned(),
            client_signature: signature_raw.as_ref().to_vec().to_owned(),
            client_public_key: public_key_raw.as_ref().to_vec().to_owned(),
        };
        let res = verify_query_session(&query_session);
        assert!(res.is_err());
        Ok(())
    }
}
