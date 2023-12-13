use neo4rs::{query, ConfigBuilder, Graph};
use tungstenite::Message;
use url::Url;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct Transaction {
    previous_tx_hash: Option<String>,
    amount: u128,
    from_did: String,
    to_did: String,
    issued_at: i64,
    expiry_at: i64,
    tx_hash: String,
    signed_tx_hash: String,
}
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
struct Res {
    transaction: Option<Transaction>,
    ack_id: String,
}
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
struct WSClaimResponse {
    data: Res,
    success: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
struct Msg {
    message: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
struct WSAckResponse {
    data: Msg,
    success: bool,
}
#[tokio::main]
async fn main() {
    // FETCH ALL BANK DUES
    fetch_bank_dues().await;

    let (mut socket, response) =
        tungstenite::connect(Url::parse("ws://localhost:8001/v3/api/offline/claim/").unwrap())
            .expect("Can't connect");

    socket
        .write_message(Message::Text(
            r#"{
                "event":"claim",
                "data": {
                    "did": "0x6469643a6e62673a626f62000000000000000000000000000000000000000000",
                    "bank_did": "0x6469643a6e62673a62616e6b5f62000000000000000000000000000000000000",
                    "amount":2,
                    "chained_transactions":[
                        [
                          {
                            "previous_tx_hash": null,
                            "amount": 90,
                            "from_did": "0x6469643a6e62673a62616e6b0000000000000000000000000000000000000000",
                            "to_did": "0x6469643a6e62673a616c69636500000000000000000000000000000000000000",
                            "issued_at": 1702472809124,
                            "expiry_at": 1734008809124,
                            "tx_hash": "0x756e42f807e015837509f1e4b56e4418480b9989680fbae9c604c6b82d8255d8",
                            "signed_tx_hash": "0xfaf2927bf4130c30abb32b44ea80c38a83ba8ed46965f9d08ce03e9eb328292a20730c40419d8112cc3c265bf0afa508666b8638663f6ab48b3fae427afc0f84"
                          },
                          {
                            "previous_tx_hash": "0x756e42f807e015837509f1e4b56e4418480b9989680fbae9c604c6b82d8255d8",
                            "amount": 10,
                            "from_did": "0x6469643a6e62673a616c69636500000000000000000000000000000000000000",
                            "to_did": "0x6469643a6e62673a626f62000000000000000000000000000000000000000000",
                            "issued_at": 1702472836843,
                            "expiry_at": 1734008809124,
                            "tx_hash": "0xbee8c04c5e200dd96ba04346a57afb4be5277b2dea382f7ed5579d7f9a74281b",
                            "signed_tx_hash": "0x1a2d02c2f38821818e5c3e8767c9c9dd932f51cbd3e301c03c9bab61778bc44dbca4fd3aed96bde88012a1dc5b6e8c9592a2b347a3daf9f98d673aca6ba0568e"
                          },
                          {
                            "previous_tx_hash": "0xbee8c04c5e200dd96ba04346a57afb4be5277b2dea382f7ed5579d7f9a74281b",
                            "amount": 10,
                            "from_did": "0x6469643a6e62673a626f62000000000000000000000000000000000000000000",
                            "to_did": "0x6469643a6e62673a62616e6b5f62000000000000000000000000000000000000",
                            "issued_at": 1702472841801,
                            "expiry_at": 1734008809124,
                            "tx_hash": "0xde2beebdc0967a864e973a3d3d3971d14bb1afddd38f1e3a33b0f7aec3dcc8d0",
                            "signed_tx_hash": "0x76afcb8473ea6109fe164876cd2220c06902c004a9aeac1ca2b7fbc62df21d062dedc23db0af5da98e146e94d8c16234f61cbfc3179313195c4766f5579f0c8b"
                          }
                        ]
                      ]
                }
            }"#
            .into(),
        ))
        .unwrap();

    let msg: Message = socket.read_message().expect("Error reading message");
    let msg = match msg {
        tungstenite::Message::Text(s) => s,
        _ => {
            panic!()
        }
    };

    let parsed_claim_response: WSClaimResponse = serde_json::from_str(&msg).unwrap();
    println!("{:#?}", parsed_claim_response);

    if parsed_claim_response.success {
        #[derive(serde::Serialize, Debug)]
        struct AckReq {
            ack_id: String,
            event: String,
        }
        let msg = AckReq {
            ack_id: parsed_claim_response.data.ack_id,
            event: String::from("ack"),
        };

        // let msg = format!(
        //     r#"{{"ack_id": {},"event": "ack"}}"#,
        //     parsed_claim_response.data.ack_id
        // );
        println!("{:#?}", msg);
        socket
            .write_message(Message::Text(serde_json::to_string(&msg).unwrap()))
            .unwrap();

        let msg: Message = socket.read_message().expect("Error reading message");
        let msg = match msg {
            tungstenite::Message::Text(s) => s,
            _ => {
                panic!("failed to get String")
            }
        };

        let parsed_ack_response: WSAckResponse = serde_json::from_str(&msg).unwrap();
        println!("{:#?}", parsed_ack_response);
    }

    socket.close(None).unwrap();
}

async fn fetch_bank_dues() {
    let graph = connect().await;

    let query =
        query("MATCH (:InterBankClearance)-[r {state:'PENDING'}]->(:InterBankClearance) RETURN r");
    let mut db_res = graph.execute(query).await.unwrap();

    while let Some(row) = db_res.next().await.unwrap() {
        println!("{:#?}", row)
    }
}
async fn connect() -> Graph {
    let config = ConfigBuilder::default()
        .uri("localhost:7687")
        .user("neo4j")
        .password("buttercup@2")
        // .db("neo4j")
        // .fetch_size(500)
        // .max_connections(10)
        .build()
        .unwrap();
    Graph::connect(config).await.unwrap()
}
