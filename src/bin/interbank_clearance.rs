use chrono::{Duration, Utc};
use neo4rs::{query, ConfigBuilder, Graph};
use tungstenite::Message;
use url::Url;

const CLEARANCE_MIN_THRESHOLD: u128 = 100;
const CLEARANCE_MAX_THRESHOLD: u128 = u128::MAX;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Default)]
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ClaimRequest {
    pub did: String,      // hex
    pub bank_did: String, // hex
    pub amount: u128,
    pub chained_transactions: Vec<Vec<Transaction>>,
    clearance: Option<bool>, // todo: sign and hash? to be discussed
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct WSClaimRequest {
    event: String,
    data: ClaimRequest,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct Res {
    transaction: Option<Transaction>,
    ack_id: String,
}
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct WSClaimResponse {
    data: Res,
    success: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct Msg {
    message: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct WSAckResponse {
    data: Msg,
    success: bool,
}
#[tokio::main]
async fn main() {
    // FETCH ALL BANK DUES
    let mut db_res = fetch_bank_dues().await.unwrap();

    let mut total_clearance_amount: u128 = 0;

    let mut clearance_txs: Vec<Transaction> = vec![];

    while let Some(row) = db_res.next().await.unwrap() {
        let mut tx: Transaction = Default::default();
        if let Some(rel) = row.get("relation") {
            let relation: neo4rs::Relation = rel;

            tx = Transaction {
                previous_tx_hash: None,
                amount: relation
                    .get::<String>("amount")
                    .expect("amount")
                    .parse::<u128>()
                    .unwrap(),
                from_did: relation.get::<String>("to_bank").expect("from"),
                to_did: relation.get::<String>("from_bank").expect("to"),
                issued_at: relation.get::<i64>("issued_at").expect("time"),
                expiry_at: (Utc::now() + Duration::days(365)).timestamp_millis(),
                tx_hash: String::new(),
                signed_tx_hash: String::new(),
            };

            total_clearance_amount += tx.amount;
            clearance_txs.push(tx)
        }
    }
    if total_clearance_amount < CLEARANCE_MIN_THRESHOLD {
        println!("Very smol claimable amount: {}", total_clearance_amount);
        return;
    }
    for tx in clearance_txs.clone() {
            let claim_req = ClaimRequest {
                did: tx.from_did.clone(),
                bank_did: tx.to_did.clone(),
                amount: tx.amount,
                chained_transactions: vec![vec![tx.clone()]],
                clearance: Some(true),
            };

            let ws_claim_req = WSClaimRequest {
                event: "claim".to_string(),
                data: claim_req,
            };
            println!("From: {:#?}", ws_claim_req);

            let (mut socket, response) =
                tungstenite::connect(Url::parse("ws://localhost:8000/v3/api/offline/claim/").unwrap())
                    .expect("Can't connect");

            socket
                .write_message(Message::Text(serde_json::to_string(&ws_claim_req).unwrap()))
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

                if parsed_ack_response.success {
                    update_status(tx.to_did, tx.from_did, tx.amount, tx.issued_at)
                    .await;
                }
            }
            socket.close(None).unwrap();
    }
    println!("From: {:#?}", total_clearance_amount);
}

async fn fetch_bank_dues() -> Result<neo4rs::RowStream, neo4rs::Error> {
    let graph = connect().await;

    let query =
        query("MATCH (:InterBankClearance)-[relation {status:'PENDING'}]->(:InterBankClearance) RETURN relation");
    let db_res = graph.execute(query).await;

    db_res
}

async fn update_status(from_bank: String, to_bank: String, amount: u128, issued_at: i64) {
    let graph = connect().await;

    let query =
        query("MATCH (from_bank:InterBankClearance)-[relation]->(to_bank:InterBankClearance) WHERE from_bank.name = $from_bank AND to_bank.name = $to_bank AND relation.status = 'PENDING' AND relation.issued_at = $issued_at AND relation.amount = $amount SET relation.status = 'CLAIMED'")
        .param("from_bank", from_bank)
        .param("to_bank", to_bank)
        .param("issued_at", issued_at)
        .param("amount", amount.to_string())
        ;
    graph.run(query).await;
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
