use std::collections::HashMap;

use chrono::Utc;
use neo4rs::{query, ConfigBuilder, Graph};

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
#[derive(Debug, Clone)]
struct InterBankAmount {
    from_bank: String,
    to_bank: String,
    amount: u128,
}

#[tokio::main]
async fn main() {
    let req_json_data =
        std::fs::read_to_string("multibanksync_1.json").expect("Unable to read file");
    // let req_json_data = fs::read_to_string("multibanksync.json").expect("Unable to read file");
    let tx_chain_array: Vec<Vec<Transaction>> = serde_json::from_str(&req_json_data).unwrap();

    let mut inter_bank_amount: Vec<InterBankAmount> = Vec::new();
    for chain in tx_chain_array {
        let issuance = chain.first().unwrap();
        let sync = chain.last().unwrap();

        let from_bank = issuance.from_did.clone();
        let to_bank = sync.to_did.clone();

        if from_bank != to_bank {
            let ib_amount = InterBankAmount {
                from_bank,
                to_bank,
                amount: sync.amount,
            };
            inter_bank_amount.push(ib_amount)
        }
    }
    // no duplicates now exist in amount map
    type Amount = HashMap<(String, String), u128>;
    let mut interbank_amount_map: Amount = Amount::new();
    for element in inter_bank_amount.clone() {
        *interbank_amount_map
            .entry((element.from_bank, element.to_bank))
            .or_insert(0) += element.amount;
    }
    println!("{interbank_amount_map:#?}");

    // for ((from, to), amount) in hm.clone() {
    //     let key = (from.clone(), to.clone());
    //     let reverse_key = (to, from);
    //     *temp.entry(key.clone()).or_insert(0) += amount;
    //     if hm.contains_key(&(reverse_key)) {
    //         *temp.entry(key.clone()).or_insert(0) -= hm[&reverse_key];
    //         // hm.remove(&reverse_key);
    //         // hm.remove(&key);
    //     }
    // }
    // println!("{:#?}", inter_bank_amount);

    persist_tx(interbank_amount_map).await;
}
async fn persist_tx(inter_bank_amount: HashMap<(String, String), u128>) {
    let graph = connect().await;
    // add r properties:tx_hash, sign
    for ((from_bank, to_bank), amount) in inter_bank_amount {
        let query_str = format!("
        MERGE (source:InterBankClearance{{name:$from_bank}}) 
            ON CREATE SET source.created_at = $now  
            ON MATCH SET source.updated_at = $now 
        MERGE (destination:InterBankClearance{{name:$to_bank}})  
            ON CREATE SET destination.created_at = $now 
            ON MATCH SET destination.updated_at = $now  
        MERGE (source)-[relation:`{}`{{status:'PENDING',from_bank:$from_bank,to_bank:$to_bank,amount:$amount,issued_at:$now}}]->(destination)   
            ON MATCH SET relation.updated_at = $now
            
            ",amount);
        let query = query(&query_str)
            .param("from_bank", from_bank)
            .param("to_bank", to_bank)
            .param("amount", amount.to_string())
            .param("now", Utc::now().timestamp_millis());

        graph.run(query).await.unwrap();
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
