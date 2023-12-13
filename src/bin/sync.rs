use std::collections::HashMap;

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
#[derive(Debug)]
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

        let ib_amount = InterBankAmount {
            from_bank: issuance.from_did.clone(),
            to_bank: sync.to_did.clone(),
            amount: sync.amount,
        };

        inter_bank_amount.push(ib_amount)
    }
    // remove duplicates now exist in hm
    // type Amount = HashMap<(String, String), i128>;
    // let mut hm: Amount = Amount::new();
    // let mut temp: Amount = Amount::new();
    //  for element in ib_amount {
    //     *hm.entry((element.from, element.to)).or_insert(0) += element.amount;
    // }
    // println!("{hm:#?}");

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
    println!("{:#?}", inter_bank_amount);

    persist_tx(inter_bank_amount).await;
}

async fn persist_tx(inter_bank_amount: Vec<InterBankAmount>) {
    let graph = connect().await;
    // add r properties:tx_hash, sign
    for tx in inter_bank_amount {
        let query_str = format!("CREATE (:InterBankClearance{{name:$from_bank}})-[:`{}`{{state:'PENDING',from_bank:$from_bank,to_bank:$to_bank,amount:$amount}}]->(:InterBankClearance{{name:$to_bank}})",tx.amount);
        let query = query(&query_str)
            .param("from_bank", tx.from_bank)
            .param("to_bank", tx.to_bank)
            .param("amount", tx.amount.to_string())
            ;

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
