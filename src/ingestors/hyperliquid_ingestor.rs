use futures::{sink::SinkExt, StreamExt};
use redis::AsyncCommands;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::HashMap;

pub async fn run(symbols: Vec<String>) {
    let funding_rates = Arc::new(RwLock::new(HashMap::<String, String>::new()));
    
    // Background Task
    let f_map_clone = Arc::clone(&funding_rates);
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let body = json!({"type": "metaAndAssetCtxs"});
            if let Ok(resp) = client.post("https://api.hyperliquid.xyz/info").json(&body).send().await {
                if let Ok(json) = resp.json::<Value>().await {
                    let mut lock = f_map_clone.write().await;
                    if let (Some(meta), Some(ctxs)) = (json[0]["universe"].as_array(), json[1].as_array()) {
                        for (i, asset) in meta.iter().enumerate() {
                            let sym = asset["name"].as_str().unwrap_or("");
                            let rate = ctxs[i]["funding"].as_str().unwrap_or("0.0").to_string();
                            lock.insert(sym.to_string(), rate);
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await.unwrap();

    let url = "wss://api.hyperliquid.xyz/ws";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    let (mut write, mut read) = ws_stream.split();

    for sym in symbols {
        let sub = json!({"method": "subscribe", "subscription": { "type": "l2Book", "coin": sym }});
        write.send(Message::Text(sub.to_string().into())).await.ok();
    }

    while let Some(Ok(Message::Text(text))) = read.next().await {
        if let Ok(v) = serde_json::from_str::<Value>(text.as_str()) {
            if let Some(levels) = v["data"]["levels"].as_array() {
                let coin = v["data"]["coin"].as_str().unwrap_or("UNKNOWN");
                let b = &levels[0][0]; 
                let a = &levels[1][0]; 
                
                let lock = funding_rates.read().await;
                let funding = lock.get(coin).cloned().unwrap_or_else(|| "0.0".to_string());

                let payload = format!("hyperliquid:{}:{},{}|{},{}:{}", 
                    coin, b["px"], b["sz"], a["px"], a["sz"], funding);
                
                let _: () = redis_conn.publish("market:data", &payload).await.unwrap_or(());
            }
        }
    }
}