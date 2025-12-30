use futures::StreamExt;
use redis::AsyncCommands;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::Value;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

pub async fn run() {
    let watchlist = vec!["btc", "eth", "sol", "tia", "arb"];
    
    // 1. Thread-safe storage for funding rates
    let funding_rates = Arc::new(RwLock::new(HashMap::<String, String>::new()));
    
    // 2. Background Task: Fetch funding rates from Binance REST API every 30s
    let f_map_clone = Arc::clone(&funding_rates);
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            // GET the premium index (contains funding rates for all perpetuals)
            let url = "https://fapi.binance.com/fapi/v1/premiumIndex";
            if let Ok(resp) = client.get(url).send().await {
                // E0282 Fix: Explicitly tell Rust this is a Value (JSON)
                if let Ok(json) = resp.json::<Value>().await {
                    let mut lock = f_map_clone.write().await;
                    if let Some(items) = json.as_array() {
                        for item in items {
                            if let Some(symbol_raw) = item["symbol"].as_str() {
                                let clean_sym = symbol_raw.replace("USDT", "");
                                let rate = item["lastFundingRate"].as_str().unwrap_or("0.0").to_string();
                                lock.insert(clean_sym, rate);
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    // 3. Setup WebSocket
    let streams = watchlist
        .iter()
        .map(|s| format!("{}usdt@bookTicker", s))
        .collect::<Vec<_>>()
        .join("/");
    
    let url = format!("wss://stream.binance.com:9443/stream?streams={}", streams);
    let redis_client = redis::Client::open("redis://localhost/").unwrap();
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await.expect("Redis Fail");

    loop {
        println!("🔌 [Binance] Connecting...");
        let (ws_stream, _) = match connect_async(&url).await {
            Ok(s) => s,
            Err(e) => { 
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (_, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    let data = v.get("data").unwrap_or(&v);
                    
                    if let Some(symbol_raw) = data.get("s").and_then(|s| s.as_str()) {
                        let clean_symbol = symbol_raw.replace("USDT", "");

                        // Get funding from cache
                        let lock = funding_rates.read().await;
                        let funding = lock.get(&clean_symbol).cloned().unwrap_or_else(|| "0.0".to_string());

                        let payload = format!(
                            "binance:{}:{},{}|{},{}:{}", 
                            clean_symbol, 
                            data["b"].as_str().unwrap_or("0"), 
                            data["B"].as_str().unwrap_or("0"), 
                            data["a"].as_str().unwrap_or("0"), 
                            data["A"].as_str().unwrap_or("0"),
                            funding
                        );

                        let _: () = redis_conn.publish("market:data", &payload).await.unwrap_or(());
                    }
                }
            }
        }
    }
}