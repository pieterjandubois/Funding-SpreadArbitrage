use bybit::ws::response::FuturePublicResponse;
use bybit::ws::future::OrderbookDepth; 
use bybit::WebSocketApiClient;
use redis::AsyncCommands; 
use tokio::sync::{mpsc, RwLock}; // Added RwLock
use std::sync::Arc;
use std::collections::HashMap;
use serde_json::Value;

pub async fn run(symbols: Vec<String>) {
    // 1. Initialize the Map INSIDE the run function
    let funding_rates = Arc::new(RwLock::new(HashMap::<String, String>::new()));
    
    // 2. Background Task to fetch funding
    let f_map_clone = Arc::clone(&funding_rates);
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let url = "https://api.bybit.com/v5/market/tickers?category=linear";
            if let Ok(resp) = client.get(url).send().await {
                if let Ok(json) = resp.json::<Value>().await {
                    let mut lock = f_map_clone.write().await;
                    if let Some(list) = json["result"]["list"].as_array() {
                        for item in list {
                            let sym = item["symbol"].as_str().unwrap_or("").replace("USDT", "");
                            let rate = item["fundingRate"].as_str().unwrap_or("0.0").to_string();
                            lock.insert(sym, rate);
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    let redis_client = redis::Client::open("redis://localhost/").unwrap();
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await.unwrap();
    let (tx, mut rx) = mpsc::channel::<String>(1000);
    let mut client = WebSocketApiClient::future_linear().build();
    
    for sym in &symbols {
        client.subscribe_orderbook(&format!("{}USDT", sym), OrderbookDepth::Level1); 
    }

    // Capture the Arc in the callback
    let f_map_for_cb = Arc::clone(&funding_rates);
    let callback = move |res: FuturePublicResponse| {
        if let FuturePublicResponse::Orderbook(data) = res {
            if let (Some(b), Some(a)) = (data.data.b.first(), data.data.a.first()) {
                let clean_symbol = data.data.s.replace("USDT", "");
                
                // Use blocking_read because the Bybit crate callback is synchronous
                let lock = f_map_for_cb.blocking_read();
                let funding = lock.get(&clean_symbol).cloned().unwrap_or_else(|| "0.0".to_string());

                let payload = format!(
                    "bybit:{}:{},{}|{},{}:{}", 
                    clean_symbol, b.0, b.1, a.0, a.1, funding
                );
                let _ = tx.blocking_send(payload);
            }
        }
    };

    std::thread::spawn(move || { client.run(callback).ok(); });

    while let Some(payload) = rx.recv().await {
        let _: () = redis_conn.publish("market:data", &payload).await.unwrap_or(());
    }
}