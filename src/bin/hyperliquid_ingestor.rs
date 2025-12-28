use futures::sink::SinkExt;
use futures::StreamExt;
use redis::AsyncCommands;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::{json, Value};
use std::io::{self, Write};

pub async fn run() {
    // Only install the crypto provider if it hasn't been set yet
    let _ = rustls::crypto::ring::default_provider().install_default();

    println!("📡 [HYPERLIQUID] Initializing Ingestor...");

    // 1. Setup Asynchronous Redis Connection
    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("❌ [HYPERLIQUID] Could not connect to Redis.");

    // 2. Connect to Hyperliquid WebSocket
    let url = "wss://api.hyperliquid.xyz/ws";
    let (ws_stream, _) = connect_async(url).await.expect("❌ [HYPERLIQUID] Failed to connect");
    let (mut write, mut read) = ws_stream.split();

    // 3. Subscribe to L2 Book for BTC
    let subscribe_msg = json!({
        "method": "subscribe",
        "subscription": { "type": "l2Book", "coin": "BTC" }
    });

    write.send(Message::Text(subscribe_msg.to_string().into())).await.ok();
    println!("✅ [HYPERLIQUID] Subscribed and streaming to Redis...");

    // 4. The Real-Time Loop
    while let Some(message) = read.next().await {
        if let Ok(Message::Text(text)) = message {
            if let Ok(v) = serde_json::from_str::<Value>(text.as_str()) {
                
                // Hyperliquid path drill-down
                let levels = v["data"]["levels"].as_array()
                            .or_else(|| v["data"]["data"]["levels"].as_array());

                if let Some(levels) = levels {
                    // levels[0] is bids, levels[1] is asks
                    let bid = levels[0][0]["px"].as_str().unwrap_or("0.0");
                    let ask = levels[1][0]["px"].as_str().unwrap_or("0.0");

                    if bid != "0.0" {
                        // Unified format: "hyperliquid:bid:ask"
                        let payload = format!("hyperliquid:{}:{}", bid, ask);
                        
                        let _: () = redis_conn.publish("market:data", &payload)
                            .await
                            .unwrap_or_else(|e| eprintln!("Redis Publish Error: {}", e));

                      //  print!("\r[HYPER] Bid: {} | Ask: {}      ", bid, ask);
                       // let _ = io::stdout().flush();
                    }
                }
            }
        }
    }
}

/// Standalone entry point
#[tokio::main]
async fn main() {
    run().await;
}