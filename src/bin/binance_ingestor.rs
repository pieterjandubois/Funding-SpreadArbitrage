use futures::StreamExt;
use redis::AsyncCommands;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::Value;
use std::io::{self, Write};

/// This is the entry point called by your main.rs orchestrator
pub async fn run() {
    // --- CRYPTO PROVIDER FIX ---
    // Important: Only install if not already set. In a multi-task setup, 
    // it's often better to put this in main.rs, but we'll keep it here for safety.
    let _ = rustls::crypto::ring::default_provider().install_default();
    
    println!("📡 [BINANCE] Initializing Ingestor...");

    // 1. Setup Asynchronous Redis Connection
    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("❌ [BINANCE] Could not connect to Redis. Is it running in WSL?");

    // 2. Connect to Binance WebSocket
    let url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";
    let (ws_stream, _) = connect_async(url).await.expect("❌ [BINANCE] Failed to connect to Binance");
    let (_write, mut read) = ws_stream.split();

    println!("✅ [BINANCE] Streaming live data to Redis...");

    // 3. The Real-Time Loop
    while let Some(message) = read.next().await {
        match message {
            Ok(Message::Text(text)) => {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    // Extract Bid and Ask
                    if let (Some(bid), Some(ask)) = (v["b"].as_str(), v["a"].as_str()) {
                        
                        // Unified format: "exchange_name:bid:ask"
                        let payload = format!("binance:{}:{}", bid, ask);
                        
                        // Publish to Redis asynchronously
                        let _: () = redis_conn.publish("market:data", &payload)
                            .await
                            .unwrap_or_else(|e| eprintln!("Redis Publish Error: {}", e));
                        
                        // Local log (Optional: you can comment this out to keep the terminal clean)
                       // print!("\r[BINANCE] Bid: {} | Ask: {}      ", bid, ask);
                       // let _ = io::stdout().flush();
                    }
                }
            }
            Ok(Message::Close(_)) => {
                println!("\n⚠️ [BINANCE] Connection closed by exchange.");
                break;
            }
            Err(e) => {
                eprintln!("\n❌ [BINANCE] Websocket Error: {}", e);
                break;
            }
            _ => (),
        }
    }
}

/// Allows you to still run this file standalone via 'cargo run --bin binance_ingestor'
#[tokio::main]
async fn main() {
    run().await;
}