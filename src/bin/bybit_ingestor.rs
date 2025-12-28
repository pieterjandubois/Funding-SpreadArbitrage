use bybit::ws::response::FuturePublicResponse;
use bybit::ws::future::OrderbookDepth; 
use bybit::WebSocketApiClient;
use redis::AsyncCommands; 
use std::io::{self, Write};
use tokio::sync::mpsc;

pub async fn run() {
    println!("📡 [BYBIT] Initializing Ingestor...");

    // 1. Setup Asynchronous Redis Connection
    let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("❌ [BYBIT] Could not connect to Redis. Is it running in WSL?");

    // 2. Setup Channel to bridge the Blocking Bybit callback to our Async Redis
    // Bybit's SDK uses a blocking callback, so we send the data to a channel first.
    let (tx, mut rx) = mpsc::channel::<String>(100);

    // 3. Initialize Bybit Client
    let mut client = WebSocketApiClient::future_linear().build();
    let symbol = "BTCUSDT";
    client.subscribe_orderbook(symbol, OrderbookDepth::Level1); 

    // 4. Define the Callback
    let callback = move |res: FuturePublicResponse| {
        if let FuturePublicResponse::Orderbook(data) = res {
            if let (Some(bid_item), Some(ask_item)) = (data.data.b.first(), data.data.a.first()) {
                // Format: "bybit:bid:ask"
                let payload = format!("bybit:{}:{}", bid_item.0, ask_item.0);
                // Send to the async worker
                let _ = tx.blocking_send(payload);
            }
        }
    };

    // 5. Spawn the Bybit Client in a separate thread because it is blocking
    std::thread::spawn(move || {
        if let Err(e) = client.run(callback) {
            eprintln!("❌ [BYBIT] Connection Error: {}", e);
        }
    });

    println!("✅ [BYBIT] Streaming live data to Redis...");

    // 6. Async Worker: Pull from channel and publish to Redis
    while let Some(payload) = rx.recv().await {
        let _: () = redis_conn.publish("market:data", &payload)
            .await
            .unwrap_or_else(|e| eprintln!("Redis Publish Error: {}", e));

        // Local console log
        let parts: Vec<&str> = payload.split(':').collect();
        if parts.len() == 3 {
           // print!("\r[BYBIT] Bid: {} | Ask: {}      ", parts[1], parts[2]);
            //let _ = io::stdout().flush();
        }
    }
}

/// Standalone entry point
#[tokio::main]
async fn main() {
    run().await;
}