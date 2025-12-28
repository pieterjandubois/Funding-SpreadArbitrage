mod ingestors; 

#[tokio::main]
async fn main() {
    println!("🚀 Starting Centralized Arbitrage Node...");

    // Spawn them all in the background
    let binance = tokio::spawn(ingestors::binance::run());
    let bybit = tokio::spawn(ingestors::bybit::run());
    let hyperliquid = tokio::spawn(ingestors::hyperliquid::run());
    
    println!("🧠 Brain Active - Aggregating Market Data...");
    
    // Run the scanner in the foreground so we see the output
    ingestors::scanner::run().await;

    // Wait for all (this keeps the program alive)
    let _ = tokio::join!(binance, bybit, hyperliquid);
}