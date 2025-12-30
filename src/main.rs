mod ingestors;

#[tokio::main]
async fn main() {
    // --- 1. Initialize Rustls (Required for Binance/Bybit WSS) ---
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    println!("🚀 Starting Centralized Arbitrage Node...");

    // Master Watchlist
    let symbols = vec![
        "BTC".to_string(), 
        "ETH".to_string(), 
        "SOL".to_string(), 
        "TIA".to_string(), 
        "ARB".to_string()
    ];

    // --- 2. Spawn Binance Ingestor ---
    let binance = tokio::spawn(ingestors::binance::run());
    
    // Safety check for Binance
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
    if binance.is_finished() {
        println!("❌ FATAL: Binance task died immediately! Check credentials/network.");
    }

    // --- 3. Spawn Hyperliquid Ingestor ---
    let hyperliquid = tokio::spawn(ingestors::hyperliquid::run(symbols.clone()));

    // --- 4. Spawn Bybit Ingestor ---
    let bybit = tokio::spawn(ingestors::bybit::run(symbols.clone()));

    println!("🧠 Brain Active - Aggregating Market Data...");

    // --- 5. Run Scanner (Foreground) ---
    // The matrix logic now lives inside this function
    ingestors::scanner::run().await;

    // Join handles if scanner ever exits
    let _ = tokio::join!(binance, bybit, hyperliquid);

}