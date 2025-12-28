use futures::StreamExt;
use std::collections::HashMap;
use std::io::{self, Write};
use tokio::time::{interval, Duration};

pub async fn run() {
    let total_fees = 0.00105;
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut pubsub_conn = client.get_async_pubsub().await.expect("Redis failed");
    pubsub_conn.subscribe("market:data").await.unwrap();

    let mut stream = pubsub_conn.into_on_message();
    let mut prices: HashMap<String, (f64, f64)> = HashMap::new();
    let mut render_timer = interval(Duration::from_millis(100));

    // ANSI Escape Codes for Colors
    let green = "\x1b[32m";
    let red = "\x1b[31m";
    let cyan = "\x1b[36m";
    let reset = "\x1b[0m";
    let clear_line = "\x1b[2K";

    // Clear screen once at start
    print!("{}[2J", 27 as char);

    loop {
        tokio::select! {
            Some(msg) = stream.next() => {
                let payload: String = msg.get_payload().expect("Payload error");
                let parts: Vec<&str> = payload.split(':').collect();
                if parts.len() >= 3 {
                    prices.insert(parts[0].to_string(), (parts[1].parse().unwrap_or(0.0), parts[2].parse().unwrap_or(0.0)));
                }
            }
            _ = render_timer.tick() => {
                // Reset cursor to top-left
                print!("{}[1;1H", 27 as char); 

                println!("{}═══ 📈 LIVE PRICES ═══", clear_line);
                let exchanges = vec!["binance", "bybit", "hyperliquid"];
                for ex in &exchanges {
                    if let Some((b, a)) = prices.get(*ex) {
                        println!("{}{}{:<12}{} | Bid: {:<10.2} | Ask: {:<10.2}", 
                            clear_line, cyan, ex.to_uppercase(), reset, b, a);
                    }
                }

                println!("{}\n{}═══ 🚨 ARBITRAGE ROUTES ═══", clear_line, clear_line);
                let pairs = vec![
                    ("bybit", "binance"), ("binance", "bybit"),
                    ("bybit", "hyperliquid"), ("hyperliquid", "bybit"),
                    ("hyperliquid", "binance"), ("binance", "hyperliquid"),
                ];

                for (buy_ex, sell_ex) in pairs {
                    if let (Some((_, ask_b)), Some((bid_s, _))) = (prices.get(buy_ex), prices.get(sell_ex)) {
                        let spread = (bid_s - ask_b) / ask_b;
                        let net = spread - total_fees;
                        
                        // Color logic
                        let (icon, label, color) = if net > 0.0 {
                            ("✅", "PROFIT", green)
                        } else {
                            ("❌", "NO ARB ", red)
                        };
                        
                        println!(
                            "{}{}{} {} | {:>11} -> {:<11} | Net: {:>7.4}%{}",
                            clear_line, color, icon, label, 
                            buy_ex.to_uppercase(), sell_ex.to_uppercase(), 
                            net * 100.0, reset
                        );
                    }
                }
                
                io::stdout().flush().unwrap();
            }
        }
    }
}

#[tokio::main]
async fn main() { run().await; }