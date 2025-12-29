use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use chrono::{Utc, Timelike, DateTime};
use redis::AsyncCommands;

// --- 📊 MATH & LIQUIDITY ENGINE ---

fn calculate_vwap(levels: &Vec<(f64, f64)>, target_usd: f64) -> f64 {
    let mut current_usd = 0.0;
    let mut current_qty = 0.0;
    if levels.is_empty() { return 0.0; }
    for (price, qty) in levels {
        let level_value = price * qty;
        if current_usd + level_value >= target_usd {
            let needed_usd = target_usd - current_usd;
            current_qty += needed_usd / price;
            return target_usd / current_qty;
        }
        current_usd += level_value;
        current_qty += qty;
    }
    levels.last().map(|l| l.0).unwrap_or(0.0)
}

fn calculate_regression_slope(data: &VecDeque<f64>) -> f64 {
    let n = data.len() as f64;
    if n < 10.0 { return 0.0; } 
    let (mut sum_x, mut sum_y, mut sum_xy, mut sum_xx) = (0.0, 0.0, 0.0, 0.0);
    for (i, &y) in data.iter().enumerate() {
        let x = i as f64;
        sum_x += x; sum_y += y; sum_xy += x * y; sum_xx += x * x;
    }
    let denominator = n * sum_xx - sum_x * sum_x;
    if denominator == 0.0 { return 0.0; }
    (n * sum_xy - sum_x * sum_y) / denominator
}

fn calculate_std_dev(data: &VecDeque<f64>) -> f64 {
    let n = data.len() as f64;
    if n < 2.0 { return 0.0; }
    let mean = data.iter().sum::<f64>() / n;
    let variance = data.iter().map(|&y| (y - mean).powi(2)).sum::<f64>() / n;
    variance.sqrt()
}

#[derive(Clone)]
struct MarketState {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    funding: f64,
}

pub async fn run() {
    let base_fees = 0.00105; 
    let trade_size_usd = 1000.0;
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    
    let market_data = Arc::new(RwLock::new(HashMap::<String, MarketState>::new()));
    let m_clone = Arc::clone(&market_data);

    // --- 1. TASK: ASYNC DATA LISTENER ---
    tokio::spawn(async move {
        let mut pubsub_conn = redis::Client::open("redis://127.0.0.1/").unwrap()
            .get_async_pubsub().await.unwrap();
        pubsub_conn.subscribe("market:data").await.unwrap();
        let mut stream = pubsub_conn.into_on_message();

        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload().unwrap();
            let parts: Vec<&str> = payload.split(':').collect();
            
            if parts.len() >= 4 {
                let exchange = parts[0];
                let symbol = parts[1];
                let depth_parts: Vec<&str> = parts[2].split('|').collect();
                
                let parse_depth = |s: &str| -> Vec<(f64, f64)> {
                    s.split(',')
                        .map(|val| val.trim().replace('\"', ""))
                        .collect::<Vec<String>>()
                        .chunks(2)
                        .filter(|c| c.len() == 2)
                        .map(|c| (
                            c[0].parse::<f64>().unwrap_or(0.0),
                            c[1].parse::<f64>().unwrap_or(0.0)
                        ))
                        .filter(|(p, _q)| *p > 0.0)
                        .collect()
                };

                if depth_parts.len() == 2 {
                    let state = MarketState {
                        bids: parse_depth(depth_parts[0]),
                        asks: parse_depth(depth_parts[1]),
                        funding: parts[3].parse().unwrap_or(0.0),
                    };

                    let key = format!("{}_{}", exchange, symbol);
                    let mut lock = m_clone.write().await;
                    lock.insert(key, state);
                }
            }
        }
    });

    // --- 2. MAIN BRAIN: THE DECISION LOOP ---
    let mut signal_conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut is_in_position = false;
    let mut active_pair: (String, String, String) = ("".to_string(), "".to_string(), "".to_string());
    let mut entry_basis: f64 = 0.0;
    
    let mut basis_histories: HashMap<String, VecDeque<f64>> = HashMap::new();
    let mut signal_streak: HashMap<String, u32> = HashMap::new();
    let mut last_exit_time: Option<DateTime<Utc>> = None;

    let (green, red, yellow, reset) = ("\x1b[32m", "\x1b[31m", "\x1b[33m", "\x1b[0m");
    let clr = "\x1b[K"; // ANSI: Clear from cursor to end of line
    let mut render_timer = interval(Duration::from_millis(200));

    // Initial clear once
    print!("{}[2J", 27 as char); 

    loop {
        render_timer.tick().await;
        
        // Move cursor to 1,1 WITHOUT clearing the screen (prevents flicker)
        print!("{}[1;1H", 27 as char); 
        
        let now = Utc::now();
        
        // Header with clear-to-end-of-line
        println!("═══ 🕒 {:02}:{:02}:{:02} | LIVE MATRIX | SIZE: ${} ═══{}", 
                 now.hour(), now.minute(), now.second(), trade_size_usd, clr);
        println!("{:<22} | {:>9} | {:>9} | {:>8} | {:>7} | TREND{}", 
                 "EXCHANGE PAIR", "BASIS", "NET", "FUNDING", "STREAK", clr);
        println!("{}{}", "─".repeat(85), clr);

        let m_snap = market_data.read().await.clone();
        let symbols = vec!["BTC", "ETH", "SOL", "TIA", "ARB"]; 
        let exchanges = vec!["binance", "hyperliquid", "bybit"];

        for symbol in &symbols {
            for i in 0..exchanges.len() {
                for j in (i + 1)..exchanges.len() {
                    let ex_a = exchanges[i]; 
                    let ex_b = exchanges[j]; 
                    let key_a = format!("{}_{}", ex_a, symbol);
                    let key_b = format!("{}_{}", ex_b, symbol);

                    if let (Some(state_a), Some(state_b)) = (m_snap.get(&key_a), m_snap.get(&key_b)) {
                        let a_bid = calculate_vwap(&state_a.bids, trade_size_usd);
                        let a_ask = calculate_vwap(&state_a.asks, trade_size_usd);
                        let b_bid = calculate_vwap(&state_b.bids, trade_size_usd);
                        let b_ask = calculate_vwap(&state_b.asks, trade_size_usd);

                        let (v_short, v_long, current_basis, fund_diff) = if a_bid > b_ask {
                            (ex_a, ex_b, (a_bid - b_ask) / b_ask, state_a.funding - state_b.funding)
                        } else {
                            (ex_b, ex_a, (b_bid - a_ask) / a_ask, state_b.funding - state_a.funding)
                        };

                        let net_profit = current_basis - base_fees;
                        let pair_id = format!("{}-{}-{}", v_short, v_long, symbol).to_uppercase();
                        
                        let history_limit = 1200; 
                        let history = basis_histories
                            .entry(pair_id.clone())
                            .or_insert_with(|| VecDeque::with_capacity(history_limit));

                        history.push_back(current_basis);
                        if history.len() > history_limit { history.pop_front(); }

                        let slope = calculate_regression_slope(history);
                        let vol = calculate_std_dev(history);
                        let streak = signal_streak.entry(pair_id.clone()).or_insert(0);

                        let trend_label = if slope > (vol * 0.01) { 
                            format!("{}UP ↗{}", green, reset) 
                        } else if slope < -(vol * 0.01) { 
                            format!("{}DN ↘{}", red, reset) 
                        } else { 
                            format!("{}FLAT →{}", yellow, reset) 
                        };

                        if net_profit > 0.0 && slope > 0.0 { *streak += 1; } else { *streak = 0; }

                        let b_color = if net_profit > 0.0 { green } else { reset };
                        let f_color = if fund_diff > 0.0 { green } else { yellow };

                        println!("{:<22} | {:>8.4}% | {}{:>8.4}%{} | {}{:>7.4}%{} | {:>7} | {}{}", 
                            pair_id, current_basis * 100.0, 
                            b_color, net_profit * 100.0, reset,
                            f_color, fund_diff * 100.0, reset,
                            streak, trend_label, clr
                        );

                        // --- 🚀 EXECUTION ---
                        if !is_in_position {
                            let cooled = last_exit_time.map_or(true, |t| now.signed_duration_since(t).num_minutes() >= 5);
                            if cooled && *streak >= 5 && net_profit > 0.0 {
                                // We print signals at the very bottom of the loop or in a separate log area
                                let cmd = format!("CMD:OPEN|SYMBOL:{}|S:{}|L:{}|BASIS:{}", symbol, v_short, v_long, current_basis);
                                let _: () = signal_conn.publish("trade:signals", cmd).await.unwrap_or(());
                                
                                is_in_position = true;
                                active_pair = (v_short.to_string(), v_long.to_string(), symbol.to_string());
                                entry_basis = current_basis;
                            }
                        } else if active_pair.0 == v_short && active_pair.1 == v_long && active_pair.2 == symbol.to_string() {
                             if (entry_basis - current_basis).abs() > (entry_basis.abs() * 0.7) {
                                let cmd = format!("CMD:EXIT|SYM:{}|V_S:{}|V_L:{}", symbol, v_short, v_long);
                                let _: () = signal_conn.publish("trade:signals", cmd).await.unwrap_or(());
                                is_in_position = false;
                                last_exit_time = Some(now);
                            }
                        }
                    } else {
                        println!("{:<22} | {:>9} | {:>9} | {:>8} | {:>7} | ⚠️ OFFLINE{}", 
                            format!("{}-{}-{}", ex_a, ex_b, symbol).to_uppercase(), "-", "-", "-", 0, clr);
                    }
                }
            }
        }

        println!("\n═══ 📜 RECENT TRADE LOGS ═══{}", clr);
        // If in position, show the active trade status
        if is_in_position {
            println!("{}ACTIVE TRADE: {}-{} on {}{}", green, active_pair.0, active_pair.1, active_pair.2, clr);
        }
        
        io::stdout().flush().unwrap();
    }
}