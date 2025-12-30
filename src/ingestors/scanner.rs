use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::io::{stdout, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration, Instant};
use chrono::{Utc, Timelike};
use redis::AsyncCommands;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
    style::{Color, Print, ResetColor, SetForegroundColor},
    QueueableCommand,
};

// --- 📊 TYPES & ENUMS ---

#[derive(Debug, PartialEq, Clone, Copy)]
enum TradeTier { Noise, Acceptable, GreatEntry, Sniper }

fn get_trade_tier(net_profit: f64) -> TradeTier {
    if net_profit < 0.0002 { TradeTier::Noise }
    else if net_profit < 0.0005 { TradeTier::Acceptable }
    else if net_profit < 0.0015 { TradeTier::GreatEntry }
    else { TradeTier::Sniper }
}

#[derive(Clone)]
struct MarketState {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    funding: f64,
}

/// NEW: The Sentinel ensures the OBI "Settles" before we trade.
struct TradeSentinel {
    first_favorable_obi: Option<Instant>,
    cooldown: Duration,
}

impl TradeSentinel {
    fn new() -> Self {
        Self {
            first_favorable_obi: None,
            cooldown: Duration::from_millis(500),
        }
    }

    fn check_obi_stability(&mut self, is_favorable: bool) -> bool {
        if !is_favorable {
            self.first_favorable_obi = None; // Reset timer if OBI dips
            return false;
        }
        match self.first_favorable_obi {
            Some(start) => start.elapsed() >= self.cooldown,
            None => {
                self.first_favorable_obi = Some(Instant::now());
                false
            }
        }
    }
}

// --- 🧠 MATH & LIQUIDITY ENGINE ---

fn calculate_weighted_obi(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> f64 {
    let mut bid_vol = 0.0;
    let mut ask_vol = 0.0;
    for i in 0..5 {
        let weight = (5 - i) as f64;
        if let Some(level) = bids.get(i) { bid_vol += level.1 * weight; }
        if let Some(level) = asks.get(i) { ask_vol += level.1 * weight; }
    }
    if bid_vol + ask_vol == 0.0 { return 0.0; }
    (bid_vol - ask_vol) / (bid_vol + ask_vol)
}

fn calculate_vwap(levels: &[(f64, f64)], target_usd: f64) -> f64 {
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

fn get_seconds_to_payout(exchange: &str, funding_rate: f64) -> i64 {
    let now = Utc::now();
    let window_size = if exchange == "hyperliquid" { 1 } 
                      else if exchange == "binance" && funding_rate.abs() >= 0.03 { 1 } 
                      else { 8 };
    let hour = now.hour();
    let next_payout_hour = ((hour / window_size) + 1) * window_size;
    let mut next_window = now.with_hour(next_payout_hour % 24).unwrap()
        .with_minute(0).unwrap().with_second(0).unwrap();
    if next_payout_hour >= 24 { next_window = next_window + chrono::Duration::days(1); }
    (next_window - now).num_seconds()
}

// --- 🚀 MAIN RUNNER ---

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let base_fees = 0.00105; 
    let trade_size_usd = 1000.0;
    let client = redis::Client::open("redis://localhost/")?;
    let market_data = Arc::new(RwLock::new(HashMap::<String, MarketState>::new()));
    let m_clone = Arc::clone(&market_data);

    let mut out = stdout();
    terminal::enable_raw_mode()?; 
    execute!(out, EnterAlternateScreen, cursor::Hide, Clear(ClearType::All))?;

    tokio::spawn(async move {
        let mut pubsub_conn = redis::Client::open("redis://localhost/").unwrap()
            .get_async_pubsub().await.unwrap();
        pubsub_conn.subscribe("market:data").await.unwrap();
        let mut stream = pubsub_conn.into_on_message();

        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload().unwrap();
            let parts: Vec<&str> = payload.split(':').collect();
            if parts.len() >= 4 {
                let (exchange, symbol) = (parts[0], parts[1]);
                let depth_parts: Vec<&str> = parts[2].split('|').collect();
                let parse_depth = |s: &str| -> Vec<(f64, f64)> {
                    s.split(',').map(|v| v.trim().replace('\"', ""))
                        .collect::<Vec<String>>().chunks(2).filter(|c| c.len() == 2)
                        .map(|c| (c[0].parse().unwrap_or(0.0), c[1].parse().unwrap_or(0.0)))
                        .collect()
                };
                if depth_parts.len() == 2 {
                    let state = MarketState {
                        bids: parse_depth(depth_parts[0]),
                        asks: parse_depth(depth_parts[1]),
                        funding: parts[3].parse().unwrap_or(0.0),
                    };
                    m_clone.write().await.insert(format!("{}_{}", exchange, symbol), state);
                }
            }
        }
    });

    let mut signal_conn = client.get_multiplexed_async_connection().await?;
    let mut is_in_position = false;
    let mut active_pair_id = String::new();
    let mut entry_basis = 0.0;
    let mut basis_histories: HashMap<String, VecDeque<f64>> = HashMap::new();
    let mut signal_streak: HashMap<String, u32> = HashMap::new();
    let mut sentinels: HashMap<String, TradeSentinel> = HashMap::new();

    let mut render_timer = interval(Duration::from_millis(200));

    loop {
        if event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL)) {
                    break; 
                }
            }
        }

        render_timer.tick().await;
        let now = Utc::now();
        let m_snap = market_data.read().await.clone();
        out.queue(cursor::MoveTo(0, 0))?;

        out.queue(SetForegroundColor(Color::Cyan))?;
        out.queue(Print(format!("═══ 🕒 {:02}:{:02}:{:02} | TIERED NORMALIZATION | OBI FILTERED ═══\r\n", 
            now.hour(), now.minute(), now.second())))?;
        out.queue(ResetColor)?;
        out.queue(Print(format!("{:<20} | {:>7} | {:>7} | {:>5} | {:>5} | OBI\r\n", "PAIR", "BASIS", "NET", "FUND", "NEXT")))?;
        out.queue(Print(format!("{}\r\n", "─".repeat(75))))?;

        let symbols = vec!["BTC", "ETH", "SOL", "TIA", "ARB"]; 
        let exchanges = vec!["binance", "hyperliquid", "bybit"];

        for symbol in &symbols {
            for i in 0..exchanges.len() {
                for j in (i + 1)..exchanges.len() {
                    let (ex_a, ex_b) = (exchanges[i], exchanges[j]);
                    let (k_a, k_b) = (format!("{}_{}", ex_a, symbol), format!("{}_{}", ex_b, symbol));

                    if let (Some(state_a), Some(state_b)) = (m_snap.get(&k_a), m_snap.get(&k_b)) {
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
                        let tier = get_trade_tier(net_profit);
                        let pair_id = format!("{}-{}-{}", v_short, v_long, symbol).to_uppercase();
                        
                        let short_obi = calculate_weighted_obi(if v_short == ex_a { &state_a.bids } else { &state_b.bids }, if v_short == ex_a { &state_a.asks } else { &state_b.asks });
                        let long_obi = calculate_weighted_obi(if v_long == ex_a { &state_a.bids } else { &state_b.bids }, if v_long == ex_a { &state_a.asks } else { &state_b.asks });

                        let ttl = get_seconds_to_payout(ex_a, state_a.funding).min(get_seconds_to_payout(ex_b, state_b.funding));

                        // Track History & Slope
                        let history = basis_histories.entry(pair_id.clone()).or_insert_with(|| VecDeque::with_capacity(120));
                        history.push_back(current_basis);
                        if history.len() > 120 { history.pop_front(); }
                        let slope = calculate_regression_slope(history);
                        let streak = signal_streak.entry(pair_id.clone()).or_insert(0);
                        if tier != TradeTier::Noise && slope > 0.0 { *streak += 1; } else { *streak = 0; }

                        // Sentinel Check (Stability)
                        let sentinel = sentinels.entry(pair_id.clone()).or_insert_with(TradeSentinel::new);
                        let is_favorable = short_obi < 0.6 && long_obi > -0.6;
                        let obi_stable = sentinel.check_obi_stability(is_favorable);

                        // Rendering Logic (Same as yours)
                        let tier_color = match tier {
                            TradeTier::Sniper => Color::Magenta,
                            TradeTier::GreatEntry => Color::Green,
                            TradeTier::Acceptable => Color::Yellow,
                            _ => Color::DarkGrey,
                        };

                        out.queue(SetForegroundColor(tier_color))?;
                        out.queue(Print(format!("{:<20}", pair_id)))?;
                        out.queue(ResetColor)?;
                        out.queue(Print(format!(" | {:>6.3}% | {:>6.3}% | {:>4.2}% | {:>4}m | {:+.2} {}\r\n", 
                            current_basis * 100.0, net_profit * 100.0, fund_diff * 100.0, ttl / 60, short_obi, if obi_stable {"STABLE"} else {"..."})))?;

                        // --- 🧠 STRATEGY ENGINE ---
                        if !is_in_position && tier != TradeTier::Noise {
                            let is_sniper = tier == TradeTier::Sniper || (ttl < 600 && net_profit > 0.0003);

                            if (tier == TradeTier::GreatEntry || is_sniper) && *streak >= 5 && obi_stable {
                                let cmd = format!("CMD:OPEN_LIMIT|SYM:{}|S:{}|L:{}|B:{}|T:{:?}", symbol, v_short, v_long, current_basis, tier);
                                let _: () = signal_conn.publish("trade:signals", cmd).await.unwrap_or(());
                                is_in_position = true;
                                active_pair_id = pair_id.clone();
                                entry_basis = current_basis;
                            }
                        } else if active_pair_id == pair_id {
                            let unrealized_gain = entry_basis - current_basis;
                            let normalized = if entry_basis != 0.0 { unrealized_gain / entry_basis > 0.80 } else { false };
                            let stop_loss = current_basis > (entry_basis + 0.0010);

                            if normalized || stop_loss || (ttl < 30 && unrealized_gain > 0.0) {
                                let cmd = format!("CMD:CLOSE_LIMIT|SYM:{}|S:{}|L:{}", symbol, v_short, v_long);
                                let _: () = signal_conn.publish("trade:signals", cmd).await.unwrap_or(());
                                is_in_position = false;
                                active_pair_id = String::new();
                            }
                        }
                    }
                }
            }
        }
        out.queue(Print(format!("\r\n═══ 📜 MONITOR: {} ═══\r\n", if active_pair_id.is_empty() { "SCANNING..." } else { &active_pair_id })))?;
        out.queue(Clear(ClearType::FromCursorDown))?;
        out.flush()?;
    }

    terminal::disable_raw_mode()?;
    execute!(out, cursor::Show, LeaveAlternateScreen)?;
    Ok(())
}