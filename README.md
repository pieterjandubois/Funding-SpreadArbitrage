RUN:

- Open terminal 1: commands:
              - wsl
              - redis-cli
              - SUBSCRIBE market:data[channel ...]
- Open terminal 2: commands:
              - cargo run --bin scanner-rust
  
