RUN:

- Open terminal 1: commands:
              - wsl
              - redis-cli
              - SUBSCRIBE market:data
- Open terminal 2: commands:
              - cargo run --bin scanner-rust

1. Current output
   
<img width="611" height="368" alt="image" src="https://github.com/user-attachments/assets/76d48802-c52d-44fc-bb8d-233bf908b8a7" />

