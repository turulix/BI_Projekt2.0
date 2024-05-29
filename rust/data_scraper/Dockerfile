FROM ubuntu:latest as builder

WORKDIR /src
COPY . .

# Update default packages
RUN apt-get update

RUN apt-get install -y \
    build-essential \
    curl

# Update new packages
RUN apt-get update

ENV PATH="/root/.cargo/bin:${PATH}"

# Get Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

RUN rustup install nightly
RUN rustup default nightly
RUN rustup target add x86_64-unknown-linux-musl

# Install dependencies
RUN apt-get -y install gcc musl-dev make perl libffi-dev musl-tools libssl-dev pkg-config cmake build-essential

# Build the project
RUN cargo build --release --target x86_64-unknown-linux-musl --bin data_scraper

FROM alpine:latest
WORKDIR /app
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/data_scraper /app/data_scraper

CMD ["./data_scraper"]
