fn main() {
    eprintln!("✓ Starting simple axum test...");
    
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let app = axum::Router::new().route("/", axum::routing::get(|| async { "Hello, World!" }));
        
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8081").await.unwrap();
        eprintln!("✓ Simple test server listening on 0.0.0.0:8081");
        
        axum::serve(listener, app).await.unwrap();
    });
}
