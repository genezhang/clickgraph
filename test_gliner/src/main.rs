use gline_rs::model::{
    input::text::TextInput, params::Parameters, pipeline::token::TokenMode, GLiNER,
    RuntimeParameters,
};
use log::info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let model_path = "../models/onnx/model_q4.onnx";
    let tokenizer_path = "../models/tokenizer.json";

    info!("Loading GLiNER model...");

    let model = GLiNER::<TokenMode>::new(
        Parameters::default(),
        RuntimeParameters::default(),
        tokenizer_path,
        model_path,
    )?;

    info!("Model loaded!");

    // Test with schema-like text
    let text = "user_id follower_id followed_id order_id customer_id file_id analysis_id origin_code dest_city";

    let labels = vec![
        "NODE_TABLE",
        "EDGE_TABLE",
        "FOREIGN_KEY",
        "PRIMARY_KEY",
        "DENORMALIZED_PATTERN",
        "POLYMORPHIC_INDICATOR",
    ];

    let input = TextInput::from_str(&[text], &labels)?;

    info!("Running inference...");
    let output = model.inference(input)?;

    info!("Results:");
    for (text, label, score) in output {
        println!("  {} => {} ({:.2})", text, label, score);
    }

    Ok(())
}
