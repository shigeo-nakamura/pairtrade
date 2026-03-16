use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: convert-data <input.jsonl> <output.bin>");
        process::exit(1);
    }

    let input = &args[1];
    let output = &args[2];

    eprintln!("Converting {} -> {} ...", input, output);
    match debot::ports::replay_dex::ReplayConnector::convert_jsonl_to_bincode(input, output) {
        Ok(()) => eprintln!("Done."),
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}
