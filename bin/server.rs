use ray::{
    server::{
        Config,
        serve_forever,
    }
};

use clap::{
	Arg,
	App,
};

use std::{
    fs::File,
    io::Read,
    process::exit,
};


const ABOUT: &str = "Ray server";

struct Arguments {
    config: Option<String>,
}

fn parse_arguments() -> Arguments {
	let parser = App::new("rayd")
                         .version(ray::VERSION)
                         .author(ray::AUTHORS)
                         .about(ABOUT)
                         .arg(Arg::with_name("config")
                              .short("c")
                              .long("config")
                              .value_name("CONFIG_PATH")
                              .help("path to rayd config file")
                              .takes_value(true));
    let matches = parser.get_matches();
    let config = matches.value_of("config").map(|s| s.to_string());

    Arguments { config }
}

fn read_config(path: &str) -> Config {
    let mut file = File::open(path).unwrap_or_else(|err| {
        eprintln!("Failed to open '{}': {}", path, err);
        exit(1);
    });

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap_or_else(|err| {
        eprintln!("Failed to read '{}': {}", path, err);
        exit(1);
    });

    serde_yaml::from_slice(&buffer).unwrap_or_else(|err| {
        eprintln!("Failed to parse config: {}", err);
        exit(1);
    })
}


fn main() {
    let args = parse_arguments();
    let config = args.config.map(|path| read_config(&path)).unwrap_or_default();
    serve_forever(config);
}
