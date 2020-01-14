use ray::{
    client::RayClient,
    config::DEFAULT_PORT,
};

use clap::{
	Arg,
	App,
    AppSettings,
	SubCommand,
    value_t_or_exit,
};

use byte_string::ByteStr;

use std::io::Read;

const ABOUT: &str = "Ray command line interface";

#[derive(Debug)]
enum Command {
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

#[derive(Debug)]
struct Arguments {
    address: String,
    port: u16,
    command: Command,
}

fn read_stdin() -> String {
    let mut result = String::new();
    std::io::stdin().read_to_string(&mut result).map_err(|error| {
        eprintln!("Error: {}", error);
        std::process::exit(1);
    }).unwrap();
    result
}

fn parse_arguments() -> Arguments {
    let default_port_string = DEFAULT_PORT.to_string();
	let parser = App::new(env!("CARGO_PKG_NAME"))
                         .version(ray::VERSION)
                         .author(ray::AUTHORS)
                         .about(ABOUT)
                         .setting(AppSettings::SubcommandRequiredElseHelp)
                         .arg(Arg::with_name("address")
                              .short("a")
                              .long("address")
                              .value_name("ADDRESS")
                              .help("rayd host address")
                              .takes_value(true)
                              .default_value("localhost"))
                         .arg(Arg::with_name("port")
                              .short("p")
                              .long("port")
                              .value_name("PORT")
                              .help("rayd TCP port")
                              .takes_value(true)
                              .default_value(&default_port_string))
                         .subcommand(SubCommand::with_name("get")
                                    .about("Get value of given key")
                                    .arg(Arg::with_name("key")
                                         .help("key to get")
                                         .required(true)))
                         .subcommand(SubCommand::with_name("set")
                                    .about("Set value for given key")
                                    .arg(Arg::with_name("key")
                                         .help("key to set value for")
                                         .required(true))
                                    .arg(Arg::with_name("value")
                                         .help("value to set")));
    let matches = parser.get_matches();

    let address = matches.value_of("address").unwrap().to_string();
    let port = value_t_or_exit!(matches, "port", u16);

    let command = match matches.subcommand_name().unwrap() {
        "get" => {
            let inner = matches.subcommand_matches("get").unwrap();
            Command::Get {
                key: inner.value_of("key").unwrap().into()
            }
        },
        "set" => {
            let inner = matches.subcommand_matches("set").unwrap();
            let value: String = inner.value_of("value").map(|value| value.into())
                .unwrap_or_else(read_stdin);
            Command::Set {
                key: inner.value_of("key").unwrap().into(),
                value: value.into_bytes(),
            }
        }
        _ => unreachable!(),
    };

    Arguments { address, port, command }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_arguments();
    let mut client = RayClient::connect(&args.address, args.port).await?;

    match args.command {
        Command::Set{ key, value } => {
            client.set(key, value).await?;
        },
        Command::Get{ key } => {
            let value = client.get(key).await?;
            let formatted = format!("{:?}", ByteStr::new(&value));
            println!("{}", &formatted[1..]);
        },
    };

    Ok(())
}
