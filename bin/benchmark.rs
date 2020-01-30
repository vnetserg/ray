use ray::{
    benchmark::{
        BenchmarkConfig,
        run_benchmark,
        SimpleReadBenchmark,
        SimpleWriteBenchmark,
    },
    server::Config,
};

use clap::{value_t_or_exit, App, AppSettings, Arg, SubCommand};

use log::LevelFilter;
use simplelog::{SimpleLogger, LevelPadding};

const ABOUT: &str = "Ray benchmark tool";

enum BenchmarkKind {
    Read,
    Write,
}

fn parse_arguments() -> (BenchmarkConfig, BenchmarkKind) {
    let default_port_string = Config::default().rpc.port.to_string();
    let parser = App::new("ray")
        .version(ray::VERSION)
        .author(ray::AUTHORS)
        .about(ABOUT)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .value_name("ADDRESS")
                .help("rayd host address")
                .takes_value(true)
                .default_value("localhost"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("rayd TCP port")
                .takes_value(true)
                .default_value(&default_port_string),
        )
        .arg(
            Arg::with_name("threads")
                .short("t")
                .long("threads")
                .value_name("COUNT")
                .help("blocking threads count (0 for cpu per thread)")
                .takes_value(true)
                .default_value("0"),
        )
        .arg(
            Arg::with_name("clients")
                .short("c")
                .long("clients")
                .value_name("COUNT")
                .help("number of concurrent clients")
                .takes_value(true)
                .default_value("256"),
        )
        .subcommand(
            SubCommand::with_name("read")
                .about("Simple read benchmark: all clients fetch the same key in a loop"),
        )
        .subcommand(
            SubCommand::with_name("write")
                .about("Simple write benchmark: each client generates a random key-value pair \
                       and inserts it in a loop"),
        );

    let matches = parser.get_matches();

    let address = matches.value_of("address").unwrap().to_string();
    let port = value_t_or_exit!(matches, "port", u16);
    let threads = value_t_or_exit!(matches, "threads", u16);
    let tasks = value_t_or_exit!(matches, "clients", u16);

    let config = BenchmarkConfig { address, port, threads, tasks };

    let kind = match matches.subcommand_name().unwrap() {
        "read" => BenchmarkKind::Read,
        "write" => BenchmarkKind::Write,
        _ => unreachable!(),
    };

    (config, kind)
}

fn init_logging() {
    let config = simplelog::ConfigBuilder::new()
        .add_filter_allow_str("ray")
        .add_filter_allow_str("log_panics")
        .set_time_format_str("%T%.3f")
        .set_thread_level(LevelFilter::Off)
        .set_level_padding(LevelPadding::Off)
        .build();
    SimpleLogger::init(LevelFilter::Info, config).unwrap();
    log_panics::init();
}

fn main() {
    init_logging();

    let (config, kind) = parse_arguments();

    match kind {
        BenchmarkKind::Read => {
            let benchmark = SimpleReadBenchmark::default();
            run_benchmark(benchmark, config);
        }
        BenchmarkKind::Write => {
            let benchmark = SimpleWriteBenchmark::default();
            run_benchmark(benchmark, config);
        }
    }
}
