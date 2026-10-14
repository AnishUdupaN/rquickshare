use fern::colors::{Color, ColoredLevelConfig};
use humantime::format_rfc3339_seconds;
use std::time::SystemTime;

use crate::store::get_logging_level;

pub fn set_up_logging() -> Result<(), anyhow::Error> {
    let colors = ColoredLevelConfig::new()
        .info(Color::Green)
        .debug(Color::Cyan)
        .trace(Color::BrightBlack);

    let ll = match std::env::var("RQS_LOG") {
        Ok(l) => l,
        Err(_) => match get_logging_level() {
            Some(l) => l,
            None => "info".to_string(),
        },
    };

    let log_level = match ll.as_str() {
        "trace" => log::LevelFilter::Trace,
        "debug" => log::LevelFilter::Debug,
        "info" => log::LevelFilter::Info,
        "warn" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => log::LevelFilter::Info, // fallback
    };

    let base_config = fern::Dispatch::new()
        .level(log_level)
        .level_for("reqwest", log::LevelFilter::Info)
        .level_for("h2", log::LevelFilter::Info)
        .level_for("mdns_sd", log::LevelFilter::Info)
        .level_for("bluer", log::LevelFilter::Info)
        .level_for("zbus", log::LevelFilter::Info)
        .level_for("notify_rust", log::LevelFilter::Info)
        .level_for("tracing", log::LevelFilter::Info);

    let stdout_config = fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                format_rfc3339_seconds(SystemTime::now()),
                colors.color(record.level()),
                record.target(),
                message
            ))
        })
        .chain(std::io::stdout());

    base_config.chain(stdout_config).apply()?;

    Ok(())
}
