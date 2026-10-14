use std::{fs, path::PathBuf};
use serde::{Deserialize, Serialize};
use rqs_lib::Visibility;
use directories::ProjectDirs;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppSettings {
    #[serde(default)]
    pub autostart: bool,
    #[serde(default)]
    pub realclose: bool,
    #[serde(default = "default_visibility")]
    pub visibility: u8,
    #[serde(default)]
    pub startminimized: bool,
    pub port: Option<u32>,
    pub download_path: Option<String>,
    pub debug_level: Option<String>,
}

fn default_visibility() -> u8 {
    Visibility::Visible as u8
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            autostart: true,
            realclose: false,
            visibility: Visibility::Visible as u8,
            startminimized: false,
            port: None,
            download_path: None,
            debug_level: None,
        }
    }
}

fn get_config_path() -> PathBuf {
    let proj_dirs = ProjectDirs::from("dev", "mandre", "rquickshare").unwrap();
    let config_dir = proj_dirs.config_dir();
    if !config_dir.exists() {
        let _ = fs::create_dir_all(config_dir);
    }
    config_dir.join(".settings.json")
}

pub fn get_settings() -> AppSettings {
    let path = get_config_path();
    if let Ok(content) = fs::read_to_string(&path) {
        if let Ok(settings) = serde_json::from_str(&content) {
            return settings;
        }
    }
    AppSettings::default()
}

pub fn save_settings(settings: &AppSettings) {
    let path = get_config_path();
    if let Ok(json) = serde_json::to_string_pretty(settings) {
        let _ = fs::write(path, json);
    }
}

pub fn init_default() {
    let _ = get_settings(); // Just to load/create if needed. Or we can force save.
}

#[allow(dead_code)]
pub fn get_realclose() -> bool {
    get_settings().realclose
}

pub fn get_port() -> Option<u32> {
    get_settings().port
}

pub fn get_visibility() -> Visibility {
    Visibility::from_raw_value(get_settings().visibility as u64)
}

pub fn set_visibility(v: Visibility) -> Result<(), anyhow::Error> {
    let mut settings = get_settings();
    settings.visibility = v as u8;
    save_settings(&settings);
    // Note: Emit visibility_updated is handled differently now.
    Ok(())
}

pub fn get_download_path() -> Option<PathBuf> {
    get_settings().download_path.map(PathBuf::from)
}

pub fn get_logging_level() -> Option<String> {
    get_settings().debug_level
}

pub fn get_autostart() -> bool {
    get_settings().autostart
}

pub fn get_startminimized() -> bool {
    get_settings().startminimized
}

pub fn set_autostart(autostart: bool) {

    let mut settings = get_settings();
    settings.autostart = autostart;
    save_settings(&settings);
}

pub fn set_realclose(realclose: bool) {
    let mut settings = get_settings();
    settings.realclose = realclose;
    save_settings(&settings);
}

pub fn set_startminimized(startminimized: bool) {
    let mut settings = get_settings();
    settings.startminimized = startminimized;
    save_settings(&settings);
}

pub fn set_download_path(path: Option<String>) {
    let mut settings = get_settings();
    settings.download_path = path;
    save_settings(&settings);
}


