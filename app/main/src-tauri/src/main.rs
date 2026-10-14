slint::include_modules!();

use slint::Model;

#[macro_use]
extern crate log;

use std::sync::{Arc, Mutex};

use rqs_lib::channel::{ChannelDirection, ChannelMessage, ChannelAction};
use rqs_lib::{EndpointInfo, SendInfo, State, Visibility, RQS, DeviceType, OutboundPayload as LibOutboundPayload};
use tokio::sync::{broadcast, mpsc, watch};

use crate::logger::set_up_logging;
use crate::notification::{send_request_notification, send_temporarily_notification};
use crate::store::{
    get_download_path, get_port, get_visibility, init_default, set_visibility,
};

mod logger;
mod notification;
mod store;

pub struct AppState {
    pub message_sender: broadcast::Sender<ChannelMessage>,
    pub dch_sender: broadcast::Sender<EndpointInfo>,
    pub visibility_sender: Arc<Mutex<watch::Sender<Visibility>>>,
    pub sender_file: mpsc::Sender<SendInfo>,
    pub rqs: Mutex<RQS>,

    pub discovered_endpoints: Mutex<std::collections::HashMap<String, EndpointInfo>>,
}

#[derive(Clone, Default)]
struct ItemData {
    pub id: String,
    pub name: String,
    pub device_type: String,
    pub endpoint: bool,
    pub state: String,
    pub pin_code: String,
    pub files: Vec<String>,
    pub text_description: String,
    pub text_payload: String,
    pub text_type: String,
    pub destination: String,
    pub total_bytes: i32,
    pub ack_bytes: i32,
}

impl ItemData {
    fn to_displayed_item(&self) -> DisplayedItem {
        let mut f = Vec::new();
        for file in &self.files {
            f.push(slint::SharedString::from(file.clone()));
        }

        DisplayedItem {
            id: self.id.clone().into(),
            name: self.name.clone().into(),
            device_type: self.device_type.clone().into(),
            endpoint: self.endpoint,
            state: self.state.clone().into(),
            pin_code: self.pin_code.clone().into(),
            files: slint::ModelRc::new(slint::VecModel::from(f)),
            text_description: self.text_description.clone().into(),
            text_payload: self.text_payload.clone().into(),
            text_type: self.text_type.clone().into(),
            destination: self.destination.clone().into(),
            total_bytes: self.total_bytes,
            ack_bytes: self.ack_bytes,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    set_up_logging()?;

    debug!("Starting setup of RQuickShare app");

    init_default();

    let visibility = get_visibility();
    let port_number = get_port();
    let download_path = get_download_path();

    let mut rqs = RQS::new(visibility, port_number, download_path);
    let (sender_file, ble_receiver) = rqs.run().await.unwrap();

    let app_state = Arc::new(AppState {
        message_sender: rqs.message_sender.clone(),
        dch_sender: broadcast::channel(10).0,
        visibility_sender: rqs.visibility_sender.clone(),
        sender_file,
        rqs: Mutex::new(rqs),
        discovered_endpoints: Mutex::new(std::collections::HashMap::new()),
    });

    {
        let mut rqs_lock = app_state.rqs.lock().unwrap();
        let _ = rqs_lock.discovery(app_state.dch_sender.clone());
    }

    let state_for_ble = app_state.clone();
    tokio::spawn(async move {
        let mut ble_receiver = ble_receiver;
        let mut last_sent = std::time::Instant::now() - std::time::Duration::from_secs(120);
        loop {
            if let Ok(_) = ble_receiver.recv().await {
                let v = get_visibility();
                trace!("BLE received: {:?}", v);

                if v == Visibility::Invisible
                    && last_sent.elapsed() >= std::time::Duration::from_secs(120)
                {
                    send_temporarily_notification(state_for_ble.clone());
                    last_sent = std::time::Instant::now();
                }
            }
        }
    });

    let ui = MainWindow::new().unwrap();
    let ui_weak = ui.as_weak();

    // Since ModelRc is not Send, we cannot send it between threads.
    // Instead we will rebuild or manage the list inside invoke_from_event_loop.
    // Actually, we can get the model from ui.get_displayed_items().

    // We just maintain the pure data model in a Mutex, and send a signal to update UI.
    let items_state = Arc::new(Mutex::new(Vec::<ItemData>::new()));

    let state_for_msg = app_state.clone();
    let ui_weak_msg = ui_weak.clone();
    let items_state_msg = items_state.clone();
    tokio::spawn(async move {
        let mut receiver = state_for_msg.message_sender.subscribe();
        loop {
            if let Ok(info) = receiver.recv().await {
                if info.state.as_ref().unwrap_or(&State::Initial) == &State::WaitingForUserConsent {
                    let name = info
                        .meta
                        .as_ref()
                        .and_then(|meta| meta.source.as_ref())
                        .map(|source| source.name.clone())
                        .unwrap_or_else(|| "Unknown".to_string());
                    send_request_notification(name, info.id.clone(), state_for_msg.clone());
                }

                let info = info.clone();
                let ui_weak_msg_inner = ui_weak_msg.clone();
                let items_state_inner = items_state_msg.clone();

                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_weak_msg_inner.upgrade() {
                        let mut state = items_state_inner.lock().unwrap();

                        let idx = state.iter().position(|x| x.id == info.id);

                        if let Some(i) = idx {
                            let mut item = state[i].clone();

                            if let Some(st) = &info.state {
                                item.state = match st {
                                    State::Initial => "Initial",
                                    State::WaitingForUserConsent => "WaitingForUserConsent",
                                    State::SentIntroduction => "SentIntroduction",
                                    State::SendingFiles => "SendingFiles",
                                    State::ReceivingFiles => "ReceivingFiles",
                                    State::Finished => "Finished",
                                    State::Cancelled => "Cancelled",
                                    State::Rejected => "Rejected",
                                    State::Disconnected => "Disconnected",
                                    _ => "",
                                }.into();
                            }

                            if let Some(meta) = &info.meta {
                                if let Some(dest) = &meta.destination {
                                    item.destination = dest.into();
                                }
                                if let Some(files) = &meta.files {
                                    item.files = files.clone();
                                }
                                if let Some(tp) = &meta.text_payload {
                                    item.text_payload = tp.into();
                                    item.text_type = "Text".into();
                                }
                                if let Some(td) = &meta.text_description {
                                    item.text_description = td.into();
                                }
                                if let Some(pin) = &meta.pin_code {
                                    item.pin_code = pin.into();
                                }
                                if let Some(source) = &meta.source {
                                    item.name = source.name.clone().into();
                                    item.device_type = match source.device_type {
                                        DeviceType::Unknown => "Unknown",
                                        DeviceType::Phone => "Phone",
                                        DeviceType::Tablet => "Tablet",
                                        DeviceType::Laptop => "Laptop",
                                    }.into();
                                }
                            }

                            state[i] = item.clone();
                        } else {
                            let mut item = ItemData {
                                id: info.id.clone(),
                                name: "Unknown".into(),
                                device_type: "Unknown".into(),
                                endpoint: false,
                                ..Default::default()
                            };

                            if let Some(st) = &info.state {
                                item.state = match st {
                                    State::Initial => "Initial",
                                    State::WaitingForUserConsent => "WaitingForUserConsent",
                                    State::SentIntroduction => "SentIntroduction",
                                    State::SendingFiles => "SendingFiles",
                                    State::ReceivingFiles => "ReceivingFiles",
                                    State::Finished => "Finished",
                                    State::Cancelled => "Cancelled",
                                    State::Rejected => "Rejected",
                                    State::Disconnected => "Disconnected",
                                    _ => "",
                                }.into();
                            }

                            if let Some(meta) = &info.meta {
                                if let Some(source) = &meta.source {
                                    item.name = source.name.clone().into();
                                    item.device_type = match source.device_type {
                                        DeviceType::Unknown => "Unknown",
                                        DeviceType::Phone => "Phone",
                                        DeviceType::Tablet => "Tablet",
                                        DeviceType::Laptop => "Laptop",
                                    }.into();
                                }
                                if let Some(files) = &meta.files {
                                    item.files = files.clone();
                                }
                            }

                            state.push(item);
                        }

                        // Rebuild UI model
                        let mut new_model = Vec::new();
                        for item in state.iter() {
                            new_model.push(item.to_displayed_item());
                        }
                        ui.set_displayed_items(slint::ModelRc::new(slint::VecModel::from(new_model)));
                    }
                });
            }
        }
    });

    let state_for_dch = app_state.clone();
    let ui_weak_dch = ui_weak.clone();
    let items_state_dch = items_state.clone();
    tokio::spawn(async move {
        let mut dch_receiver = state_for_dch.dch_sender.subscribe();
        loop {
            if let Ok(info) = dch_receiver.recv().await {
                {
                    let mut eps = state_for_dch.discovered_endpoints.lock().unwrap();
                    let is_present = info.present.unwrap_or(true);
                    if is_present {
                        eps.insert(info.id.clone(), info.clone());
                    } else {
                        eps.remove(&info.id);
                    }
                }

                let ui_weak_dch_inner = ui_weak_dch.clone();
                let items_state_inner = items_state_dch.clone();

                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_weak_dch_inner.upgrade() {
                        let mut state = items_state_inner.lock().unwrap();

                        let is_present = info.present.unwrap_or(true);

                        let idx = state.iter().position(|x| x.id == info.id);

                        if let Some(i) = idx {
                            if !is_present && state[i].state == "" {
                                state.remove(i);
                            } else if is_present {
                                let mut item = state[i].clone();
                                if let Some(name) = &info.name {
                                    item.name = name.clone().into();
                                }
                                if let Some(rtype) = &info.rtype {
                                    item.device_type = match rtype {
                                        DeviceType::Unknown => "Unknown",
                                        DeviceType::Phone => "Phone",
                                        DeviceType::Tablet => "Tablet",
                                        DeviceType::Laptop => "Laptop",
                                    }.into();
                                }
                                state[i] = item;
                            }
                        } else if is_present {
                            let item = ItemData {
                                id: info.id.clone(),
                                name: info.name.clone().unwrap_or_else(|| "Unknown".to_string()),
                                device_type: match info.rtype {
                                    Some(DeviceType::Unknown) | None => "Unknown",
                                    Some(DeviceType::Phone) => "Phone",
                                    Some(DeviceType::Tablet) => "Tablet",
                                    Some(DeviceType::Laptop) => "Laptop",
                                }.into(),
                                endpoint: true,
                                ..Default::default()
                            };
                            state.push(item);
                        }

                        let mut new_model = Vec::new();
                        for item in state.iter() {
                            new_model.push(item.to_displayed_item());
                        }
                        ui.set_displayed_items(slint::ModelRc::new(slint::VecModel::from(new_model)));
                    }
                });
            }
        }
    });

    let state_for_vis = app_state.clone();
    tokio::spawn(async move {
        let mut visibility_receiver = state_for_vis.visibility_sender.lock().unwrap().subscribe();
        loop {
            if let Ok(_) = visibility_receiver.changed().await {
                let v = visibility_receiver.borrow_and_update();
                let _ = set_visibility(*v);
            }
        }
    });

    ui.on_open_url(|url| {
        let _ = open::that(url.as_str());
    });

    ui.on_open_settings(|| {
        let proj_dirs = directories::ProjectDirs::from("dev", "mandre", "rquickshare").unwrap();
        let _ = open::that(proj_dirs.config_dir());
    });

    let state_for_vis_toggle = app_state.clone();
    let ui_weak_vis = ui_weak.clone();
    ui.on_invert_visibility(move || {
        let current = store::get_visibility();
        let next = match current {
            Visibility::Visible => Visibility::Invisible,
            Visibility::Invisible => Visibility::Temporarily,
            Visibility::Temporarily => Visibility::Visible,
        };
        let _ = store::set_visibility(next.clone());
        state_for_vis_toggle.rqs.lock().unwrap().change_visibility(next.clone());

        let ui_weak = ui_weak_vis.clone();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(ui) = ui_weak.upgrade() {
                let v_str = match next {
                    Visibility::Visible => "Visible",
                    Visibility::Invisible => "Invisible",
                    Visibility::Temporarily => "Temporarily",
                };
                ui.set_visibility(v_str.into());
            }
        });
    });

    let v_str = match visibility {
        Visibility::Visible => "Visible",
        Visibility::Invisible => "Invisible",
        Visibility::Temporarily => "Temporarily",
    };
    ui.set_visibility(v_str.into());
    ui.set_version(env!("CARGO_PKG_VERSION").into());
    ui.set_dark_mode(store::get_dark_mode());
    ui.set_autostart(store::get_autostart());
    ui.set_realclose(store::get_realclose());
    ui.set_startminimized(store::get_startminimized());
    if let Some(path) = store::get_download_path() {
        ui.set_download_path(path.to_string_lossy().to_string().into());
    }

    if let Ok(hostname) = gethostname::gethostname().into_string() {
        ui.set_hostname(hostname.into());
    }

    ui.on_set_dark_mode(|val| {
        store::set_dark_mode(val);
    });

    ui.on_set_autostart(|val| {
        store::set_autostart(val);
    });

    ui.on_set_realclose(|val| {
        store::set_realclose(val);
    });

    ui.on_set_startminimized(|val| {
        store::set_startminimized(val);
    });

    let ui_weak_download = ui_weak.clone();
    ui.on_select_download_path(move || {
        let ui_weak = ui_weak_download.clone();
        tokio::spawn(async move {
            let folder = rfd::AsyncFileDialog::new().pick_folder().await;
            if let Some(folder) = folder {
                let path_str = folder.path().to_string_lossy().to_string();
                store::set_download_path(Some(path_str.clone()));
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(ui) = ui_weak.upgrade() {
                        ui.set_download_path(path_str.into());
                    }
                });
            }
        });
    });


    let ui_weak_picker = ui_weak.clone();
    ui.on_open_file_picker(move || {
        let ui_weak = ui_weak_picker.clone();
        tokio::spawn(async move {
            let files = rfd::AsyncFileDialog::new().pick_files().await;
            if let Some(files) = files {
                let mut paths = Vec::new();
                for f in files {
                    paths.push(slint::SharedString::from(f.path().to_string_lossy().into_owned()));
                }

                let _ = slint::invoke_from_event_loop({
                    let ui_weak = ui_weak.clone();
                    move || {
                        if let Some(ui) = ui_weak.upgrade() {
                            ui.set_has_outbound_payload(true);
                            let payload = OutboundPayload { files: slint::ModelRc::new(slint::VecModel::from(paths)) };
                            ui.set_outbound_payload(payload);
                        }
                    }
                });
            }
        });
    });

    let state_for_cmd = app_state.clone();
    ui.on_send_cmd(move |id, cmd| {
        let action = match cmd.as_str() {
            "AcceptTransfer" => Some(ChannelAction::AcceptTransfer),
            "RejectTransfer" => Some(ChannelAction::RejectTransfer),
            "CancelTransfer" => Some(ChannelAction::CancelTransfer),
            _ => None,
        };
        if let Some(action) = action {
            let _ = state_for_cmd.message_sender.send(ChannelMessage {
                id: id.to_string(),
                direction: ChannelDirection::FrontToLib,
                action: Some(action),
                ..Default::default()
            });
        }
    });

    let ui_weak_send = ui_weak.clone();
    let state_for_send_info = app_state.clone();
    ui.on_send_info(move |id| {
        let ui_weak_send_inner = ui_weak_send.clone();
        let state_for_send_inner = state_for_send_info.clone();
        let id_str = id.to_string();

        let _ = slint::invoke_from_event_loop(move || {
            if let Some(ui) = ui_weak_send_inner.upgrade() {
                let payload = ui.get_outbound_payload();
                let mut files = Vec::new();
                for i in 0..payload.files.row_count() {
                    if let Some(f) = payload.files.row_data(i) {
                        files.push(f.to_string());
                    }
                }

                if !files.is_empty() {
                    let mut name = String::new();
                    let mut addr = String::new();

                    {
                        let eps = state_for_send_inner.discovered_endpoints.lock().unwrap();
                        if let Some(ep) = eps.get(&id_str) {
                            if let Some(ep_name) = &ep.name {
                                name = ep_name.clone();
                            }
                            if let Some(ip) = &ep.ip {
                                if let Some(port) = &ep.port {
                                    addr = format!("{}:{}", ip, port);
                                }
                            }
                        }
                    }

                    if !name.is_empty() && !addr.is_empty() {
                        let _ = state_for_send_inner.sender_file.blocking_send(SendInfo {
                            id: id_str.clone(),
                            name,
                            addr,
                            ob: LibOutboundPayload::Files(files),
                        });
                    }
                }
            }
        });
    });

    let ui_weak_remove = ui_weak.clone();
    let items_state_remove = items_state.clone();
    ui.on_remove_request(move |id| {
        let id_str = id.to_string();
        let ui_weak_remove_inner = ui_weak_remove.clone();
        let items_state_inner = items_state_remove.clone();

        let _ = slint::invoke_from_event_loop(move || {
            if let Some(ui) = ui_weak_remove_inner.upgrade() {
                let mut state = items_state_inner.lock().unwrap();
                if let Some(i) = state.iter().position(|x| x.id == id_str) {
                    state.remove(i);
                }

                let mut new_model = Vec::new();
                for item in state.iter() {
                    new_model.push(item.to_displayed_item());
                }
                ui.set_displayed_items(slint::ModelRc::new(slint::VecModel::from(new_model)));
            }
        });
    });

    ui.on_write_to_clipboard(|text| {
        if let Ok(mut cb) = arboard::Clipboard::new() {
            let _ = cb.set_text(text.as_str());
        }
    });

    ui.on_clear_sending({
        let ui_weak = ui_weak.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_has_outbound_payload(false);
                ui.set_outbound_payload(OutboundPayload { files: slint::ModelRc::new(slint::VecModel::default()) });
            }
        }
    });

    #[cfg(target_os = "linux")]
    struct RQuickShareTray {
        ui_weak: slint::Weak<MainWindow>,
        state: Arc<AppState>,
    }

    #[cfg(target_os = "linux")]
    impl ksni::Tray for RQuickShareTray {
        fn id(&self) -> String {
            "rquickshare".into()
        }
        fn icon_name(&self) -> String {
            "rquickshare".into()
        }
        fn title(&self) -> String {
            "RQuickShare".into()
        }
        fn activate(&mut self, _x: i32, _y: i32) {
            let ui_weak = self.ui_weak.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(ui) = ui_weak.upgrade() {
                    let _ = ui.show();
                }
            });
        }
        fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
            use ksni::menu::*;
            vec![
                StandardItem {
                    label: "Open RQuickShare".into(),
                    activate: Box::new(|this: &mut Self| {
                        let ui_weak = this.ui_weak.clone();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(ui) = ui_weak.upgrade() {
                                let _ = ui.show();
                            }
                        });
                    }),
                    ..Default::default()
                }
                .into(),
                MenuItem::Separator,
                StandardItem {
                    label: "Quit".into(),
                    activate: Box::new(|this: &mut Self| {
                        let state = this.state.clone();
                        tokio::task::block_in_place(|| {
                            tokio::runtime::Handle::current().block_on(async move {
                                let _ = state.rqs.lock().unwrap().stop().await;
                            });
                        });
                        let _ = slint::quit_event_loop();
                    }),
                    ..Default::default()
                }
                .into(),
            ]
        }
    }

    #[cfg(target_os = "linux")]
    let _tray_handle = {
        let tray = RQuickShareTray {
            ui_weak: ui_weak.clone(),
            state: app_state.clone(),
        };
        let service = ksni::TrayService::new(tray);
        service.spawn()
    };

    let state_for_shutdown = app_state.clone();
    ui.window().on_close_requested(move || {
        let realclose = store::get_realclose();
        if realclose {
            let state = state_for_shutdown.clone();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    let _ = state.rqs.lock().unwrap().stop().await;
                });
            });

            let _ = slint::quit_event_loop();
        }

        slint::CloseRequestResponse::HideWindow
    });

    if store::get_startminimized() {
        let _ = ui.hide();
    }

    ui.run().unwrap();
    Ok(())
}
