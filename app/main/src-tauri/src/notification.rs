use notify_rust::Notification;
use rqs_lib::{
    channel::{ChannelAction, ChannelDirection, ChannelMessage},
    Visibility,
};
use std::sync::Arc;
use crate::AppState;


pub fn send_request_notification(name: String, id: String, state: Arc<AppState>) {
    let body = format!("{name} want to initiate a transfer");

    match Notification::new()
        .summary("RQuickShare")
        .body(&body)
        .action("accept", "Accept")
        .action("reject", "Reject")
        .show()
    {
        Ok(n) => {
            tokio::task::spawn(async move {
                n.wait_for_action(move |action| match action {
                    "accept" => {
                        let _ = state.message_sender.send(ChannelMessage {
                            id: id.clone(),
                            direction: ChannelDirection::FrontToLib,
                            action: Some(ChannelAction::AcceptTransfer),
                            ..Default::default()
                        });
                    }
                    "reject" => {
                        let _ = state.message_sender.send(ChannelMessage {
                            id: id.clone(),
                            direction: ChannelDirection::FrontToLib,
                            action: Some(ChannelAction::RejectTransfer),
                            ..Default::default()
                        });
                    }
                    _ => (),
                });
            });
        }
        Err(e) => {
            error!("Couldn't show notification: {}", e);
        }
    }
}

pub fn send_temporarily_notification(state: Arc<AppState>) {
    let body = "RQuickShare is temporarily hidden".to_string();

    match Notification::new()
        .summary("RQuickShare")
        .body(&body)
        .action("visible", "Be visible (1m)")
        .action("ignore", "Ignore")
        .id(1919)
        .show()
    {
        Ok(n) => {
            tokio::task::spawn(async move {
                n.wait_for_action(move |action| match action {
                    "visible" => {
                        state.rqs.lock().unwrap().change_visibility(Visibility::Temporarily);
                    }
                    "ignore" => {}
                    _ => (),
                });
            });
        }
        Err(e) => {
            error!("Couldn't show notification: {}", e);
        }
    }
}
