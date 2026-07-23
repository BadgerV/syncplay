use std::time::Duration;

use egui::{Color32, RichText};

use crate::audio::device::{enumerate_input_devices, enumerate_output_devices};
use crate::engine::{start_receiver, start_sender};
use crate::state::shared::{
    AppMode, AppState, ReceiverState, SharedApp, AUDIO_PORT, SAMPLE_RATE,
};

/// The main egui application.
pub struct SyncPlayApp {
    pub app: SharedApp,
}

impl SyncPlayApp {
    pub fn new(app: SharedApp) -> Self {
        Self { app }
    }
}

impl eframe::App for SyncPlayApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let shared = self.app.clone();
        let mut app = self.app.lock();

        // ── Top bar: mode switch ──
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(RichText::new("♪ SyncPlay").size(20.0).strong());
                ui.separator();

                let old_mode = app.mode;
                ui.selectable_value(&mut app.mode, AppMode::Sender, "🎤 Sender");
                ui.selectable_value(&mut app.mode, AppMode::Receiver, "🔊 Receiver");

                if app.mode != old_mode {
                    // Mode switched — signal and tear down any live engine threads.
                    stop_sender(&mut app);
                    stop_receiver(&mut app);
                }
            });
        });

        // ── Central area ──
        egui::CentralPanel::default().show(ctx, |ui| {
            match app.mode {
                AppMode::Sender => sender_ui(ui, &mut app, &shared),
                AppMode::Receiver => receiver_ui(ui, &mut app, &shared),
            }
        });

        // ── Bottom status bar ──
        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                match app.mode {
                    AppMode::Sender => {
                        if app.sender.is_streaming {
                            ui.label(RichText::new("● Streaming").color(Color32::GREEN));
                            ui.separator();
                            ui.label(format!(
                                "Packets: {} | Receivers: {}",
                                app.sender.packets_sent,
                                app.sender.receiver_count,
                            ));
                        } else {
                            ui.label(RichText::new("○ Idle").color(Color32::GRAY));
                        }
                    }
                    AppMode::Receiver => {
                        if app.receiver.is_connected {
                            ui.label(RichText::new("● Connected").color(Color32::GREEN));
                            ui.separator();
                            ui.label(format!(
                                "Buf: {:.0}ms | Speed: {:+.3}% | Loss: {:.1}%",
                                app.receiver.buffer_fill_ms,
                                app.receiver.current_speed_adjust * 100.0,
                                loss_percent(&app.receiver),
                            ));
                        } else {
                            ui.label(RichText::new("○ Idle").color(Color32::GRAY));
                        }
                    }
                }
            });
        });

        // Refresh device lists periodically
        ctx.request_repaint_after(Duration::from_millis(1000));
    }
}

// ─── Sender UI ─────────────────────────────────────────

fn sender_ui(ui: &mut egui::Ui, app: &mut AppState, shared: &SharedApp) {
    // Refresh device list
    if app.sender.available_input_devices.is_empty() {
        app.sender.available_input_devices = enumerate_input_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
        app.sender.available_output_devices = enumerate_output_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
    }

    let s = &mut app.sender;

    ui.heading("Audio Setup");

    // Input device selector
    ui.horizontal(|ui| {
        ui.label("Input:");
        egui::ComboBox::from_id_salt("input_device")
            .selected_text(if s.selected_input_device.is_empty() {
                "(select device)"
            } else {
                &s.selected_input_device
            })
            .show_ui(ui, |ui| {
                for dev in &s.available_input_devices {
                    ui.selectable_value(
                        &mut s.selected_input_device,
                        dev.clone(),
                        dev,
                    );
                }
            });
    });

    // Refresh button
    if ui.button("↻ Refresh").clicked() {
        s.available_input_devices = enumerate_input_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
        s.available_output_devices = enumerate_output_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
    }

    ui.separator();

    // Start / Stop — apply the action after the `s` borrow ends (below).
    let mut action = SenderAction::None;
    ui.horizontal(|ui| {
        let can_start = !s.is_streaming && !s.selected_input_device.is_empty();
        if ui
            .add_enabled(can_start, egui::Button::new("▶ Start Streaming"))
            .clicked()
        {
            action = SenderAction::Start(s.selected_input_device.clone());
        }

        if ui
            .add_enabled(s.is_streaming, egui::Button::new("⏹ Stop"))
            .clicked()
        {
            action = SenderAction::Stop;
        }
    });

    ui.separator();

    // Streaming stats
    if s.is_streaming {
        ui.heading("Stream Stats");
        ui.label(format!("Packets sent: {}", s.packets_sent));
        ui.label(format!(
            "Data sent: {:.1} MB",
            s.bytes_sent as f64 / 1_000_000.0
        ));
        ui.label(format!("Receivers: {}", s.receiver_count));

        ui.separator();

        // Peak meter
        peak_meter(ui, s.peak_level);

        ui.separator();

        ui.label("📡 Advertising via Bonjour");
        ui.label(format!("Port: {AUDIO_PORT} | {SAMPLE_RATE}Hz stereo"));
    }

    // `s` is no longer used past here — safe to mutate `app` wholesale.
    match action {
        SenderAction::Start(input) => {
            app.sender.is_streaming = true;
            app.sender.packets_sent = 0;
            app.sender.bytes_sent = 0;
            app.sender_threads = Some(start_sender(shared.clone(), input));
        }
        SenderAction::Stop => stop_sender(app),
        SenderAction::None => {}
    }
}

enum SenderAction {
    None,
    Start(String),
    Stop,
}

/// Signal the sender engine to stop and drop its handles.
fn stop_sender(app: &mut AppState) {
    if let Some(t) = app.sender_threads.take() {
        t.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    app.sender.is_streaming = false;
}

// ─── Receiver UI ────────────────────────────────────────

fn receiver_ui(ui: &mut egui::Ui, app: &mut AppState, shared: &SharedApp) {
    // Refresh output devices
    if app.receiver.available_output_devices.is_empty() {
        app.receiver.available_output_devices = enumerate_output_devices()
            .into_iter()
            .map(|d| d.name)
            .collect();
    }

    let r = &mut app.receiver;

    // Output device selector
    ui.horizontal(|ui| {
        ui.label("Output:");
        egui::ComboBox::from_id_salt("output_device")
            .selected_text(if r.selected_output_device.is_empty() {
                "(default)"
            } else {
                &r.selected_output_device
            })
            .show_ui(ui, |ui| {
                for dev in &r.available_output_devices {
                    ui.selectable_value(
                        &mut r.selected_output_device,
                        dev.clone(),
                        dev,
                    );
                }
            });
    });

    ui.separator();

    // Discovered senders
    ui.heading("Discovered Senders");
    egui::ScrollArea::vertical()
        .max_height(150.0)
        .show(ui, |ui| {
            if r.discovered_senders.is_empty() {
                ui.label(
                    RichText::new("No senders found. Make sure a sender is running on the network.")
                        .italics()
                        .color(Color32::GRAY),
                );
            } else {
                for sender in r.discovered_senders.clone() {
                    let is_connected = r.connected_sender.as_ref().is_some_and(|cs| {
                        cs.host == sender.host && cs.port == sender.port
                    });

                    ui.horizontal(|ui| {
                        let response = ui.selectable_label(
                            is_connected,
                            format!(
                                "{} ({}:{} — {}Hz)",
                                sender.name, sender.host, sender.port, sender.sample_rate
                            ),
                        );
                        if response.clicked() && !is_connected {
                            r.connected_sender = Some(sender.clone());
                        }
                    });
                }
            }
        });

    ui.separator();

    // Connect / Disconnect — apply after the `r` borrow ends (below).
    let mut action = ReceiverAction::None;
    ui.horizontal(|ui| {
        let can_connect = !r.is_connected && r.connected_sender.is_some();
        if ui
            .add_enabled(can_connect, egui::Button::new("▶ Connect"))
            .clicked()
        {
            if let Some(sender) = r.connected_sender.clone() {
                action = ReceiverAction::Connect(sender, r.selected_output_device.clone());
            }
        }

        if ui
            .add_enabled(r.is_connected, egui::Button::new("⏹ Disconnect"))
            .clicked()
        {
            action = ReceiverAction::Disconnect;
        }
    });

    ui.separator();

    if r.is_connected {
        // Delay slider
        ui.add(
            egui::Slider::new(&mut r.target_delay_ms, 0.0..=200.0)
                .text("Buffer Delay (ms)")
                .step_by(5.0),
        );
        ui.label(format!(
            "Current delay: {:.0}ms",
            r.target_delay_ms
        ));

        // Volume slider
        ui.add(
            egui::Slider::new(&mut r.volume, 0.0..=1.0)
                .text("Volume")
                .step_by(0.01),
        );
        ui.label(format!("Volume: {:.0}%", r.volume * 100.0));

        ui.separator();

        // Stream stats
        ui.heading("Stream Stats");
        ui.label(format!("Packets received: {}", r.packets_received));
        ui.label(format!("Packets lost: {}", r.packets_lost));
        ui.label(format!("Loss rate: {:.2}%", loss_percent(r)));
        ui.label(format!("Buffer fill: {:.0}ms", r.buffer_fill_ms));
        ui.label(format!(
            "Speed adj: {:+.3}%",
            r.current_speed_adjust * 100.0
        ));
        ui.label(format!("Underruns: {}", r.underruns));

        ui.separator();

        // Peak meter
        peak_meter(ui, r.peak_level);

        // Sync status indicator
        ui.separator();
        let sync_good = r.current_speed_adjust.abs() < 0.001;
        if sync_good {
            ui.label(RichText::new("🔒 Synced").color(Color32::GREEN));
        } else {
            ui.label(RichText::new("🔄 Syncing…").color(Color32::YELLOW));
        }
    }

    // `r` is no longer used past here — safe to mutate `app` wholesale.
    match action {
        ReceiverAction::Connect(sender, output) => {
            app.receiver.is_connected = true;
            app.receiver.connected_sender = Some(sender.clone());
            app.receiver.packets_received = 0;
            app.receiver.packets_lost = 0;
            app.receiver.underruns = 0;
            app.receiver_threads = Some(start_receiver(shared.clone(), sender, output));
        }
        ReceiverAction::Disconnect => stop_receiver(app),
        ReceiverAction::None => {}
    }
}

enum ReceiverAction {
    None,
    Connect(crate::state::shared::DiscoveredSender, String),
    Disconnect,
}

/// Signal the receiver engine to stop and drop its handles.
fn stop_receiver(app: &mut AppState) {
    if let Some(t) = app.receiver_threads.take() {
        t.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    app.receiver.is_connected = false;
    app.receiver.connected_sender = None;
}

// ─── Widgets ────────────────────────────────────────────

fn peak_meter(ui: &mut egui::Ui, level: f32) {
    let db = if level < 1e-10 {
        -90.0
    } else {
        20.0 * level.log10()
    };
    let frac = ((db + 60.0) / 60.0).clamp(0.0, 1.0);

    let desired = egui::vec2(ui.available_width(), 20.0);
    let (rect, _) = ui.allocate_exact_size(desired, egui::Sense::hover());

    // Background
    ui.painter()
        .rect_filled(rect, 3.0, Color32::from_gray(40));

    // Gradient fill
    let fill_width = rect.width() * frac;
    if fill_width > 0.0 {
        let fill_rect = egui::Rect::from_min_size(
            rect.min,
            egui::vec2(fill_width, rect.height()),
        );
        let color = if db > -6.0 {
            Color32::RED
        } else if db > -12.0 {
            Color32::from_rgb(255, 200, 0) // yellow-ish
        } else {
            Color32::from_rgb(0, 200, 80) // green
        };
        ui.painter().rect_filled(fill_rect, 3.0, color);
    }

    // Border
    ui.painter()
        .rect_stroke(rect, 3.0, egui::Stroke::new(1.0_f32, Color32::from_gray(100)), egui::StrokeKind::Inside);

    // Text
    let label_rect = egui::Rect::from_min_size(
        egui::pos2(rect.min.x + 4.0, rect.center().y - 8.0),
        egui::vec2(rect.width() - 8.0, 16.0),
    );
    ui.put(
        label_rect,
        egui::Label::new(
            RichText::new(format!("{db:.1} dB"))
                .size(11.0)
                .color(Color32::WHITE),
        ),
    );
}

fn loss_percent(r: &ReceiverState) -> f64 {
    let total = r.packets_received + r.packets_lost;
    if total == 0 {
        0.0
    } else {
        (r.packets_lost as f64 / total as f64) * 100.0
    }
}
