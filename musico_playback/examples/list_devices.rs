//! Dev helper: list the output devices CPAL sees and which one is default.
//! Run with: cargo run -p musico_playback --example list_devices
use cpal::traits::{DeviceTrait, HostTrait};

fn main() {
    let host = cpal::default_host();
    match host.default_output_device().and_then(|d| d.name().ok()) {
        Some(name) => println!("default = {name}"),
        None => println!("default = (none)"),
    }
    match host.output_devices() {
        Ok(devices) => {
            for d in devices {
                println!("  device: {:?}", d.name().unwrap_or_default());
            }
        }
        Err(e) => println!("enumerate failed: {e}"),
    }
}
