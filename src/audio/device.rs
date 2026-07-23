use cpal::traits::{DeviceTrait, HostTrait};

use crate::state::shared::{CHANNELS, SAMPLE_RATE};

#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub name: String,
    pub is_default: bool,
}

/// Enumerate available input audio devices.
pub fn enumerate_input_devices() -> Vec<DeviceInfo> {
    let host = cpal::default_host();
    let default_input = host
        .default_input_device()
        .and_then(|d| d.name().ok());

    let mut devices: Vec<DeviceInfo> = match host.input_devices() {
        Ok(iter) => iter
            .filter_map(|d| {
                let name = d.name().ok()?;
                let is_default = Some(&name) == default_input.as_ref();
                Some(DeviceInfo { name, is_default })
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    // Sort: default first, then alphabetical
    devices.sort_by(|a, b| {
        b.is_default
            .cmp(&a.is_default)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });
    devices
}

/// Enumerate available output audio devices.
pub fn enumerate_output_devices() -> Vec<DeviceInfo> {
    let host = cpal::default_host();
    let default_output = host
        .default_output_device()
        .and_then(|d| d.name().ok());

    let mut devices: Vec<DeviceInfo> = match host.output_devices() {
        Ok(iter) => iter
            .filter_map(|d| {
                let name = d.name().ok()?;
                let is_default = Some(&name) == default_output.as_ref();
                Some(DeviceInfo { name, is_default })
            })
            .collect(),
        Err(_) => Vec::new(),
    };

    devices.sort_by(|a, b| {
        b.is_default
            .cmp(&a.is_default)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });
    devices
}

/// Find a device by name (partial match).
pub fn find_input_device(name: &str) -> Option<cpal::Device> {
    let host = cpal::default_host();
    host.input_devices()
        .ok()
        .into_iter()
        .flatten()
        .find(|d| d.name().map(|n| n.contains(name)).unwrap_or(false))
}

/// Find an output device by name (partial match).
pub fn find_output_device(name: &str) -> Option<cpal::Device> {
    let host = cpal::default_host();
    host.output_devices()
        .ok()
        .into_iter()
        .flatten()
        .find(|d| d.name().map(|n| n.contains(name)).unwrap_or(false))
}

/// Get the default input device.
pub fn default_input_device() -> Option<cpal::Device> {
    cpal::default_host().default_input_device()
}

/// Get the default output device.
pub fn default_output_device() -> Option<cpal::Device> {
    cpal::default_host().default_output_device()
}

/// Get a suitable stereo input config for a device at 48kHz.
pub fn input_config(device: &cpal::Device) -> Result<cpal::StreamConfig, crate::error::SyncPlayError> {
    let supported: Vec<_> = device
        .supported_input_configs()
        .map_err(|e| crate::error::SyncPlayError::Audio(format!("Cannot get input configs: {e}")))?
        .collect();

    // Try to find stereo 48kHz
    for cfg in &supported {
        if cfg.channels() == CHANNELS && cfg.min_sample_rate() <= cpal::SampleRate(SAMPLE_RATE)
            && cfg.max_sample_rate() >= cpal::SampleRate(SAMPLE_RATE)
        {
            return Ok(cpal::StreamConfig {
                channels: CHANNELS,
                sample_rate: cpal::SampleRate(SAMPLE_RATE),
                buffer_size: cpal::BufferSize::Default,
            });
        }
    }

    // Fallback: first available config with our sample rate
    for cfg in &supported {
        let ch = cfg.channels().min(CHANNELS);
        if cfg.min_sample_rate() <= cpal::SampleRate(SAMPLE_RATE)
            && cfg.max_sample_rate() >= cpal::SampleRate(SAMPLE_RATE)
        {
            return Ok(cpal::StreamConfig {
                channels: ch,
                sample_rate: cpal::SampleRate(SAMPLE_RATE),
                buffer_size: cpal::BufferSize::Default,
            });
        }
    }

    // Last resort: use the default config
    Ok(supported[0].with_max_sample_rate().config())
}

/// Get a suitable stereo output config for a device at 48kHz.
pub fn output_config(device: &cpal::Device) -> Result<cpal::StreamConfig, crate::error::SyncPlayError> {
    let supported: Vec<_> = device
        .supported_output_configs()
        .map_err(|e| crate::error::SyncPlayError::Audio(format!("Cannot get output configs: {e}")))?
        .collect();

    for cfg in &supported {
        if cfg.channels() == CHANNELS && cfg.min_sample_rate() <= cpal::SampleRate(SAMPLE_RATE)
            && cfg.max_sample_rate() >= cpal::SampleRate(SAMPLE_RATE)
        {
            return Ok(cpal::StreamConfig {
                channels: CHANNELS,
                sample_rate: cpal::SampleRate(SAMPLE_RATE),
                buffer_size: cpal::BufferSize::Default,
            });
        }
    }

    for cfg in &supported {
        let ch = cfg.channels().min(CHANNELS);
        if cfg.min_sample_rate() <= cpal::SampleRate(SAMPLE_RATE)
            && cfg.max_sample_rate() >= cpal::SampleRate(SAMPLE_RATE)
        {
            return Ok(cpal::StreamConfig {
                channels: ch,
                sample_rate: cpal::SampleRate(SAMPLE_RATE),
                buffer_size: cpal::BufferSize::Default,
            });
        }
    }

    Ok(supported[0].with_max_sample_rate().config())
}
