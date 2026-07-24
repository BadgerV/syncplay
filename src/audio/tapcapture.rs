//! Core Audio **process-tap** capture with mute — the Airfoil-grade, no-BlackHole
//! source (macOS 14.4+).
//!
//! Unlike ScreenCaptureKit (which taps but can't silence), a process tap can be
//! created with `CATapMuted`, which **mutes the tapped audio's normal output to
//! hardware** while we capture it. That lets SyncPlay replay the audio on a
//! delay on *both* the source and the receivers, so every Mac plays the same
//! sample at the same instant — with no BlackHole / Multi-Output setup.
//!
//! ## Pipeline
//! `CATapDescription` (global, muted) → `AudioHardwareCreateProcessTap` →
//! private aggregate device wrapping the tap → block IOProc delivers the tapped
//! Float32 PCM → converted to interleaved-i16 packets on `tx`.
//!
//! ## Permission
//! Requires the **Audio Recording** TCC permission (`kTCCServiceAudioCapture`).
//! Tap creation fails with a clear error until granted.

use std::ffi::{c_void, CStr};
use std::ptr::NonNull;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use block2::RcBlock;
use crossbeam_channel::Sender;
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2::AllocAnyThread;
use objc2_core_audio::{
    kAudioAggregateDeviceIsPrivateKey, kAudioAggregateDeviceIsStackedKey,
    kAudioAggregateDeviceMainSubDeviceKey, kAudioAggregateDeviceNameKey,
    kAudioAggregateDeviceSubDeviceListKey, kAudioAggregateDeviceTapAutoStartKey,
    kAudioAggregateDeviceTapListKey, kAudioAggregateDeviceUIDKey, kAudioDevicePropertyDeviceUID,
    kAudioHardwarePropertyDefaultOutputDevice, kAudioObjectSystemObject, kAudioSubDeviceUIDKey,
    kAudioSubTapDriftCompensationKey, kAudioSubTapUIDKey, kAudioTapPropertyFormat,
    kAudioTapPropertyUID, AudioDeviceCreateIOProcIDWithBlock, AudioDeviceDestroyIOProcID,
    AudioDeviceIOProcID, AudioDeviceStart, AudioDeviceStop, AudioHardwareCreateAggregateDevice,
    AudioHardwareCreateProcessTap, AudioHardwareDestroyAggregateDevice,
    AudioHardwareDestroyProcessTap, AudioObjectGetPropertyData, AudioObjectID,
    AudioObjectPropertyAddress, CATapDescription, CATapMuteBehavior,
};
use objc2_core_audio_types::{
    kAudioFormatFlagIsFloat, AudioBufferList, AudioStreamBasicDescription, AudioTimeStamp,
};
use objc2_core_foundation::{CFDictionary, CFString};
use objc2_foundation::{NSArray, NSDictionary, NSNumber, NSString};

use crate::error::{Result, SyncPlayError};

const SCOPE_GLOBAL: u32 = 0x676c_6f62; // 'glob' = kAudioObjectPropertyScopeGlobal
const ELEMENT_MAIN: u32 = 0; // kAudioObjectPropertyElementMain

/// One-shot guard so the IOProc logs its buffer shape a single time.
static DIAG_DONE: AtomicBool = AtomicBool::new(false);

/// Live process-tap capture. Holds the OS handles; dropping it tears everything
/// down (stop IOProc, destroy aggregate device, destroy tap → unmutes audio).
pub struct TapCapture {
    tap_id: AudioObjectID,
    agg_id: AudioObjectID,
    proc_id: AudioDeviceIOProcID,
    // Keep the block alive for the device's lifetime (belt-and-braces; Core Audio
    // copies it, but we retain our reference too).
    _block: IoBlock,
}

type IoBlock = RcBlock<
    dyn Fn(
        NonNull<AudioTimeStamp>,
        NonNull<AudioBufferList>,
        NonNull<AudioTimeStamp>,
        NonNull<AudioBufferList>,
        NonNull<AudioTimeStamp>,
    ),
>;

// The OS handles are plain integers / copied blocks; safe to own on one thread.
unsafe impl Send for TapCapture {}

impl Drop for TapCapture {
    fn drop(&mut self) {
        unsafe {
            if self.agg_id != 0 {
                let _ = AudioDeviceStop(self.agg_id, self.proc_id);
                if self.proc_id.is_some() {
                    let _ = AudioDeviceDestroyIOProcID(self.agg_id, self.proc_id);
                }
                let _ = AudioHardwareDestroyAggregateDevice(self.agg_id);
            }
            if self.tap_id != 0 {
                let _ = AudioHardwareDestroyProcessTap(self.tap_id);
            }
        }
        tracing::info!("Process tap torn down (audio unmuted)");
    }
}

/// Start a global, muted process tap and stream its audio to `tx` as
/// interleaved-stereo i16. The source's normal output is muted while capturing.
pub fn start_tap_capture(tx: Sender<Vec<i16>>, _stop: Arc<AtomicBool>) -> Result<TapCapture> {
    unsafe { start_tap_capture_inner(tx) }
}

unsafe fn start_tap_capture_inner(tx: Sender<Vec<i16>>) -> Result<TapCapture> {
    // 1. Describe a global tap (all processes) that MUTES their normal output.
    let empty: Retained<NSArray<NSNumber>> = NSArray::from_slice(&[]);
    let desc =
        CATapDescription::initStereoGlobalTapButExcludeProcesses(CATapDescription::alloc(), &empty);
    // Muted is the point (source silenced so we can replay it delayed), but
    // allow unmuted for diagnosis: SYNCPLAY_TAP_UNMUTED=1.
    let unmuted = std::env::var("SYNCPLAY_TAP_UNMUTED").is_ok();
    desc.setMuteBehavior(if unmuted {
        CATapMuteBehavior::Unmuted
    } else {
        CATapMuteBehavior::Muted
    });
    tracing::info!("Tap description: muted={}", !unmuted);

    // 2. Create the tap object.
    let mut tap_id: AudioObjectID = 0;
    let st = AudioHardwareCreateProcessTap(Some(&desc), &mut tap_id);
    if st != 0 || tap_id == 0 {
        return Err(SyncPlayError::Config(format!(
            "AudioHardwareCreateProcessTap failed (grant Audio Recording permission): OSStatus {st}"
        )));
    }

    // 3. Reference the tap from the aggregate by its UID property. (The
    //    description's UUID string looks equivalent but does not resolve here —
    //    the aggregate ends up with no live tap and the IOProc never fires.)
    let uid = read_tap_uid(tap_id).ok_or_else(|| {
        AudioHardwareDestroyProcessTap(tap_id);
        SyncPlayError::Config("could not read process tap UID".into())
    })?;

    // 3b. Log the tap's real stream format — the authority on how to interpret
    //     the bytes the IOProc delivers.
    log_tap_format(tap_id);

    // 4. Build a private aggregate device that wraps the tap.
    let agg_id = match create_aggregate_with_tap(&uid) {
        Ok(id) => id,
        Err(e) => {
            AudioHardwareDestroyProcessTap(tap_id);
            return Err(e);
        }
    };

    // 5. Install a block IOProc that converts the tapped PCM and forwards it.
    let block: IoBlock = RcBlock::new(
        move |_in_now: NonNull<AudioTimeStamp>,
              in_data: NonNull<AudioBufferList>,
              _in_time: NonNull<AudioTimeStamp>,
              _out_data: NonNull<AudioBufferList>,
              _out_time: NonNull<AudioTimeStamp>| {
            let list = in_data.as_ref();
            // Log the delivered buffer shape once, to diagnose format issues.
            if !DIAG_DONE.swap(true, std::sync::atomic::Ordering::Relaxed) {
                let n = list.mNumberBuffers;
                let b = &*list.mBuffers.as_ptr();
                tracing::info!(
                    "Tap IOProc first buffer: buffers={} channels={} bytes={} null={}",
                    n,
                    b.mNumberChannels,
                    b.mDataByteSize,
                    b.mData.is_null(),
                );
            }
            let pcm = buffer_list_to_i16(list);
            if !pcm.is_empty() {
                let _ = tx.try_send(pcm);
            }
        },
    );

    let mut proc_id: AudioDeviceIOProcID = None;
    let block_ptr = (&*block as *const _) as *mut block2::DynBlock<_>;
    let st =
        AudioDeviceCreateIOProcIDWithBlock(NonNull::from(&mut proc_id), agg_id, None, block_ptr);
    if st != 0 || proc_id.is_none() {
        AudioHardwareDestroyAggregateDevice(agg_id);
        AudioHardwareDestroyProcessTap(tap_id);
        return Err(SyncPlayError::Config(format!(
            "AudioDeviceCreateIOProcIDWithBlock failed: OSStatus {st}"
        )));
    }

    // 6. Start IO.
    let st = AudioDeviceStart(agg_id, proc_id);
    if st != 0 {
        AudioDeviceDestroyIOProcID(agg_id, proc_id);
        AudioHardwareDestroyAggregateDevice(agg_id);
        AudioHardwareDestroyProcessTap(tap_id);
        return Err(SyncPlayError::Config(format!(
            "AudioDeviceStart failed: OSStatus {st}"
        )));
    }

    tracing::info!("Process-tap capture started (muted global tap, 48kHz stereo)");
    Ok(TapCapture {
        tap_id,
        agg_id,
        proc_id,
        _block: block,
    })
}

/// Log the tap's `kAudioTapPropertyFormat` ASBD — sample rate, channels, and
/// format flags tell us exactly how to interpret the IOProc's bytes.
unsafe fn log_tap_format(tap_id: AudioObjectID) {
    let addr = AudioObjectPropertyAddress {
        mSelector: kAudioTapPropertyFormat,
        mScope: SCOPE_GLOBAL,
        mElement: ELEMENT_MAIN,
    };
    let mut asbd = std::mem::zeroed::<AudioStreamBasicDescription>();
    let mut size = std::mem::size_of::<AudioStreamBasicDescription>() as u32;
    let st = AudioObjectGetPropertyData(
        tap_id,
        NonNull::from(&addr),
        0,
        std::ptr::null(),
        NonNull::from(&mut size),
        NonNull::from(&mut asbd).cast(),
    );
    if st == 0 {
        tracing::info!(
            "Tap format: {} Hz, {} ch, {} bits, {} bytes/frame, flags=0x{:x}, float={}",
            asbd.mSampleRate,
            asbd.mChannelsPerFrame,
            asbd.mBitsPerChannel,
            asbd.mBytesPerFrame,
            asbd.mFormatFlags,
            (asbd.mFormatFlags & kAudioFormatFlagIsFloat) != 0,
        );
    } else {
        tracing::warn!("Could not read tap format: OSStatus {st}");
    }
}

/// Read `kAudioTapPropertyUID` off the tap object as an owned `NSString`.
unsafe fn read_tap_uid(tap_id: AudioObjectID) -> Option<Retained<NSString>> {
    let addr = AudioObjectPropertyAddress {
        mSelector: kAudioTapPropertyUID,
        mScope: SCOPE_GLOBAL,
        mElement: ELEMENT_MAIN,
    };
    let mut cfstr: *const CFString = std::ptr::null();
    let mut size = std::mem::size_of::<*const CFString>() as u32;
    let st = AudioObjectGetPropertyData(
        tap_id,
        NonNull::from(&addr),
        0,
        std::ptr::null(),
        NonNull::from(&mut size),
        NonNull::new(&mut cfstr as *mut _ as *mut c_void)?,
    );
    if st != 0 || cfstr.is_null() {
        return None;
    }
    // CFString is toll-free bridged to NSString; the getter returns +1.
    Retained::from_raw(cfstr as *mut NSString)
}

/// Read the system's default output device UID — the aggregate needs it as its
/// main sub-device so the tap stream has a proper clock/timebase. Without this
/// the IOProc runs but delivers silence.
unsafe fn default_output_uid() -> Option<Retained<NSString>> {
    let addr = AudioObjectPropertyAddress {
        mSelector: kAudioHardwarePropertyDefaultOutputDevice,
        mScope: SCOPE_GLOBAL,
        mElement: ELEMENT_MAIN,
    };
    let mut dev: AudioObjectID = 0;
    let mut size = std::mem::size_of::<AudioObjectID>() as u32;
    let st = AudioObjectGetPropertyData(
        kAudioObjectSystemObject as AudioObjectID,
        NonNull::from(&addr),
        0,
        std::ptr::null(),
        NonNull::from(&mut size),
        NonNull::new(&mut dev as *mut _ as *mut c_void)?,
    );
    if st != 0 || dev == 0 {
        return None;
    }

    let addr = AudioObjectPropertyAddress {
        mSelector: kAudioDevicePropertyDeviceUID,
        mScope: SCOPE_GLOBAL,
        mElement: ELEMENT_MAIN,
    };
    let mut cfstr: *const CFString = std::ptr::null();
    let mut size = std::mem::size_of::<*const CFString>() as u32;
    let st = AudioObjectGetPropertyData(
        dev,
        NonNull::from(&addr),
        0,
        std::ptr::null(),
        NonNull::from(&mut size),
        NonNull::new(&mut cfstr as *mut _ as *mut c_void)?,
    );
    if st != 0 || cfstr.is_null() {
        return None;
    }
    // CFString is toll-free bridged to NSString; the getter returns +1.
    Retained::from_raw(cfstr as *mut NSString)
}

/// Create a private aggregate device wrapping our tap.
///
/// Follows Apple's documented recipe: the aggregate takes the default output
/// device as its *main sub-device* (providing the clock) plus a tap list entry
/// referencing the tap by UUID, with drift compensation enabled.
unsafe fn create_aggregate_with_tap(tap_uid: &NSString) -> Result<AudioObjectID> {
    let k_name = ns_from_cstr(kAudioAggregateDeviceNameKey);
    let k_uid = ns_from_cstr(kAudioAggregateDeviceUIDKey);
    let k_private = ns_from_cstr(kAudioAggregateDeviceIsPrivateKey);
    let k_stacked = ns_from_cstr(kAudioAggregateDeviceIsStackedKey);
    let k_autostart = ns_from_cstr(kAudioAggregateDeviceTapAutoStartKey);
    let k_taplist = ns_from_cstr(kAudioAggregateDeviceTapListKey);
    let k_main = ns_from_cstr(kAudioAggregateDeviceMainSubDeviceKey);
    let k_sublist = ns_from_cstr(kAudioAggregateDeviceSubDeviceListKey);
    let k_subdev_uid = ns_from_cstr(kAudioSubDeviceUIDKey);
    let k_subtap = ns_from_cstr(kAudioSubTapUIDKey);
    let k_drift = ns_from_cstr(kAudioSubTapDriftCompensationKey);

    let out_uid = default_output_uid()
        .ok_or_else(|| SyncPlayError::Config("no default output device for tap clock".into()))?;

    let name = NSString::from_str("SyncPlay-Tap");
    let uid = NSString::from_str("com.syncplay.tap.aggregate");
    let yes = NSNumber::numberWithBool(true);
    let no = NSNumber::numberWithBool(false);

    // Sub-device entry (the clock source): { "uid": <output uid> }
    let sub_keys: [&NSString; 1] = [&k_subdev_uid];
    let sub_vals: [&AnyObject; 1] = [out_uid.as_ref()];
    let subdev: Retained<NSDictionary<NSString, AnyObject>> =
        NSDictionary::from_slices(&sub_keys, &sub_vals);
    let sub_list: Retained<NSArray<NSDictionary<NSString, AnyObject>>> =
        NSArray::from_slice(&[&*subdev]);

    // Sub-tap entry: { "uid": <tap uuid>, "driftcomp": true }
    let subtap_keys: [&NSString; 2] = [&k_subtap, &k_drift];
    let subtap_vals: [&AnyObject; 2] = [tap_uid.as_ref(), yes.as_ref()];
    let subtap: Retained<NSDictionary<NSString, AnyObject>> =
        NSDictionary::from_slices(&subtap_keys, &subtap_vals);
    let tap_list: Retained<NSArray<NSDictionary<NSString, AnyObject>>> =
        NSArray::from_slice(&[&*subtap]);

    // Aggregate description dictionary.
    //
    // Including the output device as a sub-device (Apple's AudioCap recipe) gives
    // the aggregate a hardware clock, but on this OS it stops the IOProc firing
    // entirely; a tap-only aggregate does deliver buffers. Keep the sub-device
    // wiring behind an env var so it's easy to A/B against OS revisions.
    let with_subdevice = std::env::var("SYNCPLAY_TAP_SUBDEVICE").is_ok();

    let (keys, vals): (Vec<&NSString>, Vec<&AnyObject>) = if with_subdevice {
        (
            vec![
                &k_name,
                &k_uid,
                &k_private,
                &k_stacked,
                &k_autostart,
                &k_main,
                &k_sublist,
                &k_taplist,
            ],
            vec![
                name.as_ref(),
                uid.as_ref(),
                yes.as_ref(),
                no.as_ref(),
                yes.as_ref(),
                out_uid.as_ref(),
                sub_list.as_ref(),
                tap_list.as_ref(),
            ],
        )
    } else {
        (
            vec![
                &k_name,
                &k_uid,
                &k_private,
                &k_stacked,
                &k_autostart,
                &k_taplist,
            ],
            vec![
                name.as_ref(),
                uid.as_ref(),
                yes.as_ref(),
                no.as_ref(),
                yes.as_ref(),
                tap_list.as_ref(),
            ],
        )
    };
    let desc: Retained<NSDictionary<NSString, AnyObject>> = NSDictionary::from_slices(&keys, &vals);

    // NSDictionary is toll-free bridged to CFDictionary.
    let cf = &*((&*desc as *const NSDictionary<NSString, AnyObject>) as *const CFDictionary);

    let mut agg_id: AudioObjectID = 0;
    let st = AudioHardwareCreateAggregateDevice(cf, NonNull::from(&mut agg_id));
    if st != 0 || agg_id == 0 {
        return Err(SyncPlayError::Config(format!(
            "AudioHardwareCreateAggregateDevice failed: OSStatus {st}"
        )));
    }
    Ok(agg_id)
}

/// Build an `NSString` from one of Core Audio's `&CStr` dictionary-key constants.
fn ns_from_cstr(c: &CStr) -> Retained<NSString> {
    NSString::from_str(c.to_str().unwrap_or_default())
}

/// Convert a Core Audio input `AudioBufferList` (Float32 PCM, planar or
/// interleaved) into interleaved-stereo i16.
unsafe fn buffer_list_to_i16(list: &AudioBufferList) -> Vec<i16> {
    let n = list.mNumberBuffers as usize;
    if n == 0 {
        return Vec::new();
    }
    let bufs = list.mBuffers.as_ptr();

    let f32s = |i: usize| -> &[f32] {
        let b = &*bufs.add(i);
        if b.mData.is_null() || b.mDataByteSize == 0 {
            return &[];
        }
        let len = b.mDataByteSize as usize / std::mem::size_of::<f32>();
        std::slice::from_raw_parts(b.mData as *const f32, len)
    };

    if n >= 2 {
        // Planar: buffer 0 = L, buffer 1 = R.
        let left = f32s(0);
        let right = f32s(1);
        let frames = left.len().min(right.len());
        let mut v = Vec::with_capacity(frames * 2);
        for i in 0..frames {
            v.push(to_i16(left[i]));
            v.push(to_i16(right[i]));
        }
        v
    } else {
        let b = &*bufs;
        let samples = f32s(0);
        if b.mNumberChannels >= 2 {
            samples.iter().map(|&s| to_i16(s)).collect()
        } else {
            let mut v = Vec::with_capacity(samples.len() * 2);
            for &s in samples {
                let x = to_i16(s);
                v.push(x);
                v.push(x);
            }
            v
        }
    }
}

fn to_i16(s: f32) -> i16 {
    (s.clamp(-1.0, 1.0) * 32767.0) as i16
}
