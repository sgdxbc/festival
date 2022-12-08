use std::{
    ffi::{c_int, c_uint, c_void},
    marker::{PhantomData, PhantomPinned},
    ptr::null_mut,
    sync::Once,
};

#[repr(C)]
pub struct WirehairCodecRaw {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WirehairResult {
    Success = 0,
    NeedMore = 1,
    InvalidInput = 2,
    BadDenseSeed = 3,
    BadPeelSeed = 4,
    BadInputSmallN = 5,
    BadInputLargeN = 6,
    ExtraInsufficient = 7,
    Error = 8,
    OutOfMemory = 9,
    UnsupportedPlatform = 10,
    // Count
    Padding = 0x7fffffff,
}

impl WirehairResult {
    pub fn with<T>(self, value: T) -> Result<T, Self> {
        if self != Self::Success && self != Self::NeedMore {
            Err(self)
        } else {
            Ok(value)
        }
    }
}

extern "C" {
    pub fn wirehair_init_(expected_version: c_int) -> WirehairResult;
    pub fn wirehair_result_string(result: u32) -> *const u8;
    pub fn wirehair_encoder_create(
        reuse_opt: *mut WirehairCodecRaw,
        message: *const c_void,
        message_bytes: u64,
        block_bytes: u32,
    ) -> *mut WirehairCodecRaw;
    pub fn wirehair_encode(
        codec: *mut WirehairCodecRaw,
        block_id: c_uint,
        block_data_out: *mut c_void,
        out_bytes: u32,
        data_bytes_out: *mut u32,
    ) -> WirehairResult;
    pub fn wirehair_decoder_create(
        reuse_opt: *mut WirehairCodecRaw,
        message_bytes: u64,
        block_bytes: u32,
    ) -> *mut WirehairCodecRaw;
    pub fn wirehair_decode(
        codec: *mut WirehairCodecRaw,
        block_id: c_uint,
        block_data: *const c_void,
        data_bytes: u32,
    ) -> WirehairResult;
    pub fn wirehair_recover(
        codec: *mut WirehairCodecRaw,
        message_out: *mut c_void,
        message_bytes: u64,
    ) -> WirehairResult;
    pub fn wirehair_decoder_become_encoder(codec: *mut WirehairCodecRaw) -> WirehairResult;
    pub fn wirehair_free(codec: *mut WirehairCodecRaw);
}

unsafe fn wirehair_init() -> WirehairResult {
    const WIREHAIR_VERSION: c_int = 2;
    unsafe { wirehair_init_(WIREHAIR_VERSION) }
}

pub struct WirehairEncoder {
    raw: *mut WirehairCodecRaw,
    pub block_bytes: u32,
}
unsafe impl Send for WirehairEncoder {} // really?
unsafe impl Sync for WirehairEncoder {}

pub struct WirehairDecoder {
    raw: *mut WirehairCodecRaw,
    pub message_bytes: u64,
    block_bytes: u32,
    need_more: bool,
}
unsafe impl Send for WirehairDecoder {}
unsafe impl Sync for WirehairDecoder {}

static INIT: Once = Once::new();
impl WirehairEncoder {
    pub fn new(message: &[u8], block_bytes: u32) -> Self {
        INIT.call_once(|| unsafe {
            wirehair_init().with(()).unwrap();
        });
        let raw = unsafe {
            wirehair_encoder_create(
                null_mut(),
                message.as_ptr().cast(),
                message.len() as _,
                block_bytes,
            )
        };
        assert!(!raw.is_null());
        Self { raw, block_bytes }
    }

    pub fn encode(&mut self, id: u32, block: &mut [u8]) -> Result<usize, WirehairResult> {
        assert!(block.len() >= self.block_bytes as _);
        let mut out_len = Default::default();
        unsafe {
            wirehair_encode(
                self.raw,
                id as _,
                block.as_mut_ptr().cast(),
                block.len() as _,
                &mut out_len,
            )
        }
        .with(out_len as _)
    }
}

impl WirehairDecoder {
    pub fn new(message_bytes: u64, block_bytes: u32) -> Self {
        INIT.call_once(|| unsafe {
            wirehair_init().with(()).unwrap();
        });
        let raw = unsafe { wirehair_decoder_create(null_mut(), message_bytes, block_bytes) };
        assert!(!raw.is_null());
        Self {
            raw,
            message_bytes,
            block_bytes,
            need_more: true,
        }
    }

    pub fn decode(&mut self, id: u32, block: &[u8]) -> Result<bool, WirehairResult> {
        if !self.need_more {
            return Ok(true);
        }
        let result =
            unsafe { wirehair_decode(self.raw, id as _, block.as_ptr().cast(), block.len() as _) };
        self.need_more = result == WirehairResult::NeedMore;
        result.with(result == WirehairResult::Success)
    }

    pub fn recover(&mut self, message: &mut [u8]) -> Result<(), WirehairResult> {
        assert_eq!(message.len(), self.message_bytes as usize);
        unsafe { wirehair_recover(self.raw, message.as_mut_ptr().cast(), self.message_bytes) }
            .with(())
    }

    pub fn into_encoder(self) -> Result<WirehairEncoder, WirehairResult> {
        unsafe { wirehair_decoder_become_encoder(self.raw) }.with(WirehairEncoder {
            raw: self.raw,
            block_bytes: self.block_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;

    use rand::{random, thread_rng, Rng};

    use super::*;

    #[test]
    fn it_works() {
        let result = unsafe { wirehair_init() };
        assert_eq!(result, WirehairResult::Success);
        let message = "Hello, world!".as_bytes();
        let codec = unsafe {
            wirehair_encoder_create(null_mut(), message.as_ptr().cast(), message.len() as _, 4)
        };
        assert!(!codec.is_null());
        unsafe { wirehair_free(codec) }
    }

    #[test]
    fn it_works_in_rust() {
        let k = 256;
        let block_bytes = 1024;
        let mut message = vec![0; (k * block_bytes) as _];

        for _ in 0..100 {
            thread_rng().fill(&mut message[..]);
            let mut encoder = WirehairEncoder::new(&message, block_bytes);
            let mut decoder = WirehairDecoder::new(message.len() as _, block_bytes);
            for i in 0.. {
                let mut block = vec![0; block_bytes as _];
                let id = random();
                encoder.encode(id, &mut block).unwrap();
                if decoder.decode(id, &block).unwrap() {
                    assert!(i <= k + 2);
                    break;
                }
            }
        }
    }
}
