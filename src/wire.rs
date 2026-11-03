/// Conversion between values and the bytes sent over the network.
///
/// Numbers use the C++ library's default text format (`toString`/`fromString`).
/// Use [`Raw`] for its `rawPack`/`rawUnpack` format and [`Bytes`] for binary.
pub trait Wire: Sized {
    fn pack(&self) -> Vec<u8>;
    fn unpack(bytes: &[u8]) -> Option<Self>;
}

macro_rules! text_wire {
    ($($t:ty),* $(,)?) => {$(
        impl Wire for $t {
            fn pack(&self) -> Vec<u8> {
                self.to_string().into_bytes()
            }
            fn unpack(bytes: &[u8]) -> Option<Self> {
                std::str::from_utf8(bytes).ok()?.trim().parse().ok()
            }
        }
        impl Wire for Vec<$t> {
            fn pack(&self) -> Vec<u8> {
                self.iter().map(|v| format!("{v} ")).collect::<String>().into_bytes()
            }
            fn unpack(bytes: &[u8]) -> Option<Self> {
                std::str::from_utf8(bytes)
                    .ok()?
                    .split_whitespace()
                    .map(|v| v.parse().ok())
                    .collect()
            }
        }
    )*};
}

text_wire!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

impl Wire for bool {
    fn pack(&self) -> Vec<u8> {
        vec![if *self { b'1' } else { b'0' }]
    }
    fn unpack(bytes: &[u8]) -> Option<Self> {
        match std::str::from_utf8(bytes).ok()?.trim() {
            "1" => Some(true),
            "0" => Some(false),
            _ => None,
        }
    }
}

impl Wire for String {
    fn pack(&self) -> Vec<u8> {
        self.clone().into_bytes()
    }
    fn unpack(bytes: &[u8]) -> Option<Self> {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

/// Opaque binary data, sent as is.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bytes(pub Vec<u8>);

impl Wire for Bytes {
    fn pack(&self) -> Vec<u8> {
        self.0.clone()
    }
    fn unpack(bytes: &[u8]) -> Option<Self> {
        Some(Bytes(bytes.to_vec()))
    }
}

/// A number sent as its bytes in native order, like the C++ `rawPack`.
///
/// Only valid between machines with the same endianness.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Raw<T>(pub T);

macro_rules! raw_wire {
    ($($t:ty),* $(,)?) => {$(
        impl Wire for Raw<$t> {
            fn pack(&self) -> Vec<u8> {
                self.0.to_ne_bytes().to_vec()
            }
            fn unpack(bytes: &[u8]) -> Option<Self> {
                Some(Raw(<$t>::from_ne_bytes(bytes.try_into().ok()?)))
            }
        }
    )*};
}

raw_wire!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_use_text() {
        assert_eq!(42i32.pack(), b"42");
        assert_eq!(i32::unpack(b" 42 "), Some(42));
        assert_eq!(i32::unpack(b"x"), None);
        assert_eq!(f64::unpack(&1.5f64.pack()), Some(1.5));
    }

    #[test]
    fn arrays_are_space_separated_like_cpp() {
        assert_eq!(vec![1i32, 2].pack(), b"1 2 ");
        assert_eq!(Vec::<i32>::unpack(b"1 2 "), Some(vec![1, 2]));
        assert_eq!(Vec::<i32>::unpack(b"1 x"), None);
    }

    #[test]
    fn strings_and_bytes_are_verbatim() {
        assert_eq!(String::unpack(&"olá".to_owned().pack()).unwrap(), "olá");
        assert_eq!(Bytes::unpack(&[0, 1]), Some(Bytes(vec![0, 1])));
    }

    #[test]
    fn raw_requires_the_exact_size() {
        assert_eq!(Raw::<i32>::unpack(&Raw(7i32).pack()), Some(Raw(7)));
        assert_eq!(Raw::<i32>::unpack(&[1, 2]), None);
    }
}
