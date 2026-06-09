use chardetng::EncodingDetector;
use encoding_rs::Encoding;

pub fn decode_text(bytes: &[u8], encoding_hint: Option<&[u8]>) -> String {
    if let Ok(value) = std::str::from_utf8(bytes) {
        return value.to_string();
    }

    if let Some(hint) = encoding_hint {
        if let Some(encoding) = Encoding::for_label(hint) {
            let (decoded, _, _) = encoding.decode(bytes);
            return decoded.into_owned();
        }
    }

    let mut detector = EncodingDetector::new();
    detector.feed(bytes, true);
    let encoding = detector.guess(None, true);
    let (decoded, _, had_errors) = encoding.decode(bytes);
    if !had_errors {
        return decoded.into_owned();
    }

    String::from_utf8_lossy(bytes).into_owned()
}
