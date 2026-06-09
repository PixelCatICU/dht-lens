use std::collections::HashSet;

pub fn build_name_ngram(input: &str, max_len: usize) -> String {
    let normalized = normalize(input);
    let mut tokens = Vec::new();
    let mut seen = HashSet::new();

    for segment in normalized.split_whitespace() {
        if segment.is_ascii() {
            push_token(segment, &mut tokens, &mut seen);
            continue;
        }

        let chars: Vec<char> = segment.chars().collect();
        for n in [2, 3] {
            if chars.len() >= n {
                for window in chars.windows(n) {
                    let token: String = window.iter().collect();
                    push_token(&token, &mut tokens, &mut seen);
                }
            }
        }
        if chars.len() >= 2 {
            push_token(segment, &mut tokens, &mut seen);
        }
    }

    let mut out = String::new();
    for token in tokens {
        if !out.is_empty() {
            out.push(' ');
        }
        if out.len() + token.len() + 1 > max_len {
            break;
        }
        out.push_str(&token);
    }
    out
}

fn normalize(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars().flat_map(char::to_lowercase) {
        if ch.is_alphanumeric() || is_cjk(ch) {
            out.push(ch);
        } else {
            out.push(' ');
        }
    }
    out
}

fn push_token(token: &str, tokens: &mut Vec<String>, seen: &mut HashSet<String>) {
    if token.len() < 2 {
        return;
    }
    if seen.insert(token.to_string()) {
        tokens.push(token.to_string());
    }
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
    )
}

#[cfg(test)]
mod tests {
    use super::build_name_ngram;

    #[test]
    fn builds_cjk_ngrams_and_ascii_tokens() {
        let text = build_name_ngram("周杰伦演唱会.2024.1080p.BluRay.x264", 4096);
        assert!(text.contains("周杰"));
        assert!(text.contains("周杰伦"));
        assert!(text.contains("2024"));
        assert!(text.contains("bluray"));
    }
}
