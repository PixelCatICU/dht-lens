pub fn build_name_ngram(input: &str, max_len: usize) -> String {
    let mut tokens = Vec::new();
    let mut ascii = String::new();
    let mut cjk = String::new();

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if !cjk.is_empty() {
                push_cjk_tokens(&mut tokens, &cjk);
                cjk.clear();
            }
            ascii.push(ch.to_ascii_lowercase());
        } else if is_cjk(ch) {
            if !ascii.is_empty() {
                tokens.push(std::mem::take(&mut ascii));
            }
            cjk.push(ch);
        } else {
            if !ascii.is_empty() {
                tokens.push(std::mem::take(&mut ascii));
            }
            if !cjk.is_empty() {
                push_cjk_tokens(&mut tokens, &cjk);
                cjk.clear();
            }
        }
    }
    if !ascii.is_empty() {
        tokens.push(ascii);
    }
    if !cjk.is_empty() {
        push_cjk_tokens(&mut tokens, &cjk);
    }

    let mut seen = std::collections::HashSet::new();
    let mut output = String::new();
    for token in tokens {
        if token.is_empty() || !seen.insert(token.clone()) {
            continue;
        }
        let next_len = output.len() + token.len() + usize::from(!output.is_empty());
        if next_len > max_len {
            break;
        }
        if !output.is_empty() {
            output.push(' ');
        }
        output.push_str(&token);
    }
    output
}

fn push_cjk_tokens(tokens: &mut Vec<String>, input: &str) {
    let chars = input.chars().collect::<Vec<_>>();
    for n in [2, 3] {
        for window in chars.windows(n) {
            tokens.push(window.iter().collect());
        }
    }
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x4E00..=0x9FFF | 0x3040..=0x30FF | 0xAC00..=0xD7AF
    )
}

#[cfg(test)]
mod tests {
    use super::build_name_ngram;

    #[test]
    fn builds_cjk_ngrams_and_ascii_tokens() {
        let value = build_name_ngram("周杰伦演唱会.2024.1080p", 4096);
        assert!(value.contains("周杰"));
        assert!(value.contains("周杰伦"));
        assert!(value.contains("2024"));
        assert!(value.contains("1080p"));
    }
}
