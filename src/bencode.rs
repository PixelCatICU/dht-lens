use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

pub fn parse(input: &[u8]) -> Result<Value> {
    let mut parser = Parser { input, pos: 0 };
    let value = parser.value()?;
    if parser.pos != input.len() {
        bail!("trailing bencode bytes");
    }
    Ok(value)
}

pub fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(value) => {
            out.extend_from_slice(b"i");
            out.extend_from_slice(value.to_string().as_bytes());
            out.extend_from_slice(b"e");
        }
        Value::Bytes(bytes) => {
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(bytes);
        }
        Value::List(values) => {
            out.push(b'l');
            for value in values {
                encode(value, out);
            }
            out.push(b'e');
        }
        Value::Dict(dict) => {
            out.push(b'd');
            for (key, value) in dict {
                encode(&Value::Bytes(key.clone()), out);
                encode(value, out);
            }
            out.push(b'e');
        }
    }
}

pub fn dict_get<'a>(dict: &'a BTreeMap<Vec<u8>, Value>, key: &[u8]) -> Option<&'a Value> {
    dict.get(key)
}

pub fn as_bytes(value: &Value) -> Option<&[u8]> {
    match value {
        Value::Bytes(bytes) => Some(bytes),
        _ => None,
    }
}

pub fn as_int(value: &Value) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        _ => None,
    }
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn value(&mut self) -> Result<Value> {
        let byte = *self
            .input
            .get(self.pos)
            .context("unexpected end of bencode")?;
        match byte {
            b'i' => self.int(),
            b'l' => self.list(),
            b'd' => self.dict(),
            b'0'..=b'9' => self.bytes(),
            _ => bail!("invalid bencode byte {byte}"),
        }
    }

    fn int(&mut self) -> Result<Value> {
        self.pos += 1;
        let start = self.pos;
        while self.input.get(self.pos) != Some(&b'e') {
            self.pos += 1;
            if self.pos >= self.input.len() {
                bail!("unterminated bencode integer");
            }
        }
        let raw = std::str::from_utf8(&self.input[start..self.pos])?;
        self.pos += 1;
        Ok(Value::Int(raw.parse()?))
    }

    fn bytes(&mut self) -> Result<Value> {
        let start = self.pos;
        while self.input.get(self.pos) != Some(&b':') {
            self.pos += 1;
            if self.pos >= self.input.len() {
                bail!("unterminated bencode byte string length");
            }
        }
        let len = std::str::from_utf8(&self.input[start..self.pos])?.parse::<usize>()?;
        self.pos += 1;
        let end = self.pos + len;
        if end > self.input.len() {
            bail!("bencode byte string exceeds input");
        }
        let bytes = self.input[self.pos..end].to_vec();
        self.pos = end;
        Ok(Value::Bytes(bytes))
    }

    fn list(&mut self) -> Result<Value> {
        self.pos += 1;
        let mut values = Vec::new();
        while self.input.get(self.pos) != Some(&b'e') {
            values.push(self.value()?);
            if self.pos >= self.input.len() {
                bail!("unterminated bencode list");
            }
        }
        self.pos += 1;
        Ok(Value::List(values))
    }

    fn dict(&mut self) -> Result<Value> {
        self.pos += 1;
        let mut dict = BTreeMap::new();
        while self.input.get(self.pos) != Some(&b'e') {
            let key = match self.bytes()? {
                Value::Bytes(bytes) => bytes,
                _ => unreachable!(),
            };
            let value = self.value()?;
            dict.insert(key, value);
            if self.pos >= self.input.len() {
                bail!("unterminated bencode dict");
            }
        }
        self.pos += 1;
        Ok(Value::Dict(dict))
    }
}
