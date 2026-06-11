export function buildNameNgram(input, maxLen = 4096) {
  const tokens = [];
  const words = input.toLowerCase().match(/[a-z0-9]+/g) ?? [];
  tokens.push(...words);

  const cjkRuns = input.match(/[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}\p{Script=Hangul}]+/gu) ?? [];
  for (const run of cjkRuns) {
    for (const n of [2, 3]) {
      for (let i = 0; i + n <= run.length; i += 1) {
        tokens.push(run.slice(i, i + n));
      }
    }
  }

  const seen = new Set();
  let out = '';
  for (const token of tokens) {
    if (!token || seen.has(token)) continue;
    seen.add(token);
    const next = out ? `${out} ${token}` : token;
    if (next.length > maxLen) break;
    out = next;
  }
  return out;
}
