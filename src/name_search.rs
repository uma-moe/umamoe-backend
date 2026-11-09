//! Name search that treats the game's full-width symbols like their ASCII forms.
//!
//! The game stores many names with full-width symbols (`SpicA＋`), while players type
//! ASCII, so a plain `ILIKE '%SpicA+%'` never matches.

const FULL_WIDTH_OFFSET: u32 = 0xFEE0;
const IDEOGRAPHIC_SPACE: char = '\u{3000}';
/// pg_trgm only narrows a regex with width classes when it can anchor on a word this long.
const MIN_INDEXED_WORD_CHARS: usize = 3;

/// PostgreSQL regex for `~*` that finds `query` anywhere in a name. ASCII symbols and
/// spaces also match their full-width forms and the reverse; letters and digits match as
/// typed so pg_trgm GIN indexes stay selective.
pub fn name_contains_regex(query: &str) -> String {
    let match_widths = has_indexed_word(query);
    let mut regex = String::with_capacity(query.len() * 4);

    for character in query.chars() {
        match width_variant_symbol(character) {
            Some(ascii) if match_widths => push_width_class(&mut regex, ascii),
            Some(ascii) if ascii == character => push_escaped_ascii(&mut regex, ascii),
            // Full-width characters and other non-ASCII text are never regex metacharacters.
            _ => regex.push(character),
        }
    }

    regex
}

/// Folds a name for in-memory matching: full-width symbols become ASCII, then lowercase.
pub fn fold_name(name: &str) -> String {
    name.chars()
        .map(|character| width_variant_symbol(character).unwrap_or(character))
        .collect::<String>()
        .to_lowercase()
}

/// ASCII form of a space or symbol that the game may store full-width.
fn width_variant_symbol(character: char) -> Option<char> {
    let ascii = match character {
        IDEOGRAPHIC_SPACE => ' ',
        '\u{FF01}'..='\u{FF5E}' => char::from_u32(character as u32 - FULL_WIDTH_OFFSET)?,
        _ => character,
    };

    let is_symbol = ascii == ' ' || (ascii.is_ascii_graphic() && !ascii.is_ascii_alphanumeric());
    is_symbol.then_some(ascii)
}

fn has_indexed_word(query: &str) -> bool {
    query
        .split(|character: char| !character.is_alphanumeric())
        .any(|word| word.chars().count() >= MIN_INDEXED_WORD_CHARS)
}

fn push_width_class(regex: &mut String, ascii: char) {
    regex.push('[');
    if ascii == ' ' {
        regex.push(' ');
        regex.push(IDEOGRAPHIC_SPACE);
    } else {
        push_escaped_ascii(regex, ascii);
        regex.push(char::from_u32(ascii as u32 + FULL_WIDTH_OFFSET).unwrap_or(ascii));
    }
    regex.push(']');
}

/// `\` before a non-alphanumeric character is a literal in and outside brackets in PostgreSQL AREs.
fn push_escaped_ascii(regex: &mut String, ascii: char) {
    if ascii != ' ' {
        regex.push('\\');
    }
    regex.push(ascii);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_symbols_also_match_their_full_width_forms() {
        assert_eq!(name_contains_regex("SpicA+"), "SpicA[\\+＋]");
    }

    #[test]
    fn typed_full_width_symbols_also_match_ascii() {
        assert_eq!(name_contains_regex("SpicA＋"), "SpicA[\\+＋]");
    }

    #[test]
    fn spaces_match_ideographic_spaces() {
        assert_eq!(name_contains_regex("Team Spica"), "Team[ \u{3000}]Spica");
    }

    #[test]
    fn regex_and_like_metacharacters_are_literal() {
        let regex = name_contains_regex("abc]^-\\%_'.*");

        assert_eq!(
            regex,
            "abc[\\]］][\\^＾][\\-－][\\\\＼][\\%％][\\_＿][\\'＇][\\.．][\\*＊]"
        );
    }

    #[test]
    fn queries_without_an_indexable_word_match_as_typed() {
        assert_eq!(name_contains_regex("ab+ c"), "ab\\+ c");
        assert_eq!(name_contains_regex("ab＋"), "ab＋");
    }

    #[test]
    fn letters_digits_and_japanese_stay_literal() {
        assert_eq!(name_contains_regex("ＡＢＣ123スピカ"), "ＡＢＣ123スピカ");
    }

    #[test]
    fn folded_names_compare_across_widths_and_case() {
        let stored = fold_name("SpicA＋\u{3000}Team");

        assert_eq!(stored, "spica+ team");
        assert!(stored.contains(&fold_name("spica＋ t")));
    }
}
