use crate::types::{Inheritance, SupportCard};

include!("types/borrow_key.rs");

pub fn normalizeBorrowKey(
    borrow_key: Option<&str>,
    inheritance_id: i64,
    support_card_id: i32,
) -> String {
    if let Some(normalized) = normalizeStableBorrowKey(borrow_key) {
        return normalized;
    }

    keyFromLegacyIds(inheritance_id, support_card_id)
}

pub fn normalizeStableBorrowKey(borrow_key: Option<&str>) -> Option<String> {
    let normalized = borrow_key?.trim().to_ascii_lowercase();
    isValidBorrowKey(&normalized).then_some(normalized)
}

pub fn keyFromProfile(
    inheritance: Option<&Inheritance>,
    support_card: Option<&SupportCard>,
) -> String {
    let mut signature = String::with_capacity(384);

    if let Some(inheritance) = inheritance {
        pushI32(&mut signature, inheritance.main_parent_id);
        pushI32(&mut signature, inheritance.parent_left_id);
        pushI32(&mut signature, inheritance.parent_right_id);
        pushI32(&mut signature, inheritance.parent_rank);
        pushI32(&mut signature, inheritance.parent_rarity);
        pushSlice(&mut signature, &inheritance.blue_sparks);
        pushSlice(&mut signature, &inheritance.pink_sparks);
        pushSlice(&mut signature, &inheritance.green_sparks);
        pushSlice(&mut signature, &inheritance.white_sparks);
        pushI32(&mut signature, inheritance.win_count);
        pushI32(&mut signature, inheritance.white_count);
        pushI32(&mut signature, inheritance.main_blue_factors);
        pushI32(&mut signature, inheritance.main_pink_factors);
        pushI32(&mut signature, inheritance.main_green_factors);
        pushSlice(&mut signature, &inheritance.main_white_factors);
        pushI32(&mut signature, inheritance.main_white_count);
        pushI32(&mut signature, inheritance.left_blue_factors);
        pushI32(&mut signature, inheritance.left_pink_factors);
        pushI32(&mut signature, inheritance.left_green_factors);
        pushSlice(&mut signature, &inheritance.left_white_factors);
        pushI32(&mut signature, inheritance.left_white_count);
        pushI32(&mut signature, inheritance.right_blue_factors);
        pushI32(&mut signature, inheritance.right_pink_factors);
        pushI32(&mut signature, inheritance.right_green_factors);
        pushSlice(&mut signature, &inheritance.right_white_factors);
        pushI32(&mut signature, inheritance.right_white_count);
        pushSlice(&mut signature, &inheritance.main_win_saddles);
        pushSlice(&mut signature, &inheritance.left_win_saddles);
        pushSlice(&mut signature, &inheritance.right_win_saddles);
        pushSlice(&mut signature, &inheritance.race_results);
    } else {
        for _ in 0..30 {
            pushI32(&mut signature, 0);
        }
    }

    if let Some(support_card) = support_card {
        pushI32(&mut signature, support_card.support_card_id);
        pushI32(&mut signature, support_card.limit_break_count.unwrap_or(-1));
        pushI32(&mut signature, support_card.experience);
    } else {
        pushI32(&mut signature, 0);
        pushI32(&mut signature, -1);
        pushI32(&mut signature, -1);
    }

    keyFromSignature(&signature)
}

fn keyFromLegacyIds(inheritance_id: i64, support_card_id: i32) -> String {
    format!(
        "legacy:{}:{}",
        inheritance_id.max(0),
        support_card_id.max(0)
    )
}

fn keyFromSignature(signature: &str) -> String {
    format!("{}{}", PREFIX, fastHash64Hex(signature))
}

fn isValidBorrowKey(value: &str) -> bool {
    value.len() == PREFIX.len() + 16
        && value.starts_with(PREFIX)
        && value[PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

fn pushSep(out: &mut String) {
    if !out.is_empty() {
        out.push('|');
    }
}

fn pushI32(out: &mut String, value: i32) {
    pushSep(out);
    out.push_str(&value.to_string());
}

fn pushSlice(out: &mut String, values: &[i32]) {
    pushSep(out);
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
}

fn fastHash64Hex(input: &str) -> String {
    let mut h1 = 0xdead_beefu32;
    let mut h2 = 0x41c6_ce57u32;

    for byte in input.bytes() {
        let value = byte as u32;
        h1 = (h1 ^ value).wrapping_mul(2_654_435_761);
        h2 = (h2 ^ value).wrapping_mul(1_597_334_677);
    }

    h1 = (h1 ^ (h1 >> 16)).wrapping_mul(2_246_822_507)
        ^ (h2 ^ (h2 >> 13)).wrapping_mul(3_266_489_909);
    h2 = (h2 ^ (h2 >> 16)).wrapping_mul(2_246_822_507)
        ^ (h1 ^ (h1 >> 13)).wrapping_mul(3_266_489_909);

    format!("{h2:08x}{h1:08x}")
}
