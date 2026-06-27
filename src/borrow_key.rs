use crate::models::{Inheritance, SupportCard};

const PREFIX: &str = "bk1:";

pub fn normalize_borrow_key(
    borrow_key: Option<&str>,
    inheritance_id: i64,
    support_card_id: i32,
) -> String {
    if let Some(normalized) = normalize_stable_borrow_key(borrow_key) {
        return normalized;
    }

    key_from_legacy_ids(inheritance_id, support_card_id)
}

pub fn normalize_stable_borrow_key(borrow_key: Option<&str>) -> Option<String> {
    let normalized = borrow_key?.trim().to_ascii_lowercase();
    is_valid_borrow_key(&normalized).then_some(normalized)
}

pub fn key_from_profile(
    inheritance: Option<&Inheritance>,
    support_card: Option<&SupportCard>,
) -> String {
    let mut signature = String::with_capacity(384);

    if let Some(inheritance) = inheritance {
        push_i32(&mut signature, inheritance.main_parent_id);
        push_i32(&mut signature, inheritance.parent_left_id);
        push_i32(&mut signature, inheritance.parent_right_id);
        push_i32(&mut signature, inheritance.parent_rank);
        push_i32(&mut signature, inheritance.parent_rarity);
        push_slice(&mut signature, &inheritance.blue_sparks);
        push_slice(&mut signature, &inheritance.pink_sparks);
        push_slice(&mut signature, &inheritance.green_sparks);
        push_slice(&mut signature, &inheritance.white_sparks);
        push_i32(&mut signature, inheritance.win_count);
        push_i32(&mut signature, inheritance.white_count);
        push_i32(&mut signature, inheritance.main_blue_factors);
        push_i32(&mut signature, inheritance.main_pink_factors);
        push_i32(&mut signature, inheritance.main_green_factors);
        push_slice(&mut signature, &inheritance.main_white_factors);
        push_i32(&mut signature, inheritance.main_white_count);
        push_i32(&mut signature, inheritance.left_blue_factors);
        push_i32(&mut signature, inheritance.left_pink_factors);
        push_i32(&mut signature, inheritance.left_green_factors);
        push_slice(&mut signature, &inheritance.left_white_factors);
        push_i32(&mut signature, inheritance.left_white_count);
        push_i32(&mut signature, inheritance.right_blue_factors);
        push_i32(&mut signature, inheritance.right_pink_factors);
        push_i32(&mut signature, inheritance.right_green_factors);
        push_slice(&mut signature, &inheritance.right_white_factors);
        push_i32(&mut signature, inheritance.right_white_count);
        push_slice(&mut signature, &inheritance.main_win_saddles);
        push_slice(&mut signature, &inheritance.left_win_saddles);
        push_slice(&mut signature, &inheritance.right_win_saddles);
        push_slice(&mut signature, &inheritance.race_results);
    } else {
        for _ in 0..30 {
            push_i32(&mut signature, 0);
        }
    }

    if let Some(support_card) = support_card {
        push_i32(&mut signature, support_card.support_card_id);
        push_i32(&mut signature, support_card.limit_break_count.unwrap_or(-1));
        push_i32(&mut signature, support_card.experience);
    } else {
        push_i32(&mut signature, 0);
        push_i32(&mut signature, -1);
        push_i32(&mut signature, -1);
    }

    key_from_signature(&signature)
}

fn key_from_legacy_ids(inheritance_id: i64, support_card_id: i32) -> String {
    format!(
        "legacy:{}:{}",
        inheritance_id.max(0),
        support_card_id.max(0)
    )
}

fn key_from_signature(signature: &str) -> String {
    format!("{}{}", PREFIX, fast_hash64_hex(signature))
}

fn is_valid_borrow_key(value: &str) -> bool {
    value.len() == PREFIX.len() + 16
        && value.starts_with(PREFIX)
        && value[PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

fn push_sep(out: &mut String) {
    if !out.is_empty() {
        out.push('|');
    }
}

fn push_i32(out: &mut String, value: i32) {
    push_sep(out);
    out.push_str(&value.to_string());
}

fn push_slice(out: &mut String, values: &[i32]) {
    push_sep(out);
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&value.to_string());
    }
}

fn fast_hash64_hex(input: &str) -> String {
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
