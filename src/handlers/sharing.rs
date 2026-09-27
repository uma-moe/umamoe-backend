use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use sqlx::Row;

use crate::{
    errors::Result,
    types::{InheritanceShareData, SharePathParams, SupportCardShareData},
    AppState,
};

pub fn router() -> Router<AppState> {
    Router::new().route("/s/:share_type/:account_id", get(sharePage))
}

pub async fn sharePage(
    State(state): State<AppState>,
    Path(params): Path<SharePathParams>,
) -> Result<Response> {
    match params.share_type.as_str() {
        "inheritance" => inheritanceShare(&state, &params.account_id).await,
        "support-card" => supportCardShare(&state, &params.account_id).await,
        _ => {
            // Return a 404 for unknown share types
            let html = generateErrorHtml(
                "Invalid share type",
                "The requested share type is not supported.",
            );
            Ok(Html(html).into_response())
        }
    }
}

async fn inheritanceShare(state: &AppState, account_id: &str) -> Result<Response> {
    // Query to get inheritance data with character names
    let query = r#"
        SELECT 
            t.account_id,
            t.name as trainer_name,
            t.follower_num,
            i.inheritance_id,
            i.main_parent_id,
            i.parent_left_id,
            i.parent_right_id,
            i.parent_rank,
            i.parent_rarity,
            i.blue_sparks,
            i.pink_sparks,
            i.green_sparks,
            i.white_sparks,
            i.win_count,
            i.white_count,
            i.main_blue_factors,
            i.main_pink_factors,
            i.main_green_factors,
            i.main_white_factors,
            i.main_white_count
        FROM trainer t
        INNER JOIN inheritance i ON t.account_id = i.account_id
        WHERE t.account_id = $1
    "#;

    let row = match sqlx::query(query)
        .bind(account_id)
        .fetch_optional(&state.db)
        .await?
    {
        Some(row) => row,
        None => {
            let html = generateErrorHtml(
                "Inheritance Not Found",
                "The requested inheritance record could not be found.",
            );
            return Ok(Html(html).into_response());
        }
    };

    // Extract data from the row
    let trainer_name: String = row.get("trainer_name");
    let main_parent_id: i32 = row.get("main_parent_id");
    let parent_left_id: i32 = row.get("parent_left_id");
    let parent_right_id: i32 = row.get("parent_right_id");
    let parent_rank: i32 = row.get("parent_rank");
    let parent_rarity: i32 = row.get("parent_rarity");
    let win_count: i32 = row.get("win_count");
    let white_count: i32 = row.get("white_count");
    let blue_sparks: Vec<i32> = row.get("blue_sparks");
    let pink_sparks: Vec<i32> = row.get("pink_sparks");
    let green_sparks: Vec<i32> = row.get("green_sparks");
    let white_sparks: Vec<i32> = row.get("white_sparks");
    let main_blue_factors: i32 = row.get("main_blue_factors");
    let main_pink_factors: i32 = row.get("main_pink_factors");
    let main_green_factors: i32 = row.get("main_green_factors");
    let main_white_factors: Vec<i32> = row.get("main_white_factors");
    let main_white_count: i32 = row.get("main_white_count");

    // Get character names (you'll need to create this mapping)
    let character_name = getCharacterName(main_parent_id);
    let parent_left_name = getCharacterName(parent_left_id);
    let parent_right_name = getCharacterName(parent_right_id);

    // Generate summaries
    let blue_factors_summary = formatSparksSummary(&blue_sparks, "blue");
    let pink_factors_summary = formatSparksSummary(&pink_sparks, "pink");
    let green_factors_summary = formatSparksSummary(&green_sparks, "green");
    let white_factors_summary = formatSparksSummary(&white_sparks, "white");
    let main_factors_summary = format!(
        "Blue: {} • Pink: {} • Green: {} • White: {} ({})",
        main_blue_factors,
        main_pink_factors,
        main_green_factors,
        formatSparksSummary(&main_white_factors, "white"),
        main_white_count
    );

    let share_data = InheritanceShareData {
        account_id: account_id.to_string(),
        trainer_name,
        character_name,
        parent_left_name,
        parent_right_name,
        parent_rank,
        parent_rarity,
        win_count,
        white_count,
        blue_factors_summary,
        pink_factors_summary,
        green_factors_summary,
        white_factors_summary,
        main_factors_summary,
    };

    let html = generateInheritanceHtml(&share_data);

    // Set proper headers for HTML response
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/html; charset=utf-8"),
    );

    Ok((headers, Html(html)).into_response())
}

async fn supportCardShare(state: &AppState, account_id: &str) -> Result<Response> {
    // Query to get the best support card for this account
    let query = r#"
        SELECT 
            t.account_id,
            t.name as trainer_name,
            sc.support_card_id,
            sc.limit_break_count,
            sc.experience
        FROM trainer t
        INNER JOIN support_card sc ON t.account_id = sc.account_id
        WHERE t.account_id = $1
        ORDER BY sc.experience DESC, sc.support_card_id ASC
        LIMIT 1
    "#;

    let row = match sqlx::query(query)
        .bind(account_id)
        .fetch_optional(&state.db)
        .await?
    {
        Some(row) => row,
        None => {
            let html = generateErrorHtml(
                "Support Card Not Found",
                "The requested support card record could not be found.",
            );
            return Ok(Html(html).into_response());
        }
    };

    let trainer_name: String = row.get("trainer_name");
    let support_card_id: i32 = row.get("support_card_id");
    let limit_break_count: Option<i32> = row.get("limit_break_count");
    let experience: i32 = row.get("experience");

    // Get card details (you'll need to create this mapping)
    let (card_name, card_rarity, card_type) = getSupportCardDetails(support_card_id);

    let share_data = SupportCardShareData {
        account_id: account_id.to_string(),
        trainer_name,
        card_name,
        card_rarity,
        limit_break_count,
        experience,
        card_type,
    };

    let html = generateSupportCardHtml(&share_data);

    // Set proper headers for HTML response
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/html; charset=utf-8"),
    );

    Ok((headers, Html(html)).into_response())
}

fn generateInheritanceHtml(data: &InheritanceShareData) -> String {
    let title = format!(
        "{}'s {} Inheritance",
        data.trainer_name, data.character_name
    );
    let description = format!(
        "Parents: {} × {} • Rank: {} • Rarity: {} • Wins: {} • White Skills: {} • {}",
        data.parent_left_name,
        data.parent_right_name,
        getRankDisplay(data.parent_rank),
        getRarityDisplay(data.parent_rarity),
        data.win_count,
        data.white_count,
        data.main_factors_summary
    );

    let html = format!(
        "<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>{}</title>
    
    <!-- Discord Embed Meta Tags -->
    <meta property=\"og:type\" content=\"website\">
    <meta property=\"og:title\" content=\"{}\">
    <meta property=\"og:description\" content=\"{}\">
    <meta property=\"og:url\" content=\"https://honse.moe/s/inheritance/{}\">
    <meta property=\"og:site_name\" content=\"Honse.moe - Uma Musume Database\">
    <meta property=\"og:color\" content=\"#FF6B9D\">
    
    <!-- Twitter Card -->
    <meta name=\"twitter:card\" content=\"summary\">
    <meta name=\"twitter:title\" content=\"{}\">
    <meta name=\"twitter:description\" content=\"{}\">
    
    <!-- Redirect to main app -->
    <script>
        // Redirect to the main app after a short delay to allow Discord to scrape
        setTimeout(function() {{
            window.location.href = 'https://honse.moe/inheritance?trainer_id={}';
        }}, 2000);
    </script>
    
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .inheritance-card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .character-name {{
            font-size: 24px;
            font-weight: bold;
            color: #FF6B9D;
            margin-bottom: 10px;
        }}
        .trainer-name {{
            font-size: 18px;
            color: #666;
            margin-bottom: 15px;
        }}
        .parents {{
            font-size: 16px;
            margin-bottom: 10px;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-bottom: 15px;
        }}
        .stat {{
            background: #f8f9fa;
            padding: 10px;
            border-radius: 5px;
            text-align: center;
        }}
        .factors {{
            margin-top: 15px;
        }}
        .factor-group {{
            margin-bottom: 8px;
        }}
        .redirect-notice {{
            background: #e3f2fd;
            border: 1px solid #2196F3;
            border-radius: 5px;
            padding: 15px;
            text-align: center;
            color: #1976D2;
        }}
    </style>
</head>
<body>
    <div class=\"inheritance-card\">
        <div class=\"character-name\">{} Inheritance</div>
        <div class=\"trainer-name\">Trainer: {}</div>
        <div class=\"parents\">Parents: {} × {}</div>
        
        <div class=\"stats\">
            <div class=\"stat\">
                <strong>Rank</strong><br>
                {}
            </div>
            <div class=\"stat\">
                <strong>Rarity</strong><br>
                {}
            </div>
            <div class=\"stat\">
                <strong>Wins</strong><br>
                {}
            </div>
            <div class=\"stat\">
                <strong>White Skills</strong><br>
                {}
            </div>
        </div>
        
        <div class=\"factors\">
            <div class=\"factor-group\"><strong>Inherited Factors:</strong></div>
            <div class=\"factor-group\">Blue: {}</div>
            <div class=\"factor-group\">Pink: {}</div>
            <div class=\"factor-group\">Green: {}</div>
            <div class=\"factor-group\">White: {}</div>
            <div class=\"factor-group\"><strong>Main Factors:</strong> {}</div>
        </div>
    </div>
    
    <div class=\"redirect-notice\">
        Redirecting to the full database in a moment...
    </div>
</body>
</html>",
        title,
        title,
        description,
        data.account_id,
        title,
        description,
        data.account_id,
        data.character_name,
        data.trainer_name,
        data.parent_left_name,
        data.parent_right_name,
        getRankDisplay(data.parent_rank),
        getRarityDisplay(data.parent_rarity),
        data.win_count,
        data.white_count,
        data.blue_factors_summary,
        data.pink_factors_summary,
        data.green_factors_summary,
        data.white_factors_summary,
        data.main_factors_summary
    );
    html
}

fn generateSupportCardHtml(data: &SupportCardShareData) -> String {
    let title = format!("{}'s {} Support Card", data.trainer_name, data.card_name);
    let limit_break_display = match data.limit_break_count {
        Some(lb) => format!("★{}", lb),
        None => "★0".to_string(),
    };
    let description = format!(
        "{} {} • {} • Experience: {} • Trainer: {}",
        data.card_rarity, data.card_name, limit_break_display, data.experience, data.trainer_name
    );

    let html = format!(
        "<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>{}</title>
    
    <!-- Discord Embed Meta Tags -->
    <meta property=\"og:type\" content=\"website\">
    <meta property=\"og:title\" content=\"{}\">
    <meta property=\"og:description\" content=\"{}\">
    <meta property=\"og:url\" content=\"https://honse.moe/s/support-card/{}\">
    <meta property=\"og:site_name\" content=\"Honse.moe - Uma Musume Database\">
    <meta property=\"og:color\" content=\"#4CAF50\">
    
    <!-- Twitter Card -->
    <meta name=\"twitter:card\" content=\"summary\">
    <meta name=\"twitter:title\" content=\"{}\">
    <meta name=\"twitter:description\" content=\"{}\">
    
    <!-- Redirect to main app -->
    <script>
        // Redirect to the main app after a short delay to allow Discord to scrape
        setTimeout(function() {{
            window.location.href = 'https://honse.moe/support-cards?trainer_id={}';
        }}, 2000);
    </script>
    
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .card-name {{
            font-size: 24px;
            font-weight: bold;
            color: #4CAF50;
            margin-bottom: 10px;
        }}
        .trainer-name {{
            font-size: 18px;
            color: #666;
            margin-bottom: 15px;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-bottom: 15px;
        }}
        .stat {{
            background: #f8f9fa;
            padding: 10px;
            border-radius: 5px;
            text-align: center;
        }}
        .redirect-notice {{
            background: #e3f2fd;
            border: 1px solid #2196F3;
            border-radius: 5px;
            padding: 15px;
            text-align: center;
            color: #1976D2;
        }}
    </style>
</head>
<body>
    <div class=\"card\">
        <div class=\"card-name\">{}</div>
        <div class=\"trainer-name\">Trainer: {}</div>
        
        <div class=\"stats\">
            <div class=\"stat\">
                <strong>Rarity</strong><br>
                {}
            </div>
            <div class=\"stat\">
                <strong>Limit Break</strong><br>
                {}
            </div>
            <div class=\"stat\">
                <strong>Experience</strong><br>
                {}
            </div>
            <div class=\"stat\">
                <strong>Type</strong><br>
                {}
            </div>
        </div>
    </div>
    
    <div class=\"redirect-notice\">
        Redirecting to the full database in a moment...
    </div>
</body>
</html>",
        title,
        title,
        description,
        data.account_id,
        title,
        description,
        data.account_id,
        data.card_name,
        data.trainer_name,
        data.card_rarity,
        limit_break_display,
        data.experience,
        data.card_type
    );
    html
}

fn generateErrorHtml(title: &str, message: &str) -> String {
    format!(
        "<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>{}</title>
    
    <!-- Redirect to main app -->
    <script>
        setTimeout(function() {{
            window.location.href = 'https://honse.moe/';
        }}, 3000);
    </script>
    
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 50px auto;
            padding: 20px;
            text-align: center;
            background-color: #f5f5f5;
        }}
        .error-card {{
            background: white;
            border-radius: 10px;
            padding: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .error-title {{
            font-size: 24px;
            color: #f44336;
            margin-bottom: 15px;
        }}
        .error-message {{
            font-size: 16px;
            color: #666;
            margin-bottom: 20px;
        }}
        .redirect-notice {{
            background: #e3f2fd;
            border: 1px solid #2196F3;
            border-radius: 5px;
            padding: 15px;
            color: #1976D2;
        }}
    </style>
</head>
<body>
    <div class=\"error-card\">
        <div class=\"error-title\">{}</div>
        <div class=\"error-message\">{}</div>
        <div class=\"redirect-notice\">
            Redirecting to homepage in a moment...
        </div>
    </div>
</body>
</html>",
        title, title, message
    )
}

// Helper functions for mapping IDs to names (you'll need to implement these)
fn getCharacterName(character_id: i32) -> String {
    // This is a simplified mapping - you should load this from your data files
    match character_id {
        1 => "Special Week".to_string(),
        2 => "Silence Suzuka".to_string(),
        3 => "Tokai Teio".to_string(),
        4 => "Vodka".to_string(),
        5 => "Daiwa Scarlet".to_string(),
        6 => "Gold Ship".to_string(),
        7 => "Mejiro McQueen".to_string(),
        8 => "Emperor".to_string(),
        9 => "Fuji Kiseki".to_string(),
        10 => "Orfevre".to_string(),
        11 => "Agnes Tachyon".to_string(),
        12 => "Agnes Digital".to_string(),
        13 => "Haru Urara".to_string(),
        14 => "El Condor Pasa".to_string(),
        15 => "Grass Wonder".to_string(),
        16 => "Air Groove".to_string(),
        17 => "Mayano Top Gun".to_string(),
        18 => "Manhattan Cafe".to_string(),
        19 => "Mihono Bourbon".to_string(),
        20 => "Mejiro Ryan".to_string(),
        21 => "Hishi Amazon".to_string(),
        22 => "Yukino Bijin".to_string(),
        23 => "Rice Shower".to_string(),
        24 => "King Halo".to_string(),
        25 => "Matikanetannhauser".to_string(),
        26 => "Ikuno Dictus".to_string(),
        27 => "Tamamo Cross".to_string(),
        28 => "Fine Motion".to_string(),
        29 => "Biwa Hayahide".to_string(),
        30 => "Narita Taishin".to_string(),
        31 => "Winning Ticket".to_string(),
        32 => "Air Shakur".to_string(),
        33 => "Eishin Flash".to_string(),
        34 => "Copano Rickey".to_string(),
        35 => "Sinister Minister".to_string(),
        36 => "Mejiro Dober".to_string(),
        37 => "Twin Turbo".to_string(),
        38 => "Marvelous Sunday".to_string(),
        39 => "Seeking the Pearl".to_string(),
        40 => "Shinko Windy".to_string(),
        41 => "Sweep Tosho".to_string(),
        42 => "Super Creek".to_string(),
        43 => "Smart Falcon".to_string(),
        44 => "Zen-no-Rob Roy".to_string(),
        45 => "T.M. Opera O".to_string(),
        46 => "Narita Brian".to_string(),
        47 => "Symboli Rudolf".to_string(),
        48 => "Aiming for the Top".to_string(),
        49 => "Admire Vega".to_string(),
        50 => "Inari One".to_string(),
        51 => "Winning Ticket".to_string(),
        52 => "Nice Nature".to_string(),
        53 => "Tosen Jordan".to_string(),
        54 => "Mejiro Bright".to_string(),
        55 => "Satono Diamond".to_string(),
        56 => "Kitasan Black".to_string(),
        57 => "Sakura Bakushin O".to_string(),
        58 => "Sirius Symboli".to_string(),
        59 => "Mejiro Ardan".to_string(),
        60 => "Yaeno Muteki".to_string(),
        61 => "Nishino Flower".to_string(),
        62 => "Hokko Tarumae".to_string(),
        63 => "Wonder Acute".to_string(),
        64 => "Nakayama Festa".to_string(),
        65 => "Tap Dance City".to_string(),
        66 => "Curren Chan".to_string(),
        67 => "Gold City".to_string(),
        68 => "Sakura Chiyono O".to_string(),
        69 => "Meisho Doto".to_string(),
        70 => "Yamanin Zephyr".to_string(),
        71 => "K.S. Miracle".to_string(),
        72 => "Dantsu Flame".to_string(),
        73 => "Sound of Earth".to_string(),
        74 => "Duramente".to_string(),
        75 => "Daiichi Ruby".to_string(),
        76 => "Zenno Rob Roy".to_string(),
        77 => "Tagano Diamond".to_string(),
        78 => "Kawakami Princess".to_string(),
        79 => "Mejiro Palmer".to_string(),
        80 => "Neo Universe".to_string(),
        81 => "Symboli Kris S".to_string(),
        82 => "Narita Top Road".to_string(),
        83 => "Jungle Pocket".to_string(),
        84 => "Daiwa Major".to_string(),
        85 => "Yukikaze".to_string(),
        86 => "Cheval Grand".to_string(),
        87 => "Gossamer".to_string(),
        88 => "Meiner Liebe".to_string(),
        89 => "Agnes World".to_string(),
        90 => "World End".to_string(),
        91 => "Lovely Derby".to_string(),
        92 => "Bamboo Memory".to_string(),
        93 => "Hello Unique".to_string(),
        94 => "Zenith".to_string(),
        _ => format!("Character {}", character_id),
    }
}

fn getSupportCardDetails(support_card_id: i32) -> (String, String, String) {
    // This is a simplified mapping - you should load this from your data files
    // Return (name, rarity, type)
    match support_card_id {
        // This is just an example - you'll need to populate with actual support card data
        _ => (
            format!("Support Card {}", support_card_id),
            "★★★".to_string(),
            "Speed".to_string(),
        ),
    }
}

fn getRankDisplay(rank: i32) -> String {
    match rank {
        1 => "G".to_string(),
        2 => "F".to_string(),
        3 => "E".to_string(),
        4 => "D".to_string(),
        5 => "C".to_string(),
        6 => "B".to_string(),
        7 => "A".to_string(),
        8 => "S".to_string(),
        9 => "SS".to_string(),
        10 => "SSS".to_string(),
        _ => format!("Rank {}", rank),
    }
}

fn getRarityDisplay(rarity: i32) -> String {
    match rarity {
        1 => "★".to_string(),
        2 => "★★".to_string(),
        3 => "★★★".to_string(),
        _ => format!("{}★", rarity),
    }
}

fn formatSparksSummary(sparks: &[i32], _spark_type: &str) -> String {
    if sparks.is_empty() {
        return "None".to_string();
    }

    // Group sparks by factor type and count levels
    use std::collections::HashMap;
    let mut factor_counts: HashMap<i32, Vec<i32>> = HashMap::new();

    for &spark in sparks {
        let factor_id = spark / 10;
        let level = spark % 10;
        factor_counts.entry(factor_id).or_default().push(level);
    }

    let mut summary_parts: Vec<String> = Vec::new();

    for (factor_id, levels) in factor_counts {
        let factor_name = getFactorName(factor_id);
        let max_level = levels.iter().max().unwrap_or(&0);
        summary_parts.push(format!("{} ★{}", factor_name, max_level));
    }

    if summary_parts.is_empty() {
        "None".to_string()
    } else {
        summary_parts.join(" • ")
    }
}

fn getFactorName(factor_id: i32) -> String {
    match factor_id {
        // Blue factors (stats)
        1 => "Speed".to_string(),
        2 => "Stamina".to_string(),
        3 => "Power".to_string(),
        4 => "Guts".to_string(),
        5 => "Wit".to_string(),

        // Pink factors (aptitudes)
        10 => "Turf".to_string(),
        11 => "Dirt".to_string(),
        12 => "Sprint".to_string(),
        13 => "Mile".to_string(),
        14 => "Middle".to_string(),
        15 => "Long".to_string(),
        16 => "Front Runner".to_string(),
        17 => "Pace Chaser".to_string(),
        18 => "Late Surger".to_string(),
        19 => "End".to_string(),

        // Green factors (resistance)
        20 => "Summer".to_string(),
        21 => "Heavy".to_string(),

        // White factors (skills) - simplified
        _ if factor_id >= 30 => format!("Skill {}", factor_id - 29),

        _ => format!("Factor {}", factor_id),
    }
}
