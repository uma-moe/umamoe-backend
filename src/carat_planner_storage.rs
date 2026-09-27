use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate};
use serde_json::{json, Map, Value};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

include!("types/carat_planner_storage.rs");

/// Rewrites every existing v1/v2 planner row to the same sparse v3 wire format
/// used by current clients. Each row is updated atomically and remains untouched
/// if conversion fails.
pub async fn backfillSparseV3(pool: &PgPool) -> Result<u64> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query_as::<_, LegacyStateRow>(
        r#"
        SELECT user_id, revision, collection
        FROM carat_planner_states
        WHERE NOT (collection @> '{"version": 3}'::jsonb)
        ORDER BY user_id
        FOR UPDATE
        "#,
    )
    .fetch_all(&mut *transaction)
    .await?;

    let mut migrated = 0_u64;
    for row in rows {
        let compact = compactCollectionV3(&row.collection)
            .with_context(|| format!("could not compact planner state for user {}", row.user_id))?;
        let result = sqlx::query(
            r#"
            UPDATE carat_planner_states
            SET collection = $1,
                revision = revision + 1
            WHERE user_id = $2
              AND revision = $3
              AND NOT (collection @> '{"version": 3}'::jsonb)
            "#,
        )
        .bind(compact)
        .bind(row.user_id)
        .bind(row.revision)
        .execute(&mut *transaction)
        .await?;
        migrated += result.rows_affected();
    }

    sqlx::query(
        "ALTER TABLE carat_planner_states VALIDATE CONSTRAINT carat_planner_states_sparse_v3",
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(migrated)
}

fn compactCollectionV3(collection: &Value) -> Result<Value> {
    let object = collection
        .as_object()
        .ok_or_else(|| anyhow!("planner collection is not an object"))?;
    match object.get("version").and_then(Value::as_i64) {
        Some(3) => return Ok(collection.clone()),
        Some(1) | Some(2) => {}
        version => {
            return Err(anyhow!(
                "unsupported planner collection version {version:?}"
            ))
        }
    }

    let source_plans = object
        .get("plans")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("planner collection has no plans"))?;
    if source_plans.is_empty() {
        return Err(anyhow!("planner collection has no plans"));
    }
    let version = object.get("version").and_then(Value::as_i64).unwrap_or(0);
    let plans: Vec<Value> = source_plans
        .iter()
        .enumerate()
        .map(|(index, plan)| {
            if version == 1 {
                compactV1Plan(plan)
            } else {
                compactV2Plan(plan)
            }
            .with_context(|| format!("invalid plan at index {index}"))
        })
        .collect::<Result<_>>()?;
    let requested_active = object
        .get("activePlanId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let active_plan_id = plans
        .iter()
        .find_map(|plan| (arrayString(plan, 0) == Some(requested_active)).then(|| requested_active))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| arrayString(&plans[0], 0).unwrap_or_default());

    Ok(json!({
        "version": 3,
        "activePlanId": active_plan_id,
        "plans": plans,
    }))
}

fn compactV1Plan(value: &Value) -> Result<Value> {
    let plan = value
        .as_object()
        .ok_or_else(|| anyhow!("v1 plan is not an object"))?;
    let id = requiredString(plan.get("id"), "plan id")?;
    let name = requiredString(plan.get("name"), "plan name")?;
    let targets = plan
        .get("targets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let target_events: HashSet<String> = targets
        .iter()
        .filter_map(|target| {
            target
                .get("eventId")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect();
    let disabled_events = stringArray(plan.get("disabledEventIds"));
    let disabled_set: HashSet<&str> = disabled_events.iter().map(String::as_str).collect();

    let mut output = vec![
        json!(id),
        json!(name),
        instantCode(plan.get("createdAt")),
        instantCode(plan.get("updatedAt")),
        dayCode(plan.get("projectionStartDate")),
        compactBalancesFromObject(plan.get("balances")),
        compactTokenArray(plan.get("enabledIncomeRuleIds")),
        json!(stringArray(plan.get("enabledRewardIds"))),
        json!(stringArray(plan.get("disabledRewardIds"))),
        json!(disabled_events
            .iter()
            .filter(|event_id| !target_events.contains(event_id.as_str()))
            .cloned()
            .collect::<Vec<_>>()),
        compactScenariosFromObject(plan.get("scenarioSelections")),
        compactVariableRewardsFromObject(plan.get("variableRewardSelections")),
        compactFreePullsFromObject(plan.get("freePullCampaignSelections")),
        compactCustomIncomeFromObjects(plan.get("customIncome")),
        Value::Array(
            targets
                .iter()
                .map(|target| compactTargetObject(target, &disabled_set))
                .collect::<Result<_>>()?,
        ),
    ];
    let preset = compactPresetState(plan);
    if preset > 0 {
        output.push(json!(preset));
    }
    Ok(Value::Array(output))
}

fn compactV2Plan(value: &Value) -> Result<Value> {
    let outer = value
        .as_array()
        .filter(|values| values.len() >= 6)
        .ok_or_else(|| anyhow!("v2 plan tuple is invalid"))?;
    let embedded = outer[5]
        .as_array()
        .filter(|values| values.len() >= 15 && values[0].as_i64() == Some(2))
        .ok_or_else(|| anyhow!("v2 compact plan payload is invalid"))?;
    let id = requiredString(outer.first(), "plan id")?;
    let name = requiredString(embedded.get(1), "plan name")?;
    let targets = embedded[14].as_array().cloned().unwrap_or_default();
    let disabled_events = stringArray(embedded.get(8));
    let disabled_set: HashSet<&str> = disabled_events.iter().map(String::as_str).collect();
    let target_events: HashSet<String> = targets
        .iter()
        .filter_map(|target| arrayString(target, 0).map(str::to_string))
        .collect();

    let mut output = vec![
        json!(id),
        json!(name),
        instantCode(outer.get(1)),
        instantCode(outer.get(2)),
        dayCode(embedded.get(2)),
        trimNumericArray(embedded.get(3)),
        compactTokenArray(embedded.get(4)),
        json!(stringArray(embedded.get(5))),
        json!(stringArray(embedded.get(6))),
        json!(disabled_events
            .iter()
            .filter(|event_id| !target_events.contains(event_id.as_str()))
            .cloned()
            .collect::<Vec<_>>()),
        compactScenariosFromPairs(embedded.get(9)),
        compactVariableRewardsFromV2(embedded.get(10)),
        compactFreePullsFromPairs(embedded.get(11)),
        compactCustomIncomeFromV2(embedded.get(13)),
        Value::Array(
            targets
                .iter()
                .map(|target| compactTargetV2(target, &disabled_set))
                .collect::<Result<_>>()?,
        ),
    ];
    let preset = embedded.get(15).and_then(Value::as_i64).unwrap_or(0).max(0);
    if preset > 0 {
        output.push(json!(preset));
    }
    Ok(Value::Array(output))
}

fn compactBalancesFromObject(value: Option<&Value>) -> Value {
    let object = value.and_then(Value::as_object);
    trimValues(vec![
        objectNumber(object, "freeJewels"),
        objectNumber(object, "paidJewels"),
        objectNumber(object, "umaTickets"),
        objectNumber(object, "supportTickets"),
        objectNumber(object, "rainbowCrystals"),
        objectNumber(object, "goldCrystals"),
        objectNumber(object, "rainbowFullCrystals"),
        objectNumber(object, "goldFullCrystals"),
    ])
}

fn compactScenariosFromObject(value: Option<&Value>) -> Value {
    let Some(object) = value.and_then(Value::as_object) else {
        return json!([]);
    };
    compactScenarioMap(
        object
            .iter()
            .filter_map(|(group, option)| {
                option
                    .as_str()
                    .map(|option| (group.clone(), option.to_string()))
            })
            .collect(),
    )
}

fn compactScenariosFromPairs(value: Option<&Value>) -> Value {
    compactScenarioMap(
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|pair| {
                let group = arrayString(pair, 0)?;
                let option = arrayString(pair, 1)?;
                Some((group.to_string(), option.to_string()))
            })
            .collect(),
    )
}

fn compactScenarioMap(mut selections: BTreeMap<String, String>) -> Value {
    // Older clients exposed the three seasonal gifts as one switch. Current
    // clients persist them independently, while preserving any explicit newer
    // choice over the legacy fallback.
    if let Some(selection) = selections.remove("seasonal_gift_rewards") {
        for group in [
            "valentines_gift_rewards",
            "white_day_gift_rewards",
            "christmas_gift_rewards",
        ] {
            selections
                .entry(group.to_string())
                .or_insert_with(|| selection.clone());
        }
    }
    if selections
        .get("speculative_income")
        .is_some_and(String::is_empty)
    {
        selections.insert("speculative_income".into(), "none".into());
    }

    Value::Array(
        selections
            .into_iter()
            .filter_map(|(group, option)| {
                (defaultScenario(&group) != Some(option.as_str()))
                    .then(|| Value::Array(vec![tokenCode(&group), tokenCode(&option)]))
            })
            .collect(),
    )
}

fn compactVariableRewardsFromObject(value: Option<&Value>) -> Value {
    let Some(object) = value.and_then(Value::as_object) else {
        return json!([]);
    };
    Value::Array(
        object
            .iter()
            .filter_map(|(event_id, selection)| {
                let selection = selection.as_object()?;
                let option_id = selection.get("optionId")?.as_str()?;
                let amounts = selection
                    .get("amounts")
                    .and_then(Value::as_object)
                    .map(|amounts| {
                        amounts
                            .iter()
                            .filter_map(|(currency, amount)| {
                                codeOf(CURRENCIES, currency)
                                    .map(|code| Value::Array(vec![json!(code), numeric(amount)]))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                Some(Value::Array(vec![
                    json!(event_id),
                    tokenCode(option_id),
                    dayCode(selection.get("availableAt")),
                    Value::Array(amounts),
                ]))
            })
            .collect(),
    )
}

fn compactVariableRewardsFromV2(value: Option<&Value>) -> Value {
    Value::Array(
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|selection| {
                let values = selection.as_array()?;
                let event_id = values.first()?.as_str()?;
                let option_id = values.get(1)?.as_str()?;
                let amounts = values
                    .get(4)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                Some(Value::Array(vec![
                    json!(event_id),
                    tokenCode(option_id),
                    dayCode(values.get(3)),
                    Value::Array(amounts),
                ]))
            })
            .collect(),
    )
}

fn compactFreePullsFromObject(value: Option<&Value>) -> Value {
    let Some(object) = value.and_then(Value::as_object) else {
        return json!([]);
    };
    Value::Array(
        object
            .iter()
            .filter_map(|(campaign_id, event_id)| {
                event_id
                    .as_str()
                    .map(|event_id| Value::Array(vec![json!(campaign_id), tokenCode(event_id)]))
            })
            .collect(),
    )
}

fn compactFreePullsFromPairs(value: Option<&Value>) -> Value {
    Value::Array(
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|pair| {
                Some(Value::Array(vec![
                    json!(arrayString(pair, 0)?),
                    tokenCode(arrayString(pair, 1)?),
                ]))
            })
            .collect(),
    )
}

fn compactCustomIncomeFromObjects(value: Option<&Value>) -> Value {
    Value::Array(
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|income| {
                let object = income.as_object()?;
                Some(Value::Array(vec![
                    json!(object.get("label")?.as_str()?),
                    json!(codeOf(CURRENCIES, object.get("currency")?.as_str()?).unwrap_or(0)),
                    numeric(object.get("amount")?),
                    json!(codeOf(CADENCES, object.get("cadence")?.as_str()?).unwrap_or(0)),
                    dayCode(object.get("startDate")),
                    object
                        .get("endDate")
                        .filter(|date| !date.is_null())
                        .map(|date| dayCode(Some(date)))
                        .unwrap_or(Value::Null),
                    json!(object.get("every").map(integer).unwrap_or(0).max(0)),
                ]))
            })
            .collect(),
    )
}

fn compactCustomIncomeFromV2(value: Option<&Value>) -> Value {
    Value::Array(
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|income| {
                let values = income.as_array()?;
                Some(Value::Array(vec![
                    values.first()?.clone(),
                    values.get(1).cloned().unwrap_or_else(|| json!(0)),
                    values.get(2).cloned().unwrap_or_else(|| json!(0)),
                    values.get(3).cloned().unwrap_or_else(|| json!(0)),
                    dayCode(values.get(4)),
                    values
                        .get(5)
                        .filter(|date| !date.is_null())
                        .map(|date| dayCode(Some(date)))
                        .unwrap_or(Value::Null),
                    json!(values.get(6).map(integer).unwrap_or(0).max(0)),
                ]))
            })
            .collect(),
    )
}

fn compactTargetObject(value: &Value, disabled_events: &HashSet<&str>) -> Result<Value> {
    let target = value
        .as_object()
        .ok_or_else(|| anyhow!("target is not an object"))?;
    let event_id = requiredString(target.get("eventId"), "target event id")?;
    let timing = target
        .get("pullTiming")
        .and_then(Value::as_str)
        .unwrap_or("end");
    let mut flags = match timing {
        "start" => 1,
        "custom" => 2,
        _ => 0,
    };
    if !target
        .get("useTickets")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        flags |= 1 << 2;
    }
    if target
        .get("allowPaidJewels")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        flags |= 1 << 3;
    }
    if disabled_events.contains(event_id) {
        flags |= 1 << 4;
    }
    let gacha_ids = target
        .get("gachaIds")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let custom_date = target
        .get("customPullDate")
        .filter(|value| !value.is_null());
    let ticket_limit = target.get("ticketLimit").filter(|value| !value.is_null());
    let rainbow = target
        .get("rainbowCrystalsPlanned")
        .map(integer)
        .unwrap_or(0);
    let gold = target.get("goldCrystalsPlanned").map(integer).unwrap_or(0);
    if !gacha_ids.is_empty() {
        flags |= 1 << 5;
    }
    if custom_date.is_some() {
        flags |= 1 << 6;
    }
    if ticket_limit.is_some() {
        flags |= 1 << 7;
    }
    if rainbow > 0 {
        flags |= 1 << 8;
    }
    if gold > 0 {
        flags |= 1 << 9;
    }
    let mut output = vec![
        json!(event_id),
        json!(target.get("gachaId").map(integer).unwrap_or(0).max(0)),
        json!(codeOf(
            BANNER_KINDS,
            target
                .get("bannerKind")
                .and_then(Value::as_str)
                .unwrap_or("other")
        )
        .unwrap_or(0)),
        json!(target.get("plannedPulls").map(integer).unwrap_or(200) - 200),
        compactGoals(target.get("pickupGoals")),
        json!(flags),
    ];
    appendTargetOptionals(
        &mut output,
        flags,
        Value::Array(gacha_ids),
        custom_date,
        ticket_limit,
        rainbow,
        gold,
    );
    Ok(Value::Array(output))
}

fn compactTargetV2(value: &Value, disabled_events: &HashSet<&str>) -> Result<Value> {
    let target = value
        .as_array()
        .filter(|values| values.len() >= 15)
        .ok_or_else(|| anyhow!("v2 target tuple is invalid"))?;
    let event_id = requiredString(target.first(), "target event id")?;
    let mut flags = match target.get(6).and_then(Value::as_i64).unwrap_or(1) {
        0 => 1,
        2 => 2,
        _ => 0,
    };
    if target.get(12).and_then(Value::as_i64) != Some(1) {
        flags |= 1 << 2;
    }
    if target.get(14).and_then(Value::as_i64) == Some(1) {
        flags |= 1 << 3;
    }
    if disabled_events.contains(event_id) {
        flags |= 1 << 4;
    }
    let gacha_ids = target
        .get(2)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let custom_date = target.get(7).filter(|value| !value.is_null());
    let ticket_limit = target.get(13).filter(|value| !value.is_null());
    let rainbow = target.get(15).map(integer).unwrap_or(0);
    let gold = target.get(16).map(integer).unwrap_or(0);
    if !gacha_ids.is_empty() {
        flags |= 1 << 5;
    }
    if custom_date.is_some() {
        flags |= 1 << 6;
    }
    if ticket_limit.is_some() {
        flags |= 1 << 7;
    }
    if rainbow > 0 {
        flags |= 1 << 8;
    }
    if gold > 0 {
        flags |= 1 << 9;
    }
    let mut output = vec![
        json!(event_id),
        json!(target.get(1).map(integer).unwrap_or(0).max(0)),
        json!(target.get(4).map(integer).unwrap_or(3).max(0)),
        json!(target.get(8).map(integer).unwrap_or(200) - 200),
        compactGoals(target.get(11)),
        json!(flags),
    ];
    appendTargetOptionals(
        &mut output,
        flags,
        Value::Array(gacha_ids),
        custom_date,
        ticket_limit,
        rainbow,
        gold,
    );
    Ok(Value::Array(output))
}

fn appendTargetOptionals(
    output: &mut Vec<Value>,
    flags: i64,
    gacha_ids: Value,
    custom_date: Option<&Value>,
    ticket_limit: Option<&Value>,
    rainbow: i64,
    gold: i64,
) {
    if flags & (1 << 5) != 0 {
        output.push(gacha_ids);
    }
    if flags & (1 << 6) != 0 {
        output.push(dayCode(custom_date));
    }
    if flags & (1 << 7) != 0 {
        output.push(json!(ticket_limit.map(integer).unwrap_or(0).max(0)));
    }
    if flags & (1 << 8) != 0 {
        output.push(json!(rainbow.max(0)));
    }
    if flags & (1 << 9) != 0 {
        output.push(json!(gold.max(0)));
    }
}

fn compactGoals(value: Option<&Value>) -> Value {
    let goals: Vec<Value> = value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|goal| {
            if let Some(values) = goal.as_array() {
                return Some(Value::Array(vec![
                    json!(values.first().map(integer).unwrap_or(0).max(0)),
                    json!(values.get(1).map(integer).unwrap_or(1).max(1)),
                ]));
            }
            let object = goal.as_object()?;
            Some(Value::Array(vec![
                json!(object.get("pickupId").map(integer).unwrap_or(0).max(0)),
                json!(object.get("desiredCopies").map(integer).unwrap_or(1).max(1)),
            ]))
        })
        .collect();
    match goals.len() {
        0 => json!(0),
        1 => goals.into_iter().next().unwrap_or_else(|| json!(0)),
        _ => Value::Array(goals),
    }
}

fn compactPresetState(plan: &Map<String, Value>) -> i64 {
    let Some(id) = plan.get("incomePresetId").and_then(Value::as_str) else {
        return 0;
    };
    let Some(index) = PRESET_IDS.iter().position(|value| *value == id) else {
        return 0;
    };
    (index as i64 + 1)
        | if plan
            .get("incomePresetEdited")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            1 << 3
        } else {
            0
        }
}

fn compactTokenArray(value: Option<&Value>) -> Value {
    Value::Array(
        value
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(tokenCode)
            .collect(),
    )
}

fn tokenCode(value: &str) -> Value {
    TOKENS
        .iter()
        .position(|token| *token == value)
        .map(|index| json!(index))
        .unwrap_or_else(|| json!(value))
}

fn defaultScenario(group: &str) -> Option<&'static str> {
    match group {
        "speculative_income"
        | "temporary_story_rewards"
        | "story_event_rewards"
        | "factor_research_rewards"
        | "trainer_skills_test_rewards"
        | "racing_carnival_rewards"
        | "racing_carnival_mission"
        | "scenario_evaluation_rewards"
        | "main_story_rewards"
        | "limited_login_rewards"
        | "login_milestone_rewards"
        | "valentines_gift_rewards"
        | "white_day_gift_rewards"
        | "christmas_gift_rewards"
        | "limited_mission_rewards" => Some("include"),
        "masters_challenge_rewards" => Some("none"),
        _ => None,
    }
}

fn trimNumericArray(value: Option<&Value>) -> Value {
    let values = value.and_then(Value::as_array).cloned().unwrap_or_default();
    trimValues(values)
}

fn trimValues(mut values: Vec<Value>) -> Value {
    while values.last().map(integer).unwrap_or(1) == 0 {
        values.pop();
    }
    Value::Array(values)
}

fn instantCode(value: Option<&Value>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    if value.is_number() {
        return value.clone();
    }
    value
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|date| json!(date.timestamp_millis()))
        .unwrap_or_else(|| value.clone())
}

fn dayCode(value: Option<&Value>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    if value.is_number() {
        return value.clone();
    }
    let epoch = NaiveDate::from_ymd_opt(2020, 1, 1).expect("valid planner epoch");
    value
        .as_str()
        .and_then(|text| NaiveDate::parse_from_str(text, "%Y-%m-%d").ok())
        .map(|date| json!(date.signed_duration_since(epoch).num_days()))
        .unwrap_or_else(|| value.clone())
}

fn stringArray(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn requiredString<'a>(value: Option<&'a Value>, label: &str) -> Result<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{label} is missing"))
}

fn arrayString(value: &Value, index: usize) -> Option<&str> {
    value.as_array()?.get(index)?.as_str()
}

fn objectNumber(object: Option<&Map<String, Value>>, key: &str) -> Value {
    object
        .and_then(|object| object.get(key))
        .map(numeric)
        .unwrap_or_else(|| json!(0))
}

fn numeric(value: &Value) -> Value {
    value
        .as_i64()
        .map(|number| json!(number))
        .or_else(|| value.as_u64().map(|number| json!(number)))
        .or_else(|| value.as_f64().map(|number| json!(number)))
        .unwrap_or_else(|| json!(0))
}

fn integer(value: &Value) -> i64 {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| value.as_f64().map(|number| number.trunc() as i64))
        .unwrap_or(0)
}

fn codeOf(values: &[&str], value: &str) -> Option<usize> {
    values.iter().position(|candidate| *candidate == value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convertsV2AccountStateToSparseV3() {
        let source = json!({
            "version": 2,
            "activePlanId": "plan-1",
            "plans": [[
                "plan-1",
                "2026-08-17T17:57:02.714Z",
                "2026-08-19T13:41:59.652Z",
                ["old-income-row"],
                ["old-target-row"],
                [
                    2,
                    "My plan",
                    "2026-08-17",
                    [100, 0, 2, 0, 0, 0, 0, 0],
                    ["daily-missions", "club-rank-2"],
                    ["reward-opt-in"],
                    ["reward-opt-out"],
                    [],
                    ["event-disabled", "support-1"],
                    [["masters_challenge_rewards", "clear_2"]],
                    [["event-1", "rank_2", "Rank 2", "2026-08-20", [[0, 900]]]],
                    [["campaign-1", "__excluded__"]],
                    1,
                    [["One-off", 0, 100, 0, "2026-08-21", null, null]],
                    [[
                        "support-1", 30123, [30123, 30124], "Support", 1, null,
                        2, "2026-08-25", 400, 5, 30123, [[30123, 5]], 0, 7, 1, 4, 3
                    ]],
                    11
                ]
            ]]
        });

        let compact = compactCollectionV3(&source).unwrap();
        let plan = compact["plans"][0].as_array().unwrap();
        assert_eq!(compact["version"], 3);
        assert_eq!(compact["activePlanId"], "plan-1");
        assert_eq!(plan[0], "plan-1");
        assert_eq!(plan[1], "My plan");
        assert_eq!(plan[5], json!([100, 0, 2]));
        assert_eq!(plan[6], json!([0, "club-rank-2"]));
        assert_eq!(plan[9], json!(["event-disabled"]));
        assert_eq!(plan[10], json!([[6, 40]]));
        assert_eq!(plan[15], 11);
        let target = plan[14][0].as_array().unwrap();
        assert_eq!(target[0], "support-1");
        assert_eq!(target[3], 200);
        assert_eq!(target[4], json!([30123, 5]));
        assert_eq!(target[5].as_i64().unwrap() & (1 << 4), 1 << 4);
    }

    #[test]
    fn convertsFullV1StateWithoutLosingUserChoices() {
        let source = json!({
            "version": 1,
            "activePlanId": "plan-1",
            "plans": [{
                "id": "plan-1",
                "name": "Legacy",
                "createdAt": "2026-08-17T00:00:00.000Z",
                "updatedAt": "2026-08-18T00:00:00.000Z",
                "projectionStartDate": "2026-08-19",
                "balances": { "freeJewels": 500, "supportTickets": 2 },
                "enabledIncomeRuleIds": ["daily-missions"],
                "enabledRewardIds": ["manual-reward"],
                "disabledRewardIds": ["excluded-reward"],
                "disabledEventIds": ["support-1"],
                "scenarioSelections": {
                    "speculative_income": "median",
                    "masters_challenge_rewards": "none"
                },
                "variableRewardSelections": {},
                "freePullCampaignSelections": {},
                "incomePresetId": "active",
                "incomePresetEdited": true,
                "customIncome": [],
                "targets": [{
                    "eventId": "support-1",
                    "gachaId": 30123,
                    "bannerKind": "support",
                    "pullTiming": "end",
                    "plannedPulls": 200,
                    "pickupGoals": [{ "pickupId": 30123, "desiredCopies": 5 }],
                    "useTickets": true,
                    "allowPaidJewels": false
                }]
            }]
        });

        let compact = compactCollectionV3(&source).unwrap();
        let plan = compact["plans"][0].as_array().unwrap();
        assert_eq!(plan[5], json!([500, 0, 0, 2]));
        assert_eq!(plan[9], json!([]));
        assert_eq!(plan[10], json!([[3, 27]]));
        assert_eq!(plan[15], 11);
        assert_eq!(plan[14][0][5].as_i64().unwrap() & (1 << 4), 1 << 4);
    }

    #[test]
    fn migratesLegacyScenarioAliasesWithoutOverwritingNewChoices() {
        let compact = compactScenariosFromObject(Some(&json!({
            "seasonal_gift_rewards": "none",
            "valentines_gift_rewards": "include",
            "speculative_income": ""
        })));
        let selections = compact.as_array().unwrap();

        assert!(!selections
            .iter()
            .any(|pair| pair[0] == tokenCode("seasonal_gift_rewards")));
        assert!(!selections
            .iter()
            .any(|pair| pair[0] == tokenCode("valentines_gift_rewards")));
        assert!(selections.iter().any(|pair| {
            pair == &json!([tokenCode("white_day_gift_rewards"), tokenCode("none")])
        }));
        assert!(selections.iter().any(|pair| {
            pair == &json!([tokenCode("christmas_gift_rewards"), tokenCode("none")])
        }));
        assert!(selections
            .iter()
            .any(|pair| { pair == &json!([tokenCode("speculative_income"), tokenCode("none")]) }));
    }

    #[test]
    fn acceptsV2TargetsFromBeforeCrystalPlanningFields() {
        let target = json!([
            "support-1",
            30123,
            [],
            "Support",
            1,
            null,
            1,
            null,
            200,
            5,
            30123,
            [[30123, 5]],
            1,
            null,
            0
        ]);

        let compact = compactTargetV2(&target, &HashSet::new()).unwrap();
        assert_eq!(compact[0], "support-1");
        assert_eq!(compact[5], 0);
    }
}
