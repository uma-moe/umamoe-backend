// Custom deserializer for Vec<String> that handles both single string and sequence
pub fn deserialize_vec_string_from_query<'de, D>(
    deserializer: D,
) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Visitor};
    use std::fmt;

    struct StringOrVec;

    impl<'de> Visitor<'de> for StringOrVec {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(vec![value.to_owned()])
        }

        fn visit_seq<S>(self, mut visitor: S) -> Result<Self::Value, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(elem) = visitor.next_element()? {
                vec.push(elem);
            }
            Ok(vec)
        }
    }

    deserializer.deserialize_any(StringOrVec)
}

// Custom deserializer for Vec<i32> that handles both single i32 and sequence
pub fn deserialize_vec_i32_from_query<'de, D>(
    deserializer: D,
) -> Result<Vec<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Visitor};
    use std::fmt;

    struct I32OrVec;

    impl<'de> Visitor<'de> for I32OrVec {
        type Value = Vec<i32>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("i32 or list of i32")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(vec![value as i32])
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(vec![value as i32])
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            // Support comma-separated values like "102201,100602,102302"
            if value.contains(',') {
                let result: Vec<i32> = value.split(',')
                    .filter(|s| !s.trim().is_empty())
                    .filter_map(|s| s.trim().parse::<i32>().ok())
                    .collect();
                if result.is_empty() {
                    Err(E::custom(format!("no valid i32 values in: {}", value)))
                } else {
                    Ok(result)
                }
            } else {
                value.parse::<i32>()
                    .map(|v| vec![v])
                    .map_err(|_| E::custom(format!("invalid i32 string: {}", value)))
            }
        }

        fn visit_seq<S>(self, mut visitor: S) -> Result<Self::Value, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(elem) = visitor.next_element::<serde_json::Value>()? {
                match elem {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            vec.push(i as i32);
                        }
                    }
                    serde_json::Value::String(s) => {
                        if let Ok(i) = s.parse::<i32>() {
                            vec.push(i);
                        }
                    }
                    _ => {}
                }
            }
            Ok(vec)
        }
    }

    deserializer.deserialize_any(I32OrVec)
}

// Custom serializer for NaiveDateTime that formats as UTC ISO 8601 with Z suffix
pub mod naive_datetime_as_utc {
    use chrono::NaiveDateTime;
    use serde::{self, Serializer};

    pub fn serialize<S>(date: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}Z", date.format("%Y-%m-%dT%H:%M:%S"));
        serializer.serialize_str(&s)
    }
}

// Custom serializer for Option<NaiveDateTime> that formats as UTC ISO 8601 with Z suffix
pub mod option_naive_datetime_as_utc {
    use chrono::NaiveDateTime;
    use serde::{self, Serializer};

    pub fn serialize<S>(date: &Option<NaiveDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(d) => {
                let s = format!("{}Z", d.format("%Y-%m-%dT%H:%M:%S"));
                serializer.serialize_str(&s)
            }
            None => serializer.serialize_none(),
        }
    }
}
