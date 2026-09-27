#![allow(non_snake_case)]

use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Instant,
};

include!("../types/bin/analyze_circle_rank_threshold_history.rs");

impl Default for CircleData {
    fn default() -> Self {
        Self {
            first_day: u8::MAX,
            points: [0; 31],
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args_os().skip(1);
    let input = PathBuf::from(
        args.next()
            .ok_or("usage: analyzer INPUT.tsv.gz OUTPUT.csv")?,
    );
    let output = PathBuf::from(
        args.next()
            .ok_or("usage: analyzer INPUT.tsv.gz OUTPUT.csv")?,
    );
    if args.next().is_some() {
        return Err("usage: analyzer INPUT.tsv.gz OUTPUT.csv".into());
    }

    let started = Instant::now();
    let (mut reader, mut decompressor) = openReader(&input)?;
    let mut header = String::new();
    reader.read_line(&mut header)?;
    if header.trim_end() != "circle_id\tviewer_id\tyear\tmonth\tdaily_fans\tlast_updated" {
        return Err(format!("unexpected header: {}", header.trim_end()).into());
    }

    let mut months: HashMap<Month, MonthData> = HashMap::new();
    let mut viewers: HashMap<ViewerMonth, ViewerMonthData> = HashMap::new();
    let mut line = Vec::with_capacity(512);
    let mut fans = [0_i64; 32];
    let mut rows = 0_u64;

    while reader.read_until(b'\n', &mut line)? != 0 {
        rows += 1;
        analyzeRow(&line, &mut fans, &mut months, &mut viewers)
            .map_err(|error| format!("row {}: {error}", rows + 1))?;
        line.clear();

        if rows % 1_000_000 == 0 {
            eprintln!(
                "read {rows} rows ({:.0} rows/s)",
                rows as f64 / started.elapsed().as_secs_f64()
            );
        }
    }
    drop(reader);
    if let Some(child) = decompressor.as_mut() {
        let status = child.wait()?;
        if !status.success() {
            return Err(format!("gzip exited with {status}").into());
        }
    }

    eprintln!("applying month-end rollover corrections");
    let corrections: Vec<_> = viewers
        .iter()
        .filter_map(|(key, data)| {
            let end = data.end_membership?;
            let next = viewers.get(&ViewerMonth {
                viewer_id: key.viewer_id,
                month: nextMonth(key.month),
            })?;
            let delta = next.month_start_fans - end.last_fans;
            (delta > 0).then_some((key.month, end.circle_id, delta))
        })
        .collect();

    for (month, circle_id, delta) in corrections {
        let day = daysInMonth(month) as usize - 1;
        if let Some(month_data) = months.get_mut(&month) {
            month_data.horizon = daysInMonth(month);
            month_data.circles.entry(circle_id).or_default().points[day] += delta;
        }
    }

    writeThresholds(&output, &months)?;
    eprintln!(
        "done: {rows} rows in {:.1} minutes -> {}",
        started.elapsed().as_secs_f64() / 60.0,
        output.display()
    );
    Ok(())
}

fn analyzeRow(
    line: &[u8],
    fans: &mut [i64; 32],
    months: &mut HashMap<Month, MonthData>,
    viewers: &mut HashMap<ViewerMonth, ViewerMonthData>,
) -> Result<(), &'static str> {
    let mut fields = line.split(|byte| *byte == b'\t');
    let circle_id = parseI64(fields.next().ok_or("missing circle_id")?)?;
    let viewer_id = parseI64(fields.next().ok_or("missing viewer_id")?)?;
    let year = parseI64(fields.next().ok_or("missing year")?)? as i32;
    let month_number = parseI64(fields.next().ok_or("missing month")?)? as u8;
    let fan_count = parseFans(fields.next().ok_or("missing daily_fans")?, fans)?;
    let month = Month {
        year,
        month: month_number,
    };
    let valid_days = daysInMonth(month) as usize;

    let mut baseline = 0;
    let mut current = 0;
    let mut first_day = 0;
    let mut last_day = 0;
    let month_data = months.entry(month).or_default();
    let circle = month_data.circles.entry(circle_id).or_default();

    for (index, fan_count) in fans[..fan_count.min(valid_days)]
        .iter()
        .copied()
        .enumerate()
    {
        if fan_count > 0 {
            if baseline == 0 {
                baseline = fan_count;
                first_day = index as u8 + 1;
                circle.first_day = circle.first_day.min(first_day);
            }
            current = current.max(fan_count);
            last_day = index as u8 + 1;
            month_data.horizon = month_data.horizon.max(last_day);
        }
        if current > 0 {
            circle.points[index] += (current - baseline).max(0);
        }
    }

    if baseline > 0 {
        let viewer = viewers.entry(ViewerMonth { viewer_id, month }).or_default();
        if fans[0] > 0 {
            viewer.month_start_fans = viewer.month_start_fans.max(fans[0]);
        }
        let candidate = EndMembership {
            circle_id,
            last_fans: current,
            first_day,
            last_day,
        };
        if viewer.end_membership.is_none_or(|existing| {
            (candidate.last_day, candidate.first_day, candidate.circle_id)
                > (existing.last_day, existing.first_day, existing.circle_id)
        }) {
            viewer.end_membership = Some(candidate);
        }
    }

    Ok(())
}

fn writeThresholds(path: &Path, months: &HashMap<Month, MonthData>) -> io::Result<()> {
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(
        output,
        "recorded_on,month_start,day_of_month,tier,rank_index,boundary_rank,required_fans,outside_required_fans,required_fans_per_day,required_fans_per_week,required_fans_delta_day,required_fans_delta_7d,circles_observed,is_forecast"
    )?;

    let mut month_keys: Vec<_> = months.keys().copied().collect();
    month_keys.sort_unstable();
    let latest_month = month_keys.last().copied();

    for month in month_keys {
        let data = &months[&month];
        let horizon = completedHorizon(
            data.horizon.min(daysInMonth(month)) as usize,
            Some(month) == latest_month,
        );
        let mut history = vec![vec![None; horizon]; TIERS.len()];

        for day in 1..=horizon {
            let mut values: Vec<_> = data
                .circles
                .values()
                .filter(|circle| circle.first_day as usize <= day)
                .map(|circle| circle.points[day - 1])
                .collect();
            values.sort_unstable_by(|left, right| right.cmp(left));

            for (tier_index, (_, _, boundary)) in TIERS.iter().enumerate() {
                if values.len() >= *boundary {
                    history[tier_index][day - 1] = Some(values[*boundary - 1]);
                }
            }

            for (tier_index, (tier, rank_index, boundary)) in TIERS.iter().enumerate() {
                let Some(required) = history[tier_index][day - 1] else {
                    continue;
                };
                let previous_day = day
                    .checked_sub(2)
                    .and_then(|index| history[tier_index][index]);
                let previous_week = day
                    .checked_sub(8)
                    .and_then(|index| history[tier_index][index]);
                writeRow(
                    &mut output,
                    month,
                    day,
                    tier,
                    *rank_index,
                    *boundary,
                    required,
                    values.get(*boundary).copied(),
                    previous_day.map(|value| required - value),
                    previous_week.map(|value| required - value),
                    values.len(),
                    false,
                )?;
            }
        }

        if Some(month) == latest_month && horizon < daysInMonth(month) as usize {
            for future_day in horizon + 1..=daysInMonth(month) as usize {
                for (tier_index, (tier, rank_index, boundary)) in TIERS.iter().enumerate() {
                    let observed: Vec<_> = history[tier_index]
                        .iter()
                        .enumerate()
                        .filter_map(|(day, value)| value.map(|fans| (day + 1, fans)))
                        .collect();
                    let Some(&(latest_day, latest_fans)) = observed.last() else {
                        continue;
                    };
                    let sample = &observed[observed.len().saturating_sub(14)..];
                    let slope = regressionSlope(sample)
                        .unwrap_or(latest_fans as f64 / latest_day as f64)
                        .max(0.0);
                    let predicted = (latest_fans as f64 + slope * (future_day - latest_day) as f64)
                        .round() as i64;
                    writeRow(
                        &mut output,
                        month,
                        future_day,
                        tier,
                        *rank_index,
                        *boundary,
                        predicted,
                        None,
                        None,
                        None,
                        data.circles.len(),
                        true,
                    )?;
                }
            }
        }
    }
    output.flush()
}

#[allow(clippy::too_many_arguments)]
fn writeRow(
    output: &mut impl Write,
    month: Month,
    day: usize,
    tier: &str,
    rank_index: u8,
    boundary: usize,
    required: i64,
    outside_required: Option<i64>,
    delta_day: Option<i64>,
    delta_week: Option<i64>,
    circles: usize,
    forecast: bool,
) -> io::Result<()> {
    let daily = divCeil(required, day as i64);
    let weekly = divCeil(required.saturating_mul(7), day as i64);
    writeln!(
        output,
        "{:04}-{:02}-{:02},{:04}-{:02}-01,{day},{tier},{rank_index},{boundary},{required},{},{daily},{weekly},{},{},{circles},{forecast}",
        month.year,
        month.month,
        day,
        month.year,
        month.month,
        optionalNumber(outside_required),
        optionalNumber(delta_day),
        optionalNumber(delta_week),
    )
}

fn regressionSlope(values: &[(usize, i64)]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let count = values.len() as f64;
    let mean_x = values.iter().map(|(x, _)| *x as f64).sum::<f64>() / count;
    let mean_y = values.iter().map(|(_, y)| *y as f64).sum::<f64>() / count;
    let numerator = values
        .iter()
        .map(|(x, y)| (*x as f64 - mean_x) * (*y as f64 - mean_y))
        .sum::<f64>();
    let denominator = values
        .iter()
        .map(|(x, _)| (*x as f64 - mean_x).powi(2))
        .sum::<f64>();
    (denominator > 0.0).then_some(numerator / denominator)
}

fn openReader(path: &Path) -> io::Result<(Box<dyn BufRead>, Option<Child>)> {
    if path.extension().is_some_and(|extension| extension == "gz") {
        let gzip = env::var_os("GZIP").unwrap_or_else(|| {
            let git_gzip = Path::new(r"C:\Program Files\Git\usr\bin\gzip.exe");
            if git_gzip.exists() {
                git_gzip.as_os_str().to_owned()
            } else {
                "gzip".into()
            }
        });
        let mut child = Command::new(gzip)
            .arg("-dc")
            .arg(path)
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("missing gzip stdout"))?;
        Ok((
            Box::new(BufReader::with_capacity(1024 * 1024, stdout)),
            Some(child),
        ))
    } else {
        Ok((
            Box::new(BufReader::with_capacity(1024 * 1024, File::open(path)?)),
            None,
        ))
    }
}

fn parseI64(bytes: &[u8]) -> Result<i64, &'static str> {
    let mut value = 0_i64;
    let mut negative = false;
    let mut found = false;
    for byte in bytes {
        match byte {
            b'-' if !found => negative = true,
            b'0'..=b'9' => {
                found = true;
                value = value
                    .checked_mul(10)
                    .and_then(|value| value.checked_add((byte - b'0') as i64))
                    .ok_or("integer overflow")?;
            }
            _ => break,
        }
    }
    if !found {
        return Err("invalid integer");
    }
    Ok(if negative { -value } else { value })
}

fn parseFans(bytes: &[u8], output: &mut [i64; 32]) -> Result<usize, &'static str> {
    output.fill(0);
    let mut count = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'-' || bytes[index].is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
            if count < output.len() {
                output[count] = parseI64(&bytes[start..index])?;
            }
            count += 1;
        } else {
            index += 1;
        }
    }
    Ok(count.min(output.len()))
}

fn daysInMonth(month: Month) -> u8 {
    match month.month {
        4 | 6 | 9 | 11 => 30,
        2 if month.year % 400 == 0 || (month.year % 4 == 0 && month.year % 100 != 0) => 29,
        2 => 28,
        _ => 31,
    }
}

fn nextMonth(month: Month) -> Month {
    if month.month == 12 {
        Month {
            year: month.year + 1,
            month: 1,
        }
    } else {
        Month {
            year: month.year,
            month: month.month + 1,
        }
    }
}

fn divCeil(value: i64, divisor: i64) -> i64 {
    if value <= 0 {
        0
    } else {
        value.saturating_add(divisor - 1) / divisor
    }
}

fn completedHorizon(horizon: usize, latest_month: bool) -> usize {
    if latest_month {
        horizon.saturating_sub(1)
    } else {
        horizon
    }
}

fn optionalNumber(value: Option<i64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsesExportedArrayAndCalendarEdges() {
        let mut fans = [0; 32];
        assert_eq!(parseFans(b"[0,12,34]", &mut fans), Ok(3));
        assert_eq!(&fans[..3], &[0, 12, 34]);
        assert_eq!(
            parseFans(
                b"[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33]",
                &mut fans
            ),
            Ok(32)
        );
        assert_eq!(fans[31], 32);
        assert_eq!(completedHorizon(3, true), 2);
        assert_eq!(completedHorizon(31, false), 31);
        assert_eq!(
            daysInMonth(Month {
                year: 2024,
                month: 2
            }),
            29
        );
        assert_eq!(
            nextMonth(Month {
                year: 2026,
                month: 12
            }),
            Month {
                year: 2027,
                month: 1
            }
        );
    }
}
