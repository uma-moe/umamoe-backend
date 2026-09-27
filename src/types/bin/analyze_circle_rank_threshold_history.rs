// Included by the owning module to preserve private field visibility.

const TIERS: [(&str, u8, usize); 9] = [
    ("SS", 11, 10),
    ("S+", 10, 30),
    ("S", 9, 100),
    ("A+", 8, 500),
    ("A", 7, 1_000),
    ("B+", 6, 3_000),
    ("B", 5, 5_000),
    ("C+", 4, 7_000),
    ("C", 3, 10_000),
];

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Month {
    year: i32,
    month: u8,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ViewerMonth {
    viewer_id: i64,
    month: Month,
}

#[derive(Clone, Copy)]
struct EndMembership {
    circle_id: i64,
    last_fans: i64,
    first_day: u8,
    last_day: u8,
}

#[derive(Default)]
struct ViewerMonthData {
    month_start_fans: i64,
    end_membership: Option<EndMembership>,
}

struct CircleData {
    first_day: u8,
    points: [i64; 31],
}

#[derive(Default)]
struct MonthData {
    horizon: u8,
    circles: HashMap<i64, CircleData>,
}
