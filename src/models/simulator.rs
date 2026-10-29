//! Configuration shared by the simulator forwarding routes.
#![allow(non_snake_case)]

use crate::handlers::simulator_guard::SimulatorGuard;
use std::path::PathBuf;

pub struct Simulator {
    pub(crate) client: reqwest::Client,
    pub(crate) baseUrl: reqwest::Url,
    pub(crate) allowedKeysFile: PathBuf,
    /// Website users' runs in progress and this minute's, and the answers to recent requests.
    pub(crate) guard: SimulatorGuard,
}
