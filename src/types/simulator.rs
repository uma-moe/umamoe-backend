//! Configuration shared by the simulator forwarding routes.
#![allow(non_snake_case)]

use std::path::PathBuf;

pub struct Simulator {
    pub(crate) client: reqwest::Client,
    pub(crate) baseUrl: reqwest::Url,
    pub(crate) allowedKeysFile: PathBuf,
}
