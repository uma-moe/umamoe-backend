//! Configuration shared by the simulator forwarding routes.
#![allow(non_snake_case)]

use axum::http::HeaderValue;
use std::collections::HashSet;

pub struct Simulator {
    pub(crate) client: reqwest::Client,
    pub(crate) baseUrl: reqwest::Url,
    pub(crate) backendKey: HeaderValue,
    pub(crate) allowedKeys: HashSet<[u8; 32]>,
}
