//! WASM Integration Tests
//!
//! This file is automatically generated by generate_wasm_tests.py
//! It contains WASM test functions for all tests that use run_with_both_backends.
//!
//! Total tests: 0

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

#[cfg(target_arch = "wasm32")]
use tegdb::{Database, Result, SqlValue};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test_configure!(run_in_node);

#[cfg(target_arch = "wasm32")]
#[path = "helpers/test_helpers.rs"]
mod test_helpers;
#[cfg(target_arch = "wasm32")]
use crate::test_helpers::run_with_both_backends;

// Generated WASM test functions
