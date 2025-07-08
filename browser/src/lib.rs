use wasm_bindgen::prelude::*;
use tegdb::{Database, Result as TegResult};

// Import the `console.log` function from the `console` module
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

// Define a macro for easier console logging
macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
pub fn run_tegdb_browser_test() -> Result<(), JsValue> {
    unsafe { console_log!("🚀 Starting TegDB Browser Backend Test"); }
    // Test 1: localStorage backend
    unsafe { console_log!("1. Testing localStorage backend..."); }
    test_localstorage_backend().map_err(|e| JsValue::from_str(&format!("LocalStorage test failed: {:?}", e)))?;
    // Test 2: IndexedDB-style backend  
    unsafe { console_log!("2. Testing IndexedDB-style backend..."); }
    test_indexeddb_backend().map_err(|e| JsValue::from_str(&format!("IndexedDB test failed: {:?}", e)))?;
    unsafe { console_log!("🎉 All TegDB browser tests completed successfully!"); }
    Ok(())
}

fn test_localstorage_backend() -> TegResult<()> {
    unsafe { console_log!("   Creating localStorage database..."); }
    let mut db = Database::open("localstorage://my-app-database")?;
    unsafe { console_log!("   Creating table and inserting data..."); }
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")?;
    db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")?;
    db.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")?;
    unsafe { console_log!("   Querying data..."); }
    let results = db.query("SELECT * FROM users")?;
    let mut count = 0;
    for row_result in results {
        let row = row_result?;
        unsafe { console_log!("     User: {:?}", row); }
        count += 1;
    }
    unsafe { console_log!("   ✓ Found {} users in localStorage", count); }
    // Test transactions
    unsafe { console_log!("   Testing transactions..."); }
    let mut tx = db.begin_transaction()?;
    tx.execute("INSERT INTO users (id, name, email) VALUES (3, 'Carol', 'carol@example.com')")?;
    tx.commit()?;
    let results = db.query("SELECT * FROM users")?;
    let mut final_count = 0;
    for row_result in results {
        let row = row_result?;
        unsafe { console_log!("     Final user: {:?}", row); }
        final_count += 1;
    }
    unsafe { console_log!("   Total users after transaction: {}", final_count); }
    unsafe { console_log!("   ✓ localStorage backend test completed"); }
    Ok(())
}

fn test_indexeddb_backend() -> TegResult<()> {
    unsafe { console_log!("   Creating IndexedDB-style database..."); }
    let mut db = Database::open("browser://my-web-app-db")?;
    unsafe { console_log!("   Creating table for web app data..."); }
    db.execute("CREATE TABLE sessions (id INTEGER PRIMARY KEY, token TEXT, created_at INTEGER)")?;
    db.execute("INSERT INTO sessions (id, token, created_at) VALUES (1, 'sess_abc123', 1640995200)")?;
    db.execute("INSERT INTO sessions (id, token, created_at) VALUES (2, 'sess_def456', 1640995300)")?;
    unsafe { console_log!("   Querying session data..."); }
    let results = db.query("SELECT token FROM sessions WHERE created_at > 1640995150")?;
    for row_result in results {
        let row = row_result?;
        unsafe { console_log!("     Active session: {:?}", row); }
    }
    unsafe { console_log!("   ✓ IndexedDB-style backend test completed"); }
    Ok(())
}

// Test function that can be called from JavaScript
#[wasm_bindgen]
pub fn test_tegdb_performance() -> Result<(), JsValue> {
    unsafe { console_log!("🔥 TegDB Browser Performance Test"); }
    let mut db = Database::open("localstorage://perf-test-db")
        .map_err(|e| JsValue::from_str(&format!("Failed to open database: {:?}", e)))?;
    unsafe { console_log!("Creating performance test table..."); }
    db.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, data TEXT, value REAL)")
        .map_err(|e| JsValue::from_str(&format!("Failed to create table: {:?}", e)))?;
    let start_time = js_sys::Date::now();
    unsafe { console_log!("Inserting 100 rows..."); }
    for i in 1..=100 {
        let sql = format!("INSERT INTO perf_test (id, data, value) VALUES ({}, 'data_{}', {})", 
                         i, i, i as f64 * 1.5);
        db.execute(&sql)
            .map_err(|e| JsValue::from_str(&format!("Failed to insert row {}: {:?}", i, e)))?;
    }
    let insert_time = js_sys::Date::now() - start_time;
    unsafe { console_log!("✓ Inserted 100 rows in {:.2}ms", insert_time); }
    let query_start = js_sys::Date::now();
    let results = db.query("SELECT * FROM perf_test LIMIT 5")
        .map_err(|e| JsValue::from_str(&format!("Failed to query: {:?}", e)))?;
    let mut total_count = 0;
    for row_result in results {
        let row = row_result.map_err(|e| JsValue::from_str(&format!("Failed to read row: {:?}", e)))?;
        unsafe { console_log!("Sample result: {:?}", row); }
        total_count += 1;
    }
    let query_time = js_sys::Date::now() - query_start;
    unsafe { console_log!("✓ Queried {} sample rows in {:.2}ms", total_count, query_time); }
    unsafe { console_log!("🎯 Performance test completed successfully!"); }
    Ok(())
}

#[wasm_bindgen]
pub fn query_sql(sql: &str) -> Result<String, JsValue> {
    use tegdb::Database;
    let mut db = Database::open("localstorage://my-app-database")
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    let results = db.query(sql)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    let mut out = vec![];
    for row in results {
        let row = row.map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        out.push(format!("{:?}", row));
    }
    Ok(out.join("\n"))
}

#[wasm_bindgen]
pub fn execute_sql(sql: &str) -> Result<String, JsValue> {
    use tegdb::Database;
    let mut db = Database::open("localstorage://my-app-database")
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    db.execute(sql)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    Ok("Statement executed successfully.".to_string())
}
