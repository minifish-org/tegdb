use tegdb::engine::{Engine, EngineConfig};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = PathBuf::from("/tmp/test_commit_marker.db");
    
    // Clean up if the file exists
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }
    
    // Create a new engine
    {
        let mut engine = Engine::new(db_path.clone())?;
        
        // Begin a transaction
        {
            let mut tx = engine.begin_transaction();
            tx.set(b"key1", b"value1".to_vec())?;
            tx.set(b"key2", b"value2".to_vec())?;
            tx.commit()?;
        }
        
        // Begin another transaction but don't commit
        {
            let mut tx = engine.begin_transaction();
            tx.set(b"key3", b"value3".to_vec())?;
            // Don't commit - this should be rolled back on crash recovery
        }
    }
    
    // Reopen the database to simulate crash recovery
    {
        let engine = Engine::new(db_path.clone())?;
        
        // Check that committed data is still there
        assert_eq!(engine.get(b"key1").as_deref(), Some(b"value1" as &[u8]));
        assert_eq!(engine.get(b"key2").as_deref(), Some(b"value2" as &[u8]));
        
        // Check that uncommitted data was rolled back
        assert_eq!(engine.get(b"key3"), None);
        
        // Verify the commit marker is present
        assert!(engine.get(b"__tx_commit__").is_some());
        
        println!("✅ Commit marker and crash recovery working correctly!");
    }
    
    // Clean up
    std::fs::remove_file(&db_path)?;
    
    Ok(())
}
