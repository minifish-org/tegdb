use std::path::Path;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Mutex;
use std::sync::Arc;

pub struct Logger {
    file: Mutex<std::fs::File>,
}

impl Logger {
    pub fn new(dir: &Path) -> std::io::Result<Arc<Self>> {
        let log_path = dir.join("app.log");
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(log_path)?;
        Ok(Arc::new(Logger {
            file: Mutex::new(file),
        }))
    }

    pub fn log(&self, message: &str) {
        if let Ok(mut file) = self.file.lock() {
            let _ = writeln!(file, "{}", message);
        }
    }
}
