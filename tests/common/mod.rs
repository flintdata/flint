use std::fs;
use std::path::PathBuf;
use std::process::{Command, Child};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// TestDb manages an isolated flint database for integration testing
pub struct TestDb {
    dir: PathBuf,
    server_process: Option<Child>,
}

impl TestDb {
    /// Create a new test database with isolated temp directory
    pub fn new() -> Self {
        // Kill any stray flint processes first
        let _ = Command::new("pkill")
            .args(&["-9", "-f", "target/release/flint"])
            .output();

        // Give processes time to die
        thread::sleep(Duration::from_millis(200));

        // Create unique temp dir
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        let temp_dir = format!("/tmp/flint-test-{}", now.as_nanos());
        let dir = PathBuf::from(&temp_dir);

        fs::create_dir_all(&dir).expect("failed to create temp dir");

        // Start server in temp directory
        let server_process = Self::spawn_server(&dir);

        // Wait for server to be ready
        Self::wait_for_server(30);

        TestDb {
            dir,
            server_process: Some(server_process),
        }
    }

    /// Spawn the flint server binary in the given directory
    fn spawn_server(dir: &PathBuf) -> Child {
        let binary_path = std::env::current_dir()
            .expect("failed to get current dir")
            .join("target/release/flint");

        let child = Command::new(&binary_path)
            .current_dir(dir)
            .spawn()
            .expect("failed to spawn flint server");

        // Give server time to start and bind to port
        thread::sleep(Duration::from_millis(800));

        child
    }

    /// Wait for server to be ready to accept connections
    fn wait_for_server(retries: usize) {
        for _ in 0..retries {
            let output = Command::new("psql")
                .args(&[
                    "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-c", "SELECT 1;",
                ])
                .output();

            if output.is_ok() && output.unwrap().status.success() {
                return;
            }

            thread::sleep(Duration::from_millis(100));
        }
        panic!("server failed to start after retries");
    }

    /// Execute SQL statement via psql
    pub fn execute_sql(&self, sql: &str) -> Result<String, String> {
        let output = Command::new("psql")
            .args(&[
                "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-c", sql,
            ])
            .output()
            .map_err(|e| format!("failed to execute psql: {}", e))?;

        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();

        if !output.status.success() {
            // Only return error if there's actual error output
            if !stderr.is_empty() && stderr.contains("ERROR") {
                return Err(stderr);
            }
        }

        Ok(stdout)
    }

    /// Restart database (kill server, delete files, restart)
    pub fn restart(&mut self) -> Result<(), String> {
        // Kill server
        if let Some(mut proc) = self.server_process.take() {
            let _ = proc.kill();
            let _ = proc.wait();
        }

        // Wait for port to be released
        thread::sleep(Duration::from_millis(800));

        // Delete database file to start fresh
        let db_path = self.dir.join("data.db");
        if db_path.exists() {
            fs::remove_file(&db_path).map_err(|e| format!("failed to delete db: {}", e))?;
        }

        // Restart server
        self.server_process = Some(Self::spawn_server(&self.dir));
        Self::wait_for_server(30);

        Ok(())
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Kill server process
        if let Some(mut proc) = self.server_process.take() {
            let _ = proc.kill();
            let _ = proc.wait();
        }

        // Wait for port to be released
        thread::sleep(Duration::from_millis(400));

        // Kill any remaining flint processes that might be lingering
        let _ = Command::new("pkill")
            .args(&["-9", "-f", "target/release/flint"])
            .output();

        thread::sleep(Duration::from_millis(200));

        // Cleanup temp dir - retry a few times in case of race conditions
        for _ in 0..3 {
            if fs::remove_dir_all(&self.dir).is_ok() {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}