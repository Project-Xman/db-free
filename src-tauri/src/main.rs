// SOT: binary-entry
// Prevents an extra console window on Windows in release builds.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    if let Err(err) = db_free_lib::run() {
        eprintln!("db-free failed to start: {err}");
        std::process::exit(1);
    }
}
