//! Offline restore and verification; consistent backup may run against a live DB.
use reconcile_cli::events::{EventStore, verify_backup};
use std::{path::Path, time::Duration};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command,source,destination] if command=="backup" => {
            verify_backup(Path::new(source))?;
            EventStore::open(source,Duration::from_secs(30))?.backup(Path::new(destination))?;
            println!("Verified backup saved to {destination}");
        }
        [command,source] if command=="verify" => {verify_backup(Path::new(source))?;println!("Integrity, foreign keys and schema verified");}
        [command,source,destination] if command=="restore" => {
            verify_backup(Path::new(source))?;
            // Refuse an existing database or sidecar; never replace the running store.
            for suffix in ["","-wal","-shm"] {if Path::new(&format!("{destination}{suffix}")).exists(){return Err("Restore requires a new destination without WAL/SHM files".into());}}
            EventStore::open(source, Duration::from_secs(30))?.backup(Path::new(destination))?;
            println!("Verified restore saved to {destination}; point the stopped API at this file");
        }
        _ => return Err("Usage: maintenance backup <database> <new-backup> | verify <backup> | restore <backup> <new-database>".into())
    }
    Ok(())
}
