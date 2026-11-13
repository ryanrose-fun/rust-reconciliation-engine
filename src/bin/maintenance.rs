//! Offline restore and verification; consistent backup may run against a live DB.
use reconcile_cli::events::{
    archive_database, backup_database, recovery_drill, verify_archive, verify_backup,
};
use std::path::Path;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command,source,directory,days] if command=="archive" => {
            let days:u64=days.parse()?;
            if !(1..=36500).contains(&days){return Err("Archive age must be 1..36500 days".into());}
            let cutoff=chrono::Utc::now().timestamp_millis().saturating_sub((days*86_400_000) as i64);
            println!("{}",serde_json::to_string_pretty(&archive_database(Path::new(source),Path::new(directory),cutoff)?)?);
        }
        [command,directory] if command=="verify-archive" => {verify_archive(Path::new(directory))?;println!("Archive verified");}
        [command,source,directory] if command=="drill" => {println!("{}",serde_json::to_string_pretty(&recovery_drill(Path::new(source),Path::new(directory))?)?);}

        [command,source,destination] if command=="backup" => {
            verify_backup(Path::new(source))?;
            backup_database(Path::new(source),Path::new(destination))?;
            println!("Verified backup saved to {destination}");
        }
        [command,source] if command=="verify" => {verify_backup(Path::new(source))?;println!("Integrity, foreign keys and schema verified");}
        [command,source,destination] if command=="restore" => {
            verify_backup(Path::new(source))?;
            // Refuse an existing database or sidecar; never replace the running store.
            for suffix in ["","-wal","-shm"] {if Path::new(&format!("{destination}{suffix}")).exists(){return Err("Restore requires a new destination without WAL/SHM files".into());}}
            backup_database(Path::new(source),Path::new(destination))?;
            println!("Verified restore saved to {destination}; point the stopped API at this file");
        }
        _ => return Err("Usage: maintenance archive <database> <new-directory> <older-than-days> | verify-archive <directory> | drill <database> <new-directory> | backup <database> <new-backup> | verify <backup> | restore <backup> <new-database>".into())
    }
    Ok(())
}
