use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tegdb::tegstream::{Config, Restore, Result, S3Backend, Tailer};

#[derive(Parser)]
#[command(name = "tegstream")]
#[command(about = "Streaming backup tool for TegDB databases")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run continuous replication
    Run {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Create a one-off snapshot
    Snapshot {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Restore database from S3
    Restore {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
        /// Output path for restored database
        #[arg(short, long)]
        to: PathBuf,
        /// Restore to specific offset (optional)
        #[arg(long)]
        at_offset: Option<u64>,
    },
    /// List available snapshots and segments
    List {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Prune old snapshots and segments
    Prune {
        /// Config file path
        #[arg(short, long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run_command(cli.command).await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run_command(command: Commands) -> Result<()> {
    match command {
        Commands::Run { config } => {
            let config = Config::from_file(&config)?;
            let mut tailer = Tailer::new(config).await?;
            tailer.run().await?;
        }
        Commands::Snapshot { config } => {
            let config = Config::from_file(&config)?;
            let mut tailer = Tailer::new(config).await?;
            tailer.snapshot_once().await?;
        }
        Commands::Restore {
            config,
            to,
            at_offset,
        } => {
            let config = Config::from_file(&config)?;
            let restore = Restore::new(&config.s3, &config.s3_prefix()).await?;
            restore.restore_to(&to, at_offset).await?;
        }
        Commands::List { config } => {
            let config = Config::from_file(&config)?;
            let restore = Restore::new(&config.s3, &config.s3_prefix()).await?;
            restore.list().await?;
        }
        Commands::Prune { config } => {
            let config = Config::from_file(&config)?;
            prune_snapshots(&config).await?;
        }
    }

    Ok(())
}

async fn prune_snapshots(config: &Config) -> Result<()> {
    let backend = S3Backend::new(&config.s3).await?;
    let prefix = config.s3_prefix();

    // List all bases
    let base_prefix = format!("{}/base/", prefix);
    let mut bases: Vec<String> = backend
        .list_objects(&base_prefix)
        .await?
        .iter()
        .filter(|k| k.ends_with(".snap") || k.ends_with(".snap.gz"))
        .cloned()
        .collect();

    bases.sort();

    // Keep only the last N bases
    if bases.len() > config.retention.bases {
        let to_delete = bases.len() - config.retention.bases;
        eprintln!("Deleting {} old base snapshot(s)...", to_delete);

        for base in bases.iter().take(to_delete) {
            eprintln!("  Deleting: {}", base);
            backend.delete_object(base).await?;
        }
    }

    // Prune segments for deleted bases
    // This is simplified - in practice, you'd track which segments belong to which bases
    let segment_prefix = format!("{}/segments/", prefix);
    let segments = backend.list_objects(&segment_prefix).await?;

    // Note: Actually calculating sizes requires HEAD requests, so this is simplified
    // In a production implementation, you'd want to track segment sizes

    eprintln!("Found {} segments", segments.len());
    eprintln!("Pruning based on size limits not yet fully implemented");

    Ok(())
}
