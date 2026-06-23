//! deterrust — the command-line driver for the deterministic Raft simulator.
//!
//! Subcommands:
//!   fuzz    sweep seeds across all cores, report the lowest failing one
//!   replay  deterministically re-run one seed and print the event timeline
//!   demo    plant a known bug, fuzz for it, then replay the failure end-to-end
//!   check   re-run a seed and report the standalone linearizability verdict

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use deterrust_raft::BugConfig;
use deterrust_sim::{FaultLevel, Outcome, Report, SimConfig};

#[derive(Parser)]
#[command(
    name = "deterrust",
    about = "A deterministic Raft simulator that hunts its own consensus bugs and replays them from a seed.",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Sweep seeds and report the lowest one that breaks an invariant.
    Fuzz(RunArgs),
    /// Deterministically re-run a single seed and print the timeline.
    Replay(ReplayArgs),
    /// Plant a bug, find a failing seed, and replay it end-to-end.
    Demo(DemoArgs),
    /// Re-run a seed and report the linearizability verdict for its history.
    Check(ReplayArgs),
}

#[derive(Parser, Clone)]
struct RunArgs {
    /// Number of seeds to sweep, starting at --start.
    #[arg(long, default_value_t = 10_000)]
    seeds: u64,
    /// First seed to try.
    #[arg(long, default_value_t = 0)]
    start: u64,
    /// Cluster size.
    #[arg(long, default_value_t = 5)]
    nodes: u64,
    /// Client operations per run.
    #[arg(long, default_value_t = 120)]
    ops: u64,
    /// Fault level: none | mild | aggressive.
    #[arg(long, default_value = "aggressive")]
    faults: String,
    /// Plant a bug: none | commit-single-ack | vote-no-log-check | stale-append.
    #[arg(long, default_value = "none")]
    bug: String,
}

#[derive(Parser, Clone)]
struct ReplayArgs {
    /// The seed to reproduce (accepts 0x-prefixed hex).
    #[arg(long, value_parser = parse_seed)]
    seed: u64,
    #[arg(long, default_value_t = 5)]
    nodes: u64,
    #[arg(long, default_value_t = 120)]
    ops: u64,
    #[arg(long, default_value = "aggressive")]
    faults: String,
    #[arg(long, default_value = "none")]
    bug: String,
    /// Print every trace line (otherwise the tail is summarized).
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Parser, Clone)]
struct DemoArgs {
    /// Which bug to showcase: commit-single-ack | vote-no-log-check | stale-append.
    #[arg(long, default_value = "commit-single-ack")]
    bug: String,
    #[arg(long, default_value_t = 5)]
    nodes: u64,
    #[arg(long, default_value_t = 120)]
    ops: u64,
    /// How many seeds to search before giving up.
    #[arg(long, default_value_t = 50_000)]
    seeds: u64,
}

fn parse_seed(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let parsed = if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16)
    } else {
        s.parse::<u64>()
    };
    parsed.map_err(|e| format!("invalid seed '{s}': {e}"))
}

fn bug_from(name: &str) -> BugConfig {
    match name {
        "none" => BugConfig::default(),
        "commit-single-ack" => BugConfig {
            commit_on_single_ack: true,
            ..Default::default()
        },
        "vote-no-log-check" => BugConfig {
            vote_without_log_check: true,
            ..Default::default()
        },
        "stale-append" => BugConfig {
            accept_stale_append: true,
            ..Default::default()
        },
        other => {
            eprintln!("unknown bug '{other}', running with no planted bug");
            BugConfig::default()
        }
    }
}

fn faults_from(name: &str) -> FaultLevel {
    FaultLevel::parse(name).unwrap_or_else(|| {
        eprintln!("unknown fault level '{name}', using aggressive");
        FaultLevel::Aggressive
    })
}

fn config(seed: u64, nodes: u64, ops: u64, faults: &str, bug: &str) -> SimConfig {
    let mut cfg = SimConfig::new(seed);
    cfg.nodes = nodes;
    cfg.ops = ops;
    cfg.faults = faults_from(faults);
    cfg.bug = bug_from(bug);
    cfg
}

fn run_one(seed: u64, nodes: u64, ops: u64, faults: &str, bug: &str) -> Report {
    deterrust_sim::Simulator::new(config(seed, nodes, ops, faults, bug)).run()
}

/// Parallel search for the lowest failing seed in `[start, start+count)`.
/// Returns `(lowest_failing_seed, seeds_checked)`.
fn parallel_search(args: &RunArgs) -> (Option<u64>, u64) {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let min_fail = Arc::new(AtomicU64::new(u64::MAX));
    let checked = Arc::new(AtomicUsize::new(0));
    let end = args.start + args.seeds;

    std::thread::scope(|scope| {
        for t in 0..cores {
            let min_fail = Arc::clone(&min_fail);
            let checked = Arc::clone(&checked);
            let args = args.clone();
            scope.spawn(move || {
                let mut s = args.start + t as u64;
                while s < end {
                    // A lower failing seed already exists: no point going higher.
                    if s >= min_fail.load(Ordering::Relaxed) {
                        break;
                    }
                    let r = run_one(s, args.nodes, args.ops, &args.faults, &args.bug);
                    checked.fetch_add(1, Ordering::Relaxed);
                    if r.outcome.is_failure() {
                        min_fail.fetch_min(s, Ordering::Relaxed);
                        break;
                    }
                    s += cores as u64;
                }
            });
        }
    });

    let m = min_fail.load(Ordering::Relaxed);
    let found = if m == u64::MAX { None } else { Some(m) };
    (found, checked.load(Ordering::Relaxed) as u64)
}

fn print_report_header(r: &Report) {
    let mark = if r.outcome.is_failure() { "✗" } else { "✓" };
    println!(
        "{mark} seed 0x{:016x} ({})  —  {}  [{} ops, {} leader changes, {} events]",
        r.seed,
        r.seed,
        r.outcome.label(),
        r.ops_completed,
        r.leader_changes,
        r.events_processed
    );
    match &r.outcome {
        Outcome::InvariantViolated(m) | Outcome::NonLinearizable(m) | Outcome::Stalled(m) => {
            println!("  → {m}")
        }
        Outcome::Ok => {}
    }
}

fn print_trace(r: &Report, verbose: bool) {
    let n = r.trace.len();
    let show_tail = 40;
    let skip = if verbose || n <= show_tail {
        0
    } else {
        n - show_tail
    };
    if skip > 0 {
        println!("  … {skip} earlier timeline events omitted (use -v for the full trace)");
    }
    for ev in r.trace.iter().skip(skip) {
        println!("  t={:<8} {}", ev.time, ev.text);
    }
}

fn cmd_fuzz(args: &RunArgs) {
    println!(
        "fuzzing seeds {}..{} | nodes={} ops={} faults={} bug={}",
        args.start,
        args.start + args.seeds,
        args.nodes,
        args.ops,
        args.faults,
        args.bug
    );
    let (found, checked) = parallel_search(args);
    match found {
        Some(seed) => {
            let r = run_one(seed, args.nodes, args.ops, &args.faults, &args.bug);
            print_report_header(&r);
            println!(
                "\nreproduce: deterrust replay --seed 0x{:x} --nodes {} --ops {} --faults {} --bug {} -v",
                seed, args.nodes, args.ops, args.faults, args.bug
            );
            std::process::exit(1);
        }
        None => {
            println!(
                "✓ no violation in {checked} seeds — the cluster held under {} faults.",
                args.faults
            );
        }
    }
}

fn cmd_replay(args: &ReplayArgs) {
    let r = run_one(args.seed, args.nodes, args.ops, &args.faults, &args.bug);
    print_report_header(&r);
    println!();
    print_trace(&r, args.verbose);
    if r.outcome.is_failure() {
        std::process::exit(1);
    }
}

fn cmd_check(args: &ReplayArgs) {
    let r = run_one(args.seed, args.nodes, args.ops, &args.faults, &args.bug);
    println!(
        "seed 0x{:x}: {} client ops recorded; verdict: {}",
        args.seed,
        r.ops_completed,
        match &r.outcome {
            Outcome::NonLinearizable(m) => format!("NOT LINEARIZABLE — {m}"),
            Outcome::Ok => "LINEARIZABLE".to_string(),
            other => format!("{} (no linearizability verdict)", other.label()),
        }
    );
    if r.outcome.is_failure() {
        std::process::exit(1);
    }
}

fn cmd_demo(args: &DemoArgs) {
    println!("════════════════════════════════════════════════════════════════");
    println!(
        " deterrust demo — planting the '{}' bug and hunting it",
        args.bug
    );
    println!("════════════════════════════════════════════════════════════════\n");
    println!(
        "① building a {}-node cluster with the bug enabled",
        args.nodes
    );
    println!(
        "② fuzzing up to {} seeds for a safety violation …\n",
        args.seeds
    );

    let search = RunArgs {
        seeds: args.seeds,
        start: 0,
        nodes: args.nodes,
        ops: args.ops,
        faults: "aggressive".to_string(),
        bug: args.bug.clone(),
    };
    let (found, checked) = parallel_search(&search);
    let Some(seed) = found else {
        println!("no violation found in {checked} seeds (try --seeds higher).");
        std::process::exit(1);
    };
    println!("   ✗ found it: seed 0x{seed:x} breaks the cluster.\n");
    println!("③ replaying seed 0x{seed:x} deterministically — same bug, every time:\n");
    let r = run_one(seed, args.nodes, args.ops, "aggressive", &args.bug);
    print_trace(&r, false);
    println!();
    print_report_header(&r);
    println!(
        "\n   reproduce forever: deterrust replay --seed 0x{:x} --bug {} -v",
        seed, args.bug
    );
    println!("\nThis is deterministic simulation testing: the bug is not flaky, it is a");
    println!("function of the seed. Fix the bug, re-run the seed, and it is gone.");
}

fn main() {
    let cli = Cli::parse();
    match &cli.cmd {
        Cmd::Fuzz(a) => cmd_fuzz(a),
        Cmd::Replay(a) => cmd_replay(a),
        Cmd::Demo(a) => cmd_demo(a),
        Cmd::Check(a) => cmd_check(a),
    }
}
