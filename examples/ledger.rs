use reconcile_cli::ledger::{Journal, Ledger, Posting};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ledger-demo.db".into());
    let mut ledger = Ledger::open(path)?;
    let journal = Journal {
        reference: "demo-settlement-001".into(),
        description: "GBP 100 settlement with GBP 0.25 fee".into(),
        postings: vec![
            Posting {
                account: "cash".into(),
                currency: "GBP".into(),
                minor_units: 9975,
            },
            Posting {
                account: "processing-fees".into(),
                currency: "GBP".into(),
                minor_units: 25,
            },
            Posting {
                account: "merchant-receivable".into(),
                currency: "GBP".into(),
                minor_units: -10000,
            },
        ],
    };
    println!("Posting: {:?}", ledger.post(&journal)?);
    for ((account, currency), amount) in ledger.balances()? {
        println!("{account}: {amount} {currency} minor units");
    }
    Ok(())
}
