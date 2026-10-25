//! s2z_kv_puts_total: the object-store PUTs of each partition.

mod common;

use common::*;

#[tokio::test]
async fn puts_are_counted_by_partition() {
    let store = kvd::metrics::count_puts(mem());
    let p = open(&store).await;
    let before = kvd::metrics::puts(PID);
    assert!(before > 0, "the takeover writes a manifest");
    for i in 0..5 {
        put(&p, i, "v").await.unwrap();
    }
    let after = kvd::metrics::puts(PID);
    assert!(after > before, "each durable commit PUTs a WAL object");
    let text = kvd::metrics::render(None);
    assert!(
        text.contains(&format!("s2z_kv_puts_total{{partition=\"{PID}\"}} ")),
        "{text}"
    );
    assert_eq!(kvd::metrics::puts(PID + 1), 0, "another partition counts nothing");
}
