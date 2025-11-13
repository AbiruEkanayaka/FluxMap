# Project TODOs and Future Improvements

This document lists potential improvements and areas for future work identified during a code review.

- [ ] **Implement a Structured Logging Strategy**
  - **Observation:** Background threads (e.g., vacuum, snapshotter) currently use `eprintln!` for error reporting.
  - **Suggestion:** Replace `eprintln!` with a logging facade like the `log` or `tracing` crate. This will allow library users to control log routing, formatting, and verbosity, which is essential for production use.

- [ ] **Re-evaluate Fatal Error Handling**
  - **Observation:** The persistence engine panics on critical errors like a corrupted snapshot file.
  - **Suggestion:** While "crashing loudly" is a valid strategy, consider an alternative where background threads can communicate fatal, unrecoverable errors back to the main `Database` object. This could be done via a shared atomic `Result` or similar mechanism. The `Database` could then return a specific `FluxError::FatalPersistenceError` on subsequent API calls, giving the application owner more control over how to handle the shutdown.

- [ ] **Enhance `range_stream` for True Streaming in Transactions**
  - **Observation:** The `range_stream` and `prefix_scan_stream` methods currently buffer all results when used inside an explicit transaction to correctly handle Read-Your-Own-Writes (RYOW).
  - **Suggestion:** For a future performance enhancement, implement a true streaming merge iterator. This iterator would combine the stream from the underlying `SkipList` with the contents of the transaction's `workspace` on the fly, providing better memory efficiency for very large range scans within a transaction.

- [ ] **Optimize the Memory Eviction Strategy**
  - **Observation:** The `evict_if_needed` function evicts keys one by one in a loop until memory usage is below the limit.
  - **Suggestion:** If memory is significantly over the limit, this one-by-one approach could be inefficient. Explore a batch eviction strategy where the system estimates how many keys need to be removed and then finds and evicts a batch of victims in a single, more efficient pass.

- [ ] **Improve `unsafe` Code Documentation**
  - **Observation:** The `unsafe` blocks in `lib.rs` are necessary and the `SAFETY` comments provide good context.
  - **Suggestion:** Enhance the `SAFETY` comments to be even more rigorous by explicitly stating the invariants that must be upheld for the `unsafe` block to be sound. This makes the code easier to audit, verify, and maintain, which is critical for the most sensitive parts of the codebase.
