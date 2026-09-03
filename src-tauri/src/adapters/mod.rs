// SOT: adapters-index, vendor-boundary
//
// WHAT:  The only place vendor crates for secrets live (`aes_gcm`, `keyring`).
// WHY:   Swapping a vendor is then one file, not a refactor of every call site.
// WHERE: scripts/guardrail.py enforces the import boundary.

pub mod crypto;
pub mod keyring;
