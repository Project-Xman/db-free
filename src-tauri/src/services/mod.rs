// SOT: services-index, orchestration-layer
//
// WHAT:  Services orchestrate store + integrations for one use case each.
// WHY:   Commands stay thin (validate, guard, call); integrations stay dumb.
// WHERE: scripts/guardrail.py — commands may not import store/integrations directly.

pub mod ai;
pub mod buffers;
pub mod changes;
pub mod connection;
pub mod data;
pub mod documents;
pub mod history;
pub mod objects;
pub mod query;
pub mod saved_queries;
pub mod schema;
pub mod settings;
pub mod transfer;
