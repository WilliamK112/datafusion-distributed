#![cfg(feature = "integration")]

#[path = "dynamic_filtering/aggregates.rs"]
mod aggregates;
#[path = "dynamic_filtering/collect_left_join.rs"]
mod collect_left_join;
#[path = "dynamic_filtering/common.rs"]
mod common;
#[path = "dynamic_filtering/partitioned_join.rs"]
mod partitioned_join;
#[path = "dynamic_filtering/sorts.rs"]
mod sorts;
