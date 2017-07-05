#[macro_use]
extern crate log;
extern crate env_logger;
extern crate interval_set;
extern crate batsim;
#[macro_use]
extern crate serde_json;

mod common;
mod meta_scheduler;
mod sub_scheduler;

fn main() {
    env_logger::init().unwrap();
    let mut scheduler = meta_scheduler::MetaScheduler::new();
    let mut batsim = batsim::Batsim::new(&mut scheduler);

    batsim.run_simulation().unwrap();
}
