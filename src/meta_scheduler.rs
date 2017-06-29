extern crate serde_json;

use batsim::*;
use interval_set::{Interval, IntervalSet, ToIntervalSet};
use sub_scheduler::SubScheduler;
use common::Allocation;
use std::str::FromStr;
use std::rc::Rc;

pub struct MetaScheduler {
    nb_resources: u32,
    time: f64,
    schedulers: Vec<SubScheduler>,
    config: serde_json::Value,
    max_job_size: usize,
    threshold: i32,
    nb_dangling_resources: i32,
}

impl Scheduler for MetaScheduler {
    fn simulation_begins(&mut self,
                         timestamp: &f64,
                         nb_resources: i32,
                         config: serde_json::Value)
                         -> Option<Vec<BatsimEvent>> {

        info!("MainScheduler Initialized with {} resources", nb_resources);
        self.nb_resources = nb_resources as u32;
        let nb_grps: u32 = (nb_resources as f64).log(2_f64).floor() as u32;
        // self.max_job_size = (2 as u32).pow(nb_grps - 1) as usize;
        self.max_job_size = self.nb_resources as usize;

        info!("We can construct {} groups with {} resources",
              nb_grps,
              nb_resources);
        info!("The max job size accepted is {}", self.max_job_size);

        self.config = config;
        // We get the threshold from the configuration
        let threshold: i32 = i32::from_str(&self.config["rejection"]["threshold"].to_string())
            .unwrap();

        self.threshold = threshold;
        info!("Threshold is set at: {}", threshold);

        let mut nb_machines_used = 0;
        for idx in 0..nb_grps {
            let interval = Interval::new(2_u32.pow(idx) as u32, (2_u32.pow(idx + 1) - 1) as u32);
            let mut scheduler: SubScheduler = SubScheduler::new(interval.clone());

            nb_machines_used += interval.range_size();
            info!("Groupe(n={}) uses {} nodes", idx, interval.range_size());

            self.schedulers.insert(idx as usize, scheduler);
        }

        info!("We use only {} over {}",
              nb_machines_used,
              self.nb_resources - 1);

        self.nb_dangling_resources = (self.nb_resources - 1 - nb_machines_used) as i32;
        if self.nb_dangling_resources <= 0 {
            panic!("No ressource available for rejection");
        }

        Some(vec![notify_event(*timestamp, String::from("submission_finished"))])
    }

    fn on_job_submission(&mut self,
                         timestamp: &f64,
                         job: Job,
                         _: Option<Profile>)
                         -> Option<Vec<BatsimEvent>> {
        Some(vec![reject_job_event(*timestamp, &job)])
    }

    fn on_job_completed(&mut self, _: &f64, _: String, _: String) -> Option<Vec<BatsimEvent>> {
        None
    }

    fn on_simulation_ends(&mut self, timestamp: &f64) {
        println!("Simulation ends: {}", timestamp);
    }

    fn on_job_killed(&mut self, _: &f64, _: Vec<String>) -> Option<Vec<BatsimEvent>> {
        None
    }

    fn on_message_received_end(&mut self, timestamp: &mut f64) -> Option<Vec<BatsimEvent>> {
        trace!("Respond to batsim at: {}", timestamp);
        None
    }

    fn on_message_received_begin(&mut self, timestamp: &f64) -> Option<Vec<BatsimEvent>> {
        trace!("Received new batsim message at {}", timestamp);
        None
    }
}
