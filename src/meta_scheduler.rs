extern crate serde_json;

use batsim::*;
use interval_set::{Interval, IntervalSet, ToIntervalSet};
use sub_scheduler::SubScheduler;
use common::Allocation;
use std::str::FromStr;
use std::cell::Cell;
use std::rc::Rc;
use std::collections::HashMap;

#[derive(Debug)]
pub struct RejectedJob {
    job_id: String,
    initial_job: Rc<Job>,
    resub_job: Rc<Job>,
    ack_received: Cell<bool>,
    finished: Cell<bool>,
}

pub struct MetaScheduler {
    nb_resources: u32,
    time: f64,
    schedulers: Vec<SubScheduler>,
    config: serde_json::Value,
    max_job_size: usize,
    greater_grp_size: usize,
    threshold: i32,
    nb_dangling_resources: i32,

    profiles: HashMap<String, Profile>,
    jobs: HashMap<String, Rc<Job>>,
    rejected_jobs: HashMap<String, Rc<RejectedJob>>,
}

impl MetaScheduler {
    pub fn new() -> MetaScheduler {
        MetaScheduler {
            nb_resources: 0,
            time: 0f64,
            schedulers: vec![],
            config: json!(null),
            max_job_size: 0,
            greater_grp_size: 0,
            nb_dangling_resources: 0,
            threshold: 0,
            profiles: HashMap::new(),
            jobs: HashMap::new(),
            rejected_jobs: HashMap::new(),
        }
    }
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

        //The maximun job size accepted
        self.max_job_size = (2 as u32).pow(nb_grps) as usize - 1;

        //The size of the greates group of resources.
        self.greater_grp_size = (2 as u32).pow(nb_grps - 1) as usize - 1;

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
                         profile: Option<Profile>)
                         -> Option<Vec<BatsimEvent>> {

        if job.res >= self.max_job_size as i32 {
            trace!("Job too big: {:?}", job);
            return Some(vec![reject_job_event(*timestamp, &job)])
        }

        // We save the profile of the new job
        match profile {
            Some(p) => {
                self.profiles.insert(job.profile.clone(), p);
            }
            None => panic!("Did you forget to activate the profile forwarding ?"),
        }

        // In waiting for a better idea, I use the job id to
        // differentiate rejected jobs from casual jobs
        let (w_id, j_id) = Job::split_id(&job.id);
        match w_id.as_ref() {
            "rej!" => {
                // If a jobs has the wokload id "rej" w need to handle
                // it separatly.
                trace!("REJECTED job(={:?}) with size {}", job, job.res);
                // Righ now we just panicking
                panic!("Impossible to handle rejected job: {:?}", w_id);
            }
            _ => {
                trace!("Get a new job(={:?}) with size {}", job, job.res);
                let grp_idx = get_job_grp(&job);
                let job_rc = Rc::new(job);

                self.jobs.insert(job_rc.id.clone(), job_rc.clone());

                if job_rc.res <= self.greater_grp_size as i32 {
                    self.schedulers
                        .get_mut(grp_idx)
                        .ok_or("No scheduler can handle this job")
                        .unwrap()
                        .add_job(Rc::new(Allocation::new(job_rc.clone())));
                } else {
                    trace!("New huge job has been added {:?}", job_rc);
                    let nm_res = job_rc.res;
                    // If the jobs do not fit into any of the sub schedulers
                    // we send it to schedulers till we have enought cores.
                    let mut iter_sched = self.schedulers.iter_mut().enumerate().rev();
                    let mut sched = iter_sched.next().unwrap();
                    let mut cores_remaining = job_rc.res;

                    let shared_allocation = Rc::new(Allocation::new(job_rc.clone()));
                    while cores_remaining > 0 && sched.0 > 0 {
                        sched.1.add_job(shared_allocation.clone());

                        cores_remaining -= 2i32.pow(sched.0 as u32);
                        sched = iter_sched.next().unwrap();
                    }
                }
            }
        }
        None
    }

    fn on_job_completed(&mut self, _: &f64, job_id: String, _: String) -> Option<Vec<BatsimEvent>> {
        trace!("Job completed: {}", job_id);
        let finished_job = self.jobs
            .get(&job_id)
            .ok_or("No job registered with this id")
            .unwrap();

        let grp_idx = get_job_grp(&finished_job);
        self.schedulers
            .get_mut(grp_idx)
            .ok_or("No scheduler can handle this job")
            .unwrap()
            .job_finished(job_id.clone());

        None
    }

    fn on_simulation_ends(&mut self, timestamp: &f64) {
        info!("Simulation ends: {}", timestamp);
    }

    fn on_job_killed(&mut self, _: &f64, _: Vec<String>) -> Option<Vec<BatsimEvent>> {
        None
    }

    fn on_message_received_end(&mut self, timestamp: &mut f64) -> Option<Vec<BatsimEvent>> {
        // All events that will be send to batsim (all allocation that are ready)
        let mut events: Vec<BatsimEvent> = vec![];

        for scheduler in &mut self.schedulers {
            let (allocations, rejeted) = scheduler.schedule_jobs(self.time);
            match allocations {
                Some(allocs) => {
                    let ready_allocations: Vec<Rc<Allocation>> = allocs
                        .into_iter()
                        .filter_map(|alloc| if alloc.nb_of_res_to_complete() == 0 {
                                        return Some(alloc.clone());
                                    } else {
                                        return None;
                                    })
                        .collect();

                    for ready_allocation in &ready_allocations {
                        scheduler.job_launched(ready_allocation.job.id.clone());
                    }
                    events.extend(allocations_to_batsim_events(self.time, ready_allocations));
                }
                _ => {}
            }
        }
        trace!("Respond to batsim at: {} with {} events", timestamp, events.len());
        Some(events)
    }

    fn on_message_received_begin(&mut self, timestamp: &f64) -> Option<Vec<BatsimEvent>> {
        trace!("Received new batsim message at {}", timestamp);
        None
    }
}

fn get_job_grp(job: &Job) -> usize {
    (job.res as f64).log(2_f64).ceil() as usize
}

fn allocations_to_batsim_events(now: f64, allocation: Vec<Rc<Allocation>>) -> Vec<BatsimEvent> {

    allocation
        .into_iter()
        .filter_map(|alloc| {
                        if alloc.job.res as u32 <= alloc.nodes.borrow().size() {
                            return Some(allocate_job_event(now,
                                                           &*alloc.job,
                                                           format!("{}", *alloc.nodes.borrow())));
                        }
                        None
                    })
        .collect()
}
