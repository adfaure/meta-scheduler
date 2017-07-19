extern crate serde_json;

use batsim::*;
use interval_set::{Interval, IntervalSet, ToIntervalSet};
use sub_scheduler::SubSchedulerRejection;
use fcfs_scheduler::SubSchedulerFcfs;
use common::*;
use std::str::FromStr;
use std::cell::Cell;
use std::rc::Rc;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

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
    schedulers: Vec<Uuid>,
    rejection_scheduler: Uuid,
    schedulers_map: HashMap<Uuid, Box<SubScheduler>>,
    config: serde_json::Value,
    max_job_size: usize,
    greater_grp_size: usize,
    threshold: i32,
    nb_dangling_resources: i32,

    profiles: HashMap<String, Profile>,
    jobs: HashMap<String, Rc<Allocation>>,
    rejected_jobs: HashMap<String, Rc<RejectedJob>>,
}

impl MetaScheduler {
    pub fn new() -> MetaScheduler {
        MetaScheduler {
            nb_resources: 0,
            time: 0f64,
            schedulers: vec![],
            rejection_scheduler: Uuid::new_v4(),
            schedulers_map: HashMap::new(),
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
        //self.max_job_size = (2 as u32).pow(nb_grps) as usize - 1;
        self.max_job_size = nb_resources as usize - 1;

        //The size of the greatest group of resources.
        self.greater_grp_size = (2 as u32).pow(nb_grps - 1) as usize;

        info!("We can construct {} groups with {} resources",
              nb_grps,
              nb_resources);
        info!("The max job size accepted is {}", self.max_job_size);
        info!("The biggest group has  {} resources", self.greater_grp_size);


        self.config = config;
        // We get the threshold from the configuration
        let threshold: i32 = i32::from_str(&self.config["rejection"]["threshold"].to_string())
            .unwrap();

        self.threshold = threshold;
        info!("Threshold is set at: {}", threshold);

        let mut nb_machines_used = 0;
        for idx in 0..nb_grps {
            let interval = Interval::new(2_u32.pow(idx) as u32 - 1,
                                         (2_u32.pow(idx + 1) - 2) as u32);
            let scheduler: Box<SubScheduler> =
                Box::new(SubSchedulerRejection::new(interval.clone()));

            nb_machines_used += interval.range_size();
            info!("Groupe(n={}) uses {} nodes: {:?} id: {}",
                  idx,
                  interval.range_size(),
                  interval,
                  scheduler.get_uuid());

            self.schedulers
                .insert(idx as usize, scheduler.get_uuid());
            self.schedulers_map
                .insert(scheduler.get_uuid(), scheduler);
        }


        self.nb_dangling_resources = (self.nb_resources - nb_machines_used) as i32;
        if self.nb_dangling_resources <= 0 {
            panic!("No ressource available for rejection");
        }

        info!("We use only {} over {}: {}",
              nb_machines_used,
              self.nb_resources,
              self.nb_dangling_resources);

        let scheduler: Box<SubScheduler> =
            Box::new(SubSchedulerFcfs::new(Interval::new(nb_machines_used, self.nb_resources - 1)));
        self.rejection_scheduler = scheduler.get_uuid();
        self.schedulers_map
            .insert(scheduler.get_uuid(), scheduler);

        // We tell batsim that it does not need to wait for us
        Some(vec![notify_event(*timestamp, String::from("submission_finished"))])
    }

    fn on_job_submission(&mut self,
                         timestamp: &f64,
                         job: Job,
                         profile: Option<Profile>)
                         -> Option<Vec<BatsimEvent>> {

        if job.res > self.max_job_size as i32 {
            trace!("Job too big: {:?}", job);
            return Some(vec![reject_job_event(*timestamp, &job)]);
        }

        // We save the profile of the new job
        match profile {
            Some(p) => {
                self.profiles.insert(job.profile.clone(), p);
            }
            None => panic!("Did you forget to activate the profile forwarding ?"),
        }

        trace!("Get a new job(={:?}) with size {}", job, job.res);
        let job_rc = Rc::new(job);
        let shared_allocation = Rc::new(Allocation::new(job_rc.clone()));

        self.jobs
            .insert(shared_allocation.job.id.clone(), shared_allocation.clone());

        self.register_schedulers(shared_allocation.clone());
        self.schedulers_for(shared_allocation.clone(),
                            &mut |scheduler| scheduler.add_job(shared_allocation.clone()));
        None
    }

    fn on_job_completed(&mut self, _: &f64, job_id: String, _: String) -> Option<Vec<BatsimEvent>> {
        trace!("Job completed: {}", job_id);
        // Finished job is an `Allocation`
        let finished_job = self.jobs
            .get(&job_id)
            .ok_or("No job registered with this id")
            .unwrap()
            .clone();

        self.schedulers_for(finished_job.clone(),
                            &mut |scheduler| {
                                     scheduler.job_finished(finished_job.job.id.clone());
                                 });

        None
    }

    fn on_simulation_ends(&mut self, timestamp: &f64) {
        info!("Simulation ends: {}", timestamp);
    }

    fn on_job_killed(&mut self, timestamp: &f64, jobs: Vec<String>) -> Option<Vec<BatsimEvent>> {
        let mut events: Vec<BatsimEvent> = vec![];
        for job_id in jobs {
            trace!("Job {} has been succesfully killed", &job_id);
            let killed_job = self.jobs
                .get(&job_id)
                .ok_or("No job registered with this id")
                .unwrap()
                .clone();

            self.schedulers_for(killed_job.clone(),
                                &mut |scheduler| scheduler.job_killed(job_id.clone()));

            let profile: Option<&Profile> = self.profiles.get(&killed_job.job.profile);

            let resub = self.rejected_jobs
                .get(&MetaScheduler::job_id_to_rej_id(&job_id))
                .unwrap();

            trace!("Submit job {:?}", resub.resub_job);
            events.push(submit_job_event(*timestamp, &resub.resub_job, profile));
        }
        Some(events)
    }

    fn on_message_received_end(&mut self, timestamp: &mut f64) -> Option<Vec<BatsimEvent>> {
        // All events that will be send to batsim (all allocation that are ready)
        let mut events: Vec<BatsimEvent> = vec![];
        let mut all_allocations: Vec<Rc<Allocation>> = vec![];

        //We call a standar scheduler on each sub groups
        let (jobs, rejected) = self.schedule_jobs();
        let bf = self.easy_backfilling();

        all_allocations.extend(jobs);
        all_allocations.extend(bf);

        events.extend(MetaScheduler::allocations_to_batsim_events(self.time, all_allocations));

        if !rejected.is_empty() {
            events.push(batsim::kill_jobs_event(self.time,
                                                rejected
                                                    .iter()
                                                    .map(|alloc| &*alloc.job)
                                                    .collect()));
        }

        trace!("Respond to batsim at: {} with {} events",
               timestamp,
               events.len());

        Some(events)
    }

    fn on_message_received_begin(&mut self, timestamp: &f64) -> Option<Vec<BatsimEvent>> {
        trace!("Received new batsim message at {}", timestamp);
        None
    }
}

impl MetaScheduler {
    fn easy_backfilling(&mut self) -> Vec<Rc<Allocation>> {
        let mut all_allocations: HashSet<Rc<Allocation>> = HashSet::new();

        for scheduler_id in &self.schedulers {
            let mut scheduler = self.schedulers_map
                .get_mut(scheduler_id)
                .expect("No scheduler for the given uuid");

            let mut allocations = scheduler.easy_back_filling(self.time);
            // We arbitrary get one job (if there is one)
            match allocations.pop() {
                Some(alloc) => {
                    all_allocations.insert(alloc);
                }
                _ => {}
            }
        }

        let time: f64 = self.time;
        for alloc in &all_allocations {
            self.schedulers_for(alloc.clone(),
                                &mut |scheduler| {
                                         scheduler.job_backfilled(time, alloc.job.id.clone());
                                     });
        }
        all_allocations.into_iter().collect()
    }

    fn schedule_rejection_scheduler(&mut self) -> Vec<Rc<Allocation>> {
        let mut all_allocations: Vec<Rc<Allocation>> = vec![];

        {
            let mut scheduler = self.schedulers_map
                .get_mut(&self.rejection_scheduler)
                .expect("No scheduler for the given uuid");

            let cb = |j: Rc<Allocation>| false;
            let mut rejected_jobs: Vec<Rc<Allocation>> = vec![];
            let (allocations, rejected) = scheduler.schedule_jobs(self.time, &cb);
            match allocations {
                Some(allocs) => {
                    all_allocations.extend(allocs);
                }
                _ => {}
            }

            match rejected {
                Some(job_id) => panic!("This scheduler is not allowed to reject jobs"),
                None => {}
            }
        }

        all_allocations
    }

    fn schedule_jobs(&mut self) -> (Vec<Rc<Allocation>>, Vec<Rc<Allocation>>) {
        let mut all_allocations: HashSet<Rc<Allocation>> = HashSet::new();
        let mut all_delayed_allocations: HashSet<Rc<Allocation>> = HashSet::new();

        let mut rejected_jobs: Vec<Rc<Allocation>> = vec![];

        let dang = self.nb_dangling_resources;
        let cb = |j: Rc<Allocation>| j.job.res <= dang;

        // In the first place we call for a normal schedule on each scheduler
        for scheduler_id in &self.schedulers {
            let mut scheduler = self.schedulers_map
                .get_mut(scheduler_id)
                .expect("No scheduler for the given uuid");

            let (allocations, rejected) = scheduler.schedule_jobs(self.time, &cb);
            match allocations {
                Some(allocs) => {
                    all_allocations.extend(allocs);
                }
                _ => {}
            }

            match rejected {
                Some(job_id) => {
                    rejected_jobs.push(self.jobs.get(&job_id).unwrap().clone());
                }
                None => {}
            }
        }

        all_allocations.extend(self.schedule_rejection_scheduler());

        for reject in &rejected_jobs {
            trace!("Job {:?} has been rejected", reject);
            let rej_job = MetaScheduler::construct_rejected_job(reject.clone());
            self.rejected_jobs
                .insert(rej_job.job_id.clone(), Rc::new(rej_job));
        }

        let (ready_allocations, delayed_allocations): (Vec<Rc<Allocation>>,
                                                       Vec<Rc<Allocation>>) =
            all_allocations
                .into_iter()
                .partition(|alloc| alloc.nb_of_res_to_complete() == 0);

        let time: f64 = self.time;
        for delayed_allocation in &delayed_allocations {
            delayed_allocation.update_sheduled_launched_time(time);
            self.schedulers_for(delayed_allocation.clone(),
                                &mut |scheduler| {
                                         trace!("Revert allocation for job: {}",
                                                delayed_allocation.job.id);
                                         scheduler.job_waiting(time, delayed_allocation.clone())
                                     });
        }

        // We notify every scheduler wereas we launch one
        // of the jobs.
        for ready_allocation in &ready_allocations {
            self.schedulers_for(ready_allocation.clone(),
                                &mut |scheduler| {
                                         scheduler.job_launched(time, ready_allocation.clone())
                                     });
        }

        (ready_allocations.into_iter().collect(), rejected_jobs)
    }

    fn schedulers_for(&mut self,
                      allocation: Rc<Allocation>,
                      func: &mut FnMut(&mut Box<SubScheduler>)) {

        for scheduler_id in &mut allocation.running_groups.borrow().iter() {
            let scheduler = self.schedulers_map.get_mut(&scheduler_id).unwrap();
            func(scheduler);
        }
    }

    fn register_schedulers(&mut self, allocation: Rc<Allocation>) {
        let job = allocation.job.clone();
        let (w_id, j_id) = Job::split_id(&job.id);

        match w_id.as_ref() {
            "rej!" => {
                trace!("Rejected job, register rej sched {}",
                       self.rejection_scheduler);
                allocation.add_group(self.rejection_scheduler.clone());
            }
            _ => {
                if job.res <= self.greater_grp_size as i32 {
                    let grp_idx = MetaScheduler::get_job_grp(&job);
                    let uuid = self.schedulers.get(grp_idx).unwrap();
                    let scheduler = self.schedulers_map
                        .get_mut(&uuid)
                        .expect("This is a bug :)");
                    scheduler.register_to_allocation(allocation.clone());
                } else {
                    let nm_res = job.res;
                    // If the jobs do not fit into any of the sub schedulers
                    // we send it to schedulers till we have enought cores.
                    let mut iter_sched = self.schedulers.iter_mut().enumerate().rev();
                    let mut sched = iter_sched.next();
                    let mut cores_remaining = job.res;

                    trace!("job res: {}", allocation.job.res);
                    while cores_remaining > 0 {
                        match sched {
                            Some(mut idx_scheduler) => {
                                let scheduler = self.schedulers_map
                                    .get_mut(idx_scheduler.1)
                                    .expect("This is a bug :0");

                                allocation.add_group(scheduler.get_uuid());
                                cores_remaining -= 2i32.pow(idx_scheduler.0 as u32);
                            }
                            None => {
                                if cores_remaining > 0 {
                                    trace!("Job also needs resources from rej: {:?} - {}",
                                           allocation,
                                           self.rejection_scheduler.clone());
                                    allocation.add_group(self.rejection_scheduler.clone());
                                }
                                break;
                            }
                        }
                        sched = iter_sched.next();
                    }
                }
            }
        }
    }

    fn job_id_to_rej_id(id: &String) -> String {
        let (_, job_id) = Job::split_id(&id);
        format!("rej!{}", job_id)
    }

    fn construct_rejected_job(allocation: Rc<Allocation>) -> RejectedJob {
        let (_, job_id) = Job::split_id(&allocation.job.id);

        let mut resub_job: Job = (*allocation.job).clone();
        resub_job.id = format!("rej!{}", job_id);

        trace!("{:?}", resub_job);
        RejectedJob {
            job_id: resub_job.id.clone(),
            initial_job: Rc::new((*allocation.job).clone()),
            resub_job: Rc::new(resub_job.clone()),
            ack_received: Cell::new(false),
            finished: Cell::new(false),
        }
    }

    fn get_job_grp(job: &Job) -> usize {
        (job.res as f64).log(2_f64).ceil() as usize
    }

    fn allocations_to_batsim_events(now: f64, allocation: Vec<Rc<Allocation>>) -> Vec<BatsimEvent> {
        allocation
            .into_iter()
            .map(|alloc| {
                     return allocate_job_event(now,
                                               &*alloc.job,
                                               format!("{}", *alloc.nodes.borrow()));
                 })
            .collect()
    }
}
