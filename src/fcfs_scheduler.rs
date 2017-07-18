use common::{Allocation, SubScheduler};
use std::collections::LinkedList;
use std::collections::VecDeque;
use std::rc::Rc;
use interval_set::{Interval, ToIntervalSet, IntervalSet};
use std::cmp;
use uuid::Uuid;
use std::fmt;
use batsim::*;

pub struct SubSchedulerFcfs {
    /// Unique identifier for a sub scheduler
    pub uuid: Uuid,
    /// Current free resources
    resources: IntervalSet,
    /// FCFS queue of job
    jobs_queue: LinkedList<Rc<Allocation>>,
    /// Jobs that are currently running with their exepecting finished time
    jobs_running: Vec<(Rc<Allocation>, f64)>,
}

impl SubSchedulerFcfs {
    pub fn new(resources: Interval) -> Self {
        info!("Init fcfs sched with {:?}", resources);
        SubSchedulerFcfs {
            uuid: Uuid::new_v4(),
            resources: resources.to_interval_set(),
            jobs_queue: LinkedList::new(),
            jobs_running: vec![],
        }
    }

    fn find_job_allocation(&self, job: &Job) -> Option<IntervalSet> {
        let current_available_size = self.resources.size();
        if current_available_size < (job.res as u32) {
            trace!("No allocation possible yet for the job {} (nb res={}) (size available={})",
                   job.id,
                   job.res,
                   current_available_size);
            return None;
        }

        trace!("Try to allocate Job={} res={}", job.id, job.res);

        let mut iter = self.resources.iter();
        let mut allocation = IntervalSet::empty();
        let mut left = job.res as u32;

        let mut interval = iter.next().unwrap();
        while allocation.size() != (job.res as u32) {
            // Note that we test earlier in the function if the interval
            // has enought resources, so this loop should not fail.
            let interval_size = interval.range_size();

            if interval_size > left {
                allocation.insert(Interval::new(interval.get_inf(), interval.get_inf() + left - 1));
            } else if interval_size == left {
                allocation.insert(interval.clone());
            } else if interval_size < left {
                allocation.insert(interval.clone());
                left -= interval_size;
                interval = iter.next().unwrap();
            }
        }

        Some(allocation)
    }
}

impl SubScheduler for SubSchedulerFcfs {
    fn easy_back_filling(&mut self, current_time: f64) -> Vec<Rc<Allocation>> {
        panic!("Not implememted");
    }

    fn schedule_jobs(&mut self, time: f64) -> (Option<Vec<Rc<Allocation>>>, Option<String>) {
        let mut res: Vec<Rc<Allocation>> = Vec::new();
        let mut optional = self.jobs_queue.pop_front();
        while let Some(alloc) = optional {
            match self.find_job_allocation(&alloc.job) {
                None => {
                    trace!("Cannot launch job={} now", alloc.job);
                    self.jobs_queue.push_front(alloc);
                    optional = None;
                }
                Some(allocation) => {
                    info!("Launch job={} now", alloc.job);
                    alloc.add_resources(allocation.clone());
                    optional = self.jobs_queue.pop_front();

                    self.resources = self.resources.clone().difference(allocation.clone());
                    self.jobs_running
                        .push((alloc.clone(), time + alloc.job.walltime));

                    assert!(alloc.nb_of_res_to_complete() == 0);
                    res.push(alloc);
                }
            }
        }
        (Some(res), None)
    }

    fn job_waiting(&mut self, time: f64, allocation: Rc<Allocation>) {
        panic!("Not implemented");
    }

    fn add_job(&mut self, allocation: Rc<Allocation>) {
        self.jobs_queue.push_back(allocation.clone());
    }

    fn job_finished(&mut self, finished_job: String) {
        //TODO Check if the job was running in this group
        trace!("Jobs has been completed: {}", finished_job);
        let idx = self.jobs_running
            .iter()
            .position(|x| finished_job == x.0.job.id)
            .unwrap();

        let mut alloc = self.jobs_running.remove(idx);

        self.resources = self.resources
            .clone()
            .union(alloc.0.nodes.borrow().clone());
        self.jobs_running.retain(|x| finished_job != x.0.job.id);
    }

    fn job_killed(&mut self, killed_job: String) {
        panic!("Not implememted");
    }

    fn job_launched(&mut self, time: f64, job: String) {
        // retrieve and remove the job from the waiting queue.
        trace!("Jobs has been launched: {} on {}", job, self.uuid);
    }

    fn job_backfilled(&mut self, time: f64, job: String) {
        panic!("Not implememted");
    }

    fn job_revert_allocation(&self, allocation: Rc<Allocation>) {
        panic!("Not implememted");
    }

    fn register_to_allocation(&self, allocation: Rc<Allocation>) {
        panic!("Not implememted");
    }

    /// Fil the resources for the given allocation
    fn allocate_job(&self, allocation: Rc<Allocation>) -> Rc<Allocation> {
        panic!("Not implememted");
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid.clone()
    }
}
