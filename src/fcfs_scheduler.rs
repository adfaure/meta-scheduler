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
    whole_resources: IntervalSet,
    /// FCFS queue of job
    jobs_queue: LinkedList<Rc<Allocation>>,
    /// Jobs that are currently running with their exepecting finished time
    jobs_running: Vec<(Rc<Allocation>, f64)>,
    jobs_waiting: Vec<(Rc<Allocation>, IntervalSet)>,
    max_size: u32,
}

impl SubSchedulerFcfs {
    pub fn new(resources: Interval) -> Self {
        info!("Init fcfs sched with {:?}", resources);
        let max = resources.range_size() as u32;

        SubSchedulerFcfs {
            uuid: Uuid::new_v4(),
            resources: resources.clone().to_interval_set(),
            whole_resources: resources.to_interval_set(),
            jobs_queue: LinkedList::new(),
            jobs_running: vec![],
            jobs_waiting: vec![],
            max_size: max,
        }
    }

    fn find_job_allocation(&self, resources: &IntervalSet, job_size: u32) -> Option<IntervalSet> {
        let current_available_size = resources.size();
        if current_available_size < (job_size as u32) {
            return None;
        }

        let mut iter = resources.iter();
        let mut allocation = IntervalSet::empty();
        let mut left = job_size as u32;

        let mut interval = iter.next().unwrap();
        while allocation.size() != (job_size as u32) {
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

    fn schedule_jobs(&mut self,
                     time: f64,
                     is_rejection_possible: &Fn(Rc<Allocation>) -> bool)
                     -> (Option<Vec<Rc<Allocation>>>, Option<String>) {

        let mut res: Vec<Rc<Allocation>> = Vec::new();
        let mut optional = self.jobs_queue.pop_front();

        let mut current_resources = self.resources.clone();

        while let Some(alloc) = optional {

            let resa_size: u32 = alloc.nb_of_res_to_complete();
            match self.find_job_allocation(&current_resources, resa_size as u32) {
                None => {
                    trace!("{} Cannot launch job={:?}", time, alloc);
                    self.jobs_queue.push_front(alloc);
                    optional = None;
                }
                Some(allocation) => {
                    alloc.add_resources(allocation.clone());

                    self.jobs_waiting
                        .push((alloc.clone(), allocation.clone()));

                    current_resources = current_resources.clone().difference(allocation.clone());
                    res.push(alloc.clone());

                    trace!("{} Launch job={:?} --- {:?}",
                           time,
                           alloc,
                           self.jobs_waiting);
                    optional = self.jobs_queue.pop_front();
                }
            }
        }
        (Some(res), None)
    }

    fn job_waiting(&mut self, time: f64, allocation: Rc<Allocation>) {
        //panic!("Not implemented");
        trace!("Jobs has been delayed: {:?} {:?}",
               allocation,
               self.jobs_waiting);
        let idx = self.jobs_waiting
            .iter()
            .position(|x| allocation.job.id == x.0.job.id);

        match idx {
            Some(pos) => {
                let alloc = self.jobs_waiting.remove(pos);
                alloc.0.remove_resources(alloc.1);
                self.jobs_queue.push_front(alloc.0.clone());
            }
            None => {}
        }
    }

    fn add_job(&mut self, allocation: Rc<Allocation>) {
        self.jobs_queue.push_back(allocation.clone());
    }

    fn job_finished(&mut self, finished_job: String) {
        //TODO Check if the job was running in this group
        let idx = self.jobs_running
            .iter()
            .position(|x| finished_job == x.0.job.id)
            .unwrap();

        let mut alloc = self.jobs_running.remove(idx);

        trace!("Jobs has been completed: {:?}", alloc);
        self.resources = self.resources
            .clone()
            .union(self.whole_resources
                       .clone()
                       .intersection(alloc.0.nodes.borrow().clone()));

        self.jobs_running.retain(|x| finished_job != x.0.job.id);
    }

    fn job_killed(&mut self, killed_job: String) {
        panic!("Not implememted");
    }

    fn job_launched(&mut self, time: f64, alloc: Rc<Allocation>) {
        // retrieve and remove the job from the waiting queue.
        trace!("Jobs has been launched: {} on {}", alloc.job.id, self.uuid);
        let idx = self.jobs_waiting
            .iter()
            .position(|x| alloc.job.id == x.0.job.id)
            .unwrap();

        let alloc = self.jobs_waiting.remove(idx);

        self.resources = self.resources.clone().difference(alloc.1.clone());
        self.jobs_running
            .push((alloc.0.clone(), time + alloc.0.job.walltime));
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
