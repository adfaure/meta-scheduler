use common::Allocation;
use std::collections::LinkedList;
use std::collections::VecDeque;
use std::rc::Rc;
use interval_set::{Interval, ToIntervalSet};
use std::cmp;
use uuid::Uuid;
use std::fmt;

pub struct SubScheduler {
    /// Unique identifier for a sub scheduler
    pub uuid: Uuid,
    /// Current free resources
    resources: Interval,
    /// FCFS queue of job
    jobs_queue: Vec<Rc<Allocation>>,
    /// Jobs that are currently running with their exepecting finished time
    jobs_running: Vec<(Rc<Allocation>, f64)>,
    /// Jobs that are currently waiting to be lauched by the
    /// by the meta scheduler
    jobs_waiting: Vec<Rc<Allocation>>,
}

impl fmt::Debug for SubScheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.jobs_running.first() {
            Some(job) => write!(f, "jobs running: {} \t", job.0.job.id),
            None => write!(f, "Jobs running: None \t"),
        };
        match self.jobs_waiting.first() {
            Some(job) => write!(f, "jobs waiting: {} \t", job.job.id),
            None => write!(f, "Jobs waiting: None"),
        };
        write!(f, "")
    }
}

impl SubScheduler {
    pub fn new(resources: Interval) -> SubScheduler {
        SubScheduler {
            uuid: Uuid::new_v4(),
            resources: resources,
            jobs_queue: vec![],
            jobs_running: vec![],
            jobs_waiting: vec![],
        }
    }

    pub fn easy_back_filling(&mut self, current_time: f64) -> Vec<Rc<Allocation>> {
        //If the group is already busy, we dont need to backfill
        if !self.jobs_running.is_empty() {
            return vec![];
        }

        // If one job is currently waiting
        // we will try to backfill a job
        match self.jobs_waiting.get(0) {
            Some(job) => {
                trace!("{} waiting job is {:?}", self.uuid, job);
                // The free time between the scheduled starting time of the job in the queue.
                let free_time = job.sheduled_lauched_time.get() - current_time;
                // We collect every jobs that will not take longer that the
                // `free_time`, and that can be launch into a single scheduler.
                self.jobs_queue
                    .iter()
                    .filter(|alloc| {
                                alloc.job.res <= self.resources.range_size() as i32 &&
                                alloc.job.walltime < free_time
                            })
                    .map(|alloc| alloc.clone())
                    .collect()
            }
            _ => vec![],
        }
    }

    /// Function called at every iteration by the meta scheduler
    /// in order to get next job to schedule, and possibly a rejected job
    /// In addition I think it is not possible to allocate a job
    /// and reject one another job in the same time.
    pub fn schedule_jobs(&mut self, time: f64) -> (Option<Vec<Rc<Allocation>>>, Option<String>) {
        // If no jobs are yet running on this scheduler
        // we will allocate the next job.
        if self.jobs_running.len() == 0 {
            if self.jobs_waiting.len() == 0 {
                match self.jobs_queue.pop() {
                    Some(mut next_allocation) => {
                        let alloc = self.allocate_job(next_allocation);
                        self.jobs_waiting.push(alloc.clone());

                        assert!(self.jobs_waiting.len() == 1);
                        return (Some(vec![alloc.clone()]), None);
                    }
                    None => {}
                }
            } else {
                match self.jobs_waiting.get(0) {
                    Some(job) => {
                        let alloc = self.allocate_job(job.clone());
                        return (Some(vec![alloc.clone()]), None);
                    }
                    None => {}
                }
            }
        } else {
            if self.jobs_queue.len() > 10 {
                trace!("We reject running job: {:?}", self.jobs_running.first().unwrap().0);
                return (None, Some(self.jobs_running.first().unwrap().0.job.id.clone()))
            }
        }
        (None, None)
    }

    /// Ask the scheduler to update/set the scheduled release date
    /// of the given allocation. The idea is to have a prediction of
    /// the future.
    pub fn job_waiting(&mut self, time: f64, allocation: Rc<Allocation>) {

        self.job_revert_allocation(allocation.clone());

        // When a jobs is not ready to run we update
        // the estimated time at which this job will be launch
        // this allow to back fill for example
        let mut cummulative_time = match self.jobs_running.get(0) {
            Some(&(_, ref expected_finish_time)) => *expected_finish_time,
            None => time,
        };

        match self.jobs_waiting
                  .iter()
                  .find(|job| job.job.id == allocation.job.id) {
            Some(waiting) => {
                allocation.may_update_sheduled_lauched_time(cummulative_time);
            }
            _ => {
                for alloc in &self.jobs_queue {
                    cummulative_time += alloc.job.walltime;
                    alloc.may_update_sheduled_lauched_time(cummulative_time);
                    if alloc.job.id == allocation.job.id {
                        break;
                    }
                }
            }
        }

    }

    /// Add an empty allocation to the queue of the sub scheduler
    /// The job is wrapped by an allocation so we can share it through
    /// several schedulers.
    pub fn add_job(&mut self, allocation: Rc<Allocation>) {
        trace!("Jobs has been added: {:?}", allocation);
        self.jobs_queue.insert(0, allocation.clone());
    }

    /// Notify the sub scheduler that the job with id `finished_job`
    /// is done.
    pub fn job_finished(&mut self, finished_job: String) {
        //TODO Check if the job was running in this group
        trace!("Jobs has been completed: {}", finished_job);
        assert!(self.jobs_running.len() == 1);
        self.jobs_running.retain(|x| finished_job != x.0.job.id);
        assert!(self.jobs_running.len() == 0);
    }

    /// Notify the sub scheduler whenever a job has been killed.
    pub fn job_killed(&mut self, killed_job: String) {
        //TODO Check if the job was running in this group
        assert!(self.jobs_running.len() == 1);
        self.jobs_running.retain(|x| killed_job != x.0.job.id);
        assert!(self.jobs_running.len() == 0);
    }

    /// notify the sub scheduler that a job has been lauched and thus voila
    pub fn job_launched(&mut self, time: f64, job: String) {
        // retrieve and remove the job from the waiting queue.
        trace!("Jobs has been launched: {} on {}", job, self.uuid);
        let idx = self.jobs_waiting
            .iter()
            .position(|x| job == x.job.id)
            .unwrap();

        let alloc = self.jobs_waiting.remove(idx);

        assert!(self.jobs_waiting.is_empty());
        assert!(job == alloc.job.id);
        assert!(self.jobs_running.len() == 0);

        self.jobs_running
            .push((alloc.clone(), time + alloc.job.walltime));
        assert!(self.jobs_running.len() == 1);
    }

    /// notify the sub scheduler that a job has been lauched from the waiting queue
    /// This is typically a situation of backfilling.
    /// The main difference with `job_lauched` is that the job is removing
    /// from the waiting queue instead of the waiting queue.
    /// The next differences is that the allocation must be already
    /// defined.
    pub fn job_backfilled(&mut self, time: f64, job: String) {
        // retrieve and remove the job from the waiting queue.
        trace!("[{}]Jobs has been backfilled: {}", self.uuid, job);
        let idx = self.jobs_queue
            .iter()
            .position(|x| job == x.job.id)
            .unwrap();

        let mut alloc = self.jobs_queue.remove(idx);
        alloc = self.allocate_job(alloc.clone());
        assert!(job == alloc.job.id);
        assert!(self.jobs_running.len() == 0);
        assert!(self.jobs_waiting.len() == 1);

        self.jobs_running
            .push((alloc.clone(), time + alloc.job.walltime));
        assert!(self.jobs_running.len() == 1);
    }

    pub fn job_revert_allocation(&self, allocation: Rc<Allocation>) {
        allocation.remove_resources(self.resources.clone().to_interval_set());
        assert!(allocation.nb_of_res_to_complete() > 0);
    }

    pub fn register_to_allocation(&self, allocation: Rc<Allocation>) {
        allocation.add_group(self.uuid.clone());
    }

    /// Fil the resources for the given allocation
    fn allocate_job(&self, allocation: Rc<Allocation>) -> Rc<Allocation> {
        let interval;

        // We ensure that the allocation is not already fullfilled
        assert!(allocation.nb_of_res_to_complete() > 0);
        if allocation.job.res <= self.resources.range_size() as i32 {
            // In this case the jobs can be lauched entirely into
            // the ressources of this scheduler.
            let nb_resources = cmp::min(self.resources.range_size(), allocation.job.res as u32);
            interval = Interval::new(self.resources.get_inf(),
                                     self.resources.get_inf() + nb_resources - 1);
        } else {
            let ressources_to_complete = cmp::min(self.resources.range_size(),
                                                  allocation.nb_of_res_to_complete());
            interval = Interval::new(self.resources.get_inf(),
                                     self.resources.get_inf() + ressources_to_complete - 1);
        }

        // We upate the allocation with the resources
        // allocated by the scheduler
        allocation.add_resources(interval.to_interval_set());
        allocation.clone()
    }

    pub fn is_busy(&self) -> bool {
        trace!("{} jobs running \t {} jobs waiting \t {} jobs in queue",
               self.jobs_running.len(),
               self.jobs_waiting.len(),

               self.jobs_queue.len());
        !self.jobs_running.is_empty()
    }
}
