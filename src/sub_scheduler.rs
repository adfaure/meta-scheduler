use common::Allocation;
use std::collections::LinkedList;
use std::collections::VecDeque;
use std::rc::Rc;
use interval_set::{Interval, ToIntervalSet};
use std::cmp;

#[derive(Debug)]
pub struct SubScheduler {
    /// Current free resources
    resources: Interval,
    /// FCFS queue of job
    jobs_queue: Vec<Rc<Allocation>>,
    /// Jobs that are currently running
    jobs_running: Vec<Rc<Allocation>>,
    /// Jobs that are currently waiting to be lauched by the
    /// by the mete scheduler
    jobs_waiting: Vec<Rc<Allocation>>,
}

impl SubScheduler {
    pub fn new(resources: Interval) -> SubScheduler {
        SubScheduler {
            resources: resources,
            jobs_queue: vec![],
            jobs_running: vec![],
            jobs_waiting: vec![],
        }
    }

    /// Function called at every iteration by the meta scheduler
    /// in order to get next job to schedule, and possibly a rejected job
    pub fn schedule_jobs(&mut self, time: f64) -> (Option<Vec<Rc<Allocation>>>, Option<String>) {
        // If no jobs are yet running on this scheduler
        // we will allocate the next job.
        if self.jobs_waiting.len() == 0 && self.jobs_running.len() == 0 {
            match self.jobs_queue.pop() {
                Some(mut next_allocation) => {
                    let alloc = self.allocate_job(next_allocation);
                    self.jobs_waiting.push(alloc.clone());
                    return (Some(vec![alloc.clone()]), None);
                }
                None => {
                    // There is no jobs waiting
                }
            }
        }
        (None, None)
    }

    /// This function will return all jobs that are elligible for the backfilling
    /// policy `EASY`.
    pub fn easy_backfill(&mut self, time: f64) -> Option<Vec<Rc<Allocation>>> {

        let mut bf_position: Option<usize> = None;

        // If no jobs are running yet on this group, but one job is waiting
        // we will try to backfill queued jobs.
        if self.jobs_running.len() == 0 {
            match self.jobs_waiting.get(0) {
                Some(waiting_alloc) => {
                    let free_running_time = waiting_alloc.sheduled_lauched_time.get() - time;

                    return Some(self.jobs_queue
                                    .iter()
                                    .enumerate()
                                    .filter_map(|alloc| if alloc.1.job.walltime <=
                                                           free_running_time {
                                                    return Some(alloc.1.clone());
                                                } else {
                                                    return None;
                                                })
                                    .collect());
                }
                None => return None,
            }
        }
        None
    }

    /// Ask the scheduler to update/set the scheduled release date
    /// of the given allocation. The idea is to have a prediction of
    /// the future.
    pub fn calculate_expected_release_date(&mut self, time: f64, allocation: Rc<Allocation>) {
        let mut cummulative_time = time;
        for alloc in &self.jobs_queue {
            cummulative_time += alloc.job.walltime;
            alloc.may_update_sheduled_lauched_time(cummulative_time);
            if alloc.job.id == allocation.job.id {
                break;
            }
        }
    }

    /// Add an empty allocation to the queue of the sub scheduler
    /// The job is wrapped by an allocation so we can share it through
    /// several schedulers.
    pub fn add_job(&mut self, allocation: Rc<Allocation>) {
        trace!("Jobs has been added: {:?}", allocation);
        self.jobs_queue.push(allocation.clone());
    }

    /// Notify the sub scheduler that the job with id `finished_job`
    /// is done.
    pub fn job_finished(&mut self, finished_job: String) {
        //TODO Check if the job was running in this group
        trace!("Jobs has been completed: {}", finished_job);
        self.jobs_running.retain(|x| finished_job != x.job.id);
    }

    /// Notify the sub scheduler whenever a job has been killed.
    pub fn job_killed(&mut self, killed_job: String) {
        //TODO Check if the job was running in this group
        self.jobs_running.retain(|x| killed_job != x.job.id);
    }

    /// notify the sub scheduler that a job has been lauched from the waiting queue
    /// This is typically a situation of backfilling.
    pub fn job_backfilled(&mut self, time: f64, job: String) {
        // retrieve and remove the job from the waiting queue.
        trace!("Jobs has been backfilled: {}", job);
        let idx = self.jobs_queue
            .iter()
            .position(|x| job == x.job.id)
            .unwrap();

        //        self.jobs_queue.sort_by(|a, b| a.job.subtime.partial_cmp(&b.job.subtime).unwrap());
        let alloc = self.jobs_queue.remove(idx);
        self.jobs_running.push(alloc);
    }

    /// notify the sub scheduler that a job has been lauched and thus voila
    pub fn job_launched(&mut self, time: f64, job: String) {
        // retrieve and remove the job from the waiting queue.
        trace!("Jobs has been launched: {}", job);
        let idx = self.jobs_waiting
            .iter()
            .position(|x| job == x.job.id)
            .unwrap();

        //        self.jobs_queue.sort_by(|a, b| a.job.subtime.partial_cmp(&b.job.subtime).unwrap());
        let alloc = self.jobs_waiting.remove(idx);
        self.jobs_running.push(alloc);
    }

    /// Fil the resources for the given allocation
    fn allocate_job(&self, allocation: Rc<Allocation>) -> Rc<Allocation> {
        let interval;
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
}
