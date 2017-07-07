use common::Allocation;
use std::collections::LinkedList;
use std::rc::Rc;
use interval_set::{Interval, ToIntervalSet};
use std::cmp;

#[derive(Debug)]
pub struct SubScheduler {
    /// Current free resources
    resources: Interval,
    /// FCFS queue of job
    jobs_queue: LinkedList<Rc<Allocation>>,
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
            jobs_queue: LinkedList::new(),
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
            let interval;
            match self.jobs_queue.pop_front() {
                Some(mut next_allocation) => {
                    if next_allocation.job.res <= self.resources.range_size() as i32 {
                        // In this case the jobs can be lauched entirely into
                        // the ressources of this scheduler.
                        let nb_resources = cmp::min(self.resources.range_size(),
                                                    next_allocation.job.res as u32);
                        interval = Interval::new(self.resources.get_inf(),
                                                 self.resources.get_inf() + nb_resources - 1);
                    } else {
                        let ressources_to_complete = cmp::min(self.resources.range_size(),
                                                              next_allocation
                                                                  .nb_of_res_to_complete());
                        interval = Interval::new(self.resources.get_inf(),
                                                 self.resources.get_inf() + ressources_to_complete -
                                                 1);
                    }
                    // We upate the allocation with the resources
                    // allocated by the scheduler
                    next_allocation.add_resources(interval.to_interval_set());

                    self.jobs_waiting.push(next_allocation.clone());
                    return (Some(vec![next_allocation.clone()]), None);
                }
                None => {
                    // There is no jobs waiting
                }
            }
        }
        (None, None)
    }

    pub fn after_schedule(&mut self, time: f64) -> Option<Vec<Rc<Allocation>>> {
        None
    }

    pub fn calculate_expected_release_date(&mut self, time: f64, allocation: Rc<Allocation>) {
        let mut cummulative_time = time;
        for alloc in &self.jobs_queue {
            if alloc.job.id == allocation.job.id {
                alloc.may_update_sheduled_lauched_time(cummulative_time);
                break;
            }
            time + alloc.job.walltime;
        }
    }

    /// Add an empty allocation to the queue of the sub scheduler
    /// The job is wrapped by an allocation so we can share it through
    /// several schedulers.
    pub fn add_job(&mut self, allocation: Rc<Allocation>) {
        trace!("Jobs has been added: {:?}", allocation);
        self.jobs_queue.push_back(allocation.clone());
    }

    /// Notify the sub scheduler that the job with id `finished_job`
    /// is done.
    pub fn job_finished(&mut self, finished_job: String) {
        //TODO Check if the job was running in this group
        trace!("Jobs has been completed: {}", finished_job);
        self.jobs_running.retain(|x| finished_job != x.job.id);
    use std::collections::LinkedList;}

    /// Notify the sub scheduler whenever a job has been killed.
    pub fn job_killed(&mut self, killed_job: String) {
        //TODO Check if the job was running in this group
        self.jobs_running.retain(|x| killed_job != x.job.id);
    }

    /// Notify the sub scheduler that a job has been lauched and thus voila
    pub fn job_launched(&mut self, time: f64, job: String) {
        // retrieve and remove the job from the waiting queue.
        trace!("Jobs has been launched: {}", job);
        let idx = self.jobs_waiting
            .iter()
            .position(|x| job == x.job.id)
            .unwrap();
        let alloc = self.jobs_waiting.remove(idx);

        let ends_at = time + alloc.job.walltime;

        match self.jobs_queue.front() {
            Some(future_alloc) => {
                future_alloc.may_update_sheduled_lauched_time(ends_at);
            },
            None => {}
        }
        self.jobs_running.push(alloc);
    }
}
