use common::Allocation;
use std::rc::Rc;
use interval_set::{Interval, ToIntervalSet};
use std::cmp;

pub struct SubScheduler {
    /// Current free resources
    resources: Interval,
    /// FCFS queue of job
    jobs_queue: Vec<Rc<Allocation>>,
    /// Jobs that are currently running
    jobs_running: Vec<Rc<Allocation>>,
    /// Jobs that are currently waiting to be lauched by the
    /// by the mete scheduler
    jobs_waiting: Vec<Rc<Allocation>>
}

impl SubScheduler {

    pub fn new(resources: Interval) -> SubScheduler {
        SubScheduler{
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
        if self.jobs_running.len() == 0 {
            let interval;
            match self.jobs_queue.pop() {
                Some(mut next_allocation) => {
                    if next_allocation.job.res <= self.resources.range_size() as i32 {
                        // In this case the jobs can be lauched entirely into
                        // the ressources of this scheduler.
                        let nb_resources = cmp::min(self.resources.range_size(), next_allocation.job.res as u32);
                        interval = Interval::new(self.resources.get_inf(), nb_resources);
                    } else {
                        let ressources_to_complete = cmp::min(self.resources.range_size(), next_allocation.nb_of_res_to_complete());
                        interval = Interval::new(self.resources.get_inf(), ressources_to_complete);
                    }
                    // We upate the allocation with the resources
                    // allocated byt the scheduler
                    Rc::get_mut(&mut next_allocation).unwrap().add_resources(interval.to_interval_set());

                    self.jobs_waiting.push(next_allocation.clone());
                    return (Some(vec![next_allocation.clone()]), None);
                },
                None => {
                    // There is no jobs waiting
                }
            }
        }
        (None, None)
    }

    /// Add an empty allocation to the queue of the sub scheduler
    /// The job is wrapped by an allocation so we can share it through
    /// several schedulers.
    pub fn add_job(&mut self, allocation: Rc<Allocation>) {
        self.jobs_queue.push(allocation.clone());
    }

    /// Notify the sub scheduler that the job with id `finished_job`
    /// is done.
    pub fn job_finished(&mut self, finished_job: String) {
        //TODO Check if the job was running in this group
        self.jobs_running.retain(|x| finished_job != x.job.id);
    }

    /// Notify the sub scheduler whenever a job has been killed.
    pub fn job_killed(&mut self, killed_job: String) {
        //TODO Check if the job was running in this group
        self.jobs_running.retain(|x| killed_job != x.job.id);
    }

    /// Notify the sub scheduler that a job has been lauched and thus voila
    pub fn job_launched(&mut self, job: String) {
        // retrieve and remove the job from the waiting queue.
        let idx = self.jobs_waiting.iter().position(|x| job == x.job.id).unwrap();
        let alloc = self.jobs_waiting.remove(idx);

        self.jobs_running.push(alloc);
    }
}
