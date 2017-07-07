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
    jobs_queue: VecDeque<Rc<Allocation>>,
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
            jobs_queue: VecDeque::new(),
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

        //
        let mut bf_position: Option<usize> = None;

        // If no jobs are running yet on this group, but one job is waiting
        // we will try to backfill queued jobs.
        if self.jobs_running.len() == 0 {
            match self.jobs_waiting.get(0) {
                Some(waiting_alloc) => {
                    let free_running_time = waiting_alloc.sheduled_lauched_time.get() - time;
                    match self.jobs_queue
                              .iter()
                              .enumerate()
                              .find(|alloc| alloc.1.job.walltime <= free_running_time) {
                        Some((pos, job)) => {
                            bf_position = Some(pos);
                        }
                        None => {}
                    }
                }
                None => {}
            }
        }

        //TODO This code is the same as the code from the schdeul functions
        match bf_position {
            Some(pos) => {
                let alloc = self.jobs_queue.remove(pos).unwrap().clone();
                let interval;

                if alloc.job.res <= self.resources.range_size() as i32 {
                    // In this case the jobs can be lauched entirely into
                    // the ressources of this scheduler.
                    let nb_resources = cmp::min(self.resources.range_size(),
                                                alloc.job.res as u32);
                    interval = Interval::new(self.resources.get_inf(),
                                             self.resources.get_inf() + nb_resources - 1);
                } else {
                    //panic!("We backfill a huge job, are you sure?");
                    let ressources_to_complete = cmp::min(self.resources.range_size(),
                                                          alloc.nb_of_res_to_complete());
                    interval = Interval::new(self.resources.get_inf(),
                                             self.resources.get_inf() + ressources_to_complete - 1);
                }
                alloc.add_resources(interval.to_interval_set());
                trace!("We backfill {:?}", alloc);
                self.jobs_waiting.push(alloc.clone());
                Some(vec![alloc.clone()])
            },
            _ => None,
        }
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
        self.jobs_queue.push_back(allocation.clone());
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
        self.jobs_running.push(alloc);
    }
}
