use batsim::json_protocol::Job;
use interval_set::*;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use std::hash::{Hash, Hasher};
use std::fmt;

pub trait SubScheduler {
    fn easy_back_filling(&mut self, current_time: f64) -> Vec<Rc<Allocation>>;
    fn get_uuid(&self) -> Uuid;
    fn schedule_jobs(&mut self, time: f64) -> (Option<Vec<Rc<Allocation>>>, Option<String>);
    fn job_waiting(&mut self, time: f64, allocation: Rc<Allocation>);
    fn add_job(&mut self, allocation: Rc<Allocation>);
    fn job_finished(&mut self, finished_job: String);
    fn job_killed(&mut self, killed_job: String);
    fn job_launched(&mut self, time: f64, job: String);
    fn job_backfilled(&mut self, time: f64, job: String);
    fn job_revert_allocation(&self, allocation: Rc<Allocation>);
    fn register_to_allocation(&self, allocation: Rc<Allocation>);
    fn allocate_job(&self, allocation: Rc<Allocation>) -> Rc<Allocation>;
}

#[derive(Clone)]
/// An allocation wrap a
/// job with data such as the ressources allocated
/// and the time at which the jobs may starts.
/// `job`: The job is the batsim job
/// `nodes`: the current allocation.
/// `sheduled_lauched_time`: the latest release date.
pub struct Allocation {
    pub job: Rc<Job>,
    pub nodes: RefCell<IntervalSet>,
    //TODO `sheduled_lauched_time` Does not seems very english
    pub sheduled_lauched_time: Cell<f64>,
    pub running_groups: RefCell<Vec<Uuid>>,
}

impl fmt::Debug for Allocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "job: {}\t", self.job.id);
        write!(f,
               "interval: {} - missing: {}",
               self.nodes.borrow().clone(),
               self.nb_of_res_to_complete())
    }
}

impl Hash for Allocation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.job.id.hash(state);
    }
}

impl PartialEq for Allocation {
    fn eq(&self, other: &Allocation) -> bool {
        self.job.id == other.job.id
    }
}

impl Eq for Allocation {}

impl Allocation {
    pub fn new(job: Rc<Job>) -> Allocation {
        Allocation {
            job: job.clone(),
            nodes: RefCell::new(IntervalSet::empty()),
            sheduled_lauched_time: Cell::default(),
            running_groups: RefCell::new(vec![]),
        }
    }

    pub fn nb_of_res_to_complete(&self) -> u32 {
        self.job.res as u32 - self.nodes.borrow().size()
    }

    pub fn remove_resources(&self, interval: IntervalSet) {
        // We copy the curent refcell interval
        // to be able to call `into_inner` and finally clone the interval
        let temp_interval: IntervalSet = self.nodes.clone().into_inner().clone();
        *self.nodes.borrow_mut() = temp_interval.difference(interval.clone());
    }

    /// This function is in fact mutable because we take advantage
    /// of the `RefCell` at this point.
    pub fn add_resources(&self, interval: IntervalSet) {
        // We copy the curent refcell interval
        // to be able to call `into_inner` and finally clone the interval
        let temp_interval: IntervalSet = self.nodes.clone().into_inner().clone();
        *self.nodes.borrow_mut() = interval.union(temp_interval);

        if self.running_groups.borrow().len() > 1 {
            trace!("Add resources: {:?} - Resources left {}",
                   self,
                   self.nb_of_res_to_complete());
        }
    }

    pub fn add_group(&self, uuid: Uuid) {
        let mut hashset = self.running_groups.borrow_mut();
        hashset.push(uuid);
    }

    pub fn update_sheduled_launched_time(&self, time: f64) {
        self.sheduled_lauched_time.set(time);
    }

    pub fn may_update_sheduled_lauched_time(&self, time: f64) {
        if time > self.sheduled_lauched_time.get() {
            self.sheduled_lauched_time.set(time);
        }
    }
}
