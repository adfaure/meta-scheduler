use batsim::json_protocol::Job;
use interval_set::IntervalSet;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;

#[derive(Clone, Debug)]
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
    pub running_groups: RefCell<Vec<Uuid>>
}

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
        trace!("Removed {:?} \t resources: {:?} \t Resources left {}", interval, self, self.nb_of_res_to_complete());
    }

    /// This function is in fact mutable because we take advantage
    /// of the `RefCell` at this point.
    pub fn add_resources(&self, interval: IntervalSet) {
        // We copy the curent refcell interval
        // to be able to call `into_inner` and finally clone the interval
        let temp_interval: IntervalSet = self.nodes.clone().into_inner().clone();
        *self.nodes.borrow_mut() = interval.union(temp_interval);

        if self.running_groups.borrow().len() > 1 {
            info!("Add resources: {:?} - Resources left {}", self, self.nb_of_res_to_complete());
        }
    }

    pub fn add_group(&self, uuid: Uuid) {
        let mut hashset = self.running_groups.borrow_mut();
        hashset.push(uuid);
    }

    pub fn may_update_sheduled_lauched_time(&self, time: f64) {
        if time > self.sheduled_lauched_time.get() {
            self.sheduled_lauched_time.set(time);
        }
    }
}
