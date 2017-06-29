use batsim::json_protocol::Job;
use interval_set::IntervalSet;
use std::cell::RefCell;

#[derive(Clone, Debug)]
pub struct Allocation {
    pub job: Job,
    pub nodes: RefCell<IntervalSet>,
}

impl Allocation {
    pub fn nb_of_res_to_complete(&self) -> u32 {
        self.job.res as u32 - self.nodes.borrow().size()
    }

    pub fn add_resources(&mut self, interval: IntervalSet) {
        // We copy the curent refcell interval
        // to be able to call ``into_inner` and finally clone the interval
        let temp_interval: IntervalSet = self.nodes.clone().into_inner().clone();

        *self.nodes.get_mut() = interval.union(temp_interval);
    }
}
