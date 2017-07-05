use batsim::json_protocol::Job;
use interval_set::IntervalSet;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct Allocation {
    pub job: Rc<Job>,
    pub nodes: RefCell<IntervalSet>,
}

impl Allocation {
    pub fn new(job: Rc<Job>) -> Allocation {
        Allocation {
            job: job.clone(),
            nodes: RefCell::new(IntervalSet::empty()),
        }
    }

    pub fn nb_of_res_to_complete(&self) -> u32 {
        self.job.res as u32 - self.nodes.borrow().size()
    }

    pub fn add_resources(&mut self, interval: IntervalSet) {
        // We copy the curent refcell interval
        // to be able to call `into_inner` and finally clone the interval
        let temp_interval: IntervalSet = self.nodes.clone().into_inner().clone();

        println!("before {:?} {:p} ---------------\t {:?}", self, &self, temp_interval);
        *self.nodes.borrow_mut() = interval.union(temp_interval);

        let temp_interval: IntervalSet = self.nodes.clone().into_inner().clone();
        println!("before {:?} {:p} ---------------\t {:?}", self, &self, temp_interval);
    }
}
