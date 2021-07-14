use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone)]
pub struct SharedBuildProgress {
    state: Rc<RefCell<BuildProgress>>,
}

impl SharedBuildProgress {
    pub(super) fn new(state: BuildProgress) -> Self {
        Self { state: Rc::new(RefCell::new(state)) }
    }

    pub fn borrow(&self) -> Ref<'_, BuildProgress> {
        self.state.borrow()
    }

    pub(super) fn borrow_mut(&self) -> RefMut<'_, BuildProgress> {
        self.state.borrow_mut()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum BuildStage {
    BuildingTaskList,
    BuildingOperations,
    BuildingPackage,
}

#[derive(Debug)]
pub struct BuildProgress {
    /// Per worker progression (not empty and len is stable)
    pub workers: Box<[BuildWorkerProgress]>,

    pub stage: BuildStage,

    /// Current number of bytes processed
    pub processed_bytes: u64,
    /// Number of bytes to process
    pub process_bytes: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum BuildTaskStage {
    Init,
}

#[derive(Debug, Clone)]
pub struct BuildWorkerProgress {
    /// Current task name
    pub task_name: Arc<str>,
    /// Current number of bytes processed
    pub processed_bytes: u64,
    /// Number of bytes to process
    pub process_bytes: u64,
}
