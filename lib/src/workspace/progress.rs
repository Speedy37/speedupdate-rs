//! Progression reporting helpers
use std::cell::{Ref, RefCell, RefMut};
use std::fmt;
use std::ops::{Add, AddAssign, Div, Sub, SubAssign};
use std::rc::Rc;
use std::sync::Arc;

use super::updater::UpdateFilter;
use super::UpdatePosition;
use crate::histogram::Histogram;
use crate::io;
use crate::metadata::v1::StateUpdating;
use crate::metadata::{self, CleanName, CleanPath, Operation};

#[derive(Clone)]
pub struct SharedCheckProgress {
    state: Rc<RefCell<CheckProgress>>,
}

impl SharedCheckProgress {
    pub fn new(metadata: Arc<metadata::WorkspaceChecks>) -> Self {
        Self { state: Rc::new(RefCell::new(CheckProgress::new(metadata))) }
    }
    pub fn borrow(&self) -> Ref<'_, CheckProgress> {
        self.state.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> RefMut<'_, CheckProgress> {
        self.state.borrow_mut()
    }
}

#[derive(Debug)]
pub struct CheckProgress {
    pub metadata: Arc<metadata::WorkspaceChecks>,

    /// Number of files to check
    pub check_files: usize,
    /// Number of bytes to check
    pub check_bytes: u64,

    /// Current operation beeing checked
    pub checking_operation_idx: usize,

    /// Global check progression histogram
    pub histogram: Histogram<CheckProgression>,
}

#[derive(Debug, Default, Clone)]
pub struct CheckProgression {
    /// Number of files downloaded
    pub checked_files: usize,
    /// Number of bytes downloaded
    pub checked_bytes: u64,

    /// Number of errors
    pub failed_files: usize,
}

impl<'a, 'b> Add<&'a CheckProgression> for &'b CheckProgression {
    type Output = CheckProgression;

    fn add(self, other: &'a CheckProgression) -> CheckProgression {
        CheckProgression {
            checked_files: self.checked_files + other.checked_files,
            checked_bytes: self.checked_bytes + other.checked_bytes,

            failed_files: self.failed_files + other.failed_files,
        }
    }
}

impl<'a> AddAssign<&'a CheckProgression> for CheckProgression {
    fn add_assign(&mut self, other: &'a CheckProgression) {
        self.checked_files += other.checked_files;
        self.checked_bytes += other.checked_bytes;

        self.failed_files += other.failed_files;
    }
}

impl<'a, 'b> Sub<&'a CheckProgression> for &'b CheckProgression {
    type Output = CheckProgression;

    fn sub(self, other: &'a CheckProgression) -> CheckProgression {
        CheckProgression {
            checked_files: self.checked_files - other.checked_files,
            checked_bytes: self.checked_bytes - other.checked_bytes,

            failed_files: self.failed_files - other.failed_files,
        }
    }
}

impl<'a> SubAssign<&'a CheckProgression> for CheckProgression {
    fn sub_assign(&mut self, other: &'a CheckProgression) {
        self.checked_files -= other.checked_files;
        self.checked_bytes -= other.checked_bytes;

        self.failed_files -= other.failed_files;
    }
}

impl CheckProgress {
    pub fn new(metadata: Arc<metadata::WorkspaceChecks>) -> Self {
        let mut this = Self {
            metadata,
            check_files: 0,
            check_bytes: 0,
            checking_operation_idx: 0,
            histogram: Default::default(),
        };

        for operation in this.metadata.iter() {
            this.check_files += 1;
            this.check_bytes += operation.check_size();
        }
        this
    }

    pub fn current_operation(&self) -> Option<&dyn Operation> {
        let op = self.metadata.iter().skip(self.checking_operation_idx).next()?;
        Some(op)
    }
}

#[derive(Clone)]
pub struct SharedUpdateProgress {
    state: Rc<RefCell<UpdateProgress>>,
}

impl SharedUpdateProgress {
    pub fn new(target_revision: CleanName) -> Self {
        Self { state: Rc::new(RefCell::new(UpdateProgress::new(target_revision))) }
    }

    pub fn borrow(&self) -> Ref<'_, UpdateProgress> {
        self.state.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> RefMut<'_, UpdateProgress> {
        self.state.borrow_mut()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum UpdateStage {
    FindingUpdatePath,
    Updating,
    FindingRepairPath,
    Repairing,
    Uptodate,
    Failed,
}

impl Default for UpdateStage {
    fn default() -> Self {
        UpdateStage::FindingUpdatePath
    }
}

pub struct UpdateFailure {
    path: CleanPath,
    cause: io::Error,
}

impl fmt::Debug for UpdateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.cause)
    }
}

#[derive(Debug)]
pub struct UpdateProgress {
    /// Current update target revision
    pub target_revision: CleanName,

    /// Current update stage
    pub stage: UpdateStage,

    /// Number of files to download
    pub download_files: usize,
    /// Number of bytes to download
    pub download_bytes: u64,

    /// Number of files to install
    pub apply_files: usize,
    /// Number of bytes to apply
    pub apply_input_bytes: u64,
    /// Number of bytes to install
    pub apply_output_bytes: u64,

    /// Current package beeing applied
    pub downloading_package_idx: usize,
    /// Current operation beeing downloaded
    pub downloading_operation_idx: usize,

    /// Current package beeing applied
    pub applying_package_idx: usize,
    /// Current operation beeing applied
    pub applying_operation_idx: usize,

    /// Global update progression histogram
    pub histogram: Histogram<Progression>,

    /// Per step update progression
    pub steps: Vec<UpdateStepState>,
}

impl UpdateProgress {
    pub fn new(target_revision: CleanName) -> Self {
        Self {
            target_revision,
            stage: UpdateStage::FindingUpdatePath,
            download_files: 0,
            download_bytes: 0,
            apply_files: 0,
            apply_input_bytes: 0,
            apply_output_bytes: 0,
            downloading_package_idx: 0,
            downloading_operation_idx: 0,
            applying_package_idx: 0,
            applying_operation_idx: 0,
            histogram: Default::default(),
            steps: Default::default(),
        }
    }

    pub fn current_step(&self) -> Option<&UpdateStepState> {
        self.steps.get(self.downloading_package_idx)
    }

    pub fn current_step_operation(&self, operation_idx: usize) -> Option<&dyn Operation> {
        let step = self.current_step()?;
        let op = step.metadata.iter().skip(operation_idx).next()?;
        Some(op)
    }

    pub(crate) fn inc_progress(&mut self, delta: Progression) {
        if let Some(step) = self.steps.get_mut(self.downloading_package_idx) {
            step.progression += &delta;
        }
        self.histogram.inc(delta);
    }

    pub(crate) fn inc_package(&mut self) {
        self.downloading_package_idx += 1;
        self.applying_package_idx += 1;
    }

    pub(super) fn push_steps(
        &mut self,
        packages_metadata: &[Arc<metadata::PackageMetadata>],
        first_package_state: &StateUpdating,
        filter: &UpdateFilter,
    ) {
        let (mut available, mut applied, check_only) = (
            first_package_state.available.clone(),
            first_package_state.applied.clone(),
            first_package_state.check_only,
        );
        for package_metadata in packages_metadata.iter() {
            let mut step = UpdateStepState::new(package_metadata.clone());
            let mut delta = Progression::default();

            delta.downloaded_files += applied.operation_idx;
            delta.applied_files += available.operation_idx;

            for (idx, operation) in package_metadata.iter().enumerate() {
                if !filter.filter(operation) {
                    continue;
                }

                let (check_size, data_size, final_size) = if idx < applied.operation_idx {
                    (operation.check_size(), operation.data_size(), operation.final_size())
                } else {
                    (0, 0, 0)
                };

                if !check_only {
                    step.download_files += 1;
                    delta.downloaded_bytes += match idx {
                        idx if idx < available.operation_idx => operation.data_size(),
                        idx if idx == available.operation_idx => available.byte_idx,
                        _ => 0,
                    };
                    step.download_bytes += operation.data_size();

                    delta.applied_input_bytes += data_size + check_size;
                    step.apply_input_bytes += operation.data_size() + operation.check_size();
                } else {
                    delta.applied_input_bytes += final_size + check_size;
                    step.apply_input_bytes += operation.final_size() + operation.check_size();
                }

                step.apply_files += 1;
                delta.applied_output_bytes += final_size + check_size;
                step.apply_output_bytes += operation.final_size() + operation.check_size();
            }

            self.histogram.inc(delta);
            self.download_files += step.download_files;
            self.download_bytes += step.download_bytes;
            self.apply_files += step.apply_files;
            self.apply_input_bytes += step.apply_input_bytes;
            self.apply_output_bytes += step.apply_output_bytes;

            self.steps.push(step);
            available = UpdatePosition::new();
            applied = UpdatePosition::new();
        }
    }
}

/// Update step objectives
#[derive(Debug)]
pub struct UpdateStepState {
    pub metadata: Arc<metadata::PackageMetadata>,

    /// Number of files to download
    pub download_files: usize,
    /// Number of bytes to download
    pub download_bytes: u64,

    /// Number of files to install
    pub apply_files: usize,
    /// Number of bytes to apply
    pub apply_input_bytes: u64,
    /// Number of bytes to install
    pub apply_output_bytes: u64,

    pub progression: Progression,
}

impl UpdateStepState {
    fn new(metadata: Arc<metadata::PackageMetadata>) -> Self {
        Self {
            metadata,
            download_files: 0,
            download_bytes: 0,
            apply_files: 0,
            apply_input_bytes: 0,
            apply_output_bytes: 0,
            progression: Default::default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Progression {
    /// Number of files downloaded
    pub downloaded_files: usize,
    /// Number of bytes downloaded
    pub downloaded_bytes: u64,

    /// Number of files installed
    pub applied_files: usize,
    /// Number of bytes decoded
    pub applied_input_bytes: u64,
    /// Number of bytes installed
    pub applied_output_bytes: u64,

    /// Number of errors
    pub failed_files: usize,
}

impl<'a, 'b> Add<&'a Progression> for &'b Progression {
    type Output = Progression;

    fn add(self, other: &'a Progression) -> Progression {
        Progression {
            downloaded_files: self.downloaded_files + other.downloaded_files,
            downloaded_bytes: self.downloaded_bytes + other.downloaded_bytes,

            applied_files: self.applied_files + other.applied_files,
            applied_input_bytes: self.applied_input_bytes + other.applied_input_bytes,
            applied_output_bytes: self.applied_output_bytes + other.applied_output_bytes,

            failed_files: self.failed_files + other.failed_files,
        }
    }
}

impl<'a> AddAssign<&'a Progression> for Progression {
    fn add_assign(&mut self, other: &'a Progression) {
        self.downloaded_files += other.downloaded_files;
        self.downloaded_bytes += other.downloaded_bytes;

        self.applied_files += other.applied_files;
        self.applied_input_bytes += other.applied_input_bytes;
        self.applied_output_bytes += other.applied_output_bytes;

        self.failed_files += other.failed_files;
    }
}

impl<'a, 'b> Sub<&'a Progression> for &'b Progression {
    type Output = Progression;

    fn sub(self, other: &'a Progression) -> Progression {
        Progression {
            downloaded_files: self.downloaded_files - other.downloaded_files,
            downloaded_bytes: self.downloaded_bytes - other.downloaded_bytes,

            applied_files: self.applied_files - other.applied_files,
            applied_input_bytes: self.applied_input_bytes - other.applied_input_bytes,
            applied_output_bytes: self.applied_output_bytes - other.applied_output_bytes,

            failed_files: self.failed_files - other.failed_files,
        }
    }
}

impl<'a> SubAssign<&'a Progression> for Progression {
    fn sub_assign(&mut self, other: &'a Progression) {
        self.downloaded_files -= other.downloaded_files;
        self.downloaded_bytes -= other.downloaded_bytes;

        self.applied_files -= other.applied_files;
        self.applied_input_bytes -= other.applied_input_bytes;
        self.applied_output_bytes -= other.applied_output_bytes;

        self.failed_files -= other.failed_files;
    }
}

#[derive(Debug, Default, Clone)]
pub struct ProgressionPerSec {
    /// Number of files downloaded
    pub downloaded_files_per_sec: f64,
    /// Number of bytes downloaded
    pub downloaded_bytes_per_sec: f64,

    /// Number of files installed
    pub applied_files_per_sec: f64,
    /// Number of bytes decoded
    pub applied_input_bytes_per_sec: f64,
    /// Number of bytes installed
    pub applied_output_bytes_per_sec: f64,

    /// Number of errors
    pub failed_files_per_sec: f64,
}

impl Div<f64> for &'_ Progression {
    type Output = ProgressionPerSec;

    fn div(self, rhs: f64) -> Self::Output {
        ProgressionPerSec {
            downloaded_files_per_sec: self.downloaded_files as f64 / rhs,
            downloaded_bytes_per_sec: self.downloaded_bytes as f64 / rhs,
            applied_files_per_sec: self.applied_files as f64 / rhs,
            applied_input_bytes_per_sec: self.applied_input_bytes as f64 / rhs,
            applied_output_bytes_per_sec: self.applied_output_bytes as f64 / rhs,
            failed_files_per_sec: self.failed_files as f64 / rhs,
        }
    }
}
