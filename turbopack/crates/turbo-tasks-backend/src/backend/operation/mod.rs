mod aggregation_update;
mod cleanup_old_edges;
mod connect_child;
mod connect_children;
mod invalidate;
mod prepare_new_children;
mod update_cell;
mod update_collectible;

use std::{
    fmt::{Debug, Formatter},
    mem::transmute,
    sync::atomic::Ordering,
};

use bincode::{Decode, Encode};
use turbo_tasks::{
    CellId, FxIndexMap, TaskExecutionReason, TaskId, TurboTasksBackendApi, TypedSharedReference,
};

use crate::{
    backend::{
        OperationGuard, TaskDataCategory, TransientTask, TurboTasksBackend, TurboTasksBackendInner,
        storage::{SpecificTaskDataCategory, StorageWriteGuard},
        storage_schema::{TaskStorage, TaskStorageAccessors},
    },
    backing_storage::{BackingStorage, BackingStorageSealed},
    data::{ActivenessState, CollectibleRef, Dirtyness, InProgressCellState, InProgressState},
};

pub trait Operation:
    Encode + Decode<()> + Default + TryFrom<AnyOperation, Error = ()> + Into<AnyOperation>
{
    fn execute(self, ctx: &mut impl ExecuteContext<'_>);
}

#[derive(Copy, Clone)]
enum TransactionState<'a, 'tx, B: BackingStorage> {
    None,
    Borrowed(Option<&'a B::ReadTransaction<'tx>>),
    Owned(Option<B::ReadTransaction<'tx>>),
}

pub trait ExecuteContext<'e>: Sized {
    type TaskGuardImpl: TaskGuard + 'e;
    fn child_context<'l, 'r>(&'r self) -> impl ChildExecuteContext<'l> + use<'e, 'l, Self>
    where
        'e: 'l;
    fn task(&mut self, task_id: TaskId, category: TaskDataCategory) -> Self::TaskGuardImpl;
    /// Prepares (as in fetches from persistent storage) a list of tasks.
    /// The iterator should not have duplicates, as this would cause over-fetching.
    fn prepare_tasks(
        &mut self,
        task_ids: impl IntoIterator<Item = (TaskId, TaskDataCategory)> + Clone,
    );
    fn for_each_task(
        &mut self,
        task_ids: impl IntoIterator<Item = (TaskId, TaskDataCategory)>,
        func: impl FnMut(Self::TaskGuardImpl, &mut Self),
    );
    fn for_each_task_meta(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId>,
        func: impl FnMut(Self::TaskGuardImpl, &mut Self),
    ) {
        self.for_each_task(
            task_ids.into_iter().map(|id| (id, TaskDataCategory::Meta)),
            func,
        )
    }
    fn is_once_task(&self, task_id: TaskId) -> bool;
    fn task_pair(
        &mut self,
        task_id1: TaskId,
        task_id2: TaskId,
        category: TaskDataCategory,
    ) -> (Self::TaskGuardImpl, Self::TaskGuardImpl);
    fn schedule(&mut self, task_id: TaskId);
    fn schedule_task(&self, task: Self::TaskGuardImpl);
    fn operation_suspend_point<T>(&mut self, op: &T)
    where
        T: Clone + Into<AnyOperation>;
    fn suspending_requested(&self) -> bool;
    fn get_task_desc_fn(&self, task_id: TaskId) -> impl Fn() -> String + Send + Sync + 'static;
    fn get_task_description(&self, task_id: TaskId) -> String;
    fn should_track_dependencies(&self) -> bool;
    fn should_track_activeness(&self) -> bool;
}

pub trait ChildExecuteContext<'e>: Send + Sized {
    fn create(self) -> impl ExecuteContext<'e>;
}

pub struct ExecuteContextImpl<'e, 'tx, B: BackingStorage>
where
    Self: 'e,
    'tx: 'e,
{
    backend: &'e TurboTasksBackendInner<B>,
    turbo_tasks: &'e dyn TurboTasksBackendApi<TurboTasksBackend<B>>,
    _operation_guard: Option<OperationGuard<'e, B>>,
    transaction: TransactionState<'e, 'tx, B>,
    #[cfg(debug_assertions)]
    active_task_locks: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

impl<'e, 'tx, B: BackingStorage> ExecuteContextImpl<'e, 'tx, B>
where
    'tx: 'e,
{
    pub(super) fn new(
        backend: &'e TurboTasksBackendInner<B>,
        turbo_tasks: &'e dyn TurboTasksBackendApi<TurboTasksBackend<B>>,
    ) -> Self {
        Self {
            backend,
            turbo_tasks,
            _operation_guard: Some(backend.start_operation()),
            transaction: TransactionState::None,
            #[cfg(debug_assertions)]
            active_task_locks: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }

    pub(super) unsafe fn new_with_tx(
        backend: &'e TurboTasksBackendInner<B>,
        transaction: Option<&'e B::ReadTransaction<'tx>>,
        turbo_tasks: &'e dyn TurboTasksBackendApi<TurboTasksBackend<B>>,
    ) -> Self {
        Self {
            backend,
            turbo_tasks,
            _operation_guard: Some(backend.start_operation()),
            transaction: TransactionState::Borrowed(transaction),
            #[cfg(debug_assertions)]
            active_task_locks: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }

    fn ensure_transaction(&mut self) -> bool {
        if matches!(self.transaction, TransactionState::None) {
            let check_backing_storage = self.backend.should_restore()
                && self.backend.local_is_partial.load(Ordering::Acquire);
            if !check_backing_storage {
                return false;
            }
            let tx = self.backend.backing_storage.start_read_transaction();
            let tx = tx.map(|tx| {
                // Safety: self is actually valid for 'a, so it's safe to transmute 'l to 'a
                unsafe { transmute::<B::ReadTransaction<'_>, B::ReadTransaction<'tx>>(tx) }
            });
            self.transaction = TransactionState::Owned(tx);
        }
        true
    }

    /// Restore task data directly into TypedStorage using typed serialization.
    /// This bypasses the intermediate Vec<CachedDataItem> representation.
    fn restore_task_data_typed(
        &mut self,
        task_id: TaskId,
        category: TaskDataCategory,
        storage: &mut crate::backend::storage_schema::TaskStorage,
    ) {
        if !self.ensure_transaction() {
            // If we don't need to restore, nothing to do
            return;
        }
        let tx = self.get_tx();
        // Safety: `tx` is a valid transaction from `self.backend.backing_storage`.
        let result = match category {
            TaskDataCategory::Meta => unsafe {
                self.backend
                    .backing_storage
                    .lookup_typed_meta(tx, task_id, storage)
            },
            TaskDataCategory::Data => unsafe {
                self.backend
                    .backing_storage
                    .lookup_typed_data(tx, task_id, storage)
            },
            TaskDataCategory::All => {
                // Restore both meta and data
                let meta_result = unsafe {
                    self.backend
                        .backing_storage
                        .lookup_typed_meta(tx, task_id, storage)
                };
                if let Err(e) = meta_result {
                    let task_name = self.backend.get_task_description(task_id);
                    panic!(
                        "Failed to restore task meta (corrupted database or bug): {:?}",
                        e.context(format!("Meta for {task_name} ({task_id})"))
                    );
                }
                unsafe {
                    self.backend
                        .backing_storage
                        .lookup_typed_data(tx, task_id, storage)
                }
            }
        };

        if let Err(e) = result {
            let task_name = self.backend.get_task_description(task_id);
            panic!(
                "Failed to restore task data (corrupted database or bug): {:?}",
                e.context(format!("{category:?} for {task_name} ({task_id})"))
            );
        }
    }

    /// Restore a single task's data using typed serialization.
    /// Returns a TaskStorage containing the restored data.
    fn restore_task_data_typed_single(
        &mut self,
        task_id: TaskId,
        category: TaskDataCategory,
    ) -> TaskStorage {
        let mut storage = TaskStorage::new();
        if !self.ensure_transaction() {
            // If we don't need to restore, return an empty TaskStorage
            return storage;
        }
        let tx = self.get_tx();
        // Safety: `tx` is a valid transaction from `self.backend.backing_storage`.
        let result = match category {
            TaskDataCategory::Meta => unsafe {
                self.backend
                    .backing_storage
                    .lookup_typed_meta(tx, task_id, &mut storage)
            },
            TaskDataCategory::Data => unsafe {
                self.backend
                    .backing_storage
                    .lookup_typed_data(tx, task_id, &mut storage)
            },
            TaskDataCategory::All => {
                let meta_result = unsafe {
                    self.backend
                        .backing_storage
                        .lookup_typed_meta(tx, task_id, &mut storage)
                };
                if let Err(e) = meta_result {
                    let task_name = self.backend.get_task_description(task_id);
                    panic!(
                        "Failed to restore task meta (corrupted database or bug): {:?}",
                        e.context(format!("Meta for {task_name} ({task_id})"))
                    );
                }
                unsafe {
                    self.backend
                        .backing_storage
                        .lookup_typed_data(tx, task_id, &mut storage)
                }
            }
        };
        if let Err(e) = result {
            let task_name = self.backend.get_task_description(task_id);
            panic!(
                "Failed to restore task data (corrupted database or bug): {:?}",
                e.context(format!("{category:?} for {task_name} ({task_id})"))
            );
        }
        storage
    }

    /// Restore multiple tasks' data using typed serialization.
    /// Returns a vector of TypedStorage, one for each task_id.
    fn restore_task_data_typed_batch(
        &mut self,
        task_ids: &[TaskId],
        category: TaskDataCategory,
    ) -> Option<Vec<TaskStorage>> {
        debug_assert!(
            task_ids.len() > 1,
            "Use restore_task_data_typed_single for single task"
        );
        if !self.ensure_transaction() {
            // If we don't need to restore, we return None
            return None;
        }
        let tx = self.get_tx();
        // Safety: `tx` is a valid transaction from `self.backend.backing_storage`.
        let result = unsafe {
            self.backend
                .backing_storage
                .batch_lookup_typed(tx, task_ids, category)
        };
        match result {
            Ok(result) => Some(result),
            Err(e) => {
                panic!(
                    "Failed to restore task data (corrupted database or bug): {:?}",
                    e.context(format!(
                        "{category:?} for batch of {} tasks",
                        task_ids.len()
                    ))
                )
            }
        }
    }

    fn get_tx(&self) -> Option<&<B as BackingStorageSealed>::ReadTransaction<'tx>> {
        match &self.transaction {
            TransactionState::None => unreachable!(),
            TransactionState::Borrowed(tx) => *tx,
            TransactionState::Owned(tx) => tx.as_ref(),
        }
    }

    fn prepare_tasks_with_callback(
        &mut self,
        task_ids: impl IntoIterator<Item = (TaskId, TaskDataCategory)>,
        call_prepared_task_callback_for_transient_tasks: bool,
        mut prepared_task_callback: impl FnMut(
            &mut Self,
            TaskId,
            TaskDataCategory,
            StorageWriteGuard<'e>,
        ),
    ) {
        let mut data_count = 0;
        let mut meta_count = 0;
        let mut all_count = 0;
        let mut tasks = task_ids
            .into_iter()
            .filter(|&(id, category)| {
                if id.is_transient() {
                    if call_prepared_task_callback_for_transient_tasks {
                        let mut task = self.backend.storage.access_mut(id);
                        if !task.flags.is_restored(category) {
                            task.flags.set_restored(TaskDataCategory::All);
                        }
                        prepared_task_callback(self, id, category, task);
                    }
                    false
                } else {
                    true
                }
            })
            .inspect(|(_, category)| match category {
                TaskDataCategory::Data => data_count += 1,
                TaskDataCategory::Meta => meta_count += 1,
                TaskDataCategory::All => all_count += 1,
            })
            .map(|(id, category)| (id, category, None, None))
            .collect::<Vec<_>>();
        data_count += all_count;
        meta_count += all_count;

        let mut tasks_to_restore_for_data = Vec::with_capacity(data_count);
        let mut tasks_to_restore_for_data_indicies = Vec::with_capacity(data_count);
        let mut tasks_to_restore_for_meta = Vec::with_capacity(meta_count);
        let mut tasks_to_restore_for_meta_indicies = Vec::with_capacity(meta_count);
        for (i, &(task_id, category, _, _)) in tasks.iter().enumerate() {
            #[cfg(debug_assertions)]
            if self.active_task_locks.fetch_add(1, Ordering::AcqRel) != 0 {
                panic!(
                    "Concurrent task lock acquisition detected. This is not allowed and indicates \
                     a bug. It can lead to deadlocks."
                );
            }

            let task = self.backend.storage.access_mut(task_id);
            let mut ready = true;
            if matches!(category, TaskDataCategory::Data | TaskDataCategory::All)
                && !task.flags.is_restored(TaskDataCategory::Data)
            {
                tasks_to_restore_for_data.push(task_id);
                tasks_to_restore_for_data_indicies.push(i);
                ready = false;
            }
            if matches!(category, TaskDataCategory::Meta | TaskDataCategory::All)
                && !task.flags.is_restored(TaskDataCategory::Meta)
            {
                tasks_to_restore_for_meta.push(task_id);
                tasks_to_restore_for_meta_indicies.push(i);
                ready = false;
            }
            if ready {
                prepared_task_callback(self, task_id, category, task);
            }
            #[cfg(debug_assertions)]
            self.active_task_locks.fetch_sub(1, Ordering::AcqRel);
        }
        if tasks_to_restore_for_meta.is_empty() && tasks_to_restore_for_data.is_empty() {
            return;
        }

        // Restore data category using typed serialization
        match tasks_to_restore_for_data.len() {
            0 => {}
            1 => {
                let task_id = tasks_to_restore_for_data[0];
                let storage = self.restore_task_data_typed_single(task_id, TaskDataCategory::Data);
                let idx = tasks_to_restore_for_data_indicies[0];
                tasks[idx].2 = Some(storage);
            }
            _ => {
                if let Some(storages) = self.restore_task_data_typed_batch(
                    &tasks_to_restore_for_data,
                    TaskDataCategory::Data,
                ) {
                    storages
                        .into_iter()
                        .zip(tasks_to_restore_for_data_indicies)
                        .for_each(|(storage, idx)| {
                            tasks[idx].2 = Some(storage);
                        });
                } else {
                    for idx in tasks_to_restore_for_data_indicies {
                        tasks[idx].2 = Some(TaskStorage::new());
                    }
                }
            }
        }
        // Restore meta category using typed serialization
        match tasks_to_restore_for_meta.len() {
            0 => {}
            1 => {
                let task_id = tasks_to_restore_for_meta[0];
                let storage = self.restore_task_data_typed_single(task_id, TaskDataCategory::Meta);
                let idx = tasks_to_restore_for_meta_indicies[0];
                tasks[idx].3 = Some(storage);
            }
            _ => {
                if let Some(storages) = self.restore_task_data_typed_batch(
                    &tasks_to_restore_for_meta,
                    TaskDataCategory::Meta,
                ) {
                    storages
                        .into_iter()
                        .zip(tasks_to_restore_for_meta_indicies)
                        .for_each(|(storage, idx)| {
                            tasks[idx].3 = Some(storage);
                        });
                } else {
                    for idx in tasks_to_restore_for_meta_indicies {
                        tasks[idx].3 = Some(TaskStorage::new());
                    }
                }
            }
        }

        // Merge restored data into tasks using typed merge
        for (task_id, category, storage_for_data, storage_for_meta) in tasks {
            if storage_for_data.is_none() && storage_for_meta.is_none() {
                continue;
            }
            #[cfg(debug_assertions)]
            if self.active_task_locks.fetch_add(1, Ordering::AcqRel) != 0 {
                panic!(
                    "Concurrent task lock acquisition detected. This is not allowed and indicates \
                     a bug. It can lead to deadlocks."
                );
            }

            let mut task = self.backend.storage.access_mut(task_id);
            if let Some(restored_storage) = storage_for_data
                && !task.flags.is_restored(TaskDataCategory::Data)
            {
                task.restore_from(restored_storage, TaskDataCategory::Data);
                task.flags.set_restored(TaskDataCategory::Data);
            }
            if let Some(restored_storage) = storage_for_meta
                && !task.flags.is_restored(TaskDataCategory::Meta)
            {
                task.restore_from(restored_storage, TaskDataCategory::Meta);
                task.flags.set_restored(TaskDataCategory::Meta);
            }
            prepared_task_callback(self, task_id, category, task);
            #[cfg(debug_assertions)]
            self.active_task_locks.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl<'e, 'tx, B: BackingStorage> ExecuteContext<'e> for ExecuteContextImpl<'e, 'tx, B>
where
    'tx: 'e,
{
    type TaskGuardImpl = TaskGuardImpl<'e, B>;

    fn child_context<'l, 'r>(&'r self) -> impl ChildExecuteContext<'l> + use<'e, 'tx, 'l, B>
    where
        'e: 'l,
    {
        ChildExecuteContextImpl {
            backend: self.backend,
            turbo_tasks: self.turbo_tasks,
        }
    }

    fn task(&mut self, task_id: TaskId, category: TaskDataCategory) -> Self::TaskGuardImpl {
        #[cfg(debug_assertions)]
        if self.active_task_locks.fetch_add(1, Ordering::AcqRel) != 0 {
            panic!(
                "Concurrent task lock acquisition detected. This is not allowed and indicates a \
                 bug. It can lead to deadlocks."
            );
        }

        let mut task = self.backend.storage.access_mut(task_id);
        if !task.flags.is_restored(category) {
            if task_id.is_transient() {
                task.flags.set_restored(TaskDataCategory::All);
            } else {
                for category in category {
                    if !task.flags.is_restored(category) {
                        // Avoid holding the lock too long since this can also affect other tasks
                        drop(task);

                        // Create a temporary TaskStorage to decode into
                        let mut restored_storage =
                            crate::backend::storage_schema::TaskStorage::new();
                        self.restore_task_data_typed(task_id, category, &mut restored_storage);

                        task = self.backend.storage.access_mut(task_id);
                        if !task.flags.is_restored(category) {
                            // Restore the persisted data into the task's storage
                            task.restore_from(restored_storage, category);
                            task.flags.set_restored(category);
                        }
                    }
                }
            }
        }
        TaskGuardImpl {
            task,
            task_id,
            backend: self.backend,
            #[cfg(debug_assertions)]
            category,
            #[cfg(debug_assertions)]
            active_task_locks: self.active_task_locks.clone(),
        }
    }

    fn prepare_tasks(&mut self, task_ids: impl IntoIterator<Item = (TaskId, TaskDataCategory)>) {
        self.prepare_tasks_with_callback(task_ids, false, |_, _, _, _| {});
    }

    fn for_each_task(
        &mut self,
        task_ids: impl IntoIterator<Item = (TaskId, TaskDataCategory)>,
        mut func: impl FnMut(Self::TaskGuardImpl, &mut Self),
    ) {
        let backend = self.backend;
        #[cfg(debug_assertions)]
        let active_task_locks = self.active_task_locks.clone();
        self.prepare_tasks_with_callback(task_ids, true, |this, task_id, _category, task| {
            // The prepare_tasks_with_callback already increased the active_task_locks count and
            // checked for concurrent access but it will also decrement it again, so we
            // need to increase it again here as Drop will decrement it
            #[cfg(debug_assertions)]
            active_task_locks.fetch_add(1, Ordering::AcqRel);

            let guard: TaskGuardImpl<'_, B> = TaskGuardImpl {
                task,
                task_id,
                backend,
                #[cfg(debug_assertions)]
                category: _category,
                #[cfg(debug_assertions)]
                active_task_locks: active_task_locks.clone(),
            };
            func(guard, this);
        });
    }

    fn is_once_task(&self, task_id: TaskId) -> bool {
        if !task_id.is_transient() {
            return false;
        }
        if let Some(ty) = self.backend.transient_tasks.get(&task_id) {
            matches!(**ty, TransientTask::Once(_))
        } else {
            false
        }
    }

    fn task_pair(
        &mut self,
        task_id1: TaskId,
        task_id2: TaskId,
        category: TaskDataCategory,
    ) -> (Self::TaskGuardImpl, Self::TaskGuardImpl) {
        #[cfg(debug_assertions)]
        if self.active_task_locks.fetch_add(2, Ordering::AcqRel) != 0 {
            panic!(
                "Concurrent task lock acquisition detected. This is not allowed and indicates a \
                 bug. It can lead to deadlocks."
            );
        }

        let (mut task1, mut task2) = self.backend.storage.access_pair_mut(task_id1, task_id2);
        let is_restored1 = task1.flags.is_restored(category);
        let is_restored2 = task2.flags.is_restored(category);
        if !is_restored1 || !is_restored2 {
            for category in category {
                // Avoid holding the lock too long since this can also affect other tasks
                drop(task1);
                drop(task2);

                // Restore using typed storage path
                let restored1 = if !is_restored1 {
                    let mut storage = crate::backend::storage_schema::TaskStorage::new();
                    self.restore_task_data_typed(task_id1, category, &mut storage);
                    Some(storage)
                } else {
                    None
                };
                let restored2 = if !is_restored2 {
                    let mut storage = crate::backend::storage_schema::TaskStorage::new();
                    self.restore_task_data_typed(task_id2, category, &mut storage);
                    Some(storage)
                } else {
                    None
                };

                let (t1, t2) = self.backend.storage.access_pair_mut(task_id1, task_id2);
                task1 = t1;
                task2 = t2;
                if !task1.flags.is_restored(category) {
                    task1.restore_from(restored1.unwrap(), category);
                    task1.flags.set_restored(category);
                }
                if !task2.flags.is_restored(category) {
                    task2.restore_from(restored2.unwrap(), category);
                    task2.flags.set_restored(category);
                }
            }
        }
        (
            TaskGuardImpl {
                task: task1,
                task_id: task_id1,
                backend: self.backend,
                #[cfg(debug_assertions)]
                category,
                #[cfg(debug_assertions)]
                active_task_locks: self.active_task_locks.clone(),
            },
            TaskGuardImpl {
                task: task2,
                task_id: task_id2,
                backend: self.backend,
                #[cfg(debug_assertions)]
                category,
                #[cfg(debug_assertions)]
                active_task_locks: self.active_task_locks.clone(),
            },
        )
    }

    fn schedule(&mut self, task_id: TaskId) {
        let task = self.task(task_id, TaskDataCategory::All);
        self.schedule_task(task);
    }

    fn schedule_task(&self, task: Self::TaskGuardImpl) {
        self.turbo_tasks.schedule(task.id());
    }

    fn operation_suspend_point<T: Clone + Into<AnyOperation>>(&mut self, op: &T) {
        self.backend.operation_suspend_point(|| op.clone().into());
    }

    fn suspending_requested(&self) -> bool {
        self.backend.suspending_requested()
    }

    fn get_task_desc_fn(&self, task_id: TaskId) -> impl Fn() -> String + Send + Sync + 'static {
        self.backend.get_task_desc_fn(task_id)
    }

    fn get_task_description(&self, task_id: TaskId) -> String {
        self.backend.get_task_description(task_id)
    }

    fn should_track_dependencies(&self) -> bool {
        self.backend.should_track_dependencies()
    }

    fn should_track_activeness(&self) -> bool {
        self.backend.should_track_activeness()
    }
}

struct ChildExecuteContextImpl<'e, B: BackingStorage> {
    backend: &'e TurboTasksBackendInner<B>,
    turbo_tasks: &'e dyn TurboTasksBackendApi<TurboTasksBackend<B>>,
}

impl<'e, B: BackingStorage> ChildExecuteContext<'e> for ChildExecuteContextImpl<'e, B> {
    fn create(self) -> impl ExecuteContext<'e> {
        ExecuteContextImpl {
            backend: self.backend,
            turbo_tasks: self.turbo_tasks,
            _operation_guard: None,
            transaction: TransactionState::None,
            #[cfg(debug_assertions)]
            active_task_locks: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }
}

pub trait TaskGuard: Debug + TaskStorageAccessors {
    fn id(&self) -> TaskId;

    // ============ Typed Marker APIs ============
    // Flags are migrated: use TaskStorageAccessors trait methods directly
    // Generated methods: stateful(), set_stateful(), invalidator(), set_invalidator(),
    // immutable(), set_immutable()

    // ============ Output APIs ============
    // Output accessor methods are provided by the TaskStorageAccessors trait (generated by macro)
    // Methods: get_output(), has_output(), set_output(), take_output()

    // NOTE: Dirty state accessors are provided by the TaskStorageAccessors trait:
    // - get_dirty() -> Option<&Dirtyness>
    // - has_dirty() -> bool
    // - set_dirty(value) -> Option<Dirtyness> (returns old value)
    // - take_dirty() -> Option<Dirtyness> (clears and returns old value)
    //
    // NOTE: Current session clean accessors are provided by TaskStorageAccessors:
    // - current_session_clean() -> bool
    // - set_current_session_clean(bool)

    // ============ Counter Field Methods ============
    // Counter field mutation methods (update_upper_count, update_follower_count, etc.)
    // are now generated by the TaskStorage macro via TaskStorageAccessors trait.
    // Generated methods include:
    // - update_{field}_count(key, delta) -> bool
    // - update_and_get_{field}(key, delta) -> V
    // - update_{field}(key, f) (closure-based)
    // - add_{field}(key, value)
    // - remove_{field}(key) -> Option<V>
    // - update_{field}_positive_crossing(key, delta) -> bool (for i32 values)
    // - get_{field}_entry(key) -> Option<&V>

    // ============ Typed Execution State APIs ============
    // Uses typed storage directly via TaskStorageAccessors trait - execution group is migrated
    // Generated methods from TaskStorageAccessors:
    // - has_activeness() -> bool
    // - get_activeness() -> Option<&ActivenessState>
    // - set_activeness(v: ActivenessState) -> Option<ActivenessState>
    // - take_activeness() -> Option<ActivenessState>
    // - has_in_progress() -> bool
    // - get_in_progress() -> Option<&InProgressState>
    // - set_in_progress(v: InProgressState) -> Option<InProgressState>
    // - take_in_progress() -> Option<InProgressState>
    // - in_progress_cells() -> Option<&AutoMap<CellId, InProgressCellState>>
    // - in_progress_cells_mut() -> &mut AutoMap<CellId, InProgressCellState>
    // - get_activeness_mut() -> Option<&mut ActivenessState>
    // - get_in_progress_mut() -> Option<&mut InProgressState>

    /// Get mutable reference to the activeness state, inserting a new one if not present
    fn get_activeness_mut_or_insert_with<F>(&mut self, f: F) -> &mut ActivenessState
    where
        F: FnOnce() -> ActivenessState;

    /// Add an in-progress state if not already present. Returns true if newly added.
    fn add_in_progress(&mut self, value: InProgressState) -> bool;

    // ============ Aggregated Container Count (scalar) APIs ============
    // These are for the scalar total count fields, not the CounterMap per-task fields.

    /// Update the aggregated dirty container count (the scalar total count field) by the given
    /// delta and return the new value.
    fn update_and_get_aggregated_dirty_container_count(&mut self, delta: i32) -> i32 {
        let current = self
            .get_aggregated_dirty_container_count()
            .copied()
            .unwrap_or(0);
        let new_value = current + delta;
        if new_value == 0 {
            self.take_aggregated_dirty_container_count();
        } else {
            self.set_aggregated_dirty_container_count(new_value);
        }
        new_value
    }

    /// Update the aggregated current session clean container count (the scalar total count field)
    /// by the given delta and return the new value.
    fn update_and_get_aggregated_current_session_clean_container_count(
        &mut self,
        delta: i32,
    ) -> i32 {
        let current = self
            .get_aggregated_current_session_clean_container_count()
            .copied()
            .unwrap_or(0);
        let new_value = current + delta;
        if new_value == 0 {
            self.take_aggregated_current_session_clean_container_count();
        } else {
            self.set_aggregated_current_session_clean_container_count(new_value);
        }
        new_value
    }

    fn invalidate_serialization(&mut self);
    /// Determine which tasks to prefetch for a task.
    /// Only returns Some once per task.
    /// It returns a set of tasks and which info is needed.
    fn prefetch(&mut self) -> Option<FxIndexMap<TaskId, TaskDataCategory>>;
    fn is_dirty(&self) -> bool {
        self.get_dirty().is_some_and(|dirtyness| match dirtyness {
            Dirtyness::Dirty => true,
            Dirtyness::SessionDependent => !self.current_session_clean(),
        })
    }
    fn dirtyness_and_session(&self) -> Option<(Dirtyness, bool)> {
        match self.get_dirty()? {
            Dirtyness::Dirty => Some((Dirtyness::Dirty, false)),
            Dirtyness::SessionDependent => {
                Some((Dirtyness::SessionDependent, self.current_session_clean()))
            }
        }
    }
    /// Returns (is_dirty, is_clean_in_current_session)
    fn dirty_state(&self) -> (bool, bool) {
        match self.get_dirty() {
            None => (false, false),
            Some(Dirtyness::Dirty) => (true, false),
            Some(Dirtyness::SessionDependent) => (true, self.current_session_clean()),
        }
    }
    fn dirty_containers(&self) -> impl Iterator<Item = TaskId> {
        self.dirty_containers_with_count()
            .map(|(task_id, _)| task_id)
    }
    fn dirty_containers_with_count(&self) -> impl Iterator<Item = (TaskId, i32)> + '_ {
        let dirty_map = self.aggregated_dirty_containers();
        let clean_map = self.aggregated_current_session_clean_containers();
        dirty_map.into_iter().flat_map(move |map| {
            map.iter().filter_map(move |(&task_id, &count)| {
                if count > 0 {
                    let clean_count = clean_map
                        .and_then(|m| m.get(&task_id))
                        .copied()
                        .unwrap_or_default();
                    if count > clean_count {
                        return Some((task_id, count));
                    }
                }
                None
            })
        })
    }

    fn has_dirty_containers(&self) -> bool {
        let dirty_count = self
            .get_aggregated_dirty_container_count()
            .copied()
            .unwrap_or_default();
        if dirty_count <= 0 {
            return false;
        }
        let clean_count = self
            .get_aggregated_current_session_clean_container_count()
            .copied()
            .unwrap_or_default();
        dirty_count > clean_count
    }
    fn remove_cell_data(
        &mut self,
        is_serializable_cell_content: bool,
        cell: CellId,
    ) -> Option<TypedSharedReference> {
        if is_serializable_cell_content {
            self.remove_cell_data_entry(&cell)
        } else {
            self.remove_transient_cell_data_entry(&cell)
                .map(|sr| sr.into_typed(cell.type_id))
        }
    }
    fn get_cell_data(
        &self,
        is_serializable_cell_content: bool,
        cell: CellId,
    ) -> Option<TypedSharedReference> {
        if is_serializable_cell_content {
            self.cell_data().and_then(|map| map.get(&cell)).cloned()
        } else {
            self.transient_cell_data()
                .and_then(|map| map.get(&cell))
                .map(|sr| sr.clone().into_typed(cell.type_id))
        }
    }
    fn has_cell_data(&self, is_serializable_cell_content: bool, cell: CellId) -> bool {
        if is_serializable_cell_content {
            self.cell_data().is_some_and(|map| map.contains_key(&cell))
        } else {
            self.transient_cell_data()
                .is_some_and(|map| map.contains_key(&cell))
        }
    }
    /// Set cell data, returning the old value if any.
    fn set_cell_data(
        &mut self,
        is_serializable_cell_content: bool,
        cell: CellId,
        value: TypedSharedReference,
    ) -> Option<TypedSharedReference> {
        if is_serializable_cell_content {
            self.insert_cell_data_entry(cell, value)
        } else {
            self.insert_transient_cell_data_entry(cell, value.into_untyped())
                .map(|sr| sr.into_typed(cell.type_id))
        }
    }

    /// Add new cell data (asserts that the cell is new and didn't exist before).
    fn add_cell_data(
        &mut self,
        is_serializable_cell_content: bool,
        cell: CellId,
        value: TypedSharedReference,
    ) {
        let old = self.set_cell_data(is_serializable_cell_content, cell, value);
        assert!(old.is_none(), "Cell data already exists for {cell:?}");
    }

    /// Add a scheduled task item. Returns true if the task was successfully added (wasn't already
    /// present).
    #[must_use]
    fn add_scheduled<InnerFn>(
        &mut self,
        reason: TaskExecutionReason,
        description: impl FnOnce() -> InnerFn,
    ) -> bool
    where
        InnerFn: Fn() -> String + Sync + Send + 'static,
    {
        self.add_in_progress(InProgressState::new_scheduled(reason, description))
    }

    // ============ Collectible APIs ============

    /// Insert an outdated collectible with count. Returns true if it was newly inserted.
    #[must_use]
    fn insert_outdated_collectible(&mut self, collectible: CollectibleRef, value: i32) -> bool {
        // Check if already exists
        if self.get_outdated_collectibles_entry(&collectible).is_some() {
            return false;
        }
        // Insert new entry
        self.add_outdated_collectibles(collectible, value);
        true
    }

    // ============ Dependency Bulk Operations ============
    // Use generated methods from TaskStorageAccessors trait:
    // - cell_dependencies_extend(iter)
    // - output_dependencies_extend(iter)

    // NOTE: has_invalidator() is provided by the TaskStorageAccessors trait (generated by macro)

    // ============ Iterator APIs ============
    // AutoSet iterators (iter_children, iter_output_dependencies, etc.) are now generated
    // by the TaskStorage macro. Only non-AutoSet iterators and complex iterators remain here.

    /// Iterate over all follower tasks (with count > 0)
    fn iter_followers(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.iter_followers_positive_entries().map(|(&k, _)| k)
    }

    /// Iterate over all upper tasks (with count > 0)
    // TODO: Investigate when upper entries have count == 0. The semantics of storing
    // entries with zero count is unclear - consider removing such entries or documenting
    // why they're kept.
    fn iter_uppers(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.iter_upper_positive_entries().map(|(&k, _)| k)
    }

    /// Iterate over all outdated collectibles
    fn iter_outdated_collectibles(&self) -> impl Iterator<Item = CollectibleRef> + '_ {
        self.iter_outdated_collectibles_entries().map(|(&k, _)| k)
    }

    /// Iterate over all aggregated collectibles (with count > 0), returning (collectible, count)
    /// pairs
    fn iter_aggregated_collectibles(&self) -> impl Iterator<Item = (CollectibleRef, i32)> + '_ {
        self.iter_aggregated_collectibles_positive_entries()
            .map(|(&k, &v)| (k, v))
    }

    /// Iterate over all cell data entries
    fn iter_cell_data(&self) -> impl Iterator<Item = CellId> + '_ {
        self.iter_cell_data_entries().map(|(&k, _)| k)
    }

    /// Iterate over all transient cell data entries
    fn iter_transient_cell_data(&self) -> impl Iterator<Item = CellId> + '_ {
        self.iter_transient_cell_data_entries().map(|(&k, _)| k)
    }

    // NOTE: Vec-returning collection getters (get_children, get_followers, etc.) were removed.
    // Callers should use iter_* methods and call .collect() if they need a Vec.

    // ============ Extract-If APIs ============
    // These remove items matching the predicate from the typed storage

    /// Remove cell data matching the predicate.
    fn remove_cell_data_if<F>(&mut self, f: F)
    where
        F: FnMut(&CellId) -> bool;

    /// Remove transient cell data matching the predicate.
    fn remove_transient_cell_data_if<F>(&mut self, f: F)
    where
        F: FnMut(&CellId) -> bool;

    /// Remove in-progress cells matching the predicate, notifying waiters.
    fn remove_in_progress_cells_if<F>(&mut self, f: F)
    where
        F: FnMut(&CellId, &InProgressCellState) -> bool;

    // ============ Memory Management APIs ============
    // shrink_to_fit is provided by the TaskStorageAccessors trait
}

pub struct TaskGuardImpl<'a, B: BackingStorage> {
    task_id: TaskId,
    task: StorageWriteGuard<'a>,
    backend: &'a TurboTasksBackendInner<B>,
    #[cfg(debug_assertions)]
    category: TaskDataCategory,
    #[cfg(debug_assertions)]
    active_task_locks: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

#[cfg(debug_assertions)]
impl<B: BackingStorage> Drop for TaskGuardImpl<'_, B> {
    fn drop(&mut self) {
        self.active_task_locks.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<B: BackingStorage> Debug for TaskGuardImpl<'_, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("TaskGuard");
        d.field("task_id", &self.task_id);
        if let Some(task_type) = self.backend.task_cache.lookup_reverse(&self.task_id) {
            d.field("task_type", &task_type);
        };
        d.field("storage", &*self.task);
        d.finish()
    }
}

impl<B: BackingStorage> TaskGuard for TaskGuardImpl<'_, B> {
    fn id(&self) -> TaskId {
        self.task_id
    }

    fn remove_cell_data_if<F>(&mut self, mut f: F)
    where
        F: FnMut(&CellId) -> bool,
    {
        // Collect items to remove first, then remove them
        let to_remove: Vec<_> = self
            .cell_data()
            .into_iter()
            .flat_map(|m| m.keys())
            .filter(|cell| f(cell))
            .copied()
            .collect();

        for cell in to_remove {
            self.remove_cell_data_entry(&cell);
        }
    }

    fn remove_transient_cell_data_if<F>(&mut self, mut f: F)
    where
        F: FnMut(&CellId) -> bool,
    {
        // Collect items to remove first, then remove them
        let to_remove: Vec<_> = self
            .transient_cell_data()
            .into_iter()
            .flat_map(|m| m.keys())
            .filter(|cell| f(cell))
            .copied()
            .collect();

        for cell in to_remove {
            self.remove_transient_cell_data_entry(&cell);
        }
    }

    fn remove_in_progress_cells_if<F>(&mut self, mut f: F)
    where
        F: FnMut(&CellId, &InProgressCellState) -> bool,
    {
        // Collect items to remove first, then remove them
        let to_remove: Vec<_> = self
            .in_progress_cells()
            .into_iter()
            .flat_map(|m| m.iter())
            .filter(|(cell, state)| f(cell, state))
            .map(|(cell, _)| *cell)
            .collect();

        for cell in to_remove {
            self.remove_in_progress_cells_entry(&cell);
        }
    }

    fn invalidate_serialization(&mut self) {
        // TODO this causes race conditions, since we never know when a value is changed. We can't
        // "snapshot" the value correctly.
        if !self.task_id.is_transient() {
            self.task.track_modification(SpecificTaskDataCategory::Data);
            self.task.track_modification(SpecificTaskDataCategory::Meta);
        }
    }

    fn prefetch(&mut self) -> Option<FxIndexMap<TaskId, TaskDataCategory>> {
        if self.task.flags.prefetched() {
            return None;
        }

        self.task.flags.set_prefetched(true);
        // Uses typed storage iterators - dependencies are migrated auto_sets
        let map = self
            .iter_output_dependencies()
            .map(|target| (target, TaskDataCategory::Meta))
            .chain(
                self.iter_cell_dependencies()
                    .map(|target| (target.task, TaskDataCategory::All)),
            )
            .chain(
                self.iter_collectibles_dependencies()
                    .map(|target| (target.task, TaskDataCategory::All)),
            )
            .collect::<FxIndexMap<_, _>>();
        (map.len() > 1).then_some(map)
    }

    // ============ Execution fields (lazy) ============
    // Note: activeness and in_progress are transient fields, so no `check_category` call
    // is needed - transient fields are never persisted and can be accessed regardless
    // of how the task was accessed.
    //
    // These fields use lazy storage where the Vec<LazyField> presence provides optionality.
    // The LazyField variants hold the value directly (T), not Option<T>.

    fn get_activeness_mut_or_insert_with<F>(&mut self, f: F) -> &mut ActivenessState
    where
        F: FnOnce() -> ActivenessState,
    {
        if !self.has_activeness() {
            self.set_activeness(f());
        }
        self.get_activeness_mut()
            .expect("activeness should exist after set")
    }

    fn add_in_progress(&mut self, value: InProgressState) -> bool {
        if self.has_in_progress() {
            false
        } else {
            self.set_in_progress(value);
            true
        }
    }
}

impl<B: BackingStorage> TaskStorageAccessors for TaskGuardImpl<'_, B> {
    fn typed(&self) -> &crate::backend::storage_schema::TaskStorage {
        &self.task
    }

    fn typed_mut(&mut self) -> &mut crate::backend::storage_schema::TaskStorage {
        &mut self.task
    }

    fn track_modification(&mut self, category: SpecificTaskDataCategory) {
        if !self.task_id.is_transient() {
            self.task.track_modification(category);
        }
    }

    #[inline]
    #[track_caller]
    fn check_access(&self, _category: TaskDataCategory) {
        #[cfg(debug_assertions)]
        {
            match _category {
                TaskDataCategory::All => {
                    // This category is used for non-persisted/transient data - no check needed
                }
                TaskDataCategory::Data | TaskDataCategory::Meta => {
                    debug_assert!(
                        self.category == _category || self.category == TaskDataCategory::All,
                        "To access {:?} data of task {:?}, the task needs to be accessed with \
                         that category (it was accessed with {:?})",
                        _category,
                        self.task_id,
                        self.category
                    );
                }
            }
        }
    }
}

macro_rules! impl_operation {
    ($name:ident $type_path:path) => {
        impl From<$type_path> for AnyOperation {
            fn from(op: $type_path) -> Self {
                AnyOperation::$name(op)
            }
        }

        impl TryFrom<AnyOperation> for $type_path {
            type Error = ();

            fn try_from(op: AnyOperation) -> Result<Self, Self::Error> {
                match op {
                    AnyOperation::$name(op) => Ok(op),
                    _ => Err(()),
                }
            }
        }

        pub use $type_path;
    };
}

#[derive(Encode, Decode, Clone)]
pub enum AnyOperation {
    ConnectChild(connect_child::ConnectChildOperation),
    Invalidate(invalidate::InvalidateOperation),
    UpdateCell(update_cell::UpdateCellOperation),
    CleanupOldEdges(cleanup_old_edges::CleanupOldEdgesOperation),
    AggregationUpdate(aggregation_update::AggregationUpdateQueue),
    Nested(Vec<AnyOperation>),
}

impl AnyOperation {
    pub fn execute(self, ctx: &mut impl ExecuteContext<'_>) {
        match self {
            AnyOperation::ConnectChild(op) => op.execute(ctx),
            AnyOperation::Invalidate(op) => op.execute(ctx),
            AnyOperation::UpdateCell(op) => op.execute(ctx),
            AnyOperation::CleanupOldEdges(op) => op.execute(ctx),
            AnyOperation::AggregationUpdate(op) => op.execute(ctx),
            AnyOperation::Nested(ops) => {
                for op in ops {
                    op.execute(ctx);
                }
            }
        }
    }
}

impl_operation!(ConnectChild connect_child::ConnectChildOperation);
impl_operation!(Invalidate invalidate::InvalidateOperation);
impl_operation!(UpdateCell update_cell::UpdateCellOperation);
impl_operation!(CleanupOldEdges cleanup_old_edges::CleanupOldEdgesOperation);
impl_operation!(AggregationUpdate aggregation_update::AggregationUpdateQueue);

#[cfg(feature = "trace_task_dirty")]
pub use self::invalidate::TaskDirtyCause;
pub use self::{
    aggregation_update::{
        AggregatedDataUpdate, AggregationUpdateJob, ComputeDirtyAndCleanUpdate,
        get_aggregation_number, get_uppers, is_aggregating_node, is_root_node,
    },
    cleanup_old_edges::OutdatedEdge,
    connect_children::connect_children,
    invalidate::make_task_dirty_internal,
    prepare_new_children::prepare_new_children,
    update_collectible::UpdateCollectibleOperation,
};
