//! A simple, thread-safe, lock-free slab allocator.
//!
//! This allocator is designed to manage fixed-size objects, reducing contention
//! on the global allocator and improving performance for frequent allocations
//! and deallocations of objects like `Node` and `VersionNode`.
//!
//! It uses a lock-free stack (a Treiber stack) to manage the free list, making
//! the hot path (`alloc`/`free`) fully lock-free. A mutex is used only on the
//! cold path (`grow`) to prevent a thundering herd problem when the allocator
//! runs out of memory.

use crossbeam_epoch::{pin, Atomic, Shared};
use std::mem::{self, MaybeUninit};
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

const SLAB_SIZE: usize = 128; // Number of objects per slab

/// A large, contiguous chunk of memory from which individual object allocations are served.
struct Slab<T> {
    _memory: Box<[MaybeUninit<T>]>,
}

impl<T> Slab<T> {
    /// Allocates a new slab.
    fn new() -> Self {
        let mut memory = Vec::with_capacity(SLAB_SIZE);
        // SAFETY: The items are `MaybeUninit` and do not need to be initialized.
        // The memory is not read until it has been written to.
        unsafe {
            memory.set_len(SLAB_SIZE);
        }
        Slab {
            _memory: memory.into_boxed_slice(),
        }
    }

    /// Returns a raw pointer to the beginning of the slab's memory.
    fn as_ptr(&self) -> *mut T {
        self._memory.as_ptr() as *mut T
    }
}

/// An entry in the intrusive linked list of free memory slots.
/// The allocator re-purposes the memory of a freed object to store this.
#[repr(C)]
struct FreeNode {
    next: Atomic<FreeNode>,
}

/// A thread-safe, mostly lock-free allocator for objects of type `T`.
pub struct SlabAllocator<T> {
    /// A list of all slabs owned by this allocator.
    slabs: Mutex<Vec<Slab<T>>>,
    /// The head of the lock-free stack (intrusive linked list) of free memory slots.
    head: Atomic<FreeNode>,
    /// A lock to ensure only one thread can grow the slab list at a time.
    grow_lock: Mutex<()>,
}

// SAFETY: The SlabAllocator is thread-safe. The `slabs` and `grow_lock` are protected
// by Mutexes. The `head` is an atomic pointer managed by crossbeam-epoch, ensuring
// lock-free and memory-safe operations on the free list. Pointers can be sent across threads.
unsafe impl<T> Send for SlabAllocator<T> {}
unsafe impl<T> Sync for SlabAllocator<T> {}

impl<T> SlabAllocator<T> {
    /// Creates a new, empty `SlabAllocator`.
    pub fn new() -> Self {
        assert!(
            mem::size_of::<T>() >= mem::size_of::<FreeNode>(),
            "Size of T must be >= size of FreeNode"
        );
        assert!(
            mem::align_of::<T>() >= mem::align_of::<FreeNode>(),
            "Alignment of T must be >= alignment of FreeNode"
        );

        SlabAllocator {
            slabs: Mutex::new(Vec::new()),
            head: Atomic::null(),
            grow_lock: Mutex::new(()),
        }
    }

    /// Grows the allocator by adding a new slab and populating the free list with its slots.
    /// This is the cold path and is protected by a lock.
    fn grow(&self) {
        let new_slab = Slab::new();
        let slab_ptr = new_slab.as_ptr();

        // This lock is held only during the `push`, which is quick.
        self.slabs.lock().unwrap().push(new_slab);

        // Chain the new nodes together into an intrusive linked list.
        for i in 0..(SLAB_SIZE - 1) {
            let current_node = unsafe { slab_ptr.add(i) as *mut FreeNode };
            let next_node = unsafe { slab_ptr.add(i + 1) as *mut FreeNode };
            // SAFETY: We have exclusive access to this new slab's memory.
            unsafe {
                (*current_node)
                    .next
                    .store(Shared::from(next_node as *const _), Ordering::Relaxed);
            }
        }

        // The last node will be linked to the current head of the free list.
        let last_node = unsafe { slab_ptr.add(SLAB_SIZE - 1) as *mut FreeNode };

        // Atomically prepend this entire new chain to the head of the free list.
        let new_head = Shared::from(slab_ptr as *const FreeNode);
        let guard = &pin();

        loop {
            let old_head = self.head.load(Ordering::Acquire, guard);
            // SAFETY: We have exclusive access to the last node of our new slab.
            unsafe {
                (*last_node).next.store(old_head, Ordering::Relaxed);
            }

            if self
                .head
                .compare_exchange(
                    old_head,
                    new_head,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                )
                .is_ok()
            {
                break;
            }
            // Another thread might be freeing nodes, so retry the CAS.
        }
    }

    /// Allocates a memory slot for one object from the lock-free stack.
    ///
    /// If the free list is empty, this will trigger `grow()` to create a new slab.
    pub fn alloc(&self) -> NonNull<T> {
        let guard = &pin();
        loop {
            let head = self.head.load(Ordering::Acquire, guard);

            if let Some(head_ref) = unsafe { head.as_ref() } {
                let next = head_ref.next.load(Ordering::Relaxed, guard);
                if self
                    .head
                    .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire, guard)
                    .is_ok()
                {
                    // Successfully popped a node from the free list.
                    return NonNull::new(head.as_raw() as *mut T).unwrap();
                }
                // CAS failed, another thread won. Retry.
                continue;
            } else {
                // The free list is empty. We need to grow it.
                // Use a lock for growing to prevent multiple threads from growing simultaneously.
                let _lock = self.grow_lock.lock().unwrap();

                // Double-check if another thread already grew the list while we were waiting for the lock.
                if self.head.load(Ordering::Relaxed, guard).is_null() {
                    self.grow();
                }
                // Loop again to try allocating from the (now grown) list.
            }
        }
    }

    /// Returns a memory slot to the lock-free free list.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the object at `ptr` has been dropped
    /// before calling `free`, as the memory may be immediately reused.
    pub fn free(&self, ptr: NonNull<T>) {
        let node_ptr = ptr.as_ptr() as *mut FreeNode;

        // SAFETY: The pointer is valid and came from this allocator. We are re-purposing it
        // as a FreeNode, which is safe because its memory is no longer in use.
        let node_shared = Shared::from(node_ptr as *const FreeNode);
        let guard = &pin();

        loop {
            let old_head = self.head.load(Ordering::Acquire, guard);
            // SAFETY: The caller guarantees `ptr` is valid and we have exclusive access to write to it.
            unsafe {
                node_shared.deref().next.store(old_head, Ordering::Relaxed);
            }

            if self
                .head
                .compare_exchange(
                    old_head,
                    node_shared,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                )
                .is_ok()
            {
                // Successfully pushed the node back onto the free list.
                return;
            }
            // CAS failed, another thread pushed. Retry.
        }
    }
}

impl<T> Default for SlabAllocator<T> {
    fn default() -> Self {
        Self::new()
    }
}
