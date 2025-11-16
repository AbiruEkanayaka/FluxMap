//! A simple, thread-safe slab allocator.
//!
//! This allocator is designed to manage fixed-size objects, reducing contention
//! on the global allocator and improving performance for frequent allocations
//! and deallocations of objects like `Node` and `VersionNode`.

use std::mem::MaybeUninit;
use std::ptr::{NonNull};
use std::sync::Mutex;

const SLAB_SIZE: usize = 128; // Number of objects per slab

/// A large, contiguous chunk of memory from which individual object allocations are served.
struct Slab<T> {
    memory: Box<[MaybeUninit<T>]>,
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
            memory: memory.into_boxed_slice(),
        }
    }
}

/// A thread-safe allocator for objects of type `T`.
pub struct SlabAllocator<T> {
    /// A list of all slabs owned by this allocator.
    slabs: Mutex<Vec<Slab<T>>>,
    /// A list of pointers to available (free) memory slots.
    free_list: Mutex<Vec<NonNull<T>>>,
}

// SAFETY: The SlabAllocator is thread-safe because all access to its shared state
// (the slabs and the free_list) is protected by Mutexes. Raw pointers are stored,
// but not dereferenced within the allocator. The pointers themselves can be safely
// sent across threads.
unsafe impl<T> Send for SlabAllocator<T> {}
unsafe impl<T> Sync for SlabAllocator<T> {}


impl<T> SlabAllocator<T> {
    /// Creates a new, empty `SlabAllocator`.
    pub fn new() -> Self {
        SlabAllocator {
            slabs: Mutex::new(Vec::new()),
            free_list: Mutex::new(Vec::new()),
        }
    }

    /// Grows the allocator by adding a new slab and populating the free list with its slots.
    fn grow(&self) {
        // Create the new slab outside the locks.
        let new_slab = Slab::new();
        let slab_ptr = new_slab.memory.as_ptr() as *mut T;

        // Add the new slab to the allocator's list of slabs.
        self.slabs.lock().unwrap().push(new_slab);

        // Populate the free list with pointers to the new memory slots.
        let mut free_list = self.free_list.lock().unwrap();
        for i in 0..SLAB_SIZE {
            let ptr = unsafe { slab_ptr.add(i) };
            free_list.push(NonNull::new(ptr).unwrap());
        }
    }

    /// Allocates a memory slot for one object.
    ///
    /// If the free list is empty, this will trigger `grow()` to create a new slab.
    pub fn alloc(&self) -> NonNull<T> {
        loop {
            // Optimistically try to pop from the free list.
            if let Some(ptr) = self.free_list.lock().unwrap().pop() {
                return ptr;
            }
            // If the list was empty, grow it and try again.
            self.grow();
        }
    }

    /// Returns a memory slot to the free list.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the object at `ptr` has been dropped
    /// before calling `free`, as the memory may be immediately reused.
    pub fn free(&self, ptr: NonNull<T>) {
        self.free_list.lock().unwrap().push(ptr);
    }
}

impl<T> Default for SlabAllocator<T> {
    fn default() -> Self {
        Self::new()
    }
}
