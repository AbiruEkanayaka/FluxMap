//! Defines the `MemSize` trait for tracking memory usage.

use std::sync::Arc;

/// The memory eviction policy to use when the database reaches its memory limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Evicts the least recently used (LRU) items. Good for general-purpose workloads.
    Lru,
    /// Evicts the least frequently used (LFU) items. Good for workloads where some
    /// items are accessed much more frequently than others.
    Lfu,
    /// Evicts a random item. Good for workloads where access patterns are unpredictable.
    Random,
    /// Adaptive Replacement Cache. Provides a good balance between LRU and LFU.
    Arc,
    /// Manual eviction. The user is responsible for evicting keys when the memory limit is exceeded.
    #[default]
    Manual,
}

/// A trait for types to report their memory usage, including heap-allocated data.
///
/// This is crucial for the database's memory limiting and eviction functionality.
/// You must implement this trait for your custom key and value types.
pub trait MemSize {
    /// Returns the total memory size of the value in bytes.
    ///
    /// # Implementation Notes
    ///
    /// - For `String`, include `capacity()`.
    /// - For `Vec<T>`, include `capacity() * std::mem::size_of::<T>()` and the size of the elements if they also allocate.
    /// - For structs, sum the `mem_size()` of all fields plus `std::mem::size_of::<Self>()`.
    fn mem_size(&self) -> usize;
}

// Implementations for common types

impl MemSize for u8 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for u16 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for u32 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for u64 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for i8 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for i16 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for i32 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for i64 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for f32 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for f64 {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl MemSize for String {
    fn mem_size(&self) -> usize {
        // size_of::<String>() is the size of the pointer, length, and capacity.
        // self.capacity() is the allocated heap size.
        std::mem::size_of::<Self>() + self.capacity()
    }
}

impl<T> MemSize for Vec<T>
where
    T: MemSize,
{
    fn mem_size(&self) -> usize {
        let mut total = std::mem::size_of::<Self>() + (self.capacity() * std::mem::size_of::<T>());
        // If T itself contains heap-allocated data, we need to account for it.
        // This is an approximation if T is not a ZST.
        if std::mem::size_of::<T>() > 0 {
            total += self.iter().map(MemSize::mem_size).sum::<usize>()
                - (self.len() * std::mem::size_of::<T>());
        }
        total
    }
}

impl<T: ?Sized + MemSize> MemSize for Arc<T> {
    fn mem_size(&self) -> usize {
        // This is an approximation. It doesn't account for the Arc's control block,
        // but it does account for the data it points to, which is the significant part.
        self.as_ref().mem_size()
    }
}

impl<T: MemSize> MemSize for Option<T> {
    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.as_ref().map_or(0, MemSize::mem_size)
    }
}
