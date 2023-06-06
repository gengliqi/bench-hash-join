// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string.h>

#include <atomic>

namespace DB
{
    extern std::atomic_size_t allocator_mmap_counter;
}
/** Responsible for allocating / freeing memory. Used, for example, in PODArray, Arena.
  * Also used in hash tables.
  * The interface is different from std::allocator
  * - the presence of the method realloc, which for large chunks of memory uses mremap;
  * - passing the size into the `free` method;
  * - by the presence of the `alignment` argument;
  * - the possibility of zeroing memory (used in hash tables);
  */
template <bool clear_memory_>
class Allocator
{
protected:
    static constexpr bool clear_memory = clear_memory_;

public:
    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 0);

    /// Free memory range.
    void free(void * buf, size_t size);

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0);

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }
};


/** When using AllocatorWithStackMemory, located on the stack,
  *  GCC 4.9 mistakenly assumes that we can call `free` from a pointer to the stack.
  * In fact, the combination of conditions inside AllocatorWithStackMemory does not allow this.
  */
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif

/** Allocator with optimization to place small memory ranges in automatic memory.
  */
template <typename Base, size_t N = 64>
class AllocatorWithStackMemory : private Base
{
private:
    char stack_memory[N];

public:
    void * alloc(size_t size)
    {
        if (size <= N)
        {
            if (Base::clear_memory)
                memset(stack_memory, 0, N);
            return stack_memory;
        }

        return Base::alloc(size);
    }

    void free(void * buf, size_t size)
    {
        if (size > N)
            Base::free(buf, size);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size)
    {
        /// Was in stack_memory, will remain there.
        if (new_size <= N)
            return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > N)
            return Base::realloc(buf, old_size, new_size);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return N;
    }
};


#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <cstddef>
#include <sys/types.h>
#if !_MSC_VER
#include <sys/mman.h>
#endif

#if defined(MREMAP_MAYMOVE)
// we already have implementation (linux)
#else

#define MREMAP_MAYMOVE 1

void * mremap(void * old_address,
              size_t old_size,
              size_t new_size,
              int flags = 0,
              int mmap_prot = 0,
              int mmap_flags = 0,
              int mmap_fd = -1,
              off_t mmap_offset = 0)
{
    /// No actual shrink
    if (new_size < old_size)
        return old_address;

    if (!(flags & MREMAP_MAYMOVE))
    {
        errno = ENOMEM;
        return nullptr;
    }

#if _MSC_VER
    void * new_address = ::operator new(new_size);
#else
    void * new_address = mmap(nullptr, new_size, mmap_prot, mmap_flags, mmap_fd, mmap_offset);
    if (MAP_FAILED == new_address)
    {
        return MAP_FAILED;
    }
#endif

    memcpy(new_address, old_address, old_size);

#if _MSC_VER
    delete old_address;
#else
    if (munmap(old_address, old_size))
    {
        abort();
    }
#endif

    return new_address;
}

#endif

inline void * clickhouse_mremap(void * old_address,
                                size_t old_size,
                                size_t new_size,
                                int flags = 0,
                                [[maybe_unused]] int mmap_prot = 0,
                                [[maybe_unused]] int mmap_flags = 0,
                                [[maybe_unused]] int mmap_fd = -1,
                                [[maybe_unused]] off_t mmap_offset = 0)
{
    return mremap(old_address,
                  old_size,
                  new_size,
                  flags
#if !defined(MREMAP_FIXED)
            ,
                  mmap_prot,
                  mmap_flags,
                  mmap_fd,
                  mmap_offset
#endif
    );
}

// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#include <sys/mman.h>

#include <cstdlib>


/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif


namespace DB
{
    std::atomic_size_t allocator_mmap_counter;
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int CANNOT_ALLOCATE_MEMORY;
        extern const int CANNOT_MUNMAP;
        extern const int CANNOT_MREMAP;
    } // namespace ErrorCodes
} // namespace DB


/** Many modern allocators (for example, tcmalloc) do not do a mremap for realloc,
  *  even in case of large enough chunks of memory.
  * Although this allows you to increase performance and reduce memory consumption during realloc.
  * To fix this, we do mremap manually if the chunk of memory is large enough.
  * The threshold (64 MB) is chosen quite large, since changing the address space is
  *  very slow, especially in the case of a large number of threads.
  * We expect that the set of operations mmap/something to do/mremap can only be performed about 1000 times per second.
  *
  * PS. This is also required, because tcmalloc can not allocate a chunk of memory greater than 16 GB.
  */
static constexpr size_t MMAP_THRESHOLD = 64 * (1ULL << 30);
static constexpr size_t MMAP_MIN_ALIGNMENT = 4096;
static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;


template <bool clear_memory_>
void * Allocator<clear_memory_>::alloc(size_t size, size_t alignment)
{
    void * buf;

    if (size >= MMAP_THRESHOLD)
    {
        if (alignment > MMAP_MIN_ALIGNMENT)
            assert(false);
            //throw DB::Exception("Too large alignment " + formatReadableSizeWithBinarySuffix(alignment) + ": more than page size when allocating "
            //                    + formatReadableSizeWithBinarySuffix(size) + ".",
            //                    DB::ErrorCodes::BAD_ARGUMENTS);

        buf = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (MAP_FAILED == buf)
            assert(false);
            //DB::throwFromErrno("Allocator: Cannot mmap " + formatReadableSizeWithBinarySuffix(size) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        /// No need for zero-fill, because mmap guarantees it.

        DB::allocator_mmap_counter.fetch_add(size, std::memory_order_acq_rel);
    }
    else
    {
        if (alignment <= MALLOC_MIN_ALIGNMENT)
        {
            if (clear_memory)
                buf = ::calloc(size, 1);
            else
                buf = ::malloc(size);

            if (nullptr == buf)
                assert(false);
                //DB::throwFromErrno("Allocator: Cannot malloc " + formatReadableSizeWithBinarySuffix(size) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        }
        else
        {
            buf = nullptr;
            int res = posix_memalign(&buf, alignment, size);

            if (0 != res)
                assert(false);
                //DB::throwFromErrno("Cannot allocate memory (posix_memalign) " + formatReadableSizeWithBinarySuffix(size) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);

            if (clear_memory)
                memset(buf, 0, size);
        }
    }

    return buf;
}


template <bool clear_memory_>
void Allocator<clear_memory_>::free(void * buf, size_t size)
{
    if (size >= MMAP_THRESHOLD)
    {
        if (0 != munmap(buf, size))
            assert(false);
        DB::allocator_mmap_counter.fetch_sub(size, std::memory_order_acq_rel);
    }
    else
    {
        ::free(buf);
    }
}


template <bool clear_memory_>
void * Allocator<clear_memory_>::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    if (old_size == new_size)
    {
        /// nothing to do.
    }
    else if (old_size < MMAP_THRESHOLD && new_size < MMAP_THRESHOLD && alignment <= MALLOC_MIN_ALIGNMENT)
    {
        buf = ::realloc(buf, new_size);

        if (nullptr == buf)
            assert(false);
            //DB::throwFromErrno("Allocator: Cannot realloc from " + formatReadableSizeWithBinarySuffix(old_size) + DB::toString(old_size) + " to " + formatReadableSizeWithBinarySuffix(new_size) + DB::toString(new_size) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

        if (clear_memory && new_size > old_size)
            memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
    }
    else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD)
    {
        // On apple and freebsd self-implemented mremap used (common/mremap.h)
        buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (MAP_FAILED == buf)
            assert(false);
            //DB::throwFromErrno("Allocator: Cannot mremap memory chunk from " + formatReadableSizeWithBinarySuffix(old_size) + " to " + formatReadableSizeWithBinarySuffix(new_size) + ".", DB::ErrorCodes::CANNOT_MREMAP);

        /// No need for zero-fill, because mmap guarantees it.
        DB::allocator_mmap_counter.fetch_add(new_size - old_size, std::memory_order_acq_rel); // should be true even if overflow
    }
    else if (old_size >= MMAP_THRESHOLD && new_size < MMAP_THRESHOLD)
    {
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, new_size);
        if (0 != munmap(buf, old_size))
        {
            ::free(new_buf);
            assert(false);
        }
        buf = new_buf;

        DB::allocator_mmap_counter.fetch_sub(old_size, std::memory_order_acq_rel);
    }
    else
    {
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, old_size);
        free(buf, old_size);
        buf = new_buf;
    }

    return buf;
}


/// Explicit template instantiations.
template class Allocator<true>;
template class Allocator<false>;
