#pragma once

#include <string>
#include <iostream>
#include <future>
#include <vector>
#include <cassert>
#include <ctime>
#include <cstdlib>
#include <list>
#include <random>
#include <cstdio>
#include <cstring>
#include <typeinfo>
#include <typeindex>

#include "Hash.h"
#include "HashMap.h"
#include "Arena.h"
#include "Stopwatch.h"
#include "Column.h"

template<size_t payload>
struct Value
{
    char p[payload]{};
};

template<size_t payload>
struct KeyValue
{
    explicit KeyValue(uint64_t k):key(k){}
    uint64_t key;
    Value<payload> value;
    KeyValue<payload> * next = nullptr;
};

template<size_t payload>
bool compare(std::vector<KeyValue<payload>> v1, std::vector<KeyValue<payload>> v2)
{
    if (v1.size() != v2.size())
        return false;
    for (size_t i = 0; i < v1.size(); ++i)
    {
        if (v1[i].key != v2[i].key)
            return false;
        if (memcmp(v1[i].value.p, v2[i].value.p, payload) != 0)
            return false;
    }
    return true;
}

template<size_t build_payload, size_t probe_payload>
std::tuple<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> init(size_t build_size, size_t probe_size, size_t match_possibility)
{
    std::vector<KeyValue<build_payload>> build_kv;
    std::vector<KeyValue<probe_payload>> probe_kv;

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<uint32_t> int_dist;

    std::random_device rd2;
    std::mt19937 mt2(rd2());
    std::uniform_int_distribution<uint32_t> int_dist2;

    build_kv.reserve(build_size);
    for (size_t i = 0; i < build_size; ++i)
    {
        uint32_t random;
        do {
            random = int_dist(mt);
        } while (random == 0);
        build_kv.emplace_back(random);
    }

    probe_kv.reserve(probe_size);
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t is_match = int_dist2(mt2) % 100 < match_possibility;
        if (is_match)
        {
            probe_kv.emplace_back(build_kv[int_dist(mt) % build_size].key);
        }
        else
        {
            probe_kv.emplace_back(uint64_t(int_dist(mt)) + UINT32_MAX);
        }
    }

    return {build_kv, probe_kv};
}

void FlushCache()
{
    const size_t bigger_than_cachesize = 15 * 1024 * 1024;
    long * p = new long[bigger_than_cachesize];
    // When you want to "flush" cache.
    for(size_t i = 0; i < bigger_than_cachesize; ++i)
    {
        p[i] = rand();
    }
    delete [] p;
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
void TestLinear(size_t build_size, size_t probe_size, size_t match_possibility)
{
    std::string log_head = "linear " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    struct Cell
    {
        KeyValue<build_payload> * kv = nullptr;
    };

    using CKHashTable = HashMap<uint64_t, Cell, HashCRC32<uint64_t>>;

    CKHashTable hash_table;
    using MappedType = typename CKHashTable::mapped_type;

    Stopwatch watch;
    Stopwatch watch2;

    for (size_t i = 0; i < build_size; ++i)
    {
        typename CKHashTable::LookupResult it;
        bool inserted;
        hash_table.emplace(build_kv[i].key, it, inserted);
        if (inserted)
            new (&it->getMapped()) MappedType(Cell{&build_kv[i]});
        else
        {
            build_kv[i].next = it->getMapped().kv->next;
            it->getMapped().kv->next = &build_kv[i];
        }
    }

    size_t collision = hash_table.getCollisions();
    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, size %zu, buf %zu, collision %zu, displace_max_step %zu\n", log_head.c_str(), build_hash_time, hash_table.size(), hash_table.bufSize(), collision, hash_table.getDisplaceMaxStep());

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t offset = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        auto * it = hash_table.find(probe_kv[i].key);
        if (it != nullptr)
        {
            if constexpr (construct_tuple)
            {
                for (auto * p = it->getMapped().kv; p != nullptr; p = p->next)
                {
                    output_build.emplace_back(*p);
                    output_probe.emplace_back(probe_kv[i]);
                    ++offset;
                }
            }
            else
            {
                ++offset;
            }
        }
    }

    collision = hash_table.getCollisions() - collision;
    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, collision %zu\n", log_head.c_str(), probe_hash_time, offset, collision);
    else
        printf("%s probe hash table time %llu, size %lu, collision %zu\n", log_head.c_str(), probe_hash_time, offset, collision);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
void TestLinearPrefetch(size_t build_size, size_t probe_size, size_t match_possibility)
{
    std::string log_head = "linear(prefetch) " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    struct Cell
    {
        KeyValue<build_payload> * kv = nullptr;
    };

    using CKHashTable = HashMap<uint64_t, Cell, HashCRC32<uint64_t>>;

    CKHashTable hash_table;
    using MappedType = typename CKHashTable::mapped_type;

    Stopwatch watch;
    Stopwatch watch2;

    for (size_t i = 0; i < build_size; ++i)
    {
        typename CKHashTable::LookupResult it;
        bool inserted;
        hash_table.emplace(build_kv[i].key, it, inserted);
        if (inserted)
            new (&it->getMapped()) MappedType(Cell{&build_kv[i]});
        else
        {
            build_kv[i].next = it->getMapped().kv->next;
            it->getMapped().kv->next = &build_kv[i];
        }
    }

    size_t collision = hash_table.getCollisions();
    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, size %zu, buf %zu, collision %zu\n", log_head.c_str(), build_hash_time, hash_table.size(), hash_table.bufSize(), collision);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    auto hash_method = HashCRC32<uint64_t>();

    const auto PREFETCH = 16;
    size_t hashes[PREFETCH];
    for (size_t i = 0; i < PREFETCH && i < probe_size; ++i)
    {
        hashes[i] = hash_method(probe_kv[i].key);
    }
    size_t offset = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t pos = i % PREFETCH;
        size_t hash_value = hashes[pos];
        if (i + PREFETCH < probe_size)
        {
            hashes[pos] = hash_method(probe_kv[i + PREFETCH].key);
            hash_table.prefetch(hashes[pos]);
        }
        auto * it = hash_table.find(probe_kv[i].key, hash_value);
        if (it != hash_table.end())
        {
            if constexpr (construct_tuple)
            {
                for (auto *p = it->getMapped().kv; p != nullptr; p = p->next)
                {
                    output_build.emplace_back(*p);
                    output_probe.emplace_back(probe_kv[i]);
                    ++offset;
                }
            }
            else
            {
                ++offset;
            }
        }
    }

    collision = hash_table.getCollisions() - collision;
    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, collision %zu\n", log_head.c_str(), probe_hash_time, offset, collision);
    else
        printf("%s probe hash table time %llu, size %lu, collision %zu\n", log_head.c_str(), probe_hash_time, offset, collision);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
std::pair<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> TestChained(size_t build_size, size_t probe_size, size_t match_possibility, const std::tuple<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> * input = nullptr)
{
    std::string log_head = "chained " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = input ? *input : init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    size_t head_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = head_size - 1;
    std::vector<KeyValue<build_payload> *> head(head_size);
    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        build_kv[i].next = head[bucket];
        head[bucket] = &build_kv[i];
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, head_size %zu\n", log_head.c_str(), build_hash_time, head_size);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    size_t empty = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t hash = hash_method(probe_kv[i].key);
        size_t bucket = hash & hash_mask;
        auto & h = head[bucket];
        if (head[bucket] == nullptr)
            ++empty;
    }

    auto time = watch.elapsedFromLastTime();
    printf("%s just get head array time %llu, empty %zu \n", log_head.c_str(), time, empty);

    flush_cache_time += time;

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t jump_len_sum = 0;
    size_t max_len = 0;
    size_t empty_count = 0;
    size_t offset = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t bucket = hash_method(probe_kv[i].key) & hash_mask;
        auto * h = head[bucket];
        size_t len = 0;
        while (h != nullptr)
        {
            if (h->key == probe_kv[i].key)
            {
                ++offset;
                if constexpr (construct_tuple)
                {
                    output_build.emplace_back(*h);
                    output_probe.emplace_back(probe_kv[i]);
                }
            }
            ++len;
            h = h->next;
        }
        jump_len_sum += len;
        if (len == 0)
            ++empty_count;
        if (len > max_len)
            max_len = len;
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu \n", log_head.c_str(), probe_hash_time, offset, max_len, empty_count, jump_len_sum);
    else
        printf("%s probe hash table time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu \n", log_head.c_str(), probe_hash_time, offset, max_len, empty_count, jump_len_sum);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);

    return std::make_pair(std::move(output_build), std::move(output_probe));
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
std::pair<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> TestYangChained(size_t build_size, size_t probe_size, size_t match_possibility, const std::tuple<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> * input = nullptr)
{
    std::string log_head = "YangChained " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = input ? *input : init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    size_t head_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = head_size - 1;
    struct Node
    {
        size_t hash = 0;
        void * pointer = nullptr;
    };
    std::vector<Node> head(head_size);
    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        build_kv[i].next = static_cast<KeyValue<build_payload>*>(head[bucket].pointer);
        head[bucket].pointer = &build_kv[i];
        head[bucket].hash |= hash;
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, head_size %zu\n", log_head.c_str(), build_hash_time, head_size);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    size_t empty = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t hash = hash_method(probe_kv[i].key);
        size_t bucket = hash & hash_mask;
        auto & h = head[bucket];
        if (head[bucket].pointer == nullptr)
            ++empty;
    }

    auto time = watch.elapsedFromLastTime();
    printf("%s just get head array time %llu, empty %zu \n", log_head.c_str(), time, empty);

    flush_cache_time += time;

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t jump_len_sum = 0;
    size_t max_len = 0;
    size_t empty_count = 0;
    size_t offset = 0;
    size_t or_hash_stop_count = 0;

    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t hash = hash_method(probe_kv[i].key);
        size_t bucket = hash & hash_mask;
        auto & h = head[bucket];
        if (head[bucket].pointer == nullptr)
        {
            ++empty_count;
            continue;
        }
        if ((hash | head[bucket].hash) != head[bucket].hash)
        {
            ++or_hash_stop_count;
            continue;
        }
        auto * p = static_cast<KeyValue<build_payload>*>(h.pointer);
        size_t len = 0;
        while (p != nullptr)
        {
            if (p->key == probe_kv[i].key)
            {
                ++offset;
                if constexpr (construct_tuple)
                {
                    output_build.emplace_back(*p);
                    output_probe.emplace_back(probe_kv[i]);
                }
            }
            ++len;
            p = p->next;
        }
        jump_len_sum += len;
        if (len > max_len)
            max_len = len;
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu, or_hash_stop_count %zu \n", log_head.c_str(), probe_hash_time, offset, max_len, empty_count, jump_len_sum, or_hash_stop_count);
    else
        printf("%s probe hash table time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu, or_hash_stop_count %zu \n", log_head.c_str(), probe_hash_time, offset, max_len, empty_count, jump_len_sum, or_hash_stop_count);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);

    return std::make_pair(std::move(output_build), std::move(output_probe));
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
std::pair<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> TestYangHash(size_t build_size, size_t probe_size, size_t match_possibility, const std::tuple<std::vector<KeyValue<build_payload>>, std::vector<KeyValue<probe_payload>>> * input = nullptr)
{
    std::string log_head = "YangHash " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = input ? *input : init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    size_t head_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = head_size - 1;
    struct Node
    {
        uint16_t status = 0;
        uint16_t status_counter = 0;
        uint32_t length = 0;
        void * pointer = nullptr;
    };
    std::vector<Node> head(head_size);
    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        build_kv[i].next = static_cast<KeyValue<build_payload>*>(head[bucket].pointer);
        ++head[bucket].length;
        head[bucket].pointer = &build_kv[i];
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, head_size %zu\n", log_head.c_str(), build_hash_time, head_size);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    Arena arena;

    struct KeyPointer
    {
        uint64_t key;
        KeyValue<build_payload> * pointer;
    };

    size_t jump_len_sum = 0;
    size_t max_len = 0;
    size_t empty_count = 0;
    size_t offset = 0;
    size_t reconstruct_time = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t bucket = hash_method(probe_kv[i].key) & hash_mask;
        auto & h = head[bucket];
        if (h.pointer == nullptr)
        {
            ++empty_count;
            continue;
        }

        if (h.length >= 3)
        {
            if (h.status == 1)
            {
                auto * p = static_cast<KeyPointer*>(h.pointer);
                for (size_t j = 0; j < h.length; ++j)
                {
                    if (p[j].key == probe_kv[i].key)
                    {
                        if (p[j].pointer->key == probe_kv[i].key)
                            ++offset;
                        if constexpr (construct_tuple)
                        {
                            output_build.emplace_back(*p[j].pointer);
                            output_probe.emplace_back(probe_kv[i]);
                        }
                    }
                }
                continue;
            }

            ++h.status_counter;
            if (h.status_counter >= 3)
            {
                ++reconstruct_time;
                auto * p = static_cast<KeyValue<build_payload>*>(h.pointer);
                auto * new_p = reinterpret_cast<KeyPointer*>(arena.alloc(h.length * sizeof(KeyPointer)));
                size_t j = 0;
                while (p != nullptr)
                {
                    new_p[j].key = p->key;
                    new_p[j].pointer = p;
                    ++j;
                    if (p->key == probe_kv[i].key)
                    {
                        ++offset;
                        if constexpr (construct_tuple)
                        {
                            output_build.emplace_back(*p);
                            output_probe.emplace_back(probe_kv[i]);
                        }
                    }
                    p = p->next;
                }
                jump_len_sum += h.length;

                h.pointer = new_p;
                h.status = 1;
                continue;
            }
        }

        auto * p = static_cast<KeyValue<build_payload>*>(h.pointer);
        while (p != nullptr)
        {
            if (p->key == probe_kv[i].key)
            {
                ++offset;
                if constexpr (construct_tuple)
                {
                    output_build.emplace_back(*p);
                    output_probe.emplace_back(probe_kv[i]);
                }
            }
            p = p->next;
        }
        jump_len_sum += h.length;
        if (h.length > max_len)
            max_len = h.length;
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu, reconstruct_time %zu \n", log_head.c_str(), probe_hash_time, offset, max_len, empty_count, jump_len_sum, reconstruct_time);
    else
        printf("%s probe hash table time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu, reconstruct_time %zu \n", log_head.c_str(), probe_hash_time, offset, max_len, empty_count, jump_len_sum, reconstruct_time);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);

    return std::make_pair(std::move(output_build), std::move(output_probe));
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
void TestChainedPrefetch(size_t build_size, size_t probe_size, size_t match_possibility)
{
    std::string log_head = "chained(prefetch) " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    size_t head_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = head_size - 1;
    std::vector<KeyValue<build_payload> *> head(head_size);
    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        build_kv[i].next = head[bucket];
        head[bucket] = &build_kv[i];
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, head_size %zu\n", log_head.c_str(), build_hash_time, head_size);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t offset = 0;
    const size_t PREFETCH = 16;
    struct State
    {
        uint32_t stage = 0;
        uint32_t bucket;
        uint64_t key;
        KeyValue<build_payload> * pointer;
    };
    State states[PREFETCH];
    size_t k = 0, current = 0;
    while (current < probe_size)
    {
        k = k == PREFETCH ? 0 : k;
        State & s = states[k];
        if (s.stage == 0)
        {
            s.stage = 1;
            s.key = probe_kv[current].key;
            s.bucket = hash_method(s.key) & hash_mask;
            __builtin_prefetch(head.data() + s.bucket);
            ++current;
            ++k;
        }
        else if (s.stage == 1)
        {
            s.pointer = head[s.bucket];
            if (s.pointer == nullptr)
            {
                s.stage = 0;
            }
            else
            {
                s.stage = 2;
                __builtin_prefetch(s.pointer);
                ++k;
            }
        }
        else if (s.stage == 2)
        {
            if (s.pointer->key == s.key)
            {
                ++offset;
            }
            if (s.pointer->next == nullptr)
            {
                s.stage = 0;
            }
            else
            {
                s.pointer = s.pointer->next;
                __builtin_prefetch(s.pointer);
                ++k;
            }
        }
    }

    for (size_t i = 0; i < PREFETCH; ++i)
    {
        State & s = states[i];
        if (s.stage == 0)
            continue;
        if (s.stage == 1)
        {
            s.pointer = head[s.bucket];
        }
        while (s.pointer != nullptr)
        {
            if (s.pointer->key == s.key)
            {
                ++offset;
            }
            s.pointer = s.pointer->next;
        }
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu\n", log_head.c_str(), probe_hash_time, offset);
    else
        printf("%s probe hash table time %llu, size %lu\n", log_head.c_str(), probe_hash_time, offset);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
void TestMyLinear(size_t build_size, size_t probe_size, size_t match_possibility)
{
    std::string log_head = "my linear " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    struct Cell
    {
        uint64_t key = 0;
        KeyValue<build_payload> * value = nullptr;
    };

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    Allocator<true> alloc;

    Cell * hashmap;
    hashmap = static_cast<Cell*>(alloc.alloc((build_size) * sizeof(Cell)));

    size_t bucket_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = bucket_size - 1;
    std::vector<uint32_t> buckets(bucket_size + 1);

    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        ++buckets[bucket + 1];
    }

    for (size_t i = 1; i <= bucket_size; ++i)
    {
        buckets[i] += buckets[i - 1];
    }

    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;

        size_t pos = buckets[bucket];
        while (true)
        {
            if (hashmap[pos].key == 0)
            {
                hashmap[pos].key = build_kv[i].key;
                hashmap[pos].value = &build_kv[i];
                break;
            }
            else if (hashmap[pos].key == build_kv[i].key)
            {
                hashmap[pos].value->next = &build_kv[i];
                break;
            }
            ++pos;
        }
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu, bucket_size %zu\n", log_head.c_str(), build_hash_time, bucket_size);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t max_len = 0;
    size_t offset = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t bucket = hash_method(probe_kv[i].key) & hash_mask;
        size_t pos = buckets[bucket];
        size_t end_pos = buckets[bucket + 1];
        for (size_t j = pos; j < end_pos; ++j)
        {
            if (hashmap[j].key == probe_kv[i].key)
            {
                if constexpr (construct_tuple)
                {
                    for (auto *p = hashmap[j].value; p != nullptr; p = p->next)
                    {
                        output_build.emplace_back(*p);
                        output_probe.emplace_back(probe_kv[i]);
                        ++offset;
                    }
                }
                else
                {
                    ++offset;
                }
                break;
            }
        }

        if (end_pos - pos > max_len)
            max_len = end_pos - pos;
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, max_len %zu\n", log_head.c_str(), probe_hash_time, offset, max_len);
    else
        printf("%s probe hash table time %llu, size %lu, max_len %zu\n", log_head.c_str(), probe_hash_time, offset, max_len);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

template<bool construct_tuple, size_t build_payload = 8, size_t probe_payload = 8>
void TestMyLinear2(size_t build_size, size_t probe_size, size_t match_possibility)
{
    std::string log_head = "my linear2 " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility) + "/" + std::to_string(construct_tuple);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    struct Cell
    {
        int64_t pos = 0;
        uint64_t key = 0;
        KeyValue<build_payload> * value = nullptr;
    };

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    Allocator<true> alloc;

    size_t bucket_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = bucket_size - 1;

    Cell * hashmap;
    hashmap = static_cast<Cell*>(alloc.alloc((bucket_size + 1) * sizeof(Cell)));
    //std::vector<Cell> hashmap(bucket_size + 1);

    std::vector<size_t> buckets(bucket_size);

    printf("size %zu, %zu\n", build_size, bucket_size);

    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        ++buckets[bucket];
    }

    size_t next_pos = 0;
    for (size_t i = 0; i < bucket_size; ++i)
    {
        if (buckets[i] == 0)
        {
            if (i > 0 && buckets[i - 1] > 0)
                hashmap[i].pos = -buckets[i - 1];
            else
                hashmap[i].pos = -1;
            continue;
        }
        if (next_pos > i)
        {
            hashmap[i].pos = next_pos;
            next_pos += buckets[i];
        }
        else
        {
            hashmap[i].pos = i;
            next_pos = i + buckets[i];
        }
    }
    if (bucket_size > 0 && buckets[bucket_size - 1] > 0)
        hashmap[bucket_size].pos = -buckets[bucket_size - 1];

    if (next_pos > bucket_size)
    {
        size_t pos_diff = next_pos - bucket_size;
        for (size_t i = 0; i < bucket_size && pos_diff > 0; ++i)
        {
            if (buckets[i] == 0)
            {
                --pos_diff;
                continue;
            }
            hashmap[i].pos += pos_diff;
        }
        assert(pos_diff == 0);
    }

    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;

        size_t pos = hashmap[bucket].pos;

        while (true)
        {
            pos &= hash_mask;
            if (hashmap[pos].key == 0)
            {
                hashmap[pos].key = build_kv[i].key;
                hashmap[pos].value = &build_kv[i];
                break;
            }
            else if (hashmap[pos].key == build_kv[i].key)
            {
                hashmap[pos].value->next = &build_kv[i];
                break;
            }
            ++pos;
        }
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu\n", log_head.c_str(), build_hash_time);

    FlushCache();
    unsigned long long flush_cache_time = watch.elapsedFromLastTime();

    //printf("%s flush cache time %llu\n", log_head.c_str(), flush_cache_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t max_len = 0;
    size_t offset = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t bucket = hash_method(probe_kv[i].key) & hash_mask;
        if (hashmap[bucket].key == probe_kv[i].key)
        {
            if constexpr (construct_tuple)
            {
                for (auto *p = hashmap[bucket].value; p != nullptr; p = p->next)
                {
                    output_build.emplace_back(*p);
                    output_probe.emplace_back(probe_kv[i]);
                    ++offset;
                }
            }
            else
            {
                ++offset;
            }
            continue;
        }
        size_t key = hashmap[bucket].key;
        if (hashmap[bucket].pos < 0 || hashmap[bucket].key == 0)
            continue;

        size_t pos = hashmap[bucket].pos;
        size_t end_pos;
        if (hashmap[bucket + 1].pos > 0)
            end_pos = hashmap[bucket + 1].pos;
        else
            end_pos = pos + (-hashmap[bucket + 1].pos);
        for (size_t j = pos; j < end_pos; ++j)
        {
            if (hashmap[j & hash_mask].key == probe_kv[i].key)
            {
                if constexpr (construct_tuple)
                {
                    for (auto *p = hashmap[j & hash_mask].value; p != nullptr; p = p->next)
                    {
                        output_build.emplace_back(*p);
                        output_probe.emplace_back(probe_kv[i]);
                        ++offset;
                    }
                }
                else
                {
                    ++offset;
                }
                break;
            }
        }

        if (end_pos - pos > max_len)
            max_len = end_pos - pos;
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    if constexpr (construct_tuple)
        printf("%s probe hash table + construct tuple time %llu, size %lu, max_len %zu\n", log_head.c_str(), probe_hash_time, offset, max_len);
    else
        printf("%s probe hash table time %llu, size %lu, max_len %zu\n", log_head.c_str(), probe_hash_time, offset, max_len);

    unsigned long long total_time = watch2.elapsedFromLastTime() - flush_cache_time;
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

void benchHashTable(int argc, char** argv)
{
    /*auto input = init<8, 8>(1000, 10000, 25);
    auto [a1, a2] = TestChained<true>(1000, 10000, 25, &input);
    auto [b1, b2] = TestYangHash<true>(1000, 10000, 25, &input);

    if (!compare(a1, b1))
    {
        printf("a1 != b1");
        return;
    }
    if (!compare(a2, b2))
    {
        printf("a2 != b2");
        return;
    }*/

    if (argc < 6)
    {
        printf("lack argument\n");
        return;
    }

    size_t RUN, n, m, match, construct_tuple;
    sscanf(argv[1], "%zu", &RUN);
    sscanf(argv[2], "%zu", &n);
    sscanf(argv[3], "%zu", &m);
    sscanf(argv[4], "%zu", &match);
    sscanf(argv[5], "%zu", &construct_tuple);

    if (RUN == 0)
    {
        if (construct_tuple)
            TestLinear<true>(n, m, match);
        else
            TestLinear<false>(n, m, match);
    }
    else if (RUN == 1)
    {
        if (construct_tuple)
            TestLinearPrefetch<true>(n, m, match);
        else
            TestLinearPrefetch<false>(n, m, match);
    }
    else if (RUN == 2)
    {
        if (construct_tuple)
            TestChained<true>(n, m, match);
        else
            TestChained<false>(n, m, match);
    }
    else if (RUN == 3)
    {
        TestChainedPrefetch<false>(n, m, match);
    }
    else if (RUN == 4)
    {
        if (construct_tuple)
            TestMyLinear<true>(n, m, match);
        else
            TestMyLinear<false>(n, m, match);
    }
    else if (RUN == 5)
    {
        if (construct_tuple)
            TestMyLinear2<true>(n, m, match);
        else
            TestMyLinear2<false>(n, m, match);
    }
    else if (RUN == 6)
    {
        if (construct_tuple)
            TestYangHash<true>(n, m, match);
        else
            TestYangHash<false>(n, m, match);
    }
    else if (RUN == 7)
    {
        if (construct_tuple)
            TestYangChained<true>(n, m, match);
        else
            TestYangChained<false>(n, m, match);
    }
    else
    {
        printf("unknown type: %zu\n", RUN);
        return;
    }
}
