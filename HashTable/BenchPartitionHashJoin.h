#pragma once

#include "BenchHashJoin.h"

template<size_t payload>
std::vector<std::vector<KeyValue<payload>>> partition(const std::vector<KeyValue<payload>> & input, size_t partition_num)
{
    std::vector<std::vector<KeyValue<payload>>> ret;
    ret.resize(partition_num);

    size_t size = input.size();
    size_t expected_size = (size + partition_num - 1) / partition_num;
    for (size_t i = 0; i < partition_num; ++i)
    {
        ret[i].reserve(expected_size);
    }

    auto hash_method = HashCRC32<uint64_t>();
    for (size_t i = 0; i < size; ++i)
    {
        uint32_t hash = hash_method(input[i].key);
        size_t partition = (hash * partition_num) >> 32;
        ret[partition].emplace_back(input[i]);
        //__builtin_prefetch(ret[partition].data() + ret[partition].size());
    }

    return ret;
}

template<size_t build_payload = 8, size_t probe_payload = 8>
void TestPartitionLinear(size_t build_size, size_t probe_size, size_t match_possibility, size_t partition_num)
{
    std::string log_head = "partition linear " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    struct Cell
    {
        KeyValue<build_payload> * kv = nullptr;
    };

    using CKHashTable = HashMap<uint64_t, Cell, HashCRC32<uint64_t>>;

    std::vector<CKHashTable> hash_table(partition_num);

    using MappedType = typename CKHashTable::mapped_type;

    Stopwatch watch;
    Stopwatch watch2;

    auto build_partition_kv = partition<build_payload>(build_kv, partition_num);
    printf("%s partition build time %llu\n", log_head.c_str(), watch.elapsedFromLastTime());

    for (size_t part = 0; part < partition_num; ++part)
    {
        auto & build = build_partition_kv[part];
        auto & ht = hash_table[part];
        size_t size = build.size();
        for (size_t i = 0; i < size; ++i)
        {
            typename CKHashTable::LookupResult it;
            bool inserted;
            ht.emplace(build[i].key, it, inserted);
            if (inserted)
                new(&it->getMapped()) MappedType(Cell{&build_kv[i]});
            else {
                build_kv[i].next = it->getMapped().kv->next;
                it->getMapped().kv->next = &build_kv[i];
            }
        }
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    size_t hash_table_size = 0;
    size_t hash_table_buf_size = 0;
    for (size_t part = 0; part < partition_num; ++part)
    {
        hash_table_size += hash_table[part].size();
        hash_table_buf_size += hash_table[part].bufSize();
    }
    printf("%s build hash table time %llu, size %zu, buf %zu\n", log_head.c_str(), build_hash_time, hash_table_size, hash_table_buf_size);

    auto probe_partition_kv = partition<probe_payload>(probe_kv, partition_num);
    printf("%s partition probe time %llu\n", log_head.c_str(), watch.elapsedFromLastTime());

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    for (size_t part = 0; part < partition_num; ++part)
    {
        auto & probe = probe_partition_kv[part];
        auto & ht = hash_table[part];
        size_t size = probe.size();
        for (size_t i = 0; i < size; ++i)
        {
            auto * it = ht.find(probe[i].key);
            if (it != ht.end())
            {
                for (auto * p = it->getMapped().kv; p != nullptr; p = p->next)
                {
                    output_build.emplace_back(*p);
                    output_probe.emplace_back(probe_kv[i]);
                }
            }
        }
    }

    unsigned long long probe_hash_time = watch.elapsedFromLastTime();

    printf("%s probe hash table + construct tuple time %llu, size %lu\n", log_head.c_str(), probe_hash_time, output_build.size());

    unsigned long long total_time = watch2.elapsedFromLastTime();
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

template<size_t build_payload = 8, size_t probe_payload = 8>
void TestPartitionChained(size_t build_size, size_t probe_size, size_t match_possibility, size_t partition_num)
{
    std::string log_head = "chained " + std::to_string(build_size) + "/" + std::to_string(probe_size) + "/" + std::to_string(match_possibility);

    auto [build_kv, probe_kv] = init<build_payload, probe_payload>(build_size, probe_size, match_possibility);

    auto hash_method = HashCRC32<uint64_t>();

    Stopwatch watch;
    Stopwatch watch2;

    size_t head_size = 1 << (static_cast<size_t>(log2(build_size - 1)) + 2);
    size_t hash_mask = head_size - 1;

    auto build_partition_kv = partition<build_payload>(build_kv, partition_num);
    printf("%s partition build time %llu\n", log_head.c_str(), watch.elapsedFromLastTime());

    for (size_t part = 0; part < partition_num; ++part)
    {
        auto &build = build_partition_kv[part];
        size_t size = build.size();
        for (size_t i = 0; i < size; ++i)
        {

        }
    }
    std::vector<KeyValue<build_payload> *> head(head_size);
    printf("size %zu, %zu\n", build_size, head_size);
    for (size_t i = 0; i < build_size; ++i)
    {
        size_t hash = hash_method(build_kv[i].key);
        size_t bucket = hash & hash_mask;
        build_kv[i].next = head[bucket];
        head[bucket] = &build_kv[i];
    }

    unsigned long long build_hash_time = watch.elapsedFromLastTime();

    printf("%s build hash table time %llu\n", log_head.c_str(), build_hash_time);

    std::vector<KeyValue<build_payload>> output_build;
    output_build.reserve(probe_size);
    std::vector<KeyValue<probe_payload>> output_probe;
    output_probe.reserve(probe_size);

    size_t jump_len_sum = 0;
    size_t max_len = 0;
    size_t empty_count = 0;
    for (size_t i = 0; i < probe_size; ++i)
    {
        size_t bucket = hash_method(probe_kv[i].key) & hash_mask;
        auto * h = head[bucket];
        size_t len = 0;
        while (h != nullptr)
        {
            if (h->key == probe_kv[i].key)
            {
                output_build.emplace_back(*h);
                output_probe.emplace_back(probe_kv[i]);
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

    printf("%s probe hash table + construct tuple time %llu, size %lu, max_len %zu, empty_head %zu, jump_len_sum %zu \n", log_head.c_str(), probe_hash_time, output_build.size(), max_len, empty_count, jump_len_sum);

    unsigned long long total_time = watch2.elapsedFromLastTime();
    printf("%s total_time %llu\n", log_head.c_str(), total_time);
}

void benchPartitionHashTable(int argc, char** argv)
{
    if (argc < 6)
    {
        printf("lack argument\n");
        return;
    }
    size_t RUN, n, m, match, part;
    sscanf(argv[1], "%zu", &RUN);
    sscanf(argv[2], "%zu", &n);
    sscanf(argv[3], "%zu", &m);
    sscanf(argv[4], "%zu", &match);
    sscanf(argv[5], "%zu", &part);

    if (RUN == 0)
    {
        TestPartitionLinear(n, m, match, part);
    }
    else
    {
        printf("unknown type: %zu\n", RUN);
        return;
    }
}