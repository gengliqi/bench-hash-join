
#pragma once

#include <vector>

class IColumn
{
public:
    virtual void insertData(const char * pos, size_t length) = 0;
    virtual size_t size() = 0;
};

template<typename T>
class ColumnVector final : public IColumn
{
public:
    void insertData(const char * pos, size_t) override
    {
        vec.push_back(*reinterpret_cast<const T*>(pos));
    }
    size_t size() override
    {
        return vec.size();
    }
private:
    std::vector<T> vec;
};

class ColumnString final : public IColumn
{
public:
    void insertData(const char * pos, size_t length) override
    {
        size_t size = vec.size();
        vec.resize(size + length);
        memcpy(vec.data() + size, pos, length);
    }
    size_t size() override
    {
        return vec.size();
    }
private:
    std::vector<uint8_t> vec;
};

int getColumnType(IColumn * column)
{
    std::type_index ti(typeid(*column));
    if (ti == std::type_index(typeid(ColumnVector<uint8_t>)))
        return 1;
    else if (ti == std::type_index(typeid(ColumnVector<uint16_t>)))
        return 2;
    else if (ti == std::type_index(typeid(ColumnVector<uint32_t>)))
        return 3;
    else if (ti == std::type_index(typeid(ColumnVector<uint64_t>)))
        return 4;
    else if (ti == std::type_index(typeid(ColumnVector<int8_t>)))
        return 5;
    else if (ti == std::type_index(typeid(ColumnVector<int16_t>)))
        return 6;
    else if (ti == std::type_index(typeid(ColumnVector<int32_t>)))
        return 7;
    else if (ti == std::type_index(typeid(ColumnVector<int64_t>)))
        return 8;
    else if (ti == std::type_index(typeid(ColumnString)))
        return 9;
    return -1;
}

void insertRandom(std::vector<IColumn *> & columns, const std::vector<uint64_t> & inserts)
{
    size_t size = columns.size();
    for (auto & d : inserts)
    {
        for (size_t i = 0; i < size; ++i)
        {
            columns[i]->insertData(reinterpret_cast<const char*>(&d), 8);
        }
    }
    for (size_t i = 0; i < size; ++i)
        std::cout << i << ":" << columns[i]->size() << std::endl;
}

void insertRandomDevirtualize(std::vector<IColumn *> & columns, const std::vector<uint64_t> & inserts)
{
    size_t size = columns.size();
    std::vector<int> types(size);
    for (size_t i = 0; i < size; ++i)
        types[i] = getColumnType(columns[i]);
    for (auto & d : inserts)
    {
        for (size_t i = 0; i < size; ++i)
        {
            switch (types[i])
            {
                case 1:
                    static_cast<ColumnVector<uint8_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 2:
                    static_cast<ColumnVector<uint16_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 3:
                    static_cast<ColumnVector<uint32_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 4:
                    static_cast<ColumnVector<uint64_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 5:
                    static_cast<ColumnVector<int8_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 6:
                    static_cast<ColumnVector<int16_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 7:
                    static_cast<ColumnVector<int32_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 8:
                    static_cast<ColumnVector<int64_t>*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                case 9:
                    static_cast<ColumnString*>(columns[i])->insertData(reinterpret_cast<const char*>(&d), 8);
                    break;
                default:
                    exit(-1);
            }
        }
    }

    for (size_t i = 0; i < size; ++i)
        std::cout << i << ":" << columns[i]->size() << std::endl;
}
