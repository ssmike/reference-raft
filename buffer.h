#pragma once

#include "fwd.h"

#include <limits>
#include <memory>
#include <stack>
#include <string_view>
#include <vector>

namespace bus {


class BufferPool {
public:
    static constexpr size_t kInvalidBuffer = std::numeric_limits<size_t>::max();

public:
    BufferPool(int start_size, int capacity)
        : start_size_(start_size)
    {
    }

    size_t take() {
        if (free_.empty()) {
            free_.push(buffers_.size());
            buffers_.emplace_back();
            buffers_.back().reserve(start_size_);
        }
        size_t result = free_.top();
        free_.pop();
        return result;
    }

    void put(size_t num) {
        if (num != kInvalidBuffer) {
            free_.push(num);
        }
    }

    GenericBuffer& get(size_t num) {
        return buffers_[num];
    }

private:
    std::vector<bus::GenericBuffer> buffers_;
    std::stack<size_t> free_;
    size_t start_size_;
};

class ScopedBuffer {
public:
    ScopedBuffer(BufferPool& pool)
        : buf_(pool.take())
        , pool_(pool)
    {
    }

    ScopedBuffer(const ScopedBuffer&) = delete;

    ScopedBuffer(ScopedBuffer&& other)
        : pool_(other.pool_)
    {
        pool_ = other.pool_;
        buf_ = other.buf_;
        other.buf_ = BufferPool::kInvalidBuffer;
    }

    GenericBuffer& get() {
        return pool_.get(buf_);
    }

    ~ScopedBuffer() {
        pool_.put(buf_);
    }

private:
    size_t buf_ = BufferPool::kInvalidBuffer;
    BufferPool& pool_;
};

class SharedView {
public:
    SharedView(ScopedBuffer buf) {
        std::unique_ptr<ScopedBuffer> ptr(new ScopedBuffer(std::move(buf)));
        buf_ = std::move(ptr);
    }

    SharedView(const SharedView&) = default;

    SharedView slice(size_t start, size_t size) {
        SharedView result = *this;
        result.view_ = { view_.data() + start, size };
        return result;
    }

    SharedView skip(size_t start) {
        return slice(start, get().size() - start);
    }

    SharedView resize(size_t size) {
        return slice(0, size);
    }

    std::string_view get() {
        return view_;
    }

private:
    std::shared_ptr<ScopedBuffer> buf_;
    std::string_view view_;
};

}
