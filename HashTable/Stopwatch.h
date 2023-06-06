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

#include "Types.h"
#include "Defines.h"
#include <ctime>

#include <atomic>

inline UInt64 clock_gettime_ns(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts
            {
            };
    clock_gettime(clock_type, &ts);
    return UInt64(ts.tv_sec * 1000000000ULL + ts.tv_nsec);
}

/// Sometimes monotonic clock may not be monotonic (due to bug in kernel?).
/// It may cause some operations to fail with "Timeout exceeded: elapsed 18446744073.709553 seconds".
/// Takes previously returned value and returns it again if time stepped back for some reason.
inline UInt64 clock_gettime_ns_adjusted(UInt64 prev_time, clockid_t clock_type = CLOCK_MONOTONIC)
{
    UInt64 current_time = clock_gettime_ns(clock_type);
    if (likely(prev_time <= current_time))
        return current_time;

    /// Something probably went completely wrong if time stepped back for more than 1 second.
    assert(prev_time - current_time <= 1000000000ULL);
    return prev_time;
}

/** Differs from Poco::Stopwatch only by using 'clock_gettime' instead of 'gettimeofday',
  *  returns nanoseconds instead of microseconds, and also by other minor differencies.
  */
class Stopwatch
{
public:
    /** CLOCK_MONOTONIC works relatively efficient (~15 million calls/sec) and doesn't lead to syscall.
      * Pass CLOCK_MONOTONIC_COARSE, if you need better performance with acceptable cost of several milliseconds of inaccuracy.
      */
    explicit Stopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC)
            : clock_type(clock_type_)
    {
        start();
    }

    void start()
    {
        start_ns = nanoseconds();
        last_ns = start_ns;
        is_running = true;
    }

    void stop()
    {
        stop_ns = nanoseconds();
        is_running = false;
    }

    void reset()
    {
        start_ns = 0;
        stop_ns = 0;
        last_ns = 0;
        is_running = false;
    }
    void restart() { start(); }
    UInt64 elapsed() const { return is_running ? nanoseconds() - start_ns : stop_ns - start_ns; }
    UInt64 elapsedMilliseconds() const { return elapsed() / 1000000UL; }
    double elapsedSeconds() const { return static_cast<double>(elapsed()) / 1000000000ULL; }

    UInt64 elapsedFromLastTime()
    {
        const auto now_ns = nanoseconds();
        if (is_running)
        {
            auto rc = now_ns - last_ns;
            last_ns = now_ns;
            return rc;
        }
        else
        {
            return stop_ns - last_ns;
        }
    };

    UInt64 elapsedMillisecondsFromLastTime() { return elapsedFromLastTime() / 1000000UL; }
    UInt64 elapsedSecondsFromLastTime() { return elapsedFromLastTime() / 1000000UL; }

private:
    UInt64 start_ns = 0;
    UInt64 stop_ns = 0;
    UInt64 last_ns = 0;
    clockid_t clock_type;
    bool is_running = false;

    UInt64 nanoseconds() const { return clock_gettime_ns_adjusted(start_ns, clock_type); }
};
