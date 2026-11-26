//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/vacuum_seal_latch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>

namespace duckdb {

//! Holds back commits that wrote to row groups merged by a running checkpoint until that checkpoint is durable
class VacuumSealLatch {
public:
	//! Waits until the checkpoint has finished - returns whether its merges became durable
	bool Wait() {
		unique_lock<mutex> guard(lock);
		cv.wait(guard, [&]() { return released; });
		return durable;
	}
	void Release(bool durable_p) {
		{
			lock_guard<mutex> guard(lock);
			if (released) {
				return;
			}
			released = true;
			durable = durable_p;
		}
		cv.notify_all();
	}
	bool IsReleased() {
		lock_guard<mutex> guard(lock);
		return released;
	}

private:
	mutex lock;
	std::condition_variable cv;
	bool released = false;
	bool durable = false;
};

} // namespace duckdb
