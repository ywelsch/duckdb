// Measures wakeup overshoot of different macOS wait primitives for short timed waits.
// For each method and target duration: 200 iterations, reports mean and p99 overshoot.
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static mach_timebase_info_data_t tb;

static uint64_t now_ns(void) {
	return mach_absolute_time() * tb.numer / tb.denom;
}

static int cmp_u64(const void *a, const void *b) {
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
	return x < y ? -1 : x > y;
}

typedef void (*wait_fn)(uint64_t target_ns);

static void wait_sleep_for(uint64_t target_ns) { // == std::this_thread::sleep_for == nanosleep
	struct timespec ts = {0, (long)target_ns};
	nanosleep(&ts, NULL);
}

static void wait_usleep(uint64_t target_ns) {
	usleep((useconds_t)(target_ns / 1000));
}

static void wait_mach(uint64_t target_ns) {
	uint64_t deadline = mach_absolute_time() + target_ns * tb.denom / tb.numer;
	mach_wait_until(deadline);
}

static void wait_yield_spin(uint64_t target_ns) {
	uint64_t deadline = now_ns() + target_ns;
	while (now_ns() < deadline) {
		sched_yield();
	}
}

static void bench(const char *name, wait_fn fn, uint64_t target_us) {
	const int iters = 200;
	uint64_t overshoot[200];
	for (int i = 0; i < iters; i++) {
		uint64_t start = now_ns();
		fn(target_us * 1000);
		uint64_t elapsed = now_ns() - start;
		overshoot[i] = elapsed > target_us * 1000 ? elapsed - target_us * 1000 : 0;
	}
	qsort(overshoot, iters, sizeof(uint64_t), cmp_u64);
	uint64_t sum = 0;
	for (int i = 0; i < iters; i++) {
		sum += overshoot[i];
	}
	printf("%-22s target=%5lluus  mean overshoot=%7.1fus  p50=%7.1fus  p99=%7.1fus\n", name,
	       (unsigned long long)target_us, sum / (double)iters / 1000.0, overshoot[iters / 2] / 1000.0,
	       overshoot[iters - 2] / 1000.0);
}

static void set_realtime(uint64_t period_us) {
	thread_time_constraint_policy_data_t policy;
	uint64_t period_ticks = period_us * 1000 * tb.denom / tb.numer;
	policy.period = (uint32_t)period_ticks;
	policy.computation = (uint32_t)(period_ticks / 10);
	policy.constraint = (uint32_t)(period_ticks / 2);
	policy.preemptible = 1;
	kern_return_t kr = thread_policy_set(mach_thread_self(), THREAD_TIME_CONSTRAINT_POLICY,
	                                     (thread_policy_t)&policy, THREAD_TIME_CONSTRAINT_POLICY_COUNT);
	printf("realtime policy: %s\n", kr == KERN_SUCCESS ? "enabled" : "FAILED");
}

int main(void) {
	mach_timebase_info(&tb);
	uint64_t targets[] = {50, 100, 500, 1000, 5000};
	for (int t = 0; t < 5; t++) {
		bench("nanosleep/sleep_for", wait_sleep_for, targets[t]);
		bench("usleep", wait_usleep, targets[t]);
		bench("mach_wait_until", wait_mach, targets[t]);
		bench("yield_spin", wait_yield_spin, targets[t]);
		printf("\n");
	}
	printf("=== with THREAD_TIME_CONSTRAINT_POLICY (realtime band) ===\n");
	set_realtime(1000);
	for (int t = 0; t < 5; t++) {
		bench("rt:mach_wait_until", wait_mach, targets[t]);
	}
	return 0;
}
