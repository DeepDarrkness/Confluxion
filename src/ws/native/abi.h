#ifndef CONFLUXION_WS_ABI_H
#define CONFLUXION_WS_ABI_H
#include <stdint.h>
enum { WS_OK, WS_EMPTY, WS_FULL, WS_END, WS_INVALID, WS_STALE, WS_ROLE,
       WS_CLOSED, WS_BUSY, WS_FAILED, WS_PANIC, WS_PENDING };
typedef struct { uint64_t session, closer; uint32_t code, reserved; } WsStart;
typedef struct { uint64_t lease; uint32_t code, len; } WsTake;
typedef struct { uint32_t code; int32_t fd; } WsWait;
typedef struct {
  uint64_t allocations, live_bytes, live_blocks, peak_bytes, entries, descriptors,
    workers, parked, cells, cell_bytes, join_ns, abandoned, core_size, registry_size, stack_bytes, layout_overflow,
    join_started, join_completed, wake_errors;
} WsStats;
typedef struct { uint64_t size, alignment, allocated, freed, live, peak; } WsLayout;
WsStart ws_native_start(void);
uint32_t ws_native_offer(uint64_t, const uint8_t*, uint32_t);
WsTake ws_native_take(uint64_t, uint8_t*, uint32_t);
WsWait ws_native_wait_begin(uint64_t);
uint32_t ws_native_wait_end(uint64_t);
uint32_t ws_native_release(uint64_t);
WsWait ws_native_stop_begin(uint64_t, uint32_t);
uint32_t ws_native_stop_finish(uint64_t, uint32_t);
WsStats ws_native_stats(void);
uint32_t ws_native_fault(uint32_t);
uint32_t ws_native_pause(uint64_t, uint32_t);
WsLayout ws_native_layout(uint32_t);
#endif
