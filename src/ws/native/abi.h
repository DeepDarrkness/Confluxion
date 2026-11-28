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
enum { WS_HEAD_TOO_LARGE = 24, WS_OVERSIZE = 25 };
typedef struct { uint32_t kind, offset, len, reserved; } WsMessageSpan;
typedef struct { uint32_t port, host_len, target_len, protocol_len; const uint8_t *host, *target, *protocol; } WsEndpoint;
typedef struct { uint32_t code, accepted; } WsBatchAdmission;
typedef struct { const uint8_t *bytes; uint32_t len, kind; } WsBuffer;
typedef struct { const uint8_t *bytes; uint32_t code, len, kind, reserved; } WsByteView;
typedef struct { uint64_t lease; uint32_t code, count, bytes_len, terminal; const uint8_t *bytes; const WsMessageSpan *messages; } WsBatchTake;
typedef struct {
  uint32_t code, state, terminal, reserved;
  uint64_t tx_admitted, tx_complete, rx_complete, rx_consumed, tx_failed, rx_discarded;
  uint32_t tx_bytes, tx_count, rx_bytes, rx_count, lease_bytes, leases;
  uint64_t tx_completed_ns, rx_completed_ns, control_ns, native_live, native_peak, native_budget;
  uint32_t socket_send_buffer, socket_receive_buffer;
} WsSocketStatus;
WsStart ws_socket_connect_begin(const WsEndpoint*);
WsWait ws_socket_connect_wait(uint64_t);
uint32_t ws_socket_connect_finish(uint64_t);
WsBatchAdmission ws_socket_offer_batch(uint64_t, const WsMessageSpan*, uint32_t, const uint8_t*, uint32_t);
WsBatchAdmission ws_socket_offer_buffer(uint64_t, const uint8_t*, uint32_t);
WsBatchAdmission ws_socket_offer_buffers(uint64_t, const WsBuffer*, uint32_t);
WsByteView ws_socket_lease_view(uint64_t, uint32_t);
WsBatchTake ws_socket_take_batch(uint64_t, uint32_t, uint32_t);
WsWait ws_socket_wait_begin(uint64_t);
uint32_t ws_socket_wait_end(uint64_t);
uint32_t ws_socket_release(uint64_t);
WsSocketStatus ws_socket_status(uint64_t);
WsWait ws_socket_stop_begin(uint64_t, uint32_t);
uint32_t ws_socket_stop_finish(uint64_t, uint32_t);
_Static_assert(sizeof(WsMessageSpan) == 16 && sizeof(WsBatchTake) == 40 && sizeof(WsSocketStatus) == 144, "socket ABI layout");
_Static_assert(sizeof(WsBuffer) == 16 && sizeof(WsByteView) == 24, "dense socket ABI layout");
#endif
