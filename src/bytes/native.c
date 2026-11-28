#include "byte_buffer.h"
#ifdef CID(copy_message)
#include "abi.h"
#endif

#ifdef CID(allocate)
static Term byte_allocate_run(Env e, Term *f, IoWork *w) {
  if (f[0] > BYTE_BUFFER_MAX) return term_pak(CID(AllocationFailed), 25);
  Term array = byte_array_new(e, f[0]);
  if (err_seen(e.mem)) return 0;
  return io_node(e, CID(Allocated), array, 0);
}
#endif
#ifdef CID(release)
static Term byte_release_run(Env e, Term *f, IoWork *w) {
  uint8_t *data; uint32_t length, capacity;
  uint32_t code = byte_buffer_view(e, f[0], &data, &length, &capacity);
  term_drop(e, f[0]);
  return code;
}
#endif
#ifdef CID(copy_message)
static Term byte_copy_message_run(Env e, Term *f, IoWork *w) {
  WsByteView view = ws_socket_lease_view(io_hand_v(f[0]), f[1]);
  if (view.code) return io_tup(e, f[0], term_pak(CID(AllocationFailed), view.code));
  if (view.len > BYTE_BUFFER_MAX) return io_tup(e, f[0], term_pak(CID(AllocationFailed), WS_OVERSIZE));
  Term array = byte_array_new(e, view.len);
  if (err_seen(e.mem)) return 0;
  if (view.len) memcpy(byte_array_data(e, array), view.bytes, view.len);
  return io_tup(e, f[0], io_node(e, CID(Allocated), array, view.len));
}
#endif
static void __attribute__((constructor)) byte_native_use(void) {
#ifdef CID(allocate)
  io_eff(CID(allocate), byte_allocate_run);
#endif
#ifdef CID(release)
  io_eff(CID(release), byte_release_run);
#endif
#ifdef CID(copy_message)
  io_eff(CID(copy_message), byte_copy_message_run);
#endif
}
