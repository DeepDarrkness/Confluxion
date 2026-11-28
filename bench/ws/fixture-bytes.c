#include "byte_buffer.h"
#include "abi.h"
static Term fixture_record(Env e, uint32_t id, uint32_t n, Term *fields) {
  if (cid_arity(id) != n) err_fail("Fixture byte constructor layout changed");
  uint64_t loc = heap_alloc(e, cls_fit(n));
  for (uint32_t i = 0; i < n; i++) e.mem[loc+i] = io_seal(e, fields[i], id);
  return term_ctr(id, loc);
}
static Term fixture_pack(Env e, Term *f, IoWork *w) {
  Term list = f[0]; uint32_t n = 0;
  while (term_tag(list) == TAG_CTR && term_aux(list) == CID(Con)) {
    uint64_t loc = term_peek(e.mem, list);
    if (n == BYTE_BUFFER_MAX || e.mem[loc] > 255) err_fail("Invalid fixture byte list");
    n++; list = e.mem[loc+1];
  }
  if (term_tag(list) != TAG_PAK || term_aux(list) != CID(Nil)) err_fail("Invalid fixture byte list tail");
  Term array = byte_array_new(e, n); uint8_t *bytes = byte_array_data(e, array);
  list = f[0];
  for (uint32_t i = 0; i < n; i++) {uint64_t loc = term_peek(e.mem, list); bytes[i] = e.mem[loc]; list = e.mem[loc+1];}
  term_drop(e, f[0]);
  Term fields[] = {array, n}; return fixture_record(e, CID(Packed), 2, fields);
}
static Term fixture_read(Env e, Term *f, IoWork *w) {
  WsByteView view = ws_socket_lease_view(io_hand_v(f[0]), f[1]);
  if (view.code) err_fail("Invalid fixture lease view");
  Term list = io_list(e, (char*)view.bytes, view.len);
  Term fields[] = {f[0], list}; return fixture_record(e, CID(ReadLease), 2, fields);
}
static Term fixture_owned_list(Env e, Term *f, IoWork *w) {
  uint8_t *bytes; uint32_t n, capacity;
  if (byte_buffer_view(e, f[0], &bytes, &n, &capacity)) err_fail("Invalid fixture byte buffer");
  Term list = io_list(e, (char*)bytes, n);
  term_drop(e, f[0]); return list;
}
static void __attribute__((constructor)) fixture_bytes_use(void) {
  io_eff(CID(pack), fixture_pack); io_eff(CID(read), fixture_read); io_eff(CID(owned_list), fixture_owned_list);
}
