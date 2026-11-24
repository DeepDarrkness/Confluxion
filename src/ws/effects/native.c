#include "abi.h"

#ifdef CID(start)
static Term ws_start_run(Env e, Term* f, IoWork* w) {
  WsStart s = ws_native_start();
  return s.code == WS_OK
    ? io_node(e, CID(Started), io_hand(s.session), io_hand(s.closer))
    : term_pak(CID(StartFailed), s.code);
}
#endif

#ifdef CID(offer)
static Term ws_offer_run(Env e, Term* f, IoWork* w) {
  uint8_t bytes[256];
  uint32_t len = 0, code = WS_OK;
  for (Term s = f[1]; term_aux(s) == CID(Con);) {
    u64 loc = term_peek(e.mem, s);
    if (len == 256 || e.mem[loc] > 255) { code = WS_INVALID; break; }
    bytes[len++] = (uint8_t)e.mem[loc];
    s = e.mem[loc + 1];
  }
  if (code == WS_OK) code = ws_native_offer(io_hand_v(f[0]), bytes, len);
  if (code != WS_OK) {
    Term r = code == WS_FULL ? io_box(e, CID(Full), f[1])
      : io_node(e, CID(Rejected), code, f[1]);
    return io_tup(e, f[0], r);
  }
  for (Term s = f[1]; term_aux(s) == CID(Con);) {
    Term fields[2];
    spare_free(e, cls_fit(2), ctr_take(e, s, 2, fields));
    s = fields[1];
  }
  return io_tup(e, f[0], term_pak(CID(Accepted), 0));
}
#endif

#ifdef CID(take)
static Term ws_take_more(Env e, IoWork* w) {
  if (w->made) { ws_native_wait_end((uint64_t)w->hand); w->made = 0; }
  uint8_t bytes[256];
  WsTake r = ws_native_take((uint64_t)w->hand, bytes, sizeof(bytes));
  if (r.code == WS_EMPTY && w->time && io_tick() < w->time) {
    WsWait wait = ws_native_wait_begin((uint64_t)w->hand);
    if (wait.code == WS_PENDING) {
      w->made = 1;
      return io_wait_on(w, wait.fd, POLLIN, w->time, ws_take_more);
    }
    if (wait.code == WS_OK) return ws_take_more(e, w);
    r.code = wait.code;
  }
  Term value;
  if (r.code == WS_OK) value = io_node(e, CID(Payload), io_hand(r.lease), io_list(e, (char*)bytes, r.len));
  else if (r.code == WS_EMPTY) value = term_pak(CID(Empty), 0);
  else if (r.code == WS_END) value = term_pak(CID(End), 0);
  else value = term_pak(CID(ReceiveFailed), r.code);
  return io_tup(e, io_hand(w->hand), value);
}

static Term ws_take_run(Env e, Term* f, IoWork* w) {
  w->hand = (intptr_t)io_hand_v(f[0]);
  w->time = f[1] ? io_until(f[1]) : 0;
  w->made = 0;
  return ws_take_more(e, w);
}
#endif

static Term ws_release_run(Env e, Term* f, IoWork* w) {
  return ws_native_release(io_hand_v(f[0]));
}

static Term ws_stop_more(Env e, IoWork* w) {
  uint32_t code = ws_native_stop_finish((uint64_t)w->hand, (uint32_t)w->size);
  if (code == WS_PENDING) {
    return io_wait_on(w, 0, 0, io_tick() + 100000ull, ws_stop_more);
  }
  return code;
}

static Term ws_stop_with(Env e, Term* f, IoWork* w, uint32_t role) {
  w->hand = (intptr_t)io_hand_v(f[0]);
  w->size = role;
  WsWait wait = ws_native_stop_begin((uint64_t)w->hand, role);
  if (wait.code != WS_OK) return wait.code;
  if (wait.fd >= 0) return io_wait_on(w, wait.fd, POLLIN, io_tick() + 1000000000ull, ws_stop_more);
  return ws_stop_more(e, w);
}
static Term ws_stop_run(Env e, Term* f, IoWork* w) { return ws_stop_with(e, f, w, 2); }
static Term ws_retire_run(Env e, Term* f, IoWork* w) { return ws_stop_with(e, f, w, 1); }

static void __attribute__((constructor)) ws_native_use(void) {
#ifdef CID(start)
  io_eff(CID(start), ws_start_run);
#endif
#ifdef CID(offer)
  io_eff(CID(offer), ws_offer_run);
#endif
#ifdef CID(take)
  io_eff(CID(take), ws_take_run);
#endif
#ifdef CID(release)
  io_eff(CID(release), ws_release_run);
#endif
#ifdef CID(stop)
  io_eff(CID(stop), ws_stop_run);
#endif
#ifdef CID(retire)
  io_eff(CID(retire), ws_retire_run);
#endif
}
