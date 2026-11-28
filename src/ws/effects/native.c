#include "abi.h"
#include "byte_buffer.h"
typedef struct { u64 closer, deadline; u32 count, bytes, batch, failure; } WsEffectState;

static Term ws_record(Env e, u32 id, u32 n, Term *fields) {
  // Bend 2.0.40 flattens singleton record fields at constructor boundaries.
  if (cid_arity(id) != n) err_fail("WebSocket adapter constructor layout changed");
  u64 loc = heap_alloc(e, cls_fit(n));
  for (u32 i = 0; i < n; i++) e.mem[loc + i] = io_seal(e, fields[i], id);
  return term_ctr(id, loc);
}

#ifdef CID(connect)
static Term ws_connect_more(Env e, IoWork *w) {
  WsEffectState *state = (WsEffectState*)w->data;
  u32 code = state->failure ? state->failure : ws_socket_connect_finish(w->hand);
  if (code == WS_PENDING) {
    WsWait wait = ws_socket_connect_wait(w->hand);
    if (io_tick() < state->deadline && wait.code == WS_PENDING)
      return io_wait_on(w, wait.fd, POLLIN, state->deadline, ws_connect_more);
    code = WS_FAILED;
  }
  if (code == WS_OK) { u64 closer = state->closer; free(state); w->data = NULL; return io_node(e, CID(Started), io_hand(w->hand), io_hand(closer)); }
  state->failure = code;
  ws_socket_stop_begin(w->hand, 1);
  ws_socket_stop_begin(state->closer, 2);
  u32 a = ws_socket_stop_finish(w->hand, 1);
  u32 b = ws_socket_stop_finish(state->closer, 2);
  if (a == WS_PENDING || b == WS_PENDING)
    return io_wait_on(w, 0, 0, io_tick() + 100000ull, ws_connect_more);
  free(state); w->data = NULL; return term_pak(CID(StartFailed), code);
}
static u32 ws_string(Env e, Term s, u8 *out, u32 cap, u32 *len) {
  *len = 0;
  while (term_aux(s) == CID(SCon)) {
    u64 loc = term_peek(e.mem, s);
    if (*len == cap || e.mem[loc] > 127) return WS_INVALID;
    out[(*len)++] = e.mem[loc]; s = e.mem[loc + 1];
  }
  return WS_OK;
}
static Term ws_connect_run(Env e, Term *f, IoWork *w) {
  u8 host[15], target[1024], protocol[64];
  WsEndpoint p = {.port=f[1], .host=host, .target=target, .protocol=protocol};
  u32 code = ws_string(e, f[0], host, 15, &p.host_len);
  if (code == WS_OK) code = ws_string(e, f[2], target, 1024, &p.target_len);
  if (code == WS_OK) code = ws_string(e, f[3], protocol, 64, &p.protocol_len);
  term_drop(e, f[0]); term_drop(e, f[2]); term_drop(e, f[3]);
  if (code != WS_OK) return term_pak(CID(StartFailed), code);
  WsStart r = ws_socket_connect_begin(&p);
  if (r.code != WS_PENDING) return term_pak(CID(StartFailed), r.code);
  WsEffectState *state = io_mem(calloc(1, sizeof(*state)));
  state->closer = r.closer; state->deadline = io_until(2100);
  w->data = (char*)state; w->hand = r.session;
  return ws_connect_more(e, w);
}
#endif

#ifdef CID(offer)
static Term ws_offer_run(Env e, Term *f, IoWork *w) {
  u8 *bytes; u32 capacity;
  u32 code = byte_array_view(e, f[1], f[2], &bytes, &capacity);
  WsBatchAdmission a = {.code=code};
  if (code == WS_OK) a = ws_socket_offer_buffer(io_hand_v(f[0]), bytes, f[2]);
  Term fields[] = {a.code, f[1], f[2]};
  Term result;
  if (a.accepted == 1) {
    result = ws_record(e, CID(Accepted), 2, fields + 1);
  } else {
    result = a.code == WS_FULL ? ws_record(e, CID(Full), 2, fields + 1)
                              : ws_record(e, CID(Rejected), 3, fields);
  }
  return io_tup(e, f[0], result);
}
#endif
#ifdef CID(offer_batch)
static Term ws_offer_batch_run(Env e, Term *f, IoWork *w) {
  WsBuffer buffers[128]; u32 n = 0, count = 0, code = WS_OK;
  Term s = f[1];
  while (code == WS_OK && term_aux(s) == CID(BatchCon)) {
    if (count == 128) { code = WS_OVERSIZE; break; }
    if (term_tag(s) != TAG_CTR || cid_arity(term_aux(s)) != 3) { code = WS_INVALID; break; }
    u64 loc = term_peek(e.mem, s); u8 *bytes; u32 capacity;
    if (e.mem[loc + 1] > UINT32_MAX) { code = WS_INVALID; break; }
    u32 len = e.mem[loc + 1];
    code = byte_array_view(e, e.mem[loc], len, &bytes, &capacity);
    if (code != WS_OK) break;
    if (len > 65536 - n) { code = WS_OVERSIZE; break; }
    buffers[count++] = (WsBuffer){.bytes=bytes, .len=len, .kind=0};
    n += len;
    s = e.mem[loc + 2];
  }
  if (code == WS_OK && (term_tag(s) != TAG_PAK || term_aux(s) != CID(BatchNil))) code = WS_INVALID;
  WsBatchAdmission a = {.code=code};
  if (code == WS_OK) a = ws_socket_offer_buffers(io_hand_v(f[0]), buffers, count);
  if (a.code == WS_OK) return io_tup(e, f[0], io_node(e, CID(BatchAccepted), a.accepted, f[1]));
  return io_tup(e, f[0], a.code == WS_FULL ? io_box(e, CID(BatchFull), f[1]) : io_node(e, CID(BatchRejected), a.code, f[1]));
}
#endif

#if defined(CID(take)) || defined(CID(take_batch))
static Term ws_take_more(Env e, IoWork *w) {
  WsEffectState *state = (WsEffectState*)w->data;
  if (w->made) { ws_socket_wait_end(w->hand); w->made = 0; }
  WsBatchTake r = ws_socket_take_batch(w->hand, state->count, state->bytes);
  if (r.code == WS_EMPTY && state->deadline && io_tick() < state->deadline) {
    WsWait wait = ws_socket_wait_begin(w->hand);
    if (wait.code == WS_PENDING) { w->made = 1; return io_wait_on(w, wait.fd, POLLIN, state->deadline, ws_take_more); }
    if (wait.code == WS_OK) return ws_take_more(e, w);
    r.code = wait.code;
  }
  Term value = 0;
  if (r.code == WS_OK) {
#ifdef CID(take)
    if (!state->batch) {
      Term payload[] = {io_hand(r.lease), r.messages[0].kind, 0, r.messages[0].len};
      value = ws_record(e, CID(Payload), 4, payload);
    }
#endif
#ifdef CID(take_batch)
    if (state->batch) {
      Term messages = term_pak(CID(MetadataNil), 0);
      for (u32 i = r.count; i-- > 0;) {
        Term fields[] = {r.messages[i].kind, i, r.messages[i].len, messages};
        messages = ws_record(e, CID(MetadataCon), 4, fields);
      }
      value = io_node(e, CID(Messages), io_hand(r.lease), messages);
    }
#endif
  } else {
#ifdef CID(take)
    if (!state->batch) value = r.code == WS_EMPTY ? term_pak(CID(Empty), 0) : r.code == WS_END ? term_pak(CID(End), r.terminal) : term_pak(CID(ReceiveFailed), r.code);
#endif
#ifdef CID(take_batch)
    if (state->batch) value = r.code == WS_EMPTY ? term_pak(CID(BatchEmpty), 0) : r.code == WS_END ? term_pak(CID(BatchEnd), r.terminal) : term_pak(CID(BatchReceiveFailed), r.code);
#endif
  }
  Term result = io_tup(e, io_hand(w->hand), value);
  if (r.code == WS_OK && err_peek(e.mem)) ws_socket_release(r.lease);
  free(state); w->data = NULL; return result;
}
#endif
#ifdef CID(take)
static Term ws_take_run(Env e, Term *f, IoWork *w) {
  WsEffectState *state = io_mem(calloc(1, sizeof(*state)));
  state->count = 1; state->bytes = 1048576; state->deadline = f[1] ? io_until(f[1]) : 0;
  w->data = (char*)state; w->hand = io_hand_v(f[0]); w->made = 0; return ws_take_more(e, w);
}
#endif
#ifdef CID(take_batch)
static Term ws_take_batch_run(Env e, Term *f, IoWork *w) {
  if (!f[1] || f[1] > 128 || !f[2] || f[2] > 65536)
    return io_tup(e, f[0], term_pak(CID(BatchReceiveFailed), WS_INVALID));
  WsEffectState *state = io_mem(calloc(1, sizeof(*state)));
  state->count = f[1]; state->bytes = f[2]; state->batch = 1; state->deadline = f[3] ? io_until(f[3]) : 0;
  w->data = (char*)state; w->hand = io_hand_v(f[0]); w->made = 0;
  return ws_take_more(e, w);
}
#endif
#ifdef CID(status)
static Term ws_status_run(Env e, Term *f, IoWork *w) {
  WsSocketStatus s = ws_socket_status(io_hand_v(f[0]));
  u64 counts[] = {s.tx_admitted,s.tx_complete,s.rx_complete,s.rx_consumed,s.tx_failed,s.rx_discarded};
  Term fields[21] = {s.code,s.state,s.terminal};
  for (u32 i=0;i<6;i++) { fields[3+i*2]=counts[i]>>32; fields[4+i*2]=counts[i]&0xffffffff; }
  fields[15]=s.tx_bytes; fields[16]=s.tx_count; fields[17]=s.rx_bytes; fields[18]=s.rx_count; fields[19]=s.lease_bytes; fields[20]=s.leases;
  return io_tup(e, f[0], ws_record(e,CID(Status),21,fields));
}
#endif
static Term ws_release_run(Env e, Term *f, IoWork *w) { return ws_socket_release(io_hand_v(f[0])); }
static Term ws_stop_more(Env e, IoWork *w) {
  u32 code = ws_socket_stop_finish(w->hand, w->size);
  if (code == WS_PENDING) return io_wait_on(w, 0, 0, io_tick()+100000ull, ws_stop_more);
  return code;
}
static Term ws_stop_with(Env e, Term *f, IoWork *w, u32 role) {
  w->hand = io_hand_v(f[0]); w->size = role; w->made = io_until(1000);
  WsWait wait = ws_socket_stop_begin(w->hand, role);
  if (wait.code != WS_OK) return wait.code;
  if (wait.fd >= 0) return io_wait_on(w, wait.fd, POLLIN, w->made, ws_stop_more);
  return ws_stop_more(e,w);
}
static Term ws_stop_run(Env e, Term *f, IoWork *w) { return ws_stop_with(e,f,w,2); }
static Term ws_retire_run(Env e, Term *f, IoWork *w) { return ws_stop_with(e,f,w,1); }
static void __attribute__((constructor)) ws_native_use(void) {
#ifdef CID(connect)
  io_eff(CID(connect), ws_connect_run);
#endif
#ifdef CID(offer)
  io_eff(CID(offer), ws_offer_run);
#endif
#ifdef CID(offer_batch)
  io_eff(CID(offer_batch), ws_offer_batch_run);
#endif
#ifdef CID(take)
  io_eff(CID(take), ws_take_run);
#endif
#ifdef CID(take_batch)
  io_eff(CID(take_batch), ws_take_batch_run);
#endif
#ifdef CID(status)
  io_eff(CID(status), ws_status_run);
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
