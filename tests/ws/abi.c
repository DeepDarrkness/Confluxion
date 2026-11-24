extern uint64_t ws_native_probe(uint64_t key);

static Term ws_abi_open(Env e, Term* f, IoWork* w) {
  return io_hand(ws_native_probe(0xabcdef80000009ull));
}

static Term ws_abi_consume(Env e, Term* f, IoWork* w) {
  return ws_native_probe(io_hand_v(f[0])) == 0xabcdef80000009ull ? 42 : 0;
}

static void __attribute__((constructor)) ws_abi_use(void) {
  io_eff(CID(open), ws_abi_open);
  io_eff(CID(consume), ws_abi_consume);
}
