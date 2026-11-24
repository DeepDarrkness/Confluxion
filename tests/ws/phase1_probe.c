#include <dirent.h>
#include "abi.h"

static uint64_t ws_saved_session;
static uint64_t ws_checks;
static uint64_t ws_validated, ws_validated_bytes;
static unsigned ws_fault_id;
static uint64_t ws_observe_blocks, ws_observe_bytes;

static void ws_assert(uint64_t actual, uint64_t expected, uint32_t tag) {
  ws_checks++;
  if (actual != expected) {
    fprintf(stderr, "Phase1 check %u: got %llu expected %llu\n", tag,
      (unsigned long long)actual, (unsigned long long)expected);
    exit(31);
  }
}

static Term ws_check_run(Env e, Term* f, IoWork* w) {
  ws_assert(f[0], f[1], (uint32_t)f[2]);
  if (f[2] == 9) {
    printf("{\"kind\":\"fault\",\"case\":\"startup_%u\",\"actual\":%u,\"expected\":9}\n", ws_fault_id, (uint32_t)f[0]);
  }
  if ((f[2] >= 18 && f[2] <= 25) || f[2] == 29) {
    printf("{\"kind\":\"bend_boundary\",\"tag\":%u,\"actual\":%u,\"expected\":%u}\n", (uint32_t)f[2], (uint32_t)f[0], (uint32_t)f[1]);
  }
  if (f[2] == 3) {
    static const unsigned sizes[3] = {0, 4, 256};
    ws_validated_bytes += sizes[ws_validated % 3];
    ws_validated++;
  }
  return term_pak(CID(Unit), 0);
}

static unsigned ws_os_count(const char* path) {
  DIR* dir = opendir(path);
  if (!dir) exit(32);
  unsigned n = 0;
  struct dirent* entry;
  while ((entry = readdir(dir))) if (entry->d_name[0] != '.') n++;
  closedir(dir);
  return n;
}

static unsigned ws_os_maps(void) {
  FILE* file = fopen("/proc/self/maps", "r");
  if (!file) exit(35);
  unsigned n = 0; int c;
  while ((c = fgetc(file)) != EOF) n += c == '\n';
  fclose(file);
  return n;
}
static uint64_t ws_rss_bytes(void) {
  FILE* file = fopen("/proc/self/statm", "r");
  if (!file) exit(36);
  unsigned long total, resident;
  if (fscanf(file, "%lu %lu", &total, &resident) != 2) exit(37);
  fclose(file);
  return resident * (uint64_t)sysconf(_SC_PAGESIZE);
}

static Term ws_checkpoint_run(Env e, Term* f, IoWork* w) {
  WsStats s = ws_native_stats();
  printf("{\"kind\":\"checkpoint\",\"tag\":%u,\"bend_blocks\":%llu,\"bend_bytes\":%llu,"
    "\"allocations\":%llu,\"native_bytes\":%llu,\"native_blocks\":%llu,\"native_peak\":%llu,"
    "\"entries\":%llu,\"descriptors\":%llu,\"workers\":%llu,\"cells\":%llu,"
    "\"os_fds\":%u,\"os_threads\":%u,\"join_ns\":%llu,\"checks\":%llu,"
    "\"core_size\":%llu,\"registry_size\":%llu,\"abandoned\":%llu,"
    "\"c_blocks\":%llu,\"c_bytes\":%llu,\"c_allocations\":%llu,\"c_peak\":%llu,"
    "\"validated_messages\":%llu,\"validated_bytes\":%llu,\"stack_bytes\":%llu,\"os_maps\":%u,\"rss_bytes\":%llu,\"layout_overflow\":%llu,"
    "\"join_started_ns\":%llu,\"join_completed_ns\":%llu,\"wake_errors\":%llu}\n",
    (uint32_t)f[0], (unsigned long long)atomic_load(&ws_bend_blocks),
    (unsigned long long)atomic_load(&ws_bend_bytes),
    (unsigned long long)s.allocations, (unsigned long long)s.live_bytes,
    (unsigned long long)s.live_blocks, (unsigned long long)s.peak_bytes,
    (unsigned long long)s.entries, (unsigned long long)s.descriptors,
    (unsigned long long)s.workers, (unsigned long long)s.cells,
    ws_os_count("/proc/self/fd"), ws_os_count("/proc/self/task"),
    (unsigned long long)s.join_ns, (unsigned long long)ws_checks,
    (unsigned long long)s.core_size, (unsigned long long)s.registry_size,
    (unsigned long long)s.abandoned, (unsigned long long)ws_c_blocks,
    (unsigned long long)ws_c_bytes, (unsigned long long)ws_c_allocations,
    (unsigned long long)ws_c_peak, (unsigned long long)ws_validated,
    (unsigned long long)ws_validated_bytes, (unsigned long long)s.stack_bytes,
    ws_os_maps(), (unsigned long long)ws_rss_bytes(), (unsigned long long)s.layout_overflow,
    (unsigned long long)s.join_started, (unsigned long long)s.join_completed, (unsigned long long)s.wake_errors);
  if (f[0] == 3000) {
    for (unsigned i = 0; i < 16; i++) {
      WsLayout l = ws_native_layout(i);
      if (l.alignment) printf("{\"kind\":\"allocation_layout\",\"size\":%llu,\"alignment\":%llu,\"allocated\":%llu,\"freed\":%llu,\"live\":%llu,\"peak\":%llu}\n",
        (unsigned long long)l.size, (unsigned long long)l.alignment, (unsigned long long)l.allocated,
        (unsigned long long)l.freed, (unsigned long long)l.live, (unsigned long long)l.peak);
    }
  }
  fflush(stdout);
  return term_pak(CID(Unit), 0);
}

static Term ws_observe_run(Env e, Term* f, IoWork* w) {
  uint64_t blocks = atomic_load(&ws_bend_blocks), bytes = atomic_load(&ws_bend_bytes);
  if (f[1] == 1) { ws_observe_blocks = blocks; ws_observe_bytes = bytes; }
  else {
    ws_assert(blocks, ws_observe_blocks, 101);
    ws_assert(bytes, ws_observe_bytes, 102);
  }
  printf("{\"kind\":\"retained\",\"tag\":%u,\"bend_blocks\":%llu,\"bend_bytes\":%llu}\n",
    (uint32_t)f[1], (unsigned long long)blocks, (unsigned long long)bytes);
  return f[0];
}

static Term ws_fault_run(Env e, Term* f, IoWork* w) {
  ws_fault_id = (unsigned)f[0];
  ws_assert(ws_native_fault((uint32_t)f[0]), 0, 103);
  return term_pak(CID(Unit), 0);
}
static Term ws_pause_run(Env e, Term* f, IoWork* w) {
  ws_assert(ws_native_pause(io_hand_v(f[0]), (uint32_t)f[1]), 0, 104);
  return f[0];
}

static Term ws_parked_more(Env e, IoWork* w) {
  if (ws_native_stats().parked == 1) {
    puts("{\"kind\":\"fault\",\"case\":\"confirmed_parked_receiver\",\"actual\":1,\"expected\":1}");
    return term_pak(CID(Unit), 0);
  }
  if (io_tick() >= w->time) { ws_assert(0, 1, 105); }
  return io_wait_on(w, 0, 0, io_tick() + 100000ull, ws_parked_more);
}
static Term ws_parked_run(Env e, Term* f, IoWork* w) {
  w->time = io_tick() + 500000000ull;
  return ws_parked_more(e, w);
}

static Term ws_held_run(Env e, Term* f, IoWork* w) {
  WsStats s = ws_native_stats();
  ws_assert(s.cells, 1, 129);
  ws_assert(s.entries, 1, 130);
  ws_assert(s.descriptors, 0, 131);
  ws_assert(s.workers, 0, 132);
  if (!s.live_bytes) exit(38);
  puts("{\"kind\":\"fault\",\"case\":\"held_bend_lease\",\"actual\":1,\"expected\":1}");
  return term_pak(CID(Unit), 0);
}

static void ws_case(const char* name, uint32_t actual, uint32_t expected) {
  ws_assert(actual, expected, 106);
  printf("{\"kind\":\"fault\",\"case\":\"%s\",\"actual\":%u,\"expected\":%u}\n", name, actual, expected);
}
static void ws_finish(uint64_t key, uint32_t role) {
  WsWait wait = ws_native_stop_begin(key, role);
  ws_assert(wait.code, 0, 107);
  uint64_t at = io_tick();
  for (;;) {
    uint32_t code = ws_native_stop_finish(key, role);
    if (code != WS_PENDING) { ws_assert(code, 0, 108); break; }
    if (io_tick() - at > 1000000000ull) exit(33);
    struct timespec t = {0, 100000}; nanosleep(&t, NULL);
  }
}
static WsTake ws_receive(uint64_t key, uint8_t* bytes) {
  uint64_t at = io_tick();
  for (;;) {
    WsTake r = ws_native_take(key, bytes, 256);
    if (r.code != WS_EMPTY) return r;
    if (io_tick() - at > 500000000ull) exit(34);
    struct timespec t = {0, 100000}; nanosleep(&t, NULL);
  }
}
static void ws_exact(uint64_t key, const uint8_t* expected, uint32_t len, uint64_t* lease) {
  uint8_t bytes[256];
  WsTake r = ws_receive(key, bytes);
  ws_assert(r.code, 0, 109);
  ws_assert(r.len, len, 110);
  ws_assert(memcmp(bytes, expected, len), 0, 111);
  *lease = r.lease;
}
static Term ws_raw_cases_run(Env e, Term* f, IoWork* w) {
  uint8_t bytes[256], empty[1] = {0};
  for (unsigned i = 0; i < 256; i++) bytes[i] = (uint8_t)(i * 73);
  WsStart s = ws_native_start(); ws_assert(s.code, 0, 112);
  ws_case("start_busy", ws_native_start().code, WS_BUSY);
  ws_case("wrong_role_offer", ws_native_offer(s.closer, bytes, 4), WS_ROLE);
  ws_case("foreign_handle", ws_native_offer(4, bytes, 4), WS_INVALID);
  ws_case("wrong_role_stop", ws_native_stop_begin(s.session, 2).code, WS_ROLE);
  ws_case("oversize_native", ws_native_offer(s.session, bytes, 257), WS_INVALID);
  ws_native_pause(s.session, 1);
  ws_assert(ws_native_offer(s.session, bytes, 256), 0, 116);
  ws_assert(ws_native_offer(s.session, bytes, 256), 0, 117);
  ws_case("byte_full", ws_native_offer(s.session, empty, 1), WS_FULL);
  ws_assert(ws_native_stats().cell_bytes, 512, 118);
  ws_native_pause(s.session, 0);
  uint64_t leases[8];
  ws_exact(s.session, bytes, 256, &leases[0]);
  ws_exact(s.session, bytes, 256, &leases[1]);
  for (unsigned i = 2; i < 8; i++) {
    ws_assert(ws_native_offer(s.session, bytes, 256), 0, 119);
    ws_exact(s.session, bytes, 256, &leases[i]);
  }
  ws_case("cell_full", ws_native_offer(s.session, empty, 0), WS_FULL);
  ws_assert(ws_native_stats().cell_bytes, 2048, 120);
  ws_assert(ws_native_release(leases[0]), 0, 121);
  ws_case("release_restores_admission", ws_native_offer(s.session, bytes, 256), WS_OK);
  ws_exact(s.session, bytes, 256, &leases[0]);
  ws_finish(s.closer, 2);
  ws_case("repeated_shutdown", ws_native_stop_begin(s.closer, 2).code, WS_CLOSED);
  ws_case("post_close_offer", ws_native_offer(s.session, empty, 0), WS_END);
  ws_finish(s.session, 1);
  ws_assert(ws_native_stats().descriptors, 0, 122);
  ws_case("leases_survive_stop", ws_native_stats().cells, 8);
  for (unsigned i = 0; i < 8; i++) ws_assert(ws_native_release(leases[i]), 0, 123);
  ws_case("repeat_release", ws_native_release(leases[7]), WS_CLOSED);
  ws_saved_session = s.session;
  s = ws_native_start(); ws_assert(s.code, 0, 124);
  ws_case("retired_generation", ws_native_offer(ws_saved_session, empty, 0), WS_CLOSED);
  ws_finish(s.closer, 2); ws_finish(s.session, 1);
  s = ws_native_start(); ws_assert(s.code, 0, 125);
  ws_case("stale_generation", ws_native_offer(ws_saved_session, empty, 0), WS_STALE);
  ws_native_pause(s.session, 1);
  ws_assert(ws_native_offer(s.session, bytes, 256), 0, 126);
  uint64_t abandoned = ws_native_stats().abandoned;
  ws_finish(s.closer, 2); ws_finish(s.session, 1);
  ws_case("queued_close_abandoned", ws_native_stats().abandoned - abandoned, 1);
  return term_pak(CID(Unit), 0);
}

static void __attribute__((constructor)) ws_probe_use(void) {
  io_eff(CID(check), ws_check_run);
  io_eff(CID(checkpoint), ws_checkpoint_run);
  io_eff(CID(observe), ws_observe_run);
  io_eff(CID(fault), ws_fault_run);
  io_eff(CID(pause), ws_pause_run);
  io_eff(CID(raw_cases), ws_raw_cases_run);
  io_eff(CID(parked), ws_parked_run);
  io_eff(CID(held), ws_held_run);
}
