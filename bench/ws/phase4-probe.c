#include "abi.h"
#include <dirent.h>
static Term ws_record(Env, u32, u32, Term*);
static u32 ws_iterations;
static u64 ws_capacity_start;
extern void ws_socket_fixture_buffer(uint32_t);
extern void ws_socket_fixture_fault(uint32_t);
extern uint32_t ws_socket_parked(void);
extern uint32_t ws_socket_fixture_memory(uint64_t, uint64_t*, uint64_t*);
static Term phase4_settings(Env e, Term *f, IoWork *w) {
  ws_observer_fd = atoi(getenv("WS_OBSERVER_FD") ? getenv("WS_OBSERVER_FD") : "-1");
  ws_iterations = atoi(getenv("WS_ITERATIONS") ? getenv("WS_ITERATIONS") : "1000000");
  if(getenv("WS_START_FAULT")) ws_socket_fixture_fault(atoi(getenv("WS_START_FAULT")));
  if (atoi(getenv("WS_MODE")) == 1) ws_socket_fixture_buffer(16384);
   if(ws_iterations>=22&&ws_iterations<=23)ws_socket_fixture_buffer(16384);
  Term fields[] = {atoi(getenv("WS_PORT")), atoi(getenv("WS_MODE")), ws_iterations};
  return ws_record(e, CID(Settings), 3, fields);
}
static Term phase4_check(Env e, Term *f, IoWork *w) {
  if (f[0] != f[1]) { fprintf(stderr,"check tag %u: %u != %u\n",(u32)f[2],(u32)f[0],(u32)f[1]); exit(1); }
  if(f[2]==81) {printf("{\"kind\":\"startup_sibling\",\"time\":%lu}\n",ws_clock());fflush(stdout);}
  if(f[2]==24) printf("{\"kind\":\"bend_compute_input\",\"validated\":true,\"next_identity\":%u}\n",(u32)f[0]);
  return term_pak(CID(Unit),0);
}
static Term phase4_report(Env e, Term *f, IoWork *w) {
  WsSocketStatus s = ws_socket_status(io_hand_v(f[0]));
  printf("{\"kind\":\"socket_buffers\",\"tag\":%u,\"send\":%u,\"receive\":%u,\"stack_reservation\":262144}\n",(u32)f[1],s.socket_send_buffer,s.socket_receive_buffer);
  printf("{\"kind\":\"status\",\"tag\":%u,\"tx\":%u,\"rx\":%u,\"admitted\":%llu,\"sent\":%llu,\"received\":%llu,\"consumed\":%llu,\"failed\":%llu,\"tx_ns\":%llu,\"rx_ns\":%llu,\"control_ns\":%llu,\"native_live\":%llu,\"native_peak\":%llu,\"native_budget\":%llu,\"tx_count\":%u,\"rx_count\":%u,\"leases\":%u}\n",
    (u32)f[1],(u32)f[2],(u32)f[3],s.tx_admitted,s.tx_complete,s.rx_complete,s.rx_consumed,s.tx_failed,s.tx_completed_ns,s.rx_completed_ns,s.control_ns,s.native_live,s.native_peak,s.native_budget,s.tx_count,s.rx_count,s.leases);
  fflush(stdout); return f[0];
}
static Term phase4_arm(Env e, Term *f, IoWork *w) {
  WsSocketStatus s = ws_socket_status(io_hand_v(f[0]));
  if (s.tx_admitted != 3 || s.tx_complete >= 3) { fprintf(stderr,"result not pending: %llu/%llu\n",s.tx_admitted,s.tx_complete); exit(1); }
  printf("{\"kind\":\"pending_result\",\"time\":%lu,\"admitted\":%lu,\"complete\":%lu,\"queued\":%u}\n",ws_clock(),s.tx_admitted,s.tx_complete,s.tx_count);
  ws_armed = 1; return f[0];
}
static Term phase4_computed(Env e, Term *f, IoWork *w) {
  printf("{\"kind\":\"compute\",\"value\":%u,\"iterations\":%u,\"start\":%llu,\"end\":%llu,\"dispatches\":%llu}\n",(u32)f[0],ws_iterations,ws_interval_start,ws_interval_end,ws_interval_dispatches);
  return term_pak(CID(Unit),0);
}
static Term phase4_checkpoint(Env e, Term *f, IoWork *w) {
  WsStats s = ws_native_stats();
  printf("{\"kind\":\"memory_peak\",\"native_peak\":%lu,\"native_budget\":%lu}\n",s.peak_bytes,ws_observed_budget);
  if(f[0]==100 && getenv("WS_OMIT_RELEASE")) {fprintf(stderr,"omitted lease: native bytes %lu\n",s.live_bytes);if(s.live_bytes==0)abort();exit(1);}
  u32 fds=0,threads=0;DIR*d=opendir("/proc/self/fd");struct dirent*entry;
  if(!d)abort();while((entry=readdir(d)))if(entry->d_name[0]!='.')fds++;closedir(d);
  d=opendir("/proc/self/task");if(!d)abort();while((entry=readdir(d)))if(entry->d_name[0]!='.')threads++;closedir(d);
  unsigned long virtual_pages, resident_pages;FILE*statm=fopen("/proc/self/statm","r");if(!statm||fscanf(statm,"%lu %lu",&virtual_pages,&resident_pages)!=2)abort();fclose(statm);
  if(f[0]!=100)printf("{\"kind\":\"rss\",\"tag\":%u,\"bytes\":%lu}\n",(u32)f[0],resident_pages*(unsigned long)sysconf(_SC_PAGESIZE));
  printf("{\"kind\":\"checkpoint\",\"tag\":%u,\"native_bytes\":%lu,\"native_blocks\":%lu,\"layout_overflow\":%lu,\"bend_bytes\":%lu,\"bend_blocks\":%lu,\"bend_peak\":%lu,\"c_bytes\":%lu,\"c_blocks\":%lu,\"c_peak\":%lu,\"fds\":%u,\"threads\":%u}\n",(u32)f[0],s.live_bytes,s.live_blocks,s.layout_overflow,atomic_load(&ws_bend_bytes),atomic_load(&ws_bend_blocks),atomic_load(&ws_bend_peak),ws_c_bytes,ws_c_blocks,ws_c_peak,fds,threads);
  if(f[0]!=100) printf("{\"kind\":\"diagnostics\",\"tag\":%u,\"native_take_ns\":%lu,\"conversion_ns\":%lu,\"dispatch_ns\":%lu,\"dispatches\":%lu,\"single_sent\":%lu,\"batch_sent\":%lu,\"single_received\":%lu,\"batch_received\":%lu}\n",(u32)f[0],ws_copy_ns,ws_conversion_ns,ws_effect_ns,ws_effect_count,ws_single_sent,ws_batch_sent,ws_single_received,ws_batch_received);
  return term_pak(CID(Unit),0);
}
typedef struct { uint32_t phase, direction, hold, sequence; uint64_t time; } WsPauseRecord;
static Term phase4_notice(Env e, Term *f, IoWork *w) {
  WsPauseRecord record = {(u32)f[1],(u32)f[2],(u32)f[3],(u32)f[4],ws_clock()};
  if (write(ws_observer_fd,&record,sizeof(record)) != sizeof(record)) abort();
  WsSocketStatus s = ws_socket_status(io_hand_v(f[0]));
  printf("{\"kind\":\"pause\",\"phase\":%u,\"direction\":%u,\"hold\":%u,\"sequence\":%u,\"time\":%lu,\"admitted\":%lu,\"sent\":%lu,\"received\":%lu,\"consumed\":%lu,\"tx_bytes\":%u,\"tx_count\":%u,\"rx_bytes\":%u,\"rx_count\":%u,\"native_live\":%lu,\"native_budget\":%lu}\n",record.phase,record.direction,record.hold,record.sequence,record.time,s.tx_admitted,s.tx_complete,s.rx_complete,s.rx_consumed,s.tx_bytes,s.tx_count,s.rx_bytes,s.rx_count,s.native_live,s.native_budget);
  fflush(stdout);return f[0];
}
static Term phase4_parked_more(Env e, IoWork *w) {
  if (ws_socket_parked()) return term_pak(CID(Unit),0);
  if (io_tick() >= (u64)w->made) { fprintf(stderr,"receiver did not park\n");exit(1); }
  return io_wait_on(w,0,0,io_tick()+100000ull,phase4_parked_more);
}
static Term phase4_parked(Env e, Term *f, IoWork *w) { w->made=io_until(1000);return phase4_parked_more(e,w); }
#ifdef CID(inject)
static Term phase4_inject(Env e, Term *f, IoWork *w) { ws_socket_fixture_fault(f[0]);return term_pak(CID(Unit),0); }
#endif
static Term phase4_flushed_more(Env e, IoWork *w) {
  WsSocketStatus s=ws_socket_status(w->hand);
  if(s.tx_complete==s.tx_admitted)return io_hand(w->hand);
  if(s.terminal || io_tick()>=(u64)w->made){fprintf(stderr,"flush deadline or terminal: %lu/%lu code %u\n",s.tx_complete,s.tx_admitted,s.terminal);exit(1);}
  return io_wait_on(w,0,0,io_tick()+100000ull,phase4_flushed_more);
}
static Term phase4_flushed(Env e, Term *f, IoWork *w) {w->hand=io_hand_v(f[0]);w->made=io_until(2000);return phase4_flushed_more(e,w);}
static Term phase4_boundary_more(Env e, IoWork *w) {
  WsSocketStatus s = ws_socket_status(w->hand);
  if(w->size==200 || w->size==201) {
    u64 idle=0,live=0;
    if(ws_socket_fixture_memory(w->hand,&idle,&live)!=WS_OK)abort();
    int ready=w->size==200 ? s.rx_count==4096 : !s.tx_count&&!s.rx_count&&!s.leases&&idle&&idle==live;
    if(!ready) {
      if(io_tick()>=(u64)w->made){fprintf(stderr,"saturation/idle deadline: %u/%u %lu/%lu\n",s.tx_count,s.rx_count,idle,live);exit(1);}
      return io_wait_on(w,0,0,io_tick()+100000ull,phase4_boundary_more);
    }
    printf("{\"kind\":\"%s\",\"time\":%lu,\"worker_idle\":%lu,\"worker_live\":%lu,\"native_live\":%lu,\"native_peak\":%lu,\"native_budget\":%lu,\"rx_count\":%u,\"rx_bytes\":%u,\"leases\":%u}\n",w->size==200?"saturated":"connected_idle",ws_clock(),idle,live,s.native_live,s.native_peak,s.native_budget,s.rx_count,s.rx_bytes,s.leases);
    fflush(stdout);return io_hand(w->hand);
  }
  if (!s.rx_count) {
    if (io_tick() >= (u64)w->made) { fprintf(stderr,"boundary input deadline\n");exit(1); }
    return io_wait_on(w,0,0,io_tick()+100000ull,phase4_boundary_more);
  }
  if (w->size >= 14 && w->size <= 16) {
    u32 expected_count=w->size==16?2:8;
    u32 expected_bytes=w->size==16?2097152:w->size==15?0:2048;
    if(s.leases!=expected_count || s.lease_bytes!=expected_bytes)abort();
    ws_capacity_start=ws_clock();
    return io_hand(w->hand);
  }
  u32 sizes[] = {0,125,126,65535,65536,1048576};
  u32 length = sizes[w->size-8];
  if (length) {
    WsBatchTake r = ws_socket_take_batch(w->hand,1,length-1);
    u32 expected = length == 1 ? WS_INVALID : WS_HEAD_TOO_LARGE;
    if (r.code != expected || ws_socket_status(w->hand).rx_count != s.rx_count) abort();
  }
  printf("{\"kind\":\"head_capacity\",\"length\":%u,\"rx_count\":%u}\n",length,s.rx_count);
  return io_hand(w->hand);
}
static Term phase4_boundary(Env e, Term *f, IoWork *w) {
  w->hand=io_hand_v(f[0]);w->size=f[1];w->made=io_until(1000);
  if(ws_socket_release(w->hand)!=WS_ROLE || ws_socket_status(w->hand+(1ull<<32)).code!=WS_STALE || ws_socket_status(1).code!=WS_INVALID || ws_socket_stop_begin(w->hand,2).code!=WS_ROLE)abort();
  if ((f[1]>=8 && f[1]<17) || f[1]==200 || f[1]==201) return phase4_boundary_more(e,w);
  if(f[1]>=114&&f[1]<=116){
    WsSocketStatus s=ws_socket_status(w->hand);
    if(s.rx_count!=1 || ws_clock()-ws_capacity_start>=10000000ull)abort();
    printf("{\"kind\":\"lease_capacity\",\"case\":%u,\"leases\":%u,\"bytes\":%u,\"rx_count\":%u}\n",(u32)f[1],s.leases,s.lease_bytes,s.rx_count);
  }
  if(f[1]==117){
    WsSocketStatus s=ws_socket_status(w->hand);
    if(s.tx_admitted!=1 || s.tx_complete+s.tx_failed!=1 || s.terminal!=23)abort();
    printf("{\"kind\":\"committed_fault\",\"admitted\":%lu,\"sent\":%lu,\"failed\":%lu}\n",s.tx_admitted,s.tx_complete,s.tx_failed);
  }
  return f[0];
}
static void __attribute__((constructor)) phase4_use(void) {
  io_eff(CID(settings),phase4_settings); io_eff(CID(check),phase4_check); io_eff(CID(report),phase4_report);
  io_eff(CID(arm),phase4_arm); io_eff(CID(computed),phase4_computed); io_eff(CID(checkpoint),phase4_checkpoint);
  io_eff(CID(notice),phase4_notice); io_eff(CID(await_parked),phase4_parked); io_eff(CID(boundary),phase4_boundary);io_eff(CID(flushed),phase4_flushed);
#ifdef CID(inject)
  io_eff(CID(inject),phase4_inject);
#endif
}
