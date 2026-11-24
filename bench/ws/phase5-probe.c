#include "phase5-common.h"
static Term ws_record(Env,u32,u32,Term*);
static u32 ws_bytes(Env,Term,u8*,u32,u32*);
static uint64_t ws_c_bytes,ws_c_peak,ws_c_allocations;
static uint32_t p5_due_epoch;
static uint64_t p5_due_seq,p5_close_start;
static Term p5_recipe(Env e,uint32_t id,uint32_t epoch,uint32_t dir,uint64_t seq) {
  uint8_t bytes[16384];p5_payload(bytes,epoch,dir,seq);
  Term fields[]={io_list(e,(char*)bytes,56),(p5.token+seq*17+dir*7+p5.id*13+epoch*19)&255,p5.size};
  return ws_record(e,id,3,fields);
}
static uint64_t p5_initial_c,p5_initial_bend,p5_bend_allocation_start,p5_c_allocation_start;
static Term phase5_settings(Env e,Term*f,IoWork*w) {p5_init();if(!p5_cycle){p5_initial_c=ws_c_bytes;p5_initial_bend=atomic_load(&ws_bend_bytes);}p5.c_baseline=p5_initial_c;p5.bend_baseline=p5_initial_bend;p5_bend_allocation_start=atomic_load(&ws_bend_allocations);p5_c_allocation_start=ws_c_allocations;atomic_store(&ws_bend_peak,atomic_load(&ws_bend_bytes));ws_c_peak=ws_c_bytes;return p5.port;}
static Term phase5_ready(Env e,Term*f,IoWork*w) {p5_barrier(io_hand_v(f[0]));return f[0];}
static Term phase5_next(Env e,Term*f,IoWork*w) {
  uint64_t h=io_hand_v(f[0]);Term step;
  if(p5_done(h))step=term_pak(CID(ScheduleDone),0);
  else if(p5_next(h,&p5_due_epoch,&p5_due_seq))step=p5_recipe(e,CID(ScheduledDue),p5_due_epoch,0,p5_due_seq);
  else {step=term_pak(CID(ScheduleWait),0);struct timespec t={0,50000};nanosleep(&t,NULL);}
  return io_tup(e,f[0],step);
}
static Term phase5_admitted(Env e,Term*f,IoWork*w) {p5_admission(p5_due_epoch,p5_due_seq,f[0]);return term_pak(CID(Unit),0);}
static Term phase5_expect(Env e,Term*f,IoWork*w) {
  uint8_t bytes[16384];uint32_t n=0;
  if(ws_bytes(e,f[0],bytes,sizeof(bytes),&n)!=WS_OK||!p5_rx_identity(bytes,n)){p5.failure=2;p5.rx_epoch=1;p5.rx_seq=0;}
  return io_tup(e,f[0],p5_recipe(e,CID(Recipe),p5.rx_epoch,1,p5.rx_seq));
}
static Term phase5_consumed(Env e,Term*f,IoWork*w) {p5_consumed(f[0]);return term_pak(CID(Unit),0);}
static Term phase5_receive_enabled(Env e,Term*f,IoWork*w) {return p5_now()>=p5.start+p5.pause_ns?1:0;}
static Term phase5_ended(Env e,Term*f,IoWork*w) {if(f[0])p5.failure=f[0];return term_pak(CID(Unit),0);}
static Term phase5_closing_more(Env e,IoWork*w) {
  WsSocketStatus s=ws_socket_status(w->hand);
  if(s.terminal&&s.state<2) {
    if(p5_now()-p5_close_start>P5_NS)abort();
    return io_wait_on(w,0,0,io_tick()+100000ull,phase5_closing_more);
  }
  p5_status(w->hand);return io_hand(w->hand);
}
static Term phase5_closing(Env e,Term*f,IoWork*w) {p5_close_start=p5_now();p5.closing=1;w->hand=io_hand_v(f[0]);return phase5_closing_more(e,w);}
static Term phase5_finished(Env e,Term*f,IoWork*w) {
  p5.bend_allocations=atomic_load(&ws_bend_allocations)-p5_bend_allocation_start;p5.c_allocations=ws_c_allocations-p5_c_allocation_start;
  p5_report(p5_now()-p5_close_start,atomic_load(&ws_bend_bytes),atomic_load(&ws_bend_peak),ws_c_bytes,ws_c_peak);
  if(p5.failure&&!(p5_env("WS_CYCLES",1)>1&&(p5.failure==16||p5.failure==17||p5.failure==22)))exit(1);
  p5_cycle++;return p5_cycle<p5_env("WS_CYCLES",1)?1:0;
}
static void __attribute__((constructor)) phase5_use(void) {
  io_eff(CID(settings),phase5_settings);io_eff(CID(ready),phase5_ready);io_eff(CID(next),phase5_next);
  io_eff(CID(admitted),phase5_admitted);io_eff(CID(expect),phase5_expect);io_eff(CID(consumed),phase5_consumed);
  io_eff(CID(receive_enabled),phase5_receive_enabled);io_eff(CID(ended),phase5_ended);
  io_eff(CID(closing),phase5_closing);io_eff(CID(finished),phase5_finished);
}
