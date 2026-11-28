#include "phase5-common.h"
static uint64_t ws_c_bytes,ws_c_peak,ws_c_allocations;
static uint32_t p5_due_epoch;
static uint64_t p5_due_seq,p5_close_start;
static Term p5_record(Env e,uint32_t id,uint32_t n,Term *fields) {
  if(cid_arity(id)!=n)err_fail("Phase 5 fixture constructor layout changed");
  uint64_t loc=heap_alloc(e,cls_fit(n));
  for(uint32_t i=0;i<n;i++)e.mem[loc+i]=io_seal(e,fields[i],id);
  return term_ctr(id,loc);
}
static void p5_recipe_fields(Term *fields,uint32_t epoch,uint32_t dir,uint64_t seq) {
  uint64_t due=p5.rates[dir]?p5.starts[epoch]+seq*P5_NS/p5.rates[dir]:0;
  uint64_t header[]={1,p5.token,epoch,dir,p5.id,seq,due};
  for(uint32_t i=0;i<7;i++){fields[i*2]=header[i]>>32;fields[i*2+1]=header[i]&0xffffffff;}
  fields[14]=(p5.token+seq*17+dir*7+p5.id*13+epoch*19)&255;fields[15]=p5.size;
}
static Term p5_recipe(Env e,uint32_t id,uint32_t epoch,uint32_t dir,uint64_t seq) {
  Term fields[16];p5_recipe_fields(fields,epoch,dir,seq);
  return p5_record(e,id,16,fields);
}
static uint64_t p5_initial_c,p5_initial_bend,p5_bend_allocation_start,p5_c_allocation_start;
static Term phase5_settings(Env e,Term*f,IoWork*w) {p5_init();if(!p5_cycle){p5_initial_c=ws_c_bytes;p5_initial_bend=atomic_load(&ws_bend_bytes);}p5.c_baseline=p5_initial_c;p5.bend_baseline=p5_initial_bend;p5_bend_allocation_start=atomic_load(&ws_bend_allocations);p5_c_allocation_start=ws_c_allocations;atomic_store(&ws_bend_peak,atomic_load(&ws_bend_bytes));ws_c_peak=ws_c_bytes;return p5.port;}
static Term phase5_ready(Env e,Term*f,IoWork*w) {p5_barrier(io_hand_v(f[0]));return f[0];}
static Term phase5_next(Env e,Term*f,IoWork*w) {
  uint64_t h=io_hand_v(f[0]);Term step;
  if(p5_done(h))step=term_pak(CID(ScheduleDone),0);
  else if(p5_next(h,&p5_due_epoch,&p5_due_seq))step=p5_recipe(e,CID(ScheduledDue),p5_due_epoch,0,p5_due_seq);
  else {step=term_pak(CID(ScheduleWait),0);if(!p5.rx_ready){struct timespec t={0,50000};nanosleep(&t,NULL);}}
  return io_tup(e,f[0],step);
}
static Term phase5_admitted(Env e,Term*f,IoWork*w) {p5_admission(p5_due_epoch,p5_due_seq,f[0]);return term_pak(CID(Unit),0);}
static Term phase5_expect(Env e,Term*f,IoWork*w) {
  WsByteView view=ws_socket_lease_view(io_hand_v(f[0]),f[1]);
  if(view.code||view.kind||!p5_rx_identity(view.bytes,view.len)){p5.failure=2;p5.rx_epoch=1;p5.rx_seq=0;}
  Term fields[17]={f[0]};p5_recipe_fields(fields+1,p5.rx_epoch,1,p5.rx_seq);
  return p5_record(e,CID(Expected),17,fields);
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
