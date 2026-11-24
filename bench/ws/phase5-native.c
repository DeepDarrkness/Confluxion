#define _POSIX_C_SOURCE 200809L
#include "phase5-common.h"
int main(void) {
  for(p5_cycle=0;p5_cycle<p5_env("WS_CYCLES",1);p5_cycle++){
  p5_init();WsEndpoint endpoint={.port=p5.port,.host_len=9,.target_len=1,.host=(const uint8_t*)"127.0.0.1",.target=(const uint8_t*)"/"};
  WsStart r=ws_socket_connect_begin(&endpoint);if(r.code!=WS_PENDING)return 1;
  uint64_t deadline=p5_now()+2*P5_NS;uint32_t code;
  while((code=ws_socket_connect_finish(r.session))==WS_PENDING){if(p5_now()>deadline)abort();struct timespec t={0,100000};nanosleep(&t,NULL);}
  if(code!=WS_OK)return 1;p5_barrier(r.session);uint8_t bytes[16384],expected[16384];
  while(!p5_done(r.session)) {
    int progress=0;
    uint32_t e;uint64_t seq;
    if(p5_next(r.session,&e,&seq)) {progress=1;p5_payload(bytes,e,0,seq);WsMessageSpan span={.len=p5.size};WsBatchAdmission a={0};if(p5_offer_open())a=ws_socket_offer_batch(r.session,&span,1,bytes,p5.size);p5_admission(e,seq,a.accepted);}
    if(p5_now()>=p5.start+p5.pause_ns) {
      WsBatchTake b=ws_socket_take_batch(r.session,1,65536);
      if(b.code==WS_OK) {
        progress=1;
        for(uint32_t i=0;i<b.count;i++){WsMessageSpan span=b.messages[i];const uint8_t*p=b.bytes+span.offset;if(!p5_rx_identity(p,span.len))break;p5_payload(expected,p5.rx_epoch,1,p5.rx_seq);p5_consumed(span.kind||memcmp(p,expected,p5.size));}
        if(ws_socket_release(b.lease)!=WS_OK)abort();
      }else if(b.code!=WS_EMPTY)p5.failure=b.terminal?b.terminal:1;
    }
    if(!progress){struct timespec t={0,50000};nanosleep(&t,NULL);}
  }
  p5_status(r.session);uint64_t begin=p5_now();p5.closing=1;
  ws_socket_stop_begin(r.session,1);ws_socket_stop_begin(r.closer,2);
  uint32_t a,b;do {b=ws_socket_stop_finish(r.closer,2);if(p5_now()-begin>P5_NS+10000000)abort();struct timespec t={0,100000};nanosleep(&t,NULL);}while(b==WS_PENDING);
  p5_status(r.session);a=ws_socket_stop_finish(r.session,1);if(a!=WS_OK||b!=WS_OK)abort();
  p5_report(p5_now()-begin,0,0,0,0);if(p5.failure&&!(p5_env("WS_CYCLES",1)>1&&(p5.failure==16||p5.failure==17||p5.failure==22)))return 1;
  }return 0;
}
