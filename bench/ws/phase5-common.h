#ifndef PHASE5_COMMON_H
#define PHASE5_COMMON_H
#include "abi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/resource.h>
#define P5_NS 1000000000ull
#define P5_BINS 10001
#define P5_SAMPLES 1810
extern void ws_socket_fixture_buffer(uint32_t);
extern uint32_t ws_socket_fixture_memory(uint64_t,uint64_t*,uint64_t*);
extern uint32_t ws_socket_fixture_evidence(uint64_t,uint64_t*,uint32_t);
typedef struct {
  uint64_t attempted,missed,admitted,transmitted,validated,in_window,rejected,next,digest,max_latency;
  uint64_t latency[P5_BINS],lateness[P5_BINS],samples[P5_SAMPLES][5];
  uint32_t sample_count;
  uint64_t identities[4096][2],rejections[4096][2];
  uint32_t identity_count,rejection_count;
} P5Facts;
typedef struct {
  uint64_t start,starts[2],end,warm,measure,drain,token,rates[2],pause_ns,offer_ns;
  uint64_t tx_done,peak,budget,high_bytes,high_count,begin_cpu,rx_due,rx_seq,rx_last,terminal_ns,tx_failed,rx_discarded;
  uint64_t incoming_start,outgoing_start,partial_reads,partial_writes,worker_evidence[9];
  uint64_t bend_allocations,c_allocations,bend_baseline,native_allocation_start,last_offer;
   uint64_t terminal_rx_queue;
   uint64_t rx_failure_sequence,rx_failure_expected,rx_failure_epoch;
   uint32_t rx_failure_kind;
   uint32_t size,id,port,sndbuf,rcvbuf,rx_epoch,allow_gaps,failure,fault_sequence,closing,offer_expired,rx_ready;
  uint32_t pending[4096],head,count;
  uint64_t last_sample[2],idle,baseline_fds,baseline_threads,pause_until,pause_rx,pause_tx,c_baseline;
  P5Facts tx[2],rx[2];
  const char *report;
} P5;
static P5 p5;
static uint32_t p5_cycle;
static uint64_t p5_initial_fds,p5_initial_threads;
static uint32_t p5_lateness_bin;
static char p5_report_path[4096];
static char p5_report_temporary[4096];
static uint64_t p5_clock(clockid_t id) { struct timespec t; if(clock_gettime(id,&t))abort();return (uint64_t)t.tv_sec*P5_NS+t.tv_nsec; }
static uint64_t p5_now(void) {return p5_clock(CLOCK_MONOTONIC);}
static uint64_t p5_env(const char *key,uint64_t fallback) {const char *s=getenv(key);return s?strtoull(s,NULL,10):fallback;}
static uint64_t p5_get(const uint8_t *p) {uint64_t x=0;for(int i=0;i<8;i++)x=(x<<8)|p[i];return x;}
static void p5_put(uint8_t *p,uint64_t x) {for(int i=7;i>=0;i--){p[i]=x&255;x>>=8;}}
static uint32_t p5_entries(const char *path) {DIR*d=opendir(path);if(!d)abort();uint32_t n=0;struct dirent*r;while((r=readdir(d)))if(r->d_name[0]!='.')n++;closedir(d);return n;}
static void p5_init(void) {
  memset(&p5,0,sizeof(p5));
  p5.size=p5_env("WS_SIZE",256);p5.id=p5_env("WS_CONNECTION",0);p5.port=p5_env("WS_PORT",0);
  p5.rates[0]=p5_env("WS_TX_RATE",0);p5.rates[1]=p5_env("WS_RX_RATE",0);
  p5.warm=p5_env("WS_WARM",0);p5.measure=p5_env("WS_MEASURE",1);p5.drain=2;
  p5.token=p5_env("WS_TOKEN",0);p5.pause_ns=p5_env("WS_PAUSE_RECEIVE_MS",0)*1000000;
  p5.offer_ns=p5_env("WS_OFFER_NS",0);p5.allow_gaps=p5_env("WS_SATURATION",0);
  p5.report=getenv("WS_REPORT");
  if(p5_env("WS_CYCLES",1)>1){if(snprintf(p5_report_path,sizeof(p5_report_path),"%s.%u",p5.report,p5_cycle)>=sizeof(p5_report_path))abort();p5.report=p5_report_path;}
  if(!p5.report||p5.size<64||p5.size>16384||p5.measure==0||p5.measure>1800||p5.warm>10||p5.id>15)abort();
  ws_socket_fixture_buffer(p5_env("WS_SOCKET_BUFFER",262144));
  if(!p5_cycle){p5_initial_fds=p5_entries("/proc/self/fd");p5_initial_threads=p5_entries("/proc/self/task");}
  p5.baseline_fds=p5_initial_fds;p5.baseline_threads=p5_initial_threads;
  p5.native_allocation_start=ws_native_stats().allocations;
}
static void p5_barrier(uint64_t h) {
  WsSocketStatus s=ws_socket_status(h);p5.sndbuf=s.socket_send_buffer;p5.rcvbuf=s.socket_receive_buffer;
  p5.budget=s.native_budget;
  uint64_t live;if(ws_socket_fixture_memory(h,&p5.idle,&live)!=WS_OK)abort();
  puts("READY");fflush(stdout);if(scanf("%lu",&p5.start)!=1||p5.start<p5_now()+10000000)abort();
   if(p5_env("WS_CYCLES",1)>1){uint64_t rate,pause_ms;if(scanf("%lu %lu",&rate,&pause_ms)!=2||!rate||pause_ms>1000)abort();p5.rates[0]=rate;p5.rates[1]=rate;p5.pause_ns=pause_ms*1000000;}
  p5.starts[0]=p5.start;p5.starts[1]=p5.start+p5.warm*P5_NS;p5.end=p5.starts[1]+p5.measure*P5_NS;
  p5.pause_until=p5.start+p5_env("WS_OBSERVED_PAUSE_MS",0)*1000000;
  p5.begin_cpu=p5_clock(CLOCK_PROCESS_CPUTIME_ID);
}
static uint64_t p5_duration(uint32_t e) {return e?p5.measure:p5.warm;}
static void p5_range(uint64_t ranges[4096][2],uint32_t *count,uint64_t seq) {
  if(*count&&ranges[*count-1][1]==seq){ranges[*count-1][1]=seq+1;return;}
  if(*count==4096){p5.failure=4;return;}
  ranges[*count][0]=seq;ranges[*count][1]=seq+1;(*count)++;
}
static uint64_t p5_scheduled(uint32_t e,uint32_t dir) {
  if(p5.offer_ns&&e==1)return (p5.rates[dir]*p5.offer_ns+P5_NS-1)/P5_NS;
  return p5.rates[dir]*p5_duration(e);
}
static void p5_payload(uint8_t *out,uint32_t e,uint32_t dir,uint64_t seq) {
  uint64_t due=p5.rates[dir]?p5.starts[e]+seq*P5_NS/p5.rates[dir]:0;
  uint64_t header[]={1,p5.token,e,dir,p5.id,seq,due};
  for(int i=0;i<7;i++)p5_put(out+i*8,header[i]);
  for(uint32_t i=56;i<p5.size;i++)out[i]=(p5.token+seq*17+i*31+dir*7+p5.id*13+e*19)&255;
}
static void p5_status(uint64_t h) {
   WsSocketStatus s=ws_socket_status(h);
   if(s.code!=WS_OK){p5.failure=1;return;}
   p5.rx_ready=s.rx_count;
  if(ws_socket_fixture_evidence(h,p5.worker_evidence,9)!=WS_OK||p5.worker_evidence[8])abort();
  uint64_t sampled=p5_now();
  if(p5.worker_evidence[0]){if(!p5.terminal_ns)p5.incoming_start=sampled-p5.worker_evidence[0];p5.partial_reads++;}
  if(p5.worker_evidence[1]){if(!p5.terminal_ns)p5.outgoing_start=sampled-p5.worker_evidence[1];p5.partial_writes++;}
  if(s.tx_complete<p5.tx_done||s.tx_complete-p5.tx_done>p5.count)abort();
  while(p5.tx_done<s.tx_complete) {
    uint32_t e=p5.pending[p5.head];p5.head=(p5.head+1)%4096;p5.count--;p5.tx[e].transmitted++;p5.tx_done++;
  }
  if(s.native_peak>p5.peak)p5.peak=s.native_peak;
  if(p5_now()<p5.pause_until)p5.pause_tx=s.tx_complete;
  if(s.native_live>s.native_budget||s.tx_bytes>2097152||s.rx_bytes>2097152||s.tx_count>4096||s.rx_count>4096)abort();
  p5.tx_failed=s.tx_failed;p5.rx_discarded=s.rx_discarded;
  if(s.terminal)p5.terminal_rx_queue=s.rx_count;
  if(s.terminal&&!(p5.closing&&(s.terminal==12||s.terminal==13))){p5.failure=s.terminal;if(!p5.terminal_ns)p5.terminal_ns=p5_now();}
  uint64_t charged=(uint64_t)p5.count*(p5.size+14);
  if(charged>p5.high_bytes)p5.high_bytes=charged;if(p5.count>p5.high_count)p5.high_count=p5.count;
  uint64_t t=p5_now();
  for(uint32_t e=0;e<2;e++)while(p5.last_sample[e]<p5_duration(e)&&t>=p5.starts[e]+(p5.last_sample[e]+1)*P5_NS) {
    P5Facts *tx=&p5.tx[e],*rx=&p5.rx[e];uint32_t i=tx->sample_count++;
    if(i>=P5_SAMPLES)abort();
    uint64_t a[]={t,tx->attempted,tx->admitted,tx->transmitted,charged};memcpy(tx->samples[i],a,sizeof(a));
    uint64_t b[]={t,rx->validated,rx->in_window,0,0};memcpy(rx->samples[rx->sample_count++],b,sizeof(b));p5.last_sample[e]++;
  }
}
static int p5_next(uint64_t h,uint32_t *epoch,uint64_t *seq) {
  p5_status(h);uint64_t t=p5_now();
  for(uint32_t e=0;e<2;e++) {
    P5Facts*f=&p5.tx[e];
    uint64_t next=f->attempted+f->missed;
    if(p5.offer_ns&&e==1&&t>=p5.starts[e]+p5.offer_ns){f->missed=p5_scheduled(e,0)-f->attempted;continue;}
    if(!p5.rates[0]||next>=p5_scheduled(e,0)||t<p5.starts[e]+next*P5_NS/p5.rates[0])continue;
    *epoch=e;*seq=next;f->attempted++;uint64_t due=p5.starts[e]+*seq*P5_NS/p5.rates[0];
    p5_lateness_bin=((t-due+999)/1000<P5_BINS-1)?(t-due+999)/1000:P5_BINS-1;f->lateness[p5_lateness_bin]++;
    if((p5.count+1ull)*(p5.size+14)>2097152||p5.count==4096){f->rejected++;p5_range(f->rejections,&f->rejection_count,*seq);return 0;}
    return 1;
  }
  return 0;
}
static int p5_offer_open(void) {uint64_t t=p5_now();p5.offer_expired=p5.offer_ns&&t>=p5.starts[1]+p5.offer_ns;if(!p5.offer_expired)p5.last_offer=t;return !p5.offer_expired;}
static void p5_admission(uint32_t e,uint64_t seq,uint32_t accepted) {
  P5Facts*f=&p5.tx[e];if(p5.offer_expired){f->attempted--;f->missed++;f->lateness[p5_lateness_bin]--;p5.offer_expired=0;return;}if(!accepted){f->rejected++;p5_range(f->rejections,&f->rejection_count,seq);return;}
  if(p5.count==4096)abort();p5.pending[(p5.head+p5.count++)%4096]=e;f->admitted++;f->digest=f->digest*31+seq+1;
  p5_range(f->identities,&f->identity_count,seq);
  if(p5.count>p5.high_count)p5.high_count=p5.count;
  if((uint64_t)p5.count*(p5.size+14)>p5.high_bytes)p5.high_bytes=(uint64_t)p5.count*(p5.size+14);
}
static int p5_rx_identity(const uint8_t *bytes,uint32_t n) {
  if(n!=p5.size||n<56){p5.failure=2;return 0;}
  uint64_t e=p5_get(bytes+16),seq=p5_get(bytes+40);
   if(e>1||seq>=p5_scheduled(e,1)){p5.failure=2;return 0;}
   if(seq<p5.rx[e].next||(!p5.allow_gaps&&seq!=p5.rx[e].next)){
     if(!p5.rx_failure_kind){p5.rx_failure_kind=seq<p5.rx[e].next?2:1;p5.rx_failure_sequence=seq;p5.rx_failure_expected=p5.rx[e].next;p5.rx_failure_epoch=e;}
     p5.failure=2;return 0;
   }
  p5.rx_epoch=e;p5.rx_seq=seq;p5.rx_due=p5.starts[e]+seq*P5_NS/p5.rates[1];return 1;
}
static void p5_consumed(uint32_t valid) {
  if(valid){p5.failure=2;p5.fault_sequence=p5.rx_seq;return;}
  P5Facts*f=&p5.rx[p5.rx_epoch];uint64_t t=p5_now();if(t<p5.rx_due)abort();
  p5.rx_last=t;
  f->next=p5.rx_seq+1;f->validated++;f->digest=f->digest*31+p5.rx_seq+1;
  p5_range(f->identities,&f->identity_count,p5.rx_seq);
  if(t<p5.pause_until)p5.pause_rx++;
  if(t<p5.starts[p5.rx_epoch]+p5_duration(p5.rx_epoch)*P5_NS)f->in_window++;
  uint64_t latency=t-p5.rx_due;if(latency>f->max_latency)f->max_latency=latency;
  f->latency[((latency+999)/1000<P5_BINS-1)?(latency+999)/1000:P5_BINS-1]++;
}
static int p5_done(uint64_t h) {
  p5_status(h);uint64_t t=p5_now();if(p5.failure)return 1;
  uint64_t expected=p5_scheduled(1,1);int known=!p5.allow_gaps;
  if(p5.allow_gaps){const char*path=getenv("WS_PEER_ADMITTED");FILE*f=path?fopen(path,"r"):NULL;if(f){uint64_t start,count;if(fscanf(f,"%lu %lu",&start,&count)==2&&start==p5.start){expected=count;known=1;}fclose(f);}}
  if(p5.offer_ns&&t>=p5.starts[1]+p5.offer_ns)p5.tx[1].missed=p5_scheduled(1,0)-p5.tx[1].attempted;
  int exact=known;for(uint32_t e=0;e<2;e++)if(p5.tx[e].attempted+p5.tx[e].missed!=p5_scheduled(e,0)||p5.tx[e].admitted!=p5.tx[e].transmitted||p5.rx[e].validated!=(e?expected:p5_scheduled(e,1)))exact=0;
  if(t>=p5.end&&exact)return 1;
  if(t>=p5.end+2*P5_NS){if(!exact)p5.failure=3;return 1;}return 0;
}
static void p5_array(FILE*o,uint64_t *xs,uint32_t n) {fputc('[',o);for(uint32_t i=0;i<n;i++)fprintf(o,"%s%lu",i?",":"",xs[i]);fputc(']',o);}
static void p5_facts(FILE*o,P5Facts*f,uint64_t scheduled) {
  fprintf(o,"{\"scheduled\":%lu,\"generator_missed\":%lu,\"attempted\":%lu,\"admitted\":%lu,\"transmitted\":%lu,\"validated\":%lu,\"validated_in_window\":%lu,\"validated_after_window\":%lu,\"rejected\":%lu,\"digest\":\"%lu\",\"max_latency_ns\":%lu,\"latency_histogram_us\":",scheduled,f->missed,f->attempted,f->admitted,f->transmitted,f->validated,f->in_window,f->validated-f->in_window,f->rejected,f->digest,f->max_latency);
  p5_array(o,f->latency,P5_BINS);fprintf(o,",\"lateness_histogram_us\":");p5_array(o,f->lateness,P5_BINS);fprintf(o,",\"samples\":[");
  for(uint32_t i=0;i<f->sample_count;i++){if(i)fputc(',',o);p5_array(o,f->samples[i],5);}fprintf(o,"],\"identities\":[");
  for(uint32_t i=0;i<f->identity_count;i++){if(i)fputc(',',o);p5_array(o,f->identities[i],2);}fprintf(o,"],\"rejections\":[");
  for(uint32_t i=0;i<f->rejection_count;i++){if(i)fputc(',',o);p5_array(o,f->rejections[i],2);}fprintf(o,"]}");
}
static void p5_report(uint64_t close_ns,uint64_t bend_live,uint64_t bend_peak,uint64_t c_live,uint64_t c_peak) {
  WsStats s=ws_native_stats();s.allocations-=p5.native_allocation_start;if(p5.offer_ns)p5.tx[1].missed=p5_scheduled(1,0)-p5.tx[1].attempted;uint32_t fds=p5_entries("/proc/self/fd"),threads=p5_entries("/proc/self/task");if(snprintf(p5_report_temporary,sizeof(p5_report_temporary),"%s.tmp",p5.report)>=sizeof(p5_report_temporary))abort();FILE*o=fopen(p5_report_temporary,"w");if(!o)abort();struct rusage r;if(getrusage(RUSAGE_SELF,&r))abort();
  fprintf(o,"{\"last_offer_ns\":%lu,",p5.last_offer);
  fprintf(o,"\"schema\":1,\"actor\":\"client\",\"bend_baseline\":%lu,\"pid\":%u,\"cycle\":%u,\"token\":%lu,\"start_ns\":%lu,\"end_ns\":%lu,\"finished_ns\":%lu,\"cpu_ticks\":%lu,\"cpu_ticks_per_second\":1000000000,\"queue_limit_bytes\":2097152,\"queue_limit_descriptors\":4096,\"histogram_overflow_us\":10000,\"failure\":",p5.bend_baseline,(unsigned)getpid(),p5_cycle,p5.token,p5.start,p5.end,p5_now(),p5_clock(CLOCK_PROCESS_CPUTIME_ID)-p5.begin_cpu);
   if(p5.rx_failure_kind)fprintf(o,"{\"kind\":\"%s\",\"direction\":1,\"connection\":%u,\"epoch\":%lu,\"sequence\":%lu,\"expected\":%lu}",p5.rx_failure_kind==1?"sequence":"duplicate",p5.id,p5.rx_failure_epoch,p5.rx_failure_sequence,p5.rx_failure_expected);
   else if(p5.failure)fprintf(o,"{\"kind\":\"adapter\",\"code\":%u,\"sequence\":%u}",p5.failure,p5.fault_sequence);else fprintf(o,"null");
  fprintf(o,",\"resources\":{\"native_peak\":%lu,\"native_budget\":%lu,\"native_live\":%lu,\"layout_overflow\":%lu,\"worker_idle\":%lu,\"bend_live\":%lu,\"bend_peak\":%lu,\"c_live\":%lu,\"c_peak\":%lu,\"scratch_bytes\":%lu,\"rss_peak\":%lu,\"voluntary_switches\":%lu,\"involuntary_switches\":%lu,\"close_ns\":%lu,\"fds_restored\":%s,\"threads_restored\":%s,\"wake_errors\":%lu},\"streams\":[",p5.peak,p5.budget,s.live_bytes,s.layout_overflow,p5.idle,bend_live,bend_peak,c_live,c_peak,(uint64_t)sizeof(p5),r.ru_maxrss*1024ul,r.ru_nvcsw,r.ru_nivcsw,close_ns,fds==p5.baseline_fds?"true":"false",threads==p5.baseline_threads?"true":"false",s.wake_errors);
  for(uint32_t e=0;e<2;e++) {
    fprintf(o,"%s{\"connection\":%u,\"epoch\":%u,\"queue_highwater_bytes\":%lu,\"queue_highwater_descriptors\":%lu,\"sndbuf\":%u,\"rcvbuf\":%u,\"tx\":",e?",":"",p5.id,e,p5.high_bytes,p5.high_count,p5.sndbuf,p5.rcvbuf);
    p5_facts(o,&p5.tx[e],p5_scheduled(e,0));fprintf(o,",\"rx\":");p5_facts(o,&p5.rx[e],p5_scheduled(e,1));fprintf(o,"}");
  }
  fprintf(o,"],\"pause_rx_validated\":%lu,\"pause_worker_transmitted\":%lu,\"c_baseline\":%lu,\"rx_last_ns\":%lu,\"terminal_ns\":%lu,\"worker_tx_failed\":%lu,\"worker_rx_discarded\":%lu,\"incoming_partial_start_ns\":%lu,\"outgoing_partial_start_ns\":%lu,\"partial_read_observations\":%lu,\"partial_write_observations\":%lu,\"worker_turns\":%lu,\"worker_service_gap_ns\":%lu,\"worker_tid\":%lu,\"stack_scratch_ceiling_bytes\":32768,\"native_allocation_count\":%lu,\"bend_allocation_count\":%lu,\"c_allocation_count\":%lu,\"terminal_rx_queue_count\":%lu}",p5.pause_rx,p5.pause_tx,p5.c_baseline,p5.rx_last,p5.terminal_ns,p5.tx_failed,p5.rx_discarded,p5.incoming_start,p5.outgoing_start,p5.partial_reads,p5.partial_writes,p5.worker_evidence[2],p5.worker_evidence[3],p5.worker_evidence[7],s.allocations,p5.bend_allocations,p5.c_allocations,p5.terminal_rx_queue);if(fclose(o)||rename(p5_report_temporary,p5.report))abort();
}
#endif
