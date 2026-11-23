#include "App.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <array>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

constexpr size_t BUDGET = 2097152, DESCRIPTORS = 4096, BINS = 10001;
extern "C" size_t heap_peak();
extern "C" size_t heap_live();
extern "C" size_t heap_allocations();
uint64_t clock_ns(clockid_t clock = CLOCK_MONOTONIC) {
    timespec t{}; clock_gettime(clock, &t); return uint64_t(t.tv_sec)*1000000000+t.tv_nsec;
}
void put(std::string &p, size_t offset, uint64_t x) {
    for (int i=7; i>=0; i--) {p[offset+i]=char(x&255); x>>=8;}
}
uint64_t get(std::string_view p, size_t offset) {
    uint64_t x=0; for (size_t i=0;i<8;i++) x=(x<<8)|uint8_t(p[offset+i]); return x;
}
std::string payload(size_t size, uint64_t token, size_t epoch, uint64_t dir, size_t conn, uint64_t seq, uint64_t deadline) {
    std::string p(size,0);
    std::array<uint64_t,7> header{1,token,epoch,dir,conn,seq,deadline};
    for (size_t i=0;i<header.size();i++) put(p,i*8,header[i]);
    for (size_t i=56;i<size;i++) p[i]=char((token+seq*17+i*31+dir*7+conn*13+epoch*19)&255);
    return p;
}
struct Facts {
    uint64_t attempted=0, admitted=0, transmitted=0, validated=0, in_window=0, rejected=0, next=0, digest=0, max_latency=0;
    std::vector<uint64_t> latency=std::vector<uint64_t>(BINS), lateness=std::vector<uint64_t>(BINS);
    std::vector<std::array<uint64_t,5>> samples;
    template<class T> void array(std::ostream &o, const T &xs) {
        o<<'['; bool first=true; for (auto x:xs) {if (!first)o<<',';first=false;o<<x;}o<<']';
    }
    void report(std::ostream &o, uint64_t scheduled) {
        o<<"{\"scheduled\":"<<scheduled<<",\"attempted\":"<<attempted<<",\"admitted\":"<<admitted
         <<",\"transmitted\":"<<transmitted<<",\"validated\":"<<validated<<",\"validated_in_window\":"<<in_window
         <<",\"validated_after_window\":"<<validated-in_window<<",\"rejected\":"<<rejected
         <<",\"digest\":\""<<digest<<"\",\"max_latency_ns\":"<<max_latency<<",\"latency_histogram_us\":";
        array(o,latency);o<<",\"lateness_histogram_us\":";array(o,lateness);o<<",\"samples\":[";
        bool first=true;for(auto &s:samples) {if(!first)o<<',';first=false;array(o,s);}o<<"]}";
    }
};
struct Data {size_t id;};
using WS = uWS::WebSocket<false,true,Data>;
struct Connection {
    WS *ws=nullptr; int fd=-1,sndbuf=0,rcvbuf=0;
    bool peer_closed=false;
    bool paused=false;
    std::array<Facts,2> tx,rx;
    std::vector<size_t> pending;
    std::string out,input; size_t offset=0,queue_bytes=0,highwater=0,highdescriptors=0;
};
struct Engine {
    std::string mode, fault, report_path, failure="null", control;
    size_t count,size; std::array<uint64_t,2> rates,durations,starts{},last_sample{};
    uint64_t token,start=0,end=0,drain,cpu_start=clock_ns(CLOCK_PROCESS_CPUTIME_ID);
    uint64_t pause_ns=0;
    int faultdir; bool saturation;
    std::vector<std::unique_ptr<Connection>> conns;
    void options(Connection &c) {
        int value=262144,one=1;
        setsockopt(c.fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
        setsockopt(c.fd,SOL_SOCKET,SO_SNDBUF,&value,sizeof(value));
        setsockopt(c.fd,SOL_SOCKET,SO_RCVBUF,&value,sizeof(value));
        socklen_t len=4;getsockopt(c.fd,SOL_SOCKET,SO_SNDBUF,&c.sndbuf,&len);len=4;getsockopt(c.fd,SOL_SOCKET,SO_RCVBUF,&c.rcvbuf,&len);
    }
    size_t add(WS *ws, int fd) {
        auto c=std::make_unique<Connection>();c->ws=ws;c->fd=fd;
        for(size_t e=0;e<2;e++){c->tx[e].samples.reserve(durations[e]);c->rx[e].samples.reserve(durations[e]);}
        c->pending.reserve(DESCRIPTORS);c->out.reserve(BUDGET);c->input.reserve(1048576+16388);options(*c);
        conns.push_back(std::move(c));return conns.size()-1;
    }
    void fail(std::string kind, size_t id=0, size_t epoch=0, uint64_t seq=0, uint64_t expected=0) {
        if(failure!="null")return;
        failure="{\"kind\":\""+kind+"\",\"connection\":"+std::to_string(id)+",\"direction\":0,\"epoch\":"+std::to_string(epoch)+",\"sequence\":"+std::to_string(seq)+",\"expected\":"+std::to_string(expected)+"}";
    }
    void receive(size_t id, std::string_view p) {
        if(!start){fail("barrier");return;}
        if(p.size()!=size || p.size()<56){fail("corruption",id);return;}
        auto epoch=get(p,16),seq=get(p,40);
        if(epoch>1){fail("corruption",id);return;}
        auto &f=conns[id]->rx[epoch];
        if(seq<f.next || (!saturation && seq>f.next)){fail(seq<f.next?"duplicate":"sequence",id,epoch,seq,f.next);return;}
        uint64_t deadline=rates[0]?starts[epoch]+seq*1000000000/rates[0]:0;
        if(p!=payload(size,token,epoch,0,id,seq,deadline)){fail("corruption",id,epoch,seq);return;}
        auto received=clock_ns();if(received<deadline){fail("clock",id);return;}
        f.next=seq+1;f.validated++;f.digest=f.digest*31+seq+1;
        if(received<starts[epoch]+durations[epoch]*1000000000)f.in_window++;
        auto latency=received-deadline;f.max_latency=std::max(f.max_latency,latency);
        f.latency[std::min<uint64_t>((latency+999)/1000,BINS-1)]++;
    }
    void flush(Connection &c) {
        bool complete=false;
        if(c.ws) complete=c.ws->getBufferedAmount()==0;
        else {
            if(c.offset<c.out.size()) {
                auto n=::send(c.fd,c.out.data()+c.offset,c.out.size()-c.offset,MSG_NOSIGNAL);
                if(n>0)c.offset+=n;else if(n<0 && errno!=EAGAIN && errno!=EWOULDBLOCK)fail("transport");
            }
            complete=c.offset==c.out.size();
            if(complete){c.out.clear();c.offset=0;}
        }
        if(complete){for(auto e:c.pending)c.tx[e].transmitted++;c.pending.clear();c.queue_bytes=0;}
    }
    void send(Connection &c, size_t id, size_t epoch, uint64_t seq, uint64_t deadline) {
        if(c.queue_bytes+size+14>BUDGET || c.pending.size()>=DESCRIPTORS){c.tx[epoch].rejected++;return;}
        if(c.ws && c.ws->getBufferedAmount()+size+14>BUDGET){c.tx[epoch].rejected++;return;}
        auto p=payload(size,token,epoch,1,id,seq,deadline);
        if(faultdir==1 && fault=="corruption" && epoch==1 && id==0 && seq==50)p[size-1]^=1;
        bool admitted=true;
        if(c.ws) {
            auto status=c.ws->send(p,uWS::BINARY,false);
            admitted=status!=WS::DROPPED;
        } else {
            auto len=htonl(uint32_t(p.size()));c.out.append(reinterpret_cast<char*>(&len),4);c.out+=p;
        }
        if(!admitted){c.tx[epoch].rejected++;return;}
        c.tx[epoch].admitted++;c.tx[epoch].digest=c.tx[epoch].digest*31+seq+1;c.pending.push_back(epoch);c.queue_bytes+=size+14;
        c.highwater=std::max(c.highwater,c.queue_bytes);c.highdescriptors=std::max(c.highdescriptors,c.pending.size());
        flush(c);
    }
    void tick() {
        if(!start) {
            char data[128];auto n=read(0,data,sizeof(data));if(n>0)control.append(data,n);
            if(control.find('\n')!=std::string::npos) {
                if(conns.size()!=count){fail("barrier");finish();}
                start=std::stoull(control);if(start<clock_ns()+10000000){fail("late_barrier");finish();}
                starts={start,start+durations[0]*1000000000};end=starts[1]+durations[1]*1000000000;
                cpu_start=clock_ns(CLOCK_PROCESS_CPUTIME_ID);
            }
            return;
        }
        auto t=clock_ns();
        for(size_t id=0;id<conns.size();id++) {
            auto &c=*conns[id];
            if(c.peer_closed)continue;
            if(c.ws && pause_ns) {
                auto p=reinterpret_cast<us_poll_t*>(c.ws);auto loop=reinterpret_cast<us_loop_t*>(uWS::Loop::get());
                if(t<start+pause_ns){us_poll_change(p,loop,us_poll_events(p)&~EPOLLIN);c.paused=true;}
                else if(c.paused){us_poll_change(p,loop,us_poll_events(p)|EPOLLIN);c.paused=false;}
            }
            if(mode=="tcp") {
                for(size_t turn=0;turn<256;turn++) {
                    if(t<start+pause_ns)break;
                    if(c.input.size()>=4) {
                        uint32_t len;memcpy(&len,c.input.data(),4);len=ntohl(len);
                        if(len>1048576){fail("record_limit");break;}
                        if(c.input.size()>=len+4){receive(id,std::string_view(c.input).substr(4,len));c.input.erase(0,len+4);continue;}
                    }
                    char scratch[16384];auto n=recv(c.fd,scratch,sizeof(scratch),0);
                    if(n>0)c.input.append(scratch,n);else if(n==0){if(t>=end && c.pending.empty())c.peer_closed=true;else fail("transport");break;}else if(errno==EAGAIN || errno==EWOULDBLOCK)break;else {fail("transport");break;}
                    if(c.input.size()>1048576+16388){fail("input_limit");break;}
                }
            }
            flush(c);
            for(size_t epoch=0;epoch<2;epoch++) {
                if(t<starts[epoch])continue;
                if(saturation && t>=starts[epoch]+durations[epoch]*1000000000)continue;
                for(size_t turn=0;turn<256;turn++) {
                    auto &f=c.tx[epoch];auto seq=f.attempted;
                    if(!rates[1] || seq>=rates[1]*durations[epoch] || starts[epoch]+seq*1000000000/rates[1]>t)break;
                    auto deadline=starts[epoch]+seq*1000000000/rates[1];f.attempted++;
                    f.lateness[std::min<uint64_t>((clock_ns()-deadline+999)/1000,BINS-1)]++;
                    send(c,id,epoch,seq,deadline);
                    if(faultdir==1 && fault=="duplicate" && epoch==1 && id==0 && seq==50)send(c,id,epoch,seq,deadline);
                }
            }
        }
        for(size_t epoch=0;epoch<2;epoch++) {
            while(last_sample[epoch]<durations[epoch] && t>=starts[epoch]+(last_sample[epoch]+1)*1000000000) {
                last_sample[epoch]++;
                for(auto &c:conns){auto &tx=c->tx[epoch];auto &rx=c->rx[epoch];
                    tx.samples.push_back({t,tx.attempted,tx.admitted,tx.transmitted,c->queue_bytes});
                    rx.samples.push_back({t,rx.validated,rx.in_window,0,0});}
            }
        }
        if(failure!="null" || t>=end+drain*1000000000)finish();
    }
    [[noreturn]] void finish() {
        auto finished=clock_ns();std::ofstream o(report_path);
        o<<"{\"schema\":1,\"actor\":\"server\",\"token\":"<<token<<",\"start_ns\":"<<start<<",\"end_ns\":"<<end
         <<",\"finished_ns\":"<<finished<<",\"cpu_ticks\":"<<clock_ns(CLOCK_PROCESS_CPUTIME_ID)-cpu_start
         <<",\"cpu_ticks_per_second\":1000000000,\"queue_limit_bytes\":"<<BUDGET<<",\"queue_limit_descriptors\":"<<DESCRIPTORS
         <<",\"histogram_overflow_us\":10000,\"tracked_heap_limit_bytes\":134217728,\"tracked_heap_peak_bytes\":"<<heap_peak()
         <<",\"tracked_heap_live_bytes\":"<<heap_live()<<",\"allocation_count\":"<<heap_allocations()<<",\"failure\":"<<failure<<",\"streams\":[";
        bool first=true;for(size_t id=0;id<conns.size();id++)for(size_t e=0;e<2;e++) {
            auto &c=*conns[id];if(!first)o<<',';first=false;
            o<<"{\"connection\":"<<id<<",\"epoch\":"<<e<<",\"queue_highwater_bytes\":"<<c.highwater
             <<",\"queue_highwater_descriptors\":"<<c.highdescriptors<<",\"sndbuf\":"<<c.sndbuf<<",\"rcvbuf\":"<<c.rcvbuf<<",\"tx\":";
            c.tx[e].report(o,rates[1]*durations[e]);o<<",\"rx\":";c.rx[e].report(o,rates[0]*durations[e]);o<<'}';
        }
        o<<"]}";o.close();std::exit(failure=="null"?0:1);
    }
};
Engine *engine;
int main(int argc,char **argv) {
    if(argc==2 && std::string(argv[1])=="vectors") {
        auto p=payload(64,7,1,0,2,3,123456);const char *hex="0123456789abcdef";
        for(uint8_t b:p)std::cout<<hex[b>>4]<<hex[b&15];std::cout<<'\n';return 0;
    }
    if(argc!=15)return 2;
    Engine e;engine=&e;e.mode=argv[1];auto port=std::stoi(argv[2]);e.count=std::stoull(argv[3]);e.size=std::stoull(argv[4]);
    e.rates={std::stoull(argv[5]),std::stoull(argv[6])};e.durations={std::stoull(argv[7]),std::stoull(argv[8])};
    e.drain=std::stoull(argv[9]);e.token=std::stoull(argv[10]);e.faultdir=std::stoi(argv[11]);e.fault=argv[12];e.saturation=std::string(argv[13])=="1";e.report_path=argv[14];
    if(auto pause=std::getenv("WS_PAUSE_RECEIVE_MS"))e.pause_ns=std::stoull(pause)*1000000;
    if(!e.count || e.count>16 || e.size<64 || e.size>1048576 || !e.durations[1] || e.durations[1]>100)return 2;
    fcntl(0,F_SETFL,O_NONBLOCK);
    if(e.mode=="tcp") {
        int listener=socket(AF_INET,SOCK_STREAM|SOCK_NONBLOCK,0);int one=1;setsockopt(listener,SOL_SOCKET,SO_REUSEADDR,&one,4);
        sockaddr_in address{};address.sin_family=AF_INET;address.sin_port=htons(port);address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
        if(bind(listener,reinterpret_cast<sockaddr*>(&address),sizeof(address)) || listen(listener,16))return 2;
        std::cout<<"LISTENING "<<port<<std::endl;
        for(;;) {
            if(e.conns.size()<e.count) {int fd=accept4(listener,nullptr,nullptr,SOCK_NONBLOCK);if(fd>=0)e.add(nullptr,fd);}
            e.tick();timespec pause{0,100000};nanosleep(&pause,nullptr);
        }
    }
    if(e.mode!="ws")return 2;
    uWS::App app;
    app.ws<Data>("/*",{
        .compression=uWS::DISABLED,
        .maxPayloadLength=1048576,
        .idleTimeout=0,
        .maxBackpressure=BUDGET,
        .closeOnBackpressureLimit=false,
        .resetIdleTimeoutOnSend=false,
        .sendPingsAutomatically=false,
        .open=[&e](WS *ws){ws->getUserData()->id=e.add(ws,int(reinterpret_cast<uintptr_t>(us_socket_get_native_handle(0,reinterpret_cast<us_socket_t*>(ws)))));},
        .message=[&e](WS *ws,std::string_view p,uWS::OpCode code){if(code!=uWS::BINARY)e.fail("opcode");else e.receive(ws->getUserData()->id,p);},
        .drain=[&e](WS *ws){e.flush(*e.conns[ws->getUserData()->id]);},
        .close=[&e](WS *ws,int,std::string_view){auto &c=*e.conns[ws->getUserData()->id];if(e.start && clock_ns()>=e.end && c.pending.empty())c.peer_closed=true;else e.fail("transport");}
    }).listen("127.0.0.1",port,[port](auto *listener){if(!listener)std::exit(2);std::cout<<"LISTENING "<<port<<std::endl;});
    auto timer=us_create_timer((us_loop_t*)uWS::Loop::get(),0,0);
    us_timer_set(timer,[](us_timer_t*){engine->tick();},1,1);
    // The pinned Linux epoll backend represents us_timer_t as a timerfd-backed us_poll_t.
    itimerspec precise{{0,100000},{0,100000}};
    if(timerfd_settime(us_poll_fd(reinterpret_cast<us_poll_t*>(timer)),0,&precise,nullptr))return 2;
    app.run();return 2;
}
