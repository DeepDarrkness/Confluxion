#include <atomic>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <new>
#include <cstddef>

static std::atomic<size_t> live{0},peak{0},allocations{0};
struct alignas(std::max_align_t) Header {size_t bytes;};
extern "C" void *__real_malloc(size_t);
extern "C" void __real_free(void *);
extern "C" void *__wrap_malloc(size_t bytes) {
    auto total=live.fetch_add(bytes)+bytes;
    if(total>134217728)std::abort();
    auto previous=peak.load();while(previous<total && !peak.compare_exchange_weak(previous,total)){}
    allocations++;
    auto h=static_cast<Header*>(__real_malloc(bytes+sizeof(Header)));
    if(!h){live-=bytes;return nullptr;}h->bytes=bytes;return h+1;
}
extern "C" void __wrap_free(void *p) {
    if(!p)return;auto h=static_cast<Header*>(p)-1;live-=h->bytes;__real_free(h);
}
extern "C" void *__wrap_calloc(size_t n,size_t size) {
    if(size && n>size_t(-1)/size)return nullptr;
    auto p=__wrap_malloc(n*size);if(p)std::memset(p,0,n*size);return p;
}
extern "C" void *__wrap_realloc(void *p,size_t bytes) {
    if(!p)return __wrap_malloc(bytes);
    if(!bytes){__wrap_free(p);return nullptr;}
    auto next=__wrap_malloc(bytes);if(!next)return nullptr;
    std::memcpy(next,p,std::min(bytes,(static_cast<Header*>(p)-1)->bytes));__wrap_free(p);return next;
}
void *operator new(size_t bytes) {auto p=__wrap_malloc(bytes);if(!p)throw std::bad_alloc();return p;}
void *operator new[](size_t bytes) {return ::operator new(bytes);}
void operator delete(void *p) noexcept {__wrap_free(p);}
void operator delete[](void *p) noexcept {__wrap_free(p);}
void operator delete(void *p,size_t) noexcept {__wrap_free(p);}
void operator delete[](void *p,size_t) noexcept {__wrap_free(p);}
extern "C" size_t heap_peak(){return peak.load();}
extern "C" size_t heap_live(){return live.load();}
extern "C" size_t heap_allocations(){return allocations.load();}
