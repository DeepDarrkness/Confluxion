#define BC_MAX_BYTES (1u << 20)
#define BC_MAX_NODES (1u << 18)
#define BC_MARKER 1145980243u
typedef struct { uint8_t *out; const uint8_t *in; uint32_t at, limit, nodes, code; } BcCursor;
static uint32_t bc_array_view(Env e, Term array, uint32_t length, uint8_t **data, uint32_t *capacity) {
  if (term_tag(array)==TAG_BUF && term_rfc(array)) array=term_blk(false,blk_cls(array),term_peek(e.mem,array));
  return byte_array_view(e,array,length,data,capacity);
}
static bool bc_node(BcCursor *c, uint32_t depth) {
  if (depth > 64 || ++c->nodes > BC_MAX_NODES) { c->code = 2; return false; }
  return true;
}
static bool bc_span(BcCursor *c, uint32_t n) {
  if (c->at > c->limit || n > c->limit - c->at) { c->code = c->in ? 3 : 2; return false; }
  return true;
}
static void bc_put(BcCursor *c, uint64_t n, uint32_t size) {
  if (!bc_span(c,size)) return;
  if (c->out) for (uint32_t i=0;i<size;++i) c->out[c->at+i]=(uint8_t)(n >> (8*(BC_BE ? size-1-i : i)));
  c->at += size;
}
static uint64_t bc_get(BcCursor *c, uint32_t size) {
  if (!bc_span(c,size)) return 0;
  uint64_t n=0; for (uint32_t i=0;i<size;++i) n |= (uint64_t)c->in[c->at+i] << (8*(BC_BE ? size-1-i : i));
  c->at += size; return n;
}
static void bc_arity(uint32_t cid, uint32_t n) {
  if (cid && cid_arity(cid) != n) { fprintf(stderr,"bend-codec: DTO arity mismatch cid=%u actual=%u expected=%u\n",cid,(unsigned)cid_arity(cid),n); abort(); }
}
static void bc_layout(void) {
  bc_arity(BC_WORD,4); bc_arity(BC_BLOB,4);
  for (uint32_t i=0;i<sizeof(bc_types)/sizeof(*bc_types);++i)
    for (uint32_t j=0;j<bc_types[i].count;++j) bc_arity(bc_types[i].variants[j].cid,bc_types[i].variants[j].arity);
}
static uint32_t bc_width(uint32_t kind) { return kind<5?1:kind<7?4:bc_types[kind-7].width; }
static bool bc_markers(const Term *f, BcCursor *c) {
  if (f[0]!=BC_MARKER || f[1]!=1) { c->code=1; return false; } return true;
}
static Term bc_make(Env e, uint32_t cid, uint32_t n, const Term *fields) {
  if (cid_arity(cid) != n) { fprintf(stderr,"bend-codec: DTO arity mismatch\n"); abort(); }
  uint64_t loc=heap_alloc(e,cls_fit(n));
  for (uint32_t i=0;i<n;++i) e.mem[loc+i]=io_seal(e,fields[i],cid);
  return term_ctr(cid,loc);
}
static uint32_t bc_cp_bytes(uint32_t p, uint8_t b[4]) {
  if (p>0x10ffff || (p>=0xd800 && p<=0xdfff)) return 0;
  if (p<0x80) { b[0]=p; return 1; }
  if (p<0x800) { b[0]=0xc0|(p>>6); b[1]=0x80|(p&63); return 2; }
  if (p<0x10000) { b[0]=0xe0|(p>>12); b[1]=0x80|((p>>6)&63); b[2]=0x80|(p&63); return 3; }
  b[0]=0xf0|(p>>18); b[1]=0x80|((p>>12)&63); b[2]=0x80|((p>>6)&63); b[3]=0x80|(p&63); return 4;
}
static bool bc_utf8(BcCursor *c, const uint8_t *p, uint32_t n) {
  uint32_t i=0;
  while (i<n) {
    if (!bc_node(c,1)) return false;
    uint32_t b=p[i++], need=0, cp=0, minimum=0;
    if (b<128) continue;
    if (b>=0xc2 && b<=0xdf) { need=1; cp=b&31; minimum=0x80; }
    else if (b>=0xe0 && b<=0xef) { need=2; cp=b&15; minimum=0x800; }
    else if (b>=0xf0 && b<=0xf4) { need=3; cp=b&7; minimum=0x10000; }
    else { c->code=5; return false; }
    if (need>n-i) { c->code=5; return false; }
    for (uint32_t j=0;j<need;++j) { b=p[i++]; if ((b&0xc0)!=0x80) { c->code=5; return false; } cp=(cp<<6)|(b&63); }
    if (cp<minimum || cp>0x10ffff || (cp>=0xd800 && cp<=0xdfff)) { c->code=5; return false; }
  }
  return true;
}
static void bc_write(Env e, uint32_t kind, const Term *slots, BcCursor *c, uint32_t depth) {
  if (c->code || !bc_node(c,depth)) return;
  Term value=slots[0];
  if (kind==0 || kind==1) { if (value>UINT32_MAX || (kind==1 && value>1)) { c->code=4; return; } bc_put(c,value,kind==1?1:4); return; }
  if (kind==6) { const Term *f=slots; if (!bc_markers(f,c)) return; if (f[2]>UINT32_MAX || f[3]>UINT32_MAX) { c->code=1; return; } bc_put(c,((uint64_t)f[2]<<32)|f[3],8); return; }
  if (kind==5) {
    const Term *f=slots; if (!bc_markers(f,c)) return;
    uint8_t *p; uint32_t cap; if (f[3]>BC_MAX_BYTES || bc_array_view(e,f[2],(uint32_t)f[3],&p,&cap)) { c->code=2; return; }
    if (!BC_BE) bc_put(c,f[3],4);
    if (c->code || !bc_span(c,(uint32_t)f[3])) return;
    if (c->out) memcpy(c->out+c->at,p,(uint32_t)f[3]); c->at+=(uint32_t)f[3]; return;
  }
  if (kind==2) {
    Term s=value; uint32_t n=0, chars=0;
    while (term_tag(s)==TAG_CTR && term_aux(s)==CID(SCon)) {
      const Term *f=e.mem+term_peek(e.mem,s); uint8_t b[4]; uint32_t size=bc_cp_bytes((uint32_t)f[0],b);
      if (!size || f[0]>UINT32_MAX) { c->code=5; return; }
      if (++chars>BC_MAX_NODES || n>BC_MAX_BYTES-size) { c->code=2; return; } n+=size; s=f[1];
    }
    if (term_tag(s)!=TAG_PAK || term_aux(s)!=CID(SNil)) { c->code=1; return; }
    if (chars>BC_MAX_NODES-c->nodes) { c->code=2; return; } c->nodes+=chars; bc_put(c,n,4);
    if (c->code || !bc_span(c,n)) return;
    for (s=value; term_tag(s)==TAG_CTR;) { const Term *f=e.mem+term_peek(e.mem,s); uint8_t b[4]; uint32_t size=bc_cp_bytes((uint32_t)f[0],b); if (c->out) memcpy(c->out+c->at,b,size); c->at+=size; s=f[1]; }
    return;
  }
  if (kind==3 || kind==4) {
    uint32_t n=0; const uint32_t *p=NULL;
    if (kind==4) { uint8_t *bytes; uint32_t capacity; if (bc_array_view(e,value,0,&bytes,&capacity)) { c->code=2; return; } p=(const uint32_t*)bytes; n=capacity/4; }
    else {
#if BC_HAS_LIST
      Term xs=value;
      while (term_tag(xs)==TAG_CTR && term_aux(xs)==CID(Con)) { if (++n>BC_MAX_NODES) { c->code=2; return; } const Term *f=e.mem+term_peek(e.mem,xs); xs=f[1]; }
      if (term_tag(xs)!=TAG_PAK || term_aux(xs)!=CID(Nil)) { c->code=1; return; }
#else
      c->code=1; return;
#endif
    }
    if (n>BC_MAX_NODES-c->nodes) { c->code=2; return; } c->nodes+=n; bc_put(c,n,4);
    if (c->code || n>(c->limit-c->at)/4) { c->code=2; return; }
    Term xs=value;
    for (uint32_t i=0;i<n;++i) {
      if (p) bc_put(c,p[i],4);
      else { const Term *f=e.mem+term_peek(e.mem,xs); if (f[0]>UINT32_MAX) { c->code=1; return; } bc_put(c,f[0],4); xs=f[1]; }
    }
    return;
  }
  const BcType *t=&bc_types[kind-7]; uint32_t tag=0;
  if (t->count>1) { if (*slots>=t->count) { c->code=4; return; } tag=(uint32_t)*slots++; }
  const BcVariant *v=&t->variants[tag];
  if (!bc_markers(slots,c)) return;
  if (t->count>1) bc_put(c,tag,1);
  slots+=2;
  for (uint32_t i=0;i<v->count;++i) { bc_write(e,v->fields[i],slots,c,depth+1); slots+=bc_width(v->fields[i]); }
}
static Term bc_read_scalar(Env e, uint32_t kind, BcCursor *c, uint32_t depth, bool build, Term *slots);
static void bc_read(Env e, uint32_t kind, BcCursor *c, uint32_t depth, Term *slots) {
  bool build=slots!=NULL;
  if (kind<7) { Term value=bc_read_scalar(e,kind,c,depth,build,slots); if (build && kind<5) slots[0]=value; return; }
  if (c->code || !bc_node(c,depth)) return;
  const BcType *t=&bc_types[kind-7]; uint32_t tag=t->count>1?(uint32_t)bc_get(c,1):0;
  if (c->code) return; if (tag>=t->count) { c->code=4; return; }
  if (build) { memset(slots,0,t->width*sizeof(Term)); if (t->count>1) *slots++=tag; slots[0]=BC_MARKER; slots[1]=1; slots+=2; }
  const BcVariant *v=&t->variants[tag];
  for (uint32_t i=0;i<v->count && !c->code;++i) { bc_read(e,v->fields[i],c,depth+1,slots); if (build) slots+=bc_width(v->fields[i]); }
}
static Term bc_read_scalar(Env e, uint32_t kind, BcCursor *c, uint32_t depth, bool build, Term *slots) {
  if (c->code || !bc_node(c,depth)) return 0;
  if (kind==0 || kind==1) { uint64_t n=bc_get(c,kind==1?1:4); if (kind==1 && n>1) c->code=4; return n; }
  if (kind==6) { uint64_t n=bc_get(c,8); if (build) { slots[0]=BC_MARKER; slots[1]=1; slots[2]=n>>32; slots[3]=(uint32_t)n; } return 0; }
  if (kind==2 || kind==5) {
    uint32_t n=(kind==5 && BC_BE)?c->limit-c->at:(uint32_t)bc_get(c,4);
    if (c->code) return 0;
    if (n>BC_MAX_BYTES) { c->code=2; return 0; }
    if (!bc_span(c,n)) return 0;
    const uint8_t *p=c->in+c->at;
    if (kind==2 && !bc_utf8(c,p,n)) return 0;
    c->at+=n; if (!build) return 0;
    if (kind==2) return io_str(e,(const char*)p,n);
    Term array=byte_array_new(e,n); memcpy(byte_array_data(e,array),p,n);
    slots[0]=BC_MARKER; slots[1]=1; slots[2]=array; slots[3]=n; return 0;
  }
  if (kind==3 || kind==4) {
    uint32_t n=(uint32_t)bc_get(c,4); if (c->code) return 0;
    if (n>BC_MAX_NODES-c->nodes) { c->code=2; return 0; }
    if (kind==4 && (!n || (n&(n-1)))) { c->code=7; return 0; }
    if (n>(c->limit-c->at)/4) { c->code=3; return 0; } c->nodes+=n;
    if (!build) { c->at+=n*4; return 0; }
    if (kind==4) { Term array=byte_array_new(e,n*4); uint32_t *p=(uint32_t*)byte_array_data(e,array); for (uint32_t i=0;i<n;++i) p[i]=(uint32_t)bc_get(c,4); return array; }
#if BC_HAS_LIST
    Term xs=term_pak(CID(Nil),0); uint32_t start=c->at;
    for (uint32_t i=n;i>0;--i) { c->at=start+(i-1)*4; xs=io_node(e,CID(Con),bc_get(c,4),xs); }
    c->at=start+n*4; return xs;
#else
    c->code=1; return 0;
#endif
  }
  c->code=1; return 0;
}
static void bc_result(Env e, const uint8_t *bytes, uint32_t length, uint32_t root, Term *slots, uint32_t initial) {
  BcCursor check={.in=bytes,.limit=length,.code=initial};
  if (length>BC_MAX_BYTES) check.code=2;
  if (!check.code) bc_read(e,root+7,&check,0,NULL);
  if (!check.code && check.at!=length) check.code=6;
  memset(slots,0,(3+bc_types[root].width)*sizeof(Term));
  slots[0]=check.code?1:0; slots[1]=BC_MARKER; slots[2]=1;
  if (check.code) { slots[3]=check.code; return; }
  BcCursor read={.in=bytes,.limit=length}; bc_read(e,root+7,&read,0,slots+3);
}
static Term bc_encode(Env e, Term *f, uint32_t root, uint32_t output) {
  uint8_t *bytes=NULL; uint32_t capacity=0;
  uint32_t code=f[1]>BC_MAX_BYTES?2:bc_array_view(e,f[0],(uint32_t)f[1],&bytes,&capacity);
  BcCursor measure={.limit=BC_MAX_BYTES,.code=code?2:0};
  Term slots[BC_SLOTS]={0}; const BcType *t=&bc_types[root];
  uint32_t tag=0;
  for (;tag<t->count;++tag) if (term_tag(f[2])==TAG_CTR && term_aux(f[2])==t->variants[tag].cid) break;
  if (tag==t->count) measure.code=1;
  else {
    uint32_t offset=t->count>1?1:0; if (offset) slots[0]=tag;
    memcpy(slots+offset,e.mem+term_peek(e.mem,f[2]),t->variants[tag].arity*sizeof(Term));
  }
  bc_write(e,root+7,slots,&measure,0);
  if (!measure.code && measure.at>capacity) measure.code=2;
  uint32_t length=0;
  if (!measure.code) {
    if (term_rfc(f[0]) || blk_loc(e.mem,f[0])<HEAP_OFF) {
      Term owned=byte_array_new(e,capacity); term_drop(e,f[0]); f[0]=owned; bytes=byte_array_data(e,owned);
    }
    BcCursor write={.out=bytes,.limit=capacity}; bc_write(e,root+7,slots,&write,0); measure.code=write.code; if (!write.code) length=write.at;
  }
  term_drop(e,f[2]);
  Term fields[]={BC_MARKER,1,f[0],length,measure.code}; return bc_make(e,output,5,fields);
}
static Term bc_decode_buffer(Env e, Term *f, uint32_t root, uint32_t input) {
  uint8_t *bytes=NULL; uint32_t capacity=0;
  uint32_t code=f[1]>BC_MAX_BYTES?2:bc_array_view(e,f[0],(uint32_t)f[1],&bytes,&capacity);
  Term fields[BC_SLOTS]={BC_MARKER,1,f[0],f[1]};
  bc_result(e,bytes,(uint32_t)f[1],root,fields+4,code?2:0);
  return bc_make(e,input,7+bc_types[root].width,fields);
}
static Term __attribute__((unused)) bc_decode_lease(Env e, Term *f, uint32_t root, uint32_t output) {
  WsByteView view=ws_socket_lease_view(io_hand_v(f[0]),(uint32_t)f[1]);
  Term fields[BC_SLOTS]={BC_MARKER,1,f[0]};
  bc_result(e,view.bytes,view.len,root,fields+3,view.code || view.kind!=0?8:0);
  return bc_make(e,output,6+bc_types[root].width,fields);
}
