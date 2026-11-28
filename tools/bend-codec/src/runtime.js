const bcMaxBytes = 1048576, bcMaxNodes = 262144;
function bcThrow(code) { throw code; }
function bcView(words, length) {
  if (!(words instanceof Uint32Array) || !words.length || (words.length & (words.length-1)) || words.byteLength > bcMaxBytes || !Number.isInteger(length) || length < 0 || length > words.byteLength) bcThrow(2);
  return new Uint8Array(words.buffer, words.byteOffset, length);
}
function bcEncode(words, length, value, root, output) {
  let code=0, size=0;
  try {
    bcView(words,length);
    let nodes=0, at=0, bytes=null;
    const put = (n,width) => { if (at+width>bcMaxBytes) bcThrow(2); for(let i=0;i<width;++i) { if(bytes) bytes[at+i]=Number((BigInt(n)>>BigInt(8*(bcBe?width-1-i:i)))&255n); } at+=width; };
    const span = b => { if(at+b.length>bcMaxBytes) bcThrow(2); if(bytes) bytes.set(b,at); at+=b.length; };
    const write = (kind,v,depth) => {
      if(depth>64 || ++nodes>bcMaxNodes) bcThrow(2);
      if(kind===0 || kind===1) { if(!Number.isInteger(v) || v<0 || v>4294967295 || (kind===1 && v>1)) bcThrow(4); put(v,kind===1?1:4); }
      else if(kind===6) { if(v.$!==bcWord || v.marker0!==1145980243 || v.marker1!==1) bcThrow(1); put((BigInt(v.hi)<<32n)|BigInt(v.lo),8); }
      else if(kind===5) { if(v.$!==bcBlob || v.marker0!==1145980243 || v.marker1!==1) bcThrow(1); const b=bcView(v.words,v.length); if(!bcBe) put(b.length,4); span(b); }
      else if(kind===2) {
        if(typeof v!=='string') bcThrow(1);
        for(const cp of v) { const n=cp.codePointAt(0); if(n>=0xd800 && n<=0xdfff) bcThrow(5); if(++nodes>bcMaxNodes) bcThrow(2); }
        const b=new TextEncoder().encode(v); put(b.length,4); span(b);
      } else if(kind===3 || kind===4) {
        const xs=[];
        if(kind===4) { bcView(v,v.byteLength); for(const n of v) xs.push(n); }
        else { while(v.$===CID(Con)) { if(xs.length>=bcMaxNodes) bcThrow(2); xs.push(v.head); v=v.tail; } if(v.$!==CID(Nil)) bcThrow(1); }
        if(xs.length>bcMaxNodes-nodes) bcThrow(2); nodes+=xs.length; put(xs.length,4); for(const n of xs) put(n,4);
      } else {
        const vs=bcTypes[kind-7], tag=vs.findIndex(x=>x.cid===v.$); if(tag<0 || v.marker0!==1145980243 || v.marker1!==1) bcThrow(4);
        if(vs.length>1) put(tag,1); for(const [name,k] of vs[tag].fields) write(k,v[name],depth+1);
      }
    };
    write(root+7,value,0); size=at;
    if(size>words.byteLength) bcThrow(2);
    bytes=new Uint8Array(words.buffer,words.byteOffset,words.byteLength); at=0; nodes=0; write(root+7,value,0);
  } catch(e) { if(!Number.isInteger(e)) throw e; code=e; size=0; }
  return {$:output,marker0:1145980243,marker1:1,words,length:size,code};
}
function bcDecode(words,length,root,input,ok,fail) {
  let result;
  try {
    const bytes=bcView(words,length), view=new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength);
    let at=0,nodes=0;
    const get = width => { if(width>length-at) bcThrow(3); let n=0n; for(let i=0;i<width;++i) n|=BigInt(bytes[at+i])<<BigInt(8*(bcBe?width-1-i:i)); at+=width; return width===8?n:Number(n); };
    const read = (kind,depth,build) => {
      if(depth>64 || ++nodes>bcMaxNodes) bcThrow(2);
      if(kind===0 || kind===1) { const n=get(kind===1?1:4); if(kind===1 && n>1) bcThrow(4); return n; }
      if(kind===6) { const n=get(8); return build?{$:bcWord,marker0:1145980243,marker1:1,hi:Number(n>>32n),lo:Number(n&4294967295n)}:null; }
      if(kind===2 || kind===5) {
        const n=kind===5 && bcBe?length-at:get(4); if(n>bcMaxBytes) bcThrow(2); if(n>length-at) bcThrow(3);
        const b=bytes.subarray(at,at+n); at+=n;
        if(kind===2) { let s; try { s=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(b); } catch(e) { bcThrow(5); } nodes+=[...s].length; if(nodes>bcMaxNodes) bcThrow(2); return build?s:null; }
        if(!build) return null;
        let count=1; while(count*4<n) count*=2; const array=new Uint32Array(count); new Uint8Array(array.buffer).set(b); return {$:bcBlob,marker0:1145980243,marker1:1,words:array,length:n};
      }
      if(kind===3 || kind===4) {
        const n=get(4); if(n>bcMaxNodes-nodes) bcThrow(2); if(kind===4 && (!n || (n&(n-1)))) bcThrow(7); if(n>(length-at)/4) bcThrow(3); nodes+=n;
        if(!build) { at+=n*4; return null; }
        if(kind===4) { const xs=new Uint32Array(n); for(let i=0;i<n;++i) xs[i]=get(4); return xs; }
        const ns=[]; for(let i=0;i<n;++i) ns.push(get(4)); let xs={$:CID(Nil)}; for(let i=n-1;i>=0;--i) xs={$:CID(Con),head:ns[i],tail:xs}; return xs;
      }
      const vs=bcTypes[kind-7],tag=vs.length>1?get(1):0; if(tag>=vs.length) bcThrow(4); const v=vs[tag],out=build?{$:v.cid,marker0:1145980243,marker1:1}:null;
      for(const [name,k] of v.fields) { const x=read(k,depth+1,build); if(build) out[name]=x; } return out;
    };
    read(root+7,0,false); if(at!==length) bcThrow(6); at=0; nodes=0;
    result={$:ok,marker0:1145980243,marker1:1,value:read(root+7,0,true)};
  } catch(e) { if(!Number.isInteger(e)) throw e; result={$:fail,marker0:1145980243,marker1:1,code:e}; }
  return {$:input,marker0:1145980243,marker1:1,words,length,result};
}
