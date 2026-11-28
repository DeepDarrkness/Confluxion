io_eff(CID(pack), bytes => {
  const values = []; let list = bytes;
  while (list.$ === CID(Con)) {if (list.head > 255 || values.length === 1048576) throw Error("Invalid fixture byte list"); values.push(list.head); list = list.tail;}
  const words = new Uint32Array(2 ** Math.ceil(Math.log2(Math.max(1, Math.ceil(values.length / 4)))));
  values.forEach((x, i) => {words[i >>> 2] |= x << ((i & 3) * 8);});
  return {$: CID(Packed), words, length: values.length};
});
io_eff(CID(read), () => {throw Error("Fixture native lease required");});
io_eff(CID(owned_list), buffer => {
  let list = {$: CID(Nil)};
  for (let i = buffer.length; i-- > 0;) list = {$: CID(Con), head: (buffer.words[i >>> 2] >>> ((i & 3) * 8)) & 255, tail: list};
  return list;
});
