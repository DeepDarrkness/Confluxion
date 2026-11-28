#ifndef CONFLUXION_BYTE_BUFFER_H
#define CONFLUXION_BYTE_BUFFER_H
#include <stdint.h>
#include <string.h>
#if !defined(__linux__) || !defined(__BYTE_ORDER__) || __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
#error "Packed Bend byte buffers require little-endian Linux"
#endif

#define BYTE_BUFFER_MAX 1048576u
static uint32_t byte_array_view(Env e, Term array, uint32_t length,
                                uint8_t **data, uint32_t *capacity) {
  *data = NULL; *capacity = 0;
  if (term_tag(array) != TAG_BUF || term_aux(array) != blk_cls(array)) return 4;
  if (blk_cls(array) > 18) return 25;
  uint32_t cap = 4u << blk_cls(array);
  if (length > cap) return 25;
  *data = (uint8_t*)blk_ptr(e.mem, blk_loc(e.mem, array), 0);
  *capacity = cap;
  return 0;
}
static Term byte_array_new(Env e, uint32_t length) {
  if (length > BYTE_BUFFER_MAX) err_fail("Byte array allocation exceeds 1 MiB");
  uint32_t cls = 0;
  while ((4u << cls) < length) cls++;
  uint32_t words = blk_wcls(false, cls);
  uint64_t loc = heap_alloc(e, words);
  if (!err_seen(e.mem)) memset(e.mem + loc, 0, (1ull << words) * sizeof(uint64_t));
  return term_blk(false, cls, loc);
}
static uint8_t *byte_array_data(Env e, Term array) {
  uint8_t *data; uint32_t capacity;
  return byte_array_view(e, array, 0, &data, &capacity) == 0 ? data : NULL;
}
static uint32_t byte_buffer_view(Env e, Term buffer, uint8_t **data,
                                 uint32_t *length, uint32_t *capacity) {
  *data = NULL; *length = 0; *capacity = 0;
  if (term_tag(buffer) != TAG_CTR || cid_arity(term_aux(buffer)) != 2) return 4;
  uint64_t loc = term_peek(e.mem, buffer);
  if (e.mem[loc + 1] > UINT32_MAX) return 4;
  *length = (uint32_t)e.mem[loc + 1];
  return byte_array_view(e, e.mem[loc], *length, data, capacity);
}
static uint32_t byte_buffer_length(Env e, Term buffer, uint32_t length) {
  uint8_t *data; uint32_t old, capacity;
  uint32_t code = byte_buffer_view(e, buffer, &data, &old, &capacity);
  if (code) return code;
  if (length > capacity) return 25;
  e.mem[term_peek(e.mem, buffer) + 1] = length;
  return 0;
}
#endif
