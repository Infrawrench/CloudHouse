// The imports of the engine from its host, for `ObjectPart.cpp`. The host passes `Module.host` to the factory of the
// module. With JSPI, an `__async` import suspends the engine until its promise resolves: the engine reads the parts
// of `ObjectMergeTree` while it runs a query, rather than before.

addToLibrary({
  // size_t ch_host_read_range(const char * part, size_t part_size, size_t offset, size_t size, char * dest)
  ch_host_read_range__async: true,
  ch_host_read_range__sig: "pppppp",
  ch_host_read_range: async function (part, part_size, offset, size, dest) {
    const name = UTF8ToString(Number(part), Number(part_size));
    const bytes = await Module.host.readRange(name, Number(offset), Number(size));
    // The memory may have grown while the engine waited, so `HEAPU8` is the view of now.
    HEAPU8.set(bytes, Number(dest));
    return bytes.length;
  },

  // void ch_host_write_part(const char * part, size_t part_size, const char * data, size_t size)
  // The host puts a part that a query wrote in object storage while the query runs, so that the engine holds one
  // part at a time; it deletes it if the query or its commit fails.
  ch_host_write_part__async: true,
  ch_host_write_part__sig: "vpppp",
  ch_host_write_part: async function (part, part_size, data, size) {
    const name = UTF8ToString(Number(part), Number(part_size));
    // A copy: the memory may grow while the engine waits, and the engine frees the data once this returns.
    const bytes = HEAPU8.slice(Number(data), Number(data) + Number(size));
    await Module.host.writePart(name, bytes);
  },

  // size_t ch_host_read_input(char * dest, size_t size)
  // The next bytes of the body of the request of the query, at most `size`; none at its end.
  ch_host_read_input__async: true,
  ch_host_read_input__sig: "ppp",
  ch_host_read_input: async function (dest, size) {
    const bytes = await Module.host.readInput(Number(size));
    // The memory may have grown while the engine waited, so `HEAPU8` is the view of now.
    HEAPU8.set(bytes, Number(dest));
    return bytes.length;
  },

  // void ch_host_prefetch(const char * part, size_t part_size, size_t offset, size_t size)
  ch_host_prefetch__sig: "vpppp",
  ch_host_prefetch: function (part, part_size, offset, size) {
    Module.host.prefetch(UTF8ToString(Number(part), Number(part_size)), Number(offset), Number(size));
  },
});
