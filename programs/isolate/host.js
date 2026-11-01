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

  // void ch_host_prefetch(const char * part, size_t part_size, size_t offset, size_t size)
  ch_host_prefetch__sig: "vpppp",
  ch_host_prefetch: function (part, part_size, offset, size) {
    Module.host.prefetch(UTF8ToString(Number(part), Number(part_size)), Number(offset), Number(size));
  },
});
