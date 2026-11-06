// The imports of the engine from its host, for `ObjectPart.cpp`. The host passes `Module.host` to the factory of the
// module. With JSPI, an `__async` import suspends the engine until its promise resolves: the engine reads the parts
// of `ObjectMergeTree` while it runs a query, rather than before.
//
// An import never throws into the engine: an exception of JavaScript would unwind it without running its destructors.
// It returns -1 instead, and the engine takes the message (`ch_host_error_size`, `ch_host_take_error`) and throws an
// exception of its own.

addToLibrary({
  $hostError: "''",

  // size_t ch_host_error_size()
  ch_host_error_size__deps: ["$hostError"],
  ch_host_error_size__sig: "p",
  ch_host_error_size: function () {
    return lengthBytesUTF8(hostError);
  },

  // void ch_host_take_error(char * dest, size_t size)
  ch_host_take_error__deps: ["$hostError"],
  ch_host_take_error__sig: "vpp",
  ch_host_take_error: function (dest, size) {
    stringToUTF8(hostError, Number(dest), Number(size) + 1);
    hostError = "";
  },

  // size_t ch_host_read_range(const char * part, size_t part_size, size_t offset, size_t size, char * dest)
  ch_host_read_range__deps: ["$hostError"],
  ch_host_read_range__async: true,
  ch_host_read_range__sig: "pppppp",
  ch_host_read_range: async function (part, part_size, offset, size, dest) {
    const name = UTF8ToString(Number(part), Number(part_size));
    try {
      const bytes = await Module.host.readRange(name, Number(offset), Number(size));
      // The memory may have grown while the engine waited, so `HEAPU8` is the view of now.
      HEAPU8.set(bytes, Number(dest));
      return bytes.length;
    } catch (error) {
      hostError = String(error);
      return -1;
    }
  },

  // int ch_host_write_part(const char * part, size_t part_size, const char * data, size_t size)
  // The host puts a part that a query wrote in object storage while the query runs, so that the engine holds one
  // part at a time; it deletes it if the query or its commit fails. Returns 0, or -1.
  ch_host_write_part__deps: ["$hostError"],
  ch_host_write_part__async: true,
  ch_host_write_part__sig: "ipppp",
  ch_host_write_part: async function (part, part_size, data, size) {
    const name = UTF8ToString(Number(part), Number(part_size));
    // A copy: the memory may grow while the engine waits, and the engine frees the data once this returns.
    const bytes = HEAPU8.slice(Number(data), Number(data) + Number(size));
    try {
      await Module.host.writePart(name, bytes);
      return 0;
    } catch (error) {
      hostError = String(error);
      return -1;
    }
  },

  // size_t ch_host_read_input(char * dest, size_t size)
  // The next bytes of the body of the request of the query, at most `size`; none at its end.
  ch_host_read_input__deps: ["$hostError"],
  ch_host_read_input__async: true,
  ch_host_read_input__sig: "ppp",
  ch_host_read_input: async function (dest, size) {
    try {
      const bytes = await Module.host.readInput(Number(size));
      // The memory may have grown while the engine waited, so `HEAPU8` is the view of now.
      HEAPU8.set(bytes, Number(dest));
      return bytes.length;
    } catch (error) {
      hostError = String(error);
      return -1;
    }
  },

  // int ch_host_write_output(const char * data, size_t size)
  // The next bytes of the output of the query, which the host keeps. -1 if it has too much.
  ch_host_write_output__deps: ["$hostError"],
  ch_host_write_output__sig: "ipp",
  ch_host_write_output: function (data, size) {
    try {
      Module.host.writeOutput(HEAPU8.slice(Number(data), Number(data) + Number(size)));
      return 0;
    } catch (error) {
      hostError = error instanceof Error ? error.message : String(error);
      return -1;
    }
  },

  // void ch_host_prefetch(const char * part, size_t part_size, size_t offset, size_t size)
  ch_host_prefetch__sig: "vpppp",
  ch_host_prefetch: function (part, part_size, offset, size) {
    Module.host.prefetch(UTF8ToString(Number(part), Number(part_size)), Number(offset), Number(size));
  },
});
