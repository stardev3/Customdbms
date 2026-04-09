#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

// ------------------------------------------------------------
// Compatibility note (Windows + older MinGW toolchains)
// ------------------------------------------------------------
//
// Some legacy MinGW.org GCC distributions ship libstdc++ without C++11 threading
// types (std::thread / std::mutex / std::condition_variable). If that's the case,
// compilation fails even though the headers exist.
//
// To keep this project single-file and still "std::thread/std::mutex"-style,
// we provide a small Win32-backed implementation that supplies the subset we use.
//
// This is only enabled when the toolchain clearly lacks gthreads support.
// On modern compilers (MSVC, clang-cl, MinGW-w64), the real standard library is used.

#if defined(_WIN32) && defined(__MINGW32__) && !defined(_GLIBCXX_HAS_GTHREADS)
#define DBMS_WINTHREAD_FALLBACK 1
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif
#include <windows.h>
#include <process.h>

namespace std {
class mutex {
 public:
  mutex() { InitializeCriticalSection(&cs_); }
  ~mutex() { DeleteCriticalSection(&cs_); }
  void lock() { EnterCriticalSection(&cs_); }
  void unlock() { LeaveCriticalSection(&cs_); }
  CRITICAL_SECTION* native_handle() { return &cs_; }
  mutex(const mutex&) = delete;
  mutex& operator=(const mutex&) = delete;

 private:
  CRITICAL_SECTION cs_;
};

class thread {
 public:
  thread() : h_(NULL), id_(0) {}

  template <class Fn, class Arg>
  thread(Fn fn, Arg arg) : h_(NULL), id_(0) {
    // Store callable on heap (single-arg form is enough for this project).
    using Pack = pack_t<Fn, Arg>;
    Pack* p = new Pack(fn, arg);
    uintptr_t th = _beginthreadex(nullptr, 0, &thread::trampoline<Fn, Arg>, p, 0, &id_);
    h_ = reinterpret_cast<HANDLE>(th);
  }

  ~thread() {
    // If user forgets to join, detach to avoid termination.
    // (Standard std::thread would call std::terminate; we choose safety here.)
    if (joinable()) CloseHandle(h_);
  }

  thread(const thread&) = delete;
  thread& operator=(const thread&) = delete;

  thread(thread&& o) noexcept : h_(o.h_), id_(o.id_) {
    o.h_ = NULL;
    o.id_ = 0;
  }
  thread& operator=(thread&& o) noexcept {
    if (this != &o) {
      if (joinable()) CloseHandle(h_);
      h_ = o.h_;
      id_ = o.id_;
      o.h_ = NULL;
      o.id_ = 0;
    }
    return *this;
  }

  bool joinable() const { return h_ != NULL; }
  void join() {
    if (!h_) return;
    WaitForSingleObject(h_, INFINITE);
    CloseHandle(h_);
    h_ = NULL;
  }

 private:
  template <class Fn, class Arg>
  struct pack_t {
    Fn fn;
    Arg arg;
    pack_t(Fn f, Arg a) : fn(f), arg(a) {}
  };

  template <class Fn, class Arg>
  static unsigned __stdcall trampoline(void* p) {
    pack_t<Fn, Arg>* pack = reinterpret_cast<pack_t<Fn, Arg>*>(p);
    Fn fn = pack->fn;
    Arg arg = pack->arg;
    delete pack;
    fn(arg);
    return 0;
  }

  HANDLE h_;
  unsigned id_;
};
}  // namespace std
#endif

// ============================================================
// Custom In-Memory DBMS (single-file systems-style project)
// ============================================================
//
// Goals:
// - Manual storage: custom dynamic arrays + manual memory management
// - Indexing: hash index on primary key "id"
// - Query optimization: use index for WHERE id = X; otherwise full scan
// - Concurrency: concurrent SELECT (readers) + exclusive writers (INSERT/DELETE)
// - Performance measurement: per-op latency in microseconds + benchmark mode
//
// Non-goals (kept intentionally minimal):
// - Full SQL grammar, joins, transactions, durability, WAL, MVCC, etc.
// - Complex expression evaluation; we support WHERE id = <int> only.
//
// Trade-offs explained in comments near index + locking.

// ----------------------------
// Small utilities (no heavy STL usage for core storage)
// ----------------------------

static inline bool is_space(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }

static inline char to_upper_ascii(char c) {
  if (c >= 'a' && c <= 'z') return static_cast<char>(c - 'a' + 'A');
  return c;
}

static bool iequals(const char* a, const char* b) {
  while (*a && *b) {
    if (to_upper_ascii(*a) != to_upper_ascii(*b)) return false;
    ++a;
    ++b;
  }
  return *a == '\0' && *b == '\0';
}

static uint64_t now_us() {
  using namespace std::chrono;
  return duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

static void perf_print(const char* op, uint64_t start_us, uint64_t end_us) {
  std::cout << "[PERF] " << op << " latency: " << (end_us - start_us) << " us\n";
}

static char* str_dup_c(const char* s) {
  if (!s) return nullptr;
  const size_t n = std::strlen(s);
  char* out = new char[n + 1];
  std::memcpy(out, s, n + 1);
  return out;
}

// ----------------------------
// Tokenizer: manual parsing
// ----------------------------
//
// We parse commands like:
// CREATE <table>
// INSERT <table> <id> <value1> <value2> ...
// SELECT <table> [WHERE id = <id>]
// DELETE <table> WHERE id = <id>
// BENCHMARK <table>
// EXIT
//
// Tokenization is whitespace-delimited; quoted strings are supported: "Alice Bob"

struct TokenSpan {
  const char* ptr;
  int len;
};

struct TokenList {
  TokenSpan* toks;
  int count;
  int cap;

  TokenList() : toks(nullptr), count(0), cap(0) {}
  ~TokenList() { delete[] toks; }

  void reset() { count = 0; }

  void push(const char* p, int l) {
    if (count == cap) {
      int ncap = (cap == 0) ? 16 : cap * 2;
      TokenSpan* nt = new TokenSpan[ncap];
      for (int i = 0; i < count; ++i) nt[i] = toks[i];
      delete[] toks;
      toks = nt;
      cap = ncap;
    }
    toks[count++] = TokenSpan{p, l};
  }
};

static std::string token_to_string(const TokenSpan& t) {
  return std::string(t.ptr, t.ptr + t.len);
}

static bool token_ieq(const TokenSpan& t, const char* lit) {
  const int n = static_cast<int>(std::strlen(lit));
  if (t.len != n) return false;
  for (int i = 0; i < n; ++i) {
    if (to_upper_ascii(t.ptr[i]) != to_upper_ascii(lit[i])) return false;
  }
  return true;
}

static bool parse_int_token(const TokenSpan& t, int& out) {
  // strict parse (no trailing junk)
  if (t.len <= 0) return false;
  int sign = 1;
  int i = 0;
  if (t.ptr[0] == '-') {
    sign = -1;
    i = 1;
    if (t.len == 1) return false;
  }
  int64_t v = 0;
  for (; i < t.len; ++i) {
    char c = t.ptr[i];
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
    if (v > INT32_MAX) return false;
  }
  out = static_cast<int>(v * sign);
  return true;
}

static void tokenize_line_inplace(const std::string& line, TokenList& out) {
  out.reset();
  const char* s = line.c_str();
  const int n = static_cast<int>(line.size());
  int i = 0;
  while (i < n) {
    while (i < n && is_space(s[i])) ++i;
    if (i >= n) break;

    if (s[i] == '"') {
      // quoted token
      int start = ++i;
      while (i < n && s[i] != '"') ++i;
      int end = i;
      out.push(s + start, end - start);
      if (i < n && s[i] == '"') ++i;
    } else {
      int start = i;
      while (i < n && !is_space(s[i])) ++i;
      int end = i;
      out.push(s + start, end - start);
    }
  }
}

// ----------------------------
// Concurrency: custom RW lock
// ----------------------------
//
// Requirement: use std::thread + std::mutex.
// We implement a simple writer-preferring RW lock using one mutex + CV.
//
// Locking strategy:
// - SELECT holds shared lock -> many can run concurrently.
// - INSERT/DELETE hold exclusive lock -> blocks readers/writers.
//
// Trade-off:
// - Table-level RW lock is "fine-grained enough" vs a global lock.
// - We avoid a single global mutex; each table has its own lock.

struct RWLock {
  std::mutex m;
  int readers = 0;
  int writers_waiting = 0;
  bool writer_active = false;

  void lock_shared() {
    // Writer-preferring reader lock. We avoid std::condition_variable here to
    // keep compatibility with older libstdc++ toolchains.
    for (;;) {
      m.lock();
      if (!writer_active && writers_waiting == 0) {
        ++readers;
        m.unlock();
        return;
      }
      m.unlock();
#if defined(DBMS_WINTHREAD_FALLBACK)
      Sleep(0);
#else
      std::this_thread::yield();
#endif
    }
  }

  void unlock_shared() {
    std::lock_guard<std::mutex> lk(m);
    --readers;
  }

  void lock() {
    m.lock();
    ++writers_waiting;
    m.unlock();
    for (;;) {
      m.lock();
      if (!writer_active && readers == 0) {
        --writers_waiting;
        writer_active = true;
        m.unlock();
        return;
      }
      m.unlock();
#if defined(DBMS_WINTHREAD_FALLBACK)
      Sleep(0);
#else
      std::this_thread::yield();
#endif
    }
  }

  void unlock() {
    std::lock_guard<std::mutex> lk(m);
    writer_active = false;
  }
};

struct SharedGuard {
  RWLock* l;
  explicit SharedGuard(RWLock* lock) : l(lock) { l->lock_shared(); }
  ~SharedGuard() { l->unlock_shared(); }
};

struct ExclusiveGuard {
  RWLock* l;
  explicit ExclusiveGuard(RWLock* lock) : l(lock) { l->lock(); }
  ~ExclusiveGuard() { l->unlock(); }
};

// ----------------------------
// Core storage
// ----------------------------

enum ColumnType : uint8_t { COL_INT = 1, COL_TEXT = 2 };

struct Column {
  char* name;       // heap string
  ColumnType type;  // INT or TEXT
};

struct Value {
  ColumnType type;
  union {
    int i;
    char* s;
  } as;
};

struct Record {
  // We cache id for the primary-key index and keep values[0] in sync.
  int id;        // primary key (same as values[0].as.i)
  int ncols;     // number of columns (including id)
  Value* values; // heap array of typed values
};

static void record_free(Record* r) {
  if (!r) return;
  for (int i = 0; i < r->ncols; ++i) {
    if (r->values[i].type == COL_TEXT) delete[] r->values[i].as.s;
  }
  delete[] r->values;
  delete r;
}

// ----------------------------
// Index: hash table int -> slot index
// ----------------------------
//
// This is a custom open-addressing hash table with tombstones:
// - keys: int (record id)
// - values: uint32_t slot index in table storage
//
// Index usage:
// - SELECT/DELETE WHERE id = X uses index for O(1) average lookup.
// - Full scans are only used when no WHERE condition is present.
//
// Trade-offs:
// - Hash index is fast for equality lookup but not for range queries.
// - Tombstones avoid shifting but require periodic rehashing.

struct HashIndex {
  enum State : uint8_t { EMPTY = 0, FILLED = 1, TOMBSTONE = 2 };

  int* keys;
  uint32_t* vals;
  uint8_t* states;
  uint32_t cap;   // power of 2
  uint32_t size;  // number of FILLED slots
  uint32_t used;  // FILLED + TOMBSTONE

  HashIndex() : keys(nullptr), vals(nullptr), states(nullptr), cap(0), size(0), used(0) {}

  void init(uint32_t initial_cap) {
    // round up to power of 2
    uint32_t c = 1;
    while (c < initial_cap) c <<= 1;
    cap = (c < 16) ? 16 : c;
    keys = new int[cap];
    vals = new uint32_t[cap];
    states = new uint8_t[cap];
    std::memset(states, 0, cap);
    size = 0;
    used = 0;
  }

  void destroy() {
    delete[] keys;
    delete[] vals;
    delete[] states;
    keys = nullptr;
    vals = nullptr;
    states = nullptr;
    cap = size = used = 0;
  }

  static inline uint32_t mix32(uint32_t x) {
    // Simple integer hash mix (fast, decent dispersion)
    x ^= x >> 16;
    x *= 0x7feb352dU;
    x ^= x >> 15;
    x *= 0x846ca68bU;
    x ^= x >> 16;
    return x;
  }

  inline uint32_t mask() const { return cap - 1; }

  bool get(int key, uint32_t& out_val) const {
    if (cap == 0) return false;
    uint32_t h = mix32(static_cast<uint32_t>(key));
    uint32_t idx = h & mask();
    for (uint32_t probe = 0; probe < cap; ++probe) {
      uint8_t st = states[idx];
      if (st == EMPTY) return false;
      if (st == FILLED && keys[idx] == key) {
        out_val = vals[idx];
        return true;
      }
      idx = (idx + 1) & mask();
    }
    return false;
  }

  void rehash(uint32_t new_cap) {
    HashIndex ni;
    ni.init(new_cap);
    for (uint32_t i = 0; i < cap; ++i) {
      if (states[i] == FILLED) ni.put(keys[i], vals[i]);
    }
    destroy();
    *this = ni;
  }

  void maybe_grow() {
    // Grow based on "used" to control probe lengths with tombstones.
    if (cap == 0) init(64);
    // Load factor thresholds tuned for open addressing performance.
    const double used_lf = static_cast<double>(used + 1) / static_cast<double>(cap);
    if (used_lf > 0.70) rehash(cap * 2);
  }

  bool put(int key, uint32_t val) {
    maybe_grow();
    uint32_t h = mix32(static_cast<uint32_t>(key));
    uint32_t idx = h & mask();
    uint32_t first_tomb = UINT32_MAX;
    for (uint32_t probe = 0; probe < cap; ++probe) {
      uint8_t st = states[idx];
      if (st == EMPTY) {
        uint32_t dst = (first_tomb != UINT32_MAX) ? first_tomb : idx;
        if (states[dst] != FILLED) {
          if (states[dst] == EMPTY) ++used;
          states[dst] = FILLED;
          keys[dst] = key;
          vals[dst] = val;
          ++size;
          return true;
        }
      } else if (st == TOMBSTONE) {
        if (first_tomb == UINT32_MAX) first_tomb = idx;
      } else if (st == FILLED) {
        if (keys[idx] == key) {
          // update existing
          vals[idx] = val;
          return true;
        }
      }
      idx = (idx + 1) & mask();
    }
    // If table is pathologically full, force rehash.
    rehash(cap * 2);
    return put(key, val);
  }

  bool erase(int key) {
    if (cap == 0) return false;
    uint32_t h = mix32(static_cast<uint32_t>(key));
    uint32_t idx = h & mask();
    for (uint32_t probe = 0; probe < cap; ++probe) {
      uint8_t st = states[idx];
      if (st == EMPTY) return false;
      if (st == FILLED && keys[idx] == key) {
        states[idx] = TOMBSTONE;
        --size;
        // used unchanged (tombstone remains)
        return true;
      }
      idx = (idx + 1) & mask();
    }
    return false;
  }
};

// ----------------------------
// Table: manual dynamic array of record slots
// ----------------------------
//
// We store Record* in an array (slots). Delete sets slot to nullptr, leaving holes.
// Index maps id -> slot index. Holes are kept; inserts append or reuse holes via a freelist.
//
// Trade-offs:
// - Avoids moving records (stable pointers) and keeps index value stable.
// - Holes are reused via freelist, maintaining good memory locality over time.

struct FreeNode {
  uint32_t slot;
  FreeNode* next;
};

struct Table {
  char* name;
  Column* cols;
  int ncols;
  bool schema_fixed; // false for legacy CREATE (lazy schema on first INSERT)
  Record** slots;
  uint32_t cap;
  uint32_t size;      // number of live records
  uint32_t highwater; // maximum allocated slot index in use (<= cap)

  HashIndex index;    // id -> slot
  FreeNode* freelist; // slots available for reuse

  RWLock rw;          // per-table lock (readers parallel)

  // Background maintenance:
  // - Rehash index when tombstones/used factor grows
  // - Compact storage when hole ratio grows
  std::atomic<bool> stop_bg;
  std::thread maint;

  Table()
      : name(nullptr),
        cols(nullptr),
        ncols(0),
        schema_fixed(false),
        slots(nullptr),
        cap(0),
        size(0),
        highwater(0),
        freelist(nullptr),
        stop_bg(false) {}
};

static void freelist_push(Table* t, uint32_t slot) {
  FreeNode* n = new FreeNode();
  n->slot = slot;
  n->next = t->freelist;
  t->freelist = n;
}

static bool freelist_pop(Table* t, uint32_t& out_slot) {
  if (!t->freelist) return false;
  FreeNode* n = t->freelist;
  t->freelist = n->next;
  out_slot = n->slot;
  delete n;
  return true;
}

static void freelist_clear(Table* t) {
  FreeNode* n = t->freelist;
  while (n) {
    FreeNode* nxt = n->next;
    delete n;
    n = nxt;
  }
  t->freelist = nullptr;
}

static uint32_t next_pow2_u32(uint32_t x) {
  if (x <= 1) return 1;
  --x;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  return x + 1;
}

static void table_rebuild_index_locked(Table* t) {
  // Rebuild index to purge tombstones and reduce probe chains.
  HashIndex ni;
  uint32_t want = next_pow2_u32((t->size * 2u) + 16u);
  ni.init(want);
  for (uint32_t i = 0; i < t->highwater; ++i) {
    Record* r = t->slots[i];
    if (!r) continue;
    ni.put(r->id, i);
  }
  t->index.destroy();
  t->index = ni;
}

static void table_compact_locked(Table* t) {
  // Storage compaction:
  // - Densify slots to eliminate holes
  // - Rebuild freelist and index (slot numbers change)
  //
  // Trade-off: compaction is an O(n) stop-the-world operation per-table.
  // We only trigger it when hole ratio is high enough to justify the pause.

  if (t->highwater == 0) return;
  Record** ns = new Record*[t->cap];
  std::memset(ns, 0, sizeof(Record*) * t->cap);

  uint32_t w = 0;
  for (uint32_t i = 0; i < t->highwater; ++i) {
    Record* r = t->slots[i];
    if (!r) continue;
    ns[w++] = r;
  }

  delete[] t->slots;
  t->slots = ns;
  t->highwater = w;

  // All holes are now at the end; reset freelist.
  freelist_clear(t);

  // Rebuild index since slot indices changed.
  table_rebuild_index_locked(t);
}

static void table_maintenance_loop(Table* t) {
  // Background table maintenance thread.
  // It periodically checks:
  // - Index pressure (tombstones/used factor) -> rebuild hash table
  // - Storage hole ratio -> compact slot array
  //
  // Locking: takes table exclusive lock for the maintenance operation.
  while (!t->stop_bg.load(std::memory_order_relaxed)) {
#if defined(DBMS_WINTHREAD_FALLBACK)
    Sleep(200);
#else
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    if (t->stop_bg.load(std::memory_order_relaxed)) break;

    bool do_rebuild = false;
    bool do_compact = false;

    {
      // Take a shared-ish snapshot under mutex to keep it simple.
      // We use exclusive lock for the actual work.
      SharedGuard g(&t->rw);
      if (t->index.cap > 0) {
        const uint32_t tomb = (t->index.used >= t->index.size) ? (t->index.used - t->index.size) : 0u;
        const double used_lf = (t->index.cap == 0) ? 0.0 : (static_cast<double>(t->index.used) / static_cast<double>(t->index.cap));
        const double tomb_lf = (t->index.cap == 0) ? 0.0 : (static_cast<double>(tomb) / static_cast<double>(t->index.cap));
        if (used_lf > 0.85 || tomb_lf > 0.20) do_rebuild = true;
      }
      if (t->highwater > 4096) {
        const uint32_t holes = t->highwater - t->size;
        const double hole_ratio = (t->highwater == 0) ? 0.0 : (static_cast<double>(holes) / static_cast<double>(t->highwater));
        if (hole_ratio > 0.30) do_compact = true;
      }
    }

    if (!do_rebuild && !do_compact) continue;

    {
      ExclusiveGuard g(&t->rw);
      if (do_compact) table_compact_locked(t);
      else if (do_rebuild) table_rebuild_index_locked(t);
    }
  }
}

static void table_free_schema(Table* t) {
  if (!t->cols) return;
  for (int i = 0; i < t->ncols; ++i) delete[] t->cols[i].name;
  delete[] t->cols;
  t->cols = nullptr;
  t->ncols = 0;
}

static int table_find_col(const Table* t, const char* colname) {
  for (int i = 0; i < t->ncols; ++i) {
    if (t->cols[i].name && std::strcmp(t->cols[i].name, colname) == 0) return i;
  }
  return -1;
}

static void table_set_schema(Table* t, Column* cols, int ncols, bool fixed) {
  table_free_schema(t);
  t->cols = cols;
  t->ncols = ncols;
  t->schema_fixed = fixed;
}

static void table_init(Table* t, const char* nm) {
  t->name = str_dup_c(nm);
  t->cols = nullptr;
  t->ncols = 0;
  t->schema_fixed = false;
  t->cap = 1024; // start reasonably sized for systems-style usage
  t->slots = new Record*[t->cap];
  std::memset(t->slots, 0, sizeof(Record*) * t->cap);
  t->size = 0;
  t->highwater = 0;
  t->freelist = nullptr;
  t->index.init(2048); // index capacity independent from slots
  t->stop_bg.store(false, std::memory_order_relaxed);
  t->maint = std::thread(table_maintenance_loop, t);
}

static void table_destroy(Table* t) {
  if (!t) return;
  // Stop background maintenance thread (if started).
  t->stop_bg.store(true, std::memory_order_relaxed);
  if (t->maint.joinable()) t->maint.join();

  // exclusive lock not strictly needed at exit, but safe for future extension.
  ExclusiveGuard g(&t->rw);
  for (uint32_t i = 0; i < t->highwater; ++i) {
    if (t->slots[i]) record_free(t->slots[i]);
  }
  delete[] t->slots;
  t->slots = nullptr;
  t->cap = t->size = t->highwater = 0;
  t->index.destroy();
  freelist_clear(t);
  table_free_schema(t);
  delete[] t->name;
  t->name = nullptr;
}

static void table_grow_slots(Table* t, uint32_t need) {
  if (need <= t->cap) return;
  uint32_t ncap = t->cap;
  while (ncap < need) ncap *= 2;
  Record** ns = new Record*[ncap];
  std::memset(ns, 0, sizeof(Record*) * ncap);
  for (uint32_t i = 0; i < t->highwater; ++i) ns[i] = t->slots[i];
  delete[] t->slots;
  t->slots = ns;
  t->cap = ncap;
}

static bool parse_colspec(const TokenSpan& t, std::string& out_name, ColumnType& out_type) {
  // Format: name:INT or name:TEXT (case-insensitive type)
  const char* p = t.ptr;
  int n = t.len;
  int colon = -1;
  for (int i = 0; i < n; ++i) {
    if (p[i] == ':') {
      colon = i;
      break;
    }
  }
  if (colon <= 0 || colon >= n - 1) return false;
  out_name.assign(p, p + colon);
  std::string ty(p + colon + 1, p + n);
  for (size_t i = 0; i < ty.size(); ++i) ty[i] = to_upper_ascii(ty[i]);
  if (ty == "INT" || ty == "I32") {
    out_type = COL_INT;
    return true;
  }
  if (ty == "TEXT" || ty == "STR" || ty == "STRING") {
    out_type = COL_TEXT;
    return true;
  }
  return false;
}

static Record* record_create_typed(const Table* t, const TokenSpan* vals, int nvals) {
  // vals contains exactly t->ncols tokens (including id as first token).
  Record* r = new Record();
  r->ncols = t->ncols;
  r->values = new Value[r->ncols];

  for (int i = 0; i < r->ncols; ++i) {
    ColumnType ty = t->cols[i].type;
    r->values[i].type = ty;
    if (ty == COL_INT) {
      int v = 0;
      if (!parse_int_token(vals[i], v)) {
        record_free(r);
        return nullptr;
      }
      r->values[i].as.i = v;
    } else {
      std::string tmp = token_to_string(vals[i]);
      r->values[i].as.s = str_dup_c(tmp.c_str());
    }
  }

  // Primary key invariant: column 0 must be INT id.
  r->id = r->values[0].as.i;
  return r;
}

// ----------------------------
// Database: manual table registry (custom dynamic array)
// ----------------------------

struct Database {
  Table** tables; // pointers keep tables stable (mutex inside Table makes it non-copyable)
  uint32_t count;
  uint32_t cap;

  // A small mutex to protect table registry modifications (CREATE).
  // Once a table is found, operations use table-level RW lock.
  std::mutex registry_mu;

  Database() : tables(nullptr), count(0), cap(0) {}
};

static void db_init(Database* db) {
  db->cap = 16;
  db->count = 0;
  db->tables = new Table*[db->cap];
  std::memset(db->tables, 0, sizeof(Table*) * db->cap);
}

static void db_destroy(Database* db) {
  if (!db) return;
  for (uint32_t i = 0; i < db->count; ++i) {
    if (db->tables[i]) {
      table_destroy(db->tables[i]);
      delete db->tables[i];
      db->tables[i] = nullptr;
    }
  }
  delete[] db->tables;
  db->tables = nullptr;
  db->count = db->cap = 0;
}

static Table* db_find_table(Database* db, const char* name) {
  // Registry is stable after creation; we still guard read to avoid races with CREATE.
  std::lock_guard<std::mutex> lk(db->registry_mu);
  for (uint32_t i = 0; i < db->count; ++i) {
    Table* t = db->tables[i];
    if (t && t->name && std::strcmp(t->name, name) == 0) return t;
  }
  return nullptr;
}

static bool db_create_table(Database* db, const char* name) {
  std::lock_guard<std::mutex> lk(db->registry_mu);
  for (uint32_t i = 0; i < db->count; ++i) {
    Table* t = db->tables[i];
    if (t && t->name && std::strcmp(t->name, name) == 0) return false;
  }
  if (db->count == db->cap) {
    uint32_t ncap = db->cap * 2;
    Table** nt = new Table*[ncap];
    std::memset(nt, 0, sizeof(Table*) * ncap);
    for (uint32_t i = 0; i < db->count; ++i) nt[i] = db->tables[i];
    delete[] db->tables;
    db->tables = nt;
    db->cap = ncap;
  }
  Table* created = new Table();
  table_init(created, name);
  db->tables[db->count] = created;
  ++db->count;
  return true;
}

// ----------------------------
// Core operations
// ----------------------------

static bool table_ensure_legacy_schema_locked(Table* t, int total_cols) {
  // Legacy mode: schema is lazily fixed on first INSERT.
  // We model: id:INT, c1:TEXT, c2:TEXT, ...
  if (t->ncols != 0) return (t->ncols == total_cols);
  if (total_cols <= 0) return false;

  Column* cols = new Column[total_cols];
  cols[0].name = str_dup_c("id");
  cols[0].type = COL_INT;
  for (int i = 1; i < total_cols; ++i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "c%d", i);
    cols[i].name = str_dup_c(buf);
    cols[i].type = COL_TEXT;
  }
  table_set_schema(t, cols, total_cols, /*fixed=*/true);
  return true;
}

static bool InsertRecord(Table* t, const TokenSpan* vals, int nvals, bool print_perf) {
  uint64_t st = now_us();
  {
    ExclusiveGuard g(&t->rw);

    // Ensure schema exists.
    if (t->ncols == 0) {
      if (!table_ensure_legacy_schema_locked(t, nvals)) {
        std::cout << "ERR (invalid schema)\n";
        uint64_t en = now_us();
        if (print_perf) perf_print("INSERT", st, en);
        return false;
      }
    }
    if (nvals != t->ncols) {
      std::cout << "ERR (expected " << t->ncols << " values for INSERT)\n";
      uint64_t en = now_us();
      if (print_perf) perf_print("INSERT", st, en);
      return false;
    }
    if (t->cols[0].type != COL_INT || std::strcmp(t->cols[0].name, "id") != 0) {
      std::cout << "ERR (schema must start with id:INT)\n";
      uint64_t en = now_us();
      if (print_perf) perf_print("INSERT", st, en);
      return false;
    }

    int id = 0;
    if (!parse_int_token(vals[0], id)) {
      std::cout << "ERR (id must be int)\n";
      uint64_t en = now_us();
      if (print_perf) perf_print("INSERT", st, en);
      return false;
    }

    Record* nr = record_create_typed(t, vals, nvals);
    if (!nr) {
      std::cout << "ERR (type mismatch)\n";
      uint64_t en = now_us();
      if (print_perf) perf_print("INSERT", st, en);
      return false;
    }

    uint32_t existing_slot = 0;
    if (t->index.get(id, existing_slot)) {
      // Replace record (upsert-like behavior). In a real DB this would be UPDATE.
      // For this project, replacement keeps the index consistent.
      Record* old = t->slots[existing_slot];
      if (old) record_free(old);
      t->slots[existing_slot] = nr;
      uint64_t en = now_us();
      if (print_perf) perf_print("INSERT", st, en);
      return true;
    }

    uint32_t slot = 0;
    if (!freelist_pop(t, slot)) {
      slot = t->highwater;
      table_grow_slots(t, slot + 1);
      t->highwater++;
    }

    t->slots[slot] = nr;
    t->index.put(id, slot);
    t->size++;
  }
  uint64_t en = now_us();
  if (print_perf) perf_print("INSERT", st, en);
  return true;
}

static void print_record(const Record* r) {
  if (!r) return;
  std::cout << "id=" << r->id;
  for (int i = 1; i < r->ncols; ++i) {
    if (r->values[i].type == COL_INT) {
      std::cout << " " << r->values[i].as.i;
    } else {
      std::cout << " " << (r->values[i].as.s ? r->values[i].as.s : "");
    }
  }
  std::cout << "\n";
}

static bool SelectRecordById(Table* t, int id, bool print_perf) {
  uint64_t st = now_us();
  bool found = false;
  {
    SharedGuard g(&t->rw);

    // Query optimization: WHERE id = X -> index lookup.
    uint32_t slot = 0;
    if (t->index.get(id, slot)) {
      Record* r = t->slots[slot];
      if (r) {
        // Indexed path: no scan.
        print_record(r);
        found = true;
      }
    }
  }
  uint64_t en = now_us();
  if (print_perf) perf_print("SELECT", st, en);
  if (!found) std::cout << "(no rows)\n";
  return found;
}

static bool token_equals_text(const char* s, const TokenSpan& tok) {
  if (!s) return tok.len == 0;
  const int n = static_cast<int>(std::strlen(s));
  if (n != tok.len) return false;
  return std::memcmp(s, tok.ptr, tok.len) == 0;
}

static bool record_matches_eq(const Record* r, int col_idx, const TokenSpan& rhs) {
  const Value& v = r->values[col_idx];
  if (v.type == COL_INT) {
    int x = 0;
    if (!parse_int_token(rhs, x)) return false;
    return v.as.i == x;
  }
  return token_equals_text(v.as.s, rhs);
}

static void SelectWhereEq(Table* t, int col_idx, const TokenSpan& rhs, bool print_perf) {
  uint64_t st = now_us();
  uint32_t rows = 0;
  {
    SharedGuard g(&t->rw);
    // Query optimization: if WHERE is on id (col 0), use index.
    if (col_idx == 0 && t->ncols > 0 && t->cols[0].type == COL_INT) {
      int id = 0;
      if (parse_int_token(rhs, id)) {
        uint32_t slot = 0;
        if (t->index.get(id, slot)) {
          Record* r = t->slots[slot];
          if (r) {
            print_record(r);
            rows = 1;
          }
        }
      }
    } else {
      // Linear scan path for non-indexed columns.
      for (uint32_t i = 0; i < t->highwater; ++i) {
        Record* r = t->slots[i];
        if (!r) continue;
        if (col_idx >= 0 && col_idx < r->ncols && record_matches_eq(r, col_idx, rhs)) {
          print_record(r);
          ++rows;
        }
      }
    }
  }
  uint64_t en = now_us();
  if (print_perf) perf_print("SELECT", st, en);
  if (rows == 0) std::cout << "(no rows)\n";
}

static void SelectAll(Table* t, bool print_perf) {
  uint64_t st = now_us();
  uint32_t rows = 0;
  {
    SharedGuard g(&t->rw);

    // Linear scan: used only when no WHERE condition exists.
    for (uint32_t i = 0; i < t->highwater; ++i) {
      Record* r = t->slots[i];
      if (r) {
        print_record(r);
        ++rows;
      }
    }
  }
  uint64_t en = now_us();
  if (print_perf) perf_print("SELECT", st, en);
  if (rows == 0) std::cout << "(no rows)\n";
}

static bool delete_by_id_locked(Table* t, int id) {
  uint32_t slot = 0;
  if (t->index.get(id, slot)) {
    Record* r = t->slots[slot];
    if (r) {
      record_free(r);
      t->slots[slot] = nullptr;
      t->index.erase(id);
      freelist_push(t, slot);
      t->size--;
      return true;
    }
    // Index said present but slot empty; repair index.
    t->index.erase(id);
  }
  return false;
}

static bool DeleteRecordById(Table* t, int id, bool print_perf) {
  uint64_t st = now_us();
  bool deleted = false;
  {
    ExclusiveGuard g(&t->rw);
    deleted = delete_by_id_locked(t, id);
  }
  uint64_t en = now_us();
  if (print_perf) perf_print("DELETE", st, en);
  if (!deleted) std::cout << "(no rows deleted)\n";
  return deleted;
}

static uint32_t DeleteWhereEq(Table* t, int col_idx, const TokenSpan& rhs, bool print_perf) {
  uint64_t st = now_us();
  uint32_t deleted = 0;
  {
    ExclusiveGuard g(&t->rw);
    if (col_idx == 0) {
      int id = 0;
      if (parse_int_token(rhs, id)) {
        deleted = delete_by_id_locked(t, id) ? 1u : 0u;
      }
    } else {
      // Full scan delete for non-indexed columns.
      for (uint32_t i = 0; i < t->highwater; ++i) {
        Record* r = t->slots[i];
        if (!r) continue;
        if (col_idx >= 0 && col_idx < r->ncols && record_matches_eq(r, col_idx, rhs)) {
          int id = r->id;
          record_free(r);
          t->slots[i] = nullptr;
          t->index.erase(id);
          freelist_push(t, i);
          t->size--;
          ++deleted;
        }
      }
    }
  }
  uint64_t en = now_us();
  if (print_perf) perf_print("DELETE", st, en);
  if (deleted == 0) std::cout << "(no rows deleted)\n";
  return deleted;
}

// ----------------------------
// Benchmark mode
// ----------------------------
//
// BENCHMARK <table>
//
// - Runs multiple SELECT and INSERT operations.
// - SELECT benchmark: concurrent readers (threads) to exercise RW lock and index.
// - INSERT benchmark: single-threaded writes (serialized) to show write latency.
//
// Output: average microsecond latencies.

struct BenchStats {
  uint64_t total_us = 0;
  uint64_t ops = 0;
};

static void benchmark_table(Table* t) {
  const int N_SELECT = 20000;
  const int N_INSERT = 5000;
  const int THREADS = 4;

  // Ensure there is some data.
  {
    ExclusiveGuard g(&t->rw);
    // If schema is still unset (legacy CREATE), pin it to a small 2-column schema for benchmarking.
    if (t->ncols == 0) {
      (void)table_ensure_legacy_schema_locked(t, /*total_cols=*/2);
    }
    if (t->size < 1000) {
      // Populate with deterministic records.
      // We don't print per-op perf here to keep overhead minimal.
      for (int i = 1; i <= 2000; ++i) {
        uint32_t slot = 0;
        if (!t->index.get(i, slot)) {
          // Insert id + one text column.
          char idbuf[32];
          std::snprintf(idbuf, sizeof(idbuf), "%d", i);
          TokenSpan* vals = new TokenSpan[t->ncols];
          vals[0] = {idbuf, static_cast<int>(std::strlen(idbuf))};
          for (int c = 1; c < t->ncols; ++c) {
            if (t->cols[c].type == COL_INT) {
              vals[c] = {"0", 1};
            } else {
              vals[c] = {"seed", 4};
            }
          }
          uint32_t use_slot = 0;
          if (!freelist_pop(t, use_slot)) {
            use_slot = t->highwater;
            table_grow_slots(t, use_slot + 1);
            t->highwater++;
          }
          Record* r = record_create_typed(t, vals, t->ncols);
          delete[] vals;
          if (!r) continue;
          t->slots[use_slot] = r;
          t->index.put(i, use_slot);
          t->size++;
        }
      }
    }
  }

  std::cout << "Benchmark Results:\n";

  // -------- SELECT benchmark (concurrent readers)
  // We measure total wall-clock and average per operation latency in microseconds.
  // Each thread performs indexed selects.
  BenchStats sel_stats;
  std::mutex stats_mu;

  auto sel_worker = [&](int tid) {
    (void)tid;
    uint64_t local_us = 0;
    uint64_t local_ops = 0;

    for (int i = 0; i < N_SELECT / THREADS; ++i) {
      // simple xorshift for id selection (avoids <random> overhead)
      uint32_t x = static_cast<uint32_t>(i * 2654435761u + 12345u);
      x ^= x << 13;
      x ^= x >> 17;
      x ^= x << 5;
      int id = 1 + static_cast<int>(x % 2000);

      uint64_t st = now_us();
      {
        SharedGuard g(&t->rw);
        uint32_t slot = 0;
        // Indexed path only.
        if (t->index.get(id, slot)) {
          (void)t->slots[slot];
        }
      }
      uint64_t en = now_us();
      local_us += (en - st);
      local_ops += 1;
    }

    std::lock_guard<std::mutex> lk(stats_mu);
    sel_stats.total_us += local_us;
    sel_stats.ops += local_ops;
  };

  uint64_t sel_wall_st = now_us();
  std::thread th[THREADS] = {
      std::thread(sel_worker, 0),
      std::thread(sel_worker, 1),
      std::thread(sel_worker, 2),
      std::thread(sel_worker, 3),
  };
  for (int i = 0; i < THREADS; ++i) th[i].join();
  uint64_t sel_wall_en = now_us();

  double sel_avg = (sel_stats.ops == 0) ? 0.0 : (static_cast<double>(sel_stats.total_us) / static_cast<double>(sel_stats.ops));
  std::cout << "SELECT avg latency: " << sel_avg << " us"
            << " (threads=" << THREADS << ", wall=" << (sel_wall_en - sel_wall_st) << " us)\n";

  // -------- INSERT benchmark (serialized writes)
  // We measure per-op latency including lock acquisition.
  BenchStats ins_stats;
  int base_id = 1000000;
  for (int i = 0; i < N_INSERT; ++i) {
    int id = base_id + i;
    char idbuf[32];
    std::snprintf(idbuf, sizeof(idbuf), "%d", id);
    uint64_t st = now_us();
    {
      ExclusiveGuard g(&t->rw);
      if (t->ncols == 0) (void)table_ensure_legacy_schema_locked(t, 2);
      // Build a value vector matching schema.
      TokenSpan* vals = new TokenSpan[t->ncols];
      vals[0] = {idbuf, static_cast<int>(std::strlen(idbuf))};
      for (int c = 1; c < t->ncols; ++c) {
        if (t->cols[c].type == COL_INT) {
          vals[c] = {"1", 1};
        } else {
          vals[c] = {"bench", 5};
        }
      }

      uint32_t existing_slot = 0;
      if (!t->index.get(id, existing_slot)) {
        uint32_t slot = 0;
        if (!freelist_pop(t, slot)) {
          slot = t->highwater;
          table_grow_slots(t, slot + 1);
          t->highwater++;
        }
        Record* r = record_create_typed(t, vals, t->ncols);
        if (r) {
          t->slots[slot] = r;
          t->index.put(id, slot);
          t->size++;
        }
      }
      delete[] vals;
    }
    uint64_t en = now_us();
    ins_stats.total_us += (en - st);
    ins_stats.ops += 1;
  }

  double ins_avg = (ins_stats.ops == 0) ? 0.0 : (static_cast<double>(ins_stats.total_us) / static_cast<double>(ins_stats.ops));
  std::cout << "INSERT avg latency: " << ins_avg << " us\n";
}

// ----------------------------
// Query parsing / dispatch
// ----------------------------

static void usage() {
  std::cout << "Commands:\n"
            << "  CREATE <table> [col:type ...]     (type: INT|TEXT; values can be quoted)\n"
            << "    - If no schema is provided, the schema is inferred on first INSERT as: id:INT, c1:TEXT, c2:TEXT, ...\n"
            << "  INSERT <table> <v1> <v2> ...      (v1 is id; number/types must match schema)\n"
            << "  SELECT <table> [WHERE col = value]\n"
            << "  DELETE <table> WHERE col = value\n"
            << "  BENCHMARK <table>\n"
            << "  EXIT\n";
}

static bool ParseAndExecute(Database* db, const std::string& line) {
  TokenList tl;
  tokenize_line_inplace(line, tl);
  if (tl.count == 0) return true;

  // EXIT
  if (token_ieq(tl.toks[0], "EXIT") || token_ieq(tl.toks[0], "QUIT")) return false;
  if (token_ieq(tl.toks[0], "HELP")) {
    usage();
    return true;
  }

  // CREATE <table> [col:type ...]
  if (token_ieq(tl.toks[0], "CREATE")) {
    if (tl.count < 2) {
      std::cout << "Syntax: CREATE <table> [col:type ...]\n";
      return true;
    }
    std::string tname = token_to_string(tl.toks[1]);
    if (db_create_table(db, tname.c_str())) {
      Table* t = db_find_table(db, tname.c_str());
      // Optional schema specification.
      if (t && tl.count > 2) {
        const int spec_n = tl.count - 2;
        bool has_id = false;
        // First pass: count valid specs.
        for (int i = 0; i < spec_n; ++i) {
          std::string cname;
          ColumnType cty;
          if (!parse_colspec(tl.toks[2 + i], cname, cty)) {
            std::cout << "ERR (bad colspec: use name:INT or name:TEXT)\n";
            return true;
          }
          if (cname == "id") has_id = true;
        }

        const int total_cols = has_id ? spec_n : (spec_n + 1);
        Column* cols = new Column[total_cols];
        int w = 0;
        if (!has_id) {
          cols[w].name = str_dup_c("id");
          cols[w].type = COL_INT;
          ++w;
        }
        for (int i = 0; i < spec_n; ++i) {
          std::string cname;
          ColumnType cty;
          parse_colspec(tl.toks[2 + i], cname, cty);
          cols[w].name = str_dup_c(cname.c_str());
          cols[w].type = cty;
          ++w;
        }

        // Enforce id:INT at column 0 for primary-key index correctness.
        if (!(std::strcmp(cols[0].name, "id") == 0 && cols[0].type == COL_INT)) {
          std::cout << "ERR (first column must be id:INT)\n";
          for (int i = 0; i < total_cols; ++i) delete[] cols[i].name;
          delete[] cols;
          return true;
        }

        {
          ExclusiveGuard g(&t->rw);
          table_set_schema(t, cols, total_cols, /*fixed=*/true);
        }
      }
      std::cout << "OK (created table '" << tname << "')\n";
    } else {
      std::cout << "ERR (table exists)\n";
    }
    return true;
  }

  // BENCHMARK <table>
  if (token_ieq(tl.toks[0], "BENCHMARK")) {
    if (tl.count != 2) {
      std::cout << "Syntax: BENCHMARK <table>\n";
      return true;
    }
    std::string tname = token_to_string(tl.toks[1]);
    Table* t = db_find_table(db, tname.c_str());
    if (!t) {
      std::cout << "ERR (no such table)\n";
      return true;
    }
    benchmark_table(t);
    return true;
  }

  // INSERT <table> <vals...>  (vals include id as first value)
  if (token_ieq(tl.toks[0], "INSERT")) {
    if (tl.count < 3) {
      std::cout << "Syntax: INSERT <table> <v1> <v2> ...\n";
      return true;
    }
    std::string tname = token_to_string(tl.toks[1]);
    Table* t = db_find_table(db, tname.c_str());
    if (!t) {
      std::cout << "ERR (no such table)\n";
      return true;
    }
    const TokenSpan* vals = &tl.toks[2];
    int nvals = tl.count - 2;
    if (InsertRecord(t, vals, nvals, /*print_perf=*/true)) std::cout << "OK\n";
    return true;
  }

  // SELECT <table> [WHERE col = value]
  if (token_ieq(tl.toks[0], "SELECT")) {
    if (tl.count < 2) {
      std::cout << "Syntax: SELECT <table> [WHERE col = value]\n";
      return true;
    }
    std::string tname = token_to_string(tl.toks[1]);
    Table* t = db_find_table(db, tname.c_str());
    if (!t) {
      std::cout << "ERR (no such table)\n";
      return true;
    }

    if (tl.count == 2) {
      SelectAll(t, /*print_perf=*/true);
      return true;
    }

    // WHERE <col> = <value>
    if (tl.count == 6 && token_ieq(tl.toks[2], "WHERE") && tl.toks[4].len == 1 && tl.toks[4].ptr[0] == '=') {
      std::string col = token_to_string(tl.toks[3]);
      int col_idx = -1;
      {
        SharedGuard g(&t->rw);
        if (t->ncols > 0) col_idx = table_find_col(t, col.c_str());
      }
      if (col_idx < 0) {
        std::cout << "ERR (unknown column or schema not set yet)\n";
        return true;
      }
      SelectWhereEq(t, col_idx, tl.toks[5], /*print_perf=*/true);
      return true;
    }

    std::cout << "ERR (supported WHERE: WHERE <col> = <value>)\n";
    return true;
  }

  // DELETE <table> WHERE col = value
  if (token_ieq(tl.toks[0], "DELETE")) {
    if (tl.count != 6) {
      std::cout << "Syntax: DELETE <table> WHERE col = value\n";
      return true;
    }
    std::string tname = token_to_string(tl.toks[1]);
    Table* t = db_find_table(db, tname.c_str());
    if (!t) {
      std::cout << "ERR (no such table)\n";
      return true;
    }
    if (!(token_ieq(tl.toks[2], "WHERE") && tl.toks[4].len == 1 && tl.toks[4].ptr[0] == '=')) {
      std::cout << "ERR (supported WHERE: WHERE <col> = <value>)\n";
      return true;
    }
    std::string col = token_to_string(tl.toks[3]);
    int col_idx = -1;
    {
      SharedGuard g(&t->rw);
      if (t->ncols > 0) col_idx = table_find_col(t, col.c_str());
    }
    if (col_idx < 0) {
      std::cout << "ERR (unknown column or schema not set yet)\n";
      return true;
    }
    (void)DeleteWhereEq(t, col_idx, tl.toks[5], /*print_perf=*/true);
    std::cout << "OK\n";
    return true;
  }

  std::cout << "ERR (unknown command). Type HELP.\n";
  return true;
}

int main() {
  Database db;
  db_init(&db);

  std::cout << "Custom In-Memory DBMS \n";
  std::cout << "Type HELP for commands.\n";

  std::string line;
  while (true) {
    std::cout << "DBMS > ";
    if (!std::getline(std::cin, line)) break;
    bool cont = ParseAndExecute(&db, line);
    if (!cont) break;
  }

  db_destroy(&db);
  return 0;
}

