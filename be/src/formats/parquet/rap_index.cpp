// See rap_index.h. Format contract: harness/rap_index_build.py.
#include "formats/parquet/rap_index.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <system_error>

#include "base/hash/crc32c.h"
#include "common/config.h"
#include "fs/fs.h"
#include "types/datum.h"

namespace starrocks::parquet {

namespace {

constexpr char kMagic[4] = {'R', 'A', 'P', 'X'};
// slice 4 (PRD-02): the smallest footprint one declared entry can have in the body -- a count is refused against
// these BEFORE anything is reserved for it
constexpr size_t kMinValueBytes = 4 + 4; // u32 key length (an empty key) + u32 range count
constexpr size_t kRangeBytes = 16;       // i64 start + i64 end

class Cursor {
public:
    Cursor(const std::string& b) : _b(b) {}
    bool need(size_t n) const { return _o + n <= _b.size(); }
    size_t offset() const { return _o; }
    template <typename T>
    bool read(T* out) {
        if (!need(sizeof(T))) return false;
        std::memcpy(out, _b.data() + _o, sizeof(T));
        _o += sizeof(T);
        return true;
    }
    bool read_bytes(size_t n, std::string* out) {
        if (!need(n)) return false;
        out->assign(_b.data() + _o, n);
        _o += n;
        return true;
    }

private:
    const std::string& _b;
    size_t _o = 0;
};

RapIndex::Result unusable(const std::string& why) {
    RapIndex::Result r;
    r.state = RapIndex::State::UNUSABLE;
    r.reason = why;
    return r;
}

bool valid_key_type(uint8_t t) {
    return t >= static_cast<uint8_t>(RapIndex::KeyType::STRING) && t <= static_cast<uint8_t>(RapIndex::KeyType::DATETIME);
}

// one ascending, non-overlapping range list read from the cursor, every range inside [0, file_rows)
bool read_ranges(Cursor* c, uint32_t nr, uint64_t file_rows, size_t body_end, std::vector<RowRangeHint>* out, std::string* why) {
    // PRD-02: the declared count is checked against the bytes left BEFORE reserving
    if (c->offset() > body_end || nr > (body_end - c->offset()) / kRangeBytes) {
        *why = "declared count exceeds body";
        return false;
    }
    out->reserve(nr);
    int64_t last_end = -1;
    for (uint32_t k = 0; k < nr; ++k) {
        int64_t s = 0, e = 0;
        if (!c->read(&s) || !c->read(&e)) {
            *why = "truncated range";
            return false;
        }
        if (s < 0 || s >= e || s < last_end) {
            *why = "malformed range";
            return false;
        }
        if (static_cast<uint64_t>(e) > file_rows) {
            *why = "range beyond file_rows";
            return false;
        }
        last_end = e;
        out->push_back(RowRangeHint{s, e});
    }
    return true;
}

} // namespace

RapIndex::Result RapIndex::load(const std::string& path, const Identity& expect) {
    return load(nullptr, path, expect);
}

std::string RapIndex::cache_key(bool negative, const std::string& file_key, const std::string& column,
                                const std::string& generation, const std::string& directory) {
    auto field = [](const std::string& s) { return std::to_string(s.size()) + ":" + s; };
    std::string k = negative ? "rapneg|" : "rapidx|";
    k += field(file_key) + "|" + field(column) + "|" + field(generation);
    if (negative) k += "|" + field(directory); // a refusal is a fact about a directory; a READY index is not
    return k;
}

std::string RapIndex::key_of(const std::string& path) {
    // slice 2g v4 (m38 review, F-COLLISION / PRD-01): the key keeps the STORAGE NAMESPACE. `<scheme>://rest` becomes
    // `<scheme>/rest` with rest's leading slashes dropped; a path without a scheme, and a file:// URI, are the local
    // filesystem and become `file/rest`. gs://b/t/data/x, s3://b/t/data/x and /b/t/data/x are three objects and get
    // three keys. Nothing is shortened: v2's "path after the last /data/" dropped the table (two tables with one suffix
    // shared a sidecar); v3 dropped the scheme (two stores with one bucket name shared a sidecar). A scheme spelled
    // differently (s3a:// vs s3://) is keyed differently: a miss, never another object's postings.
    std::string scheme = "file", rest = path;
    const auto sch = path.find("://");
    if (sch != std::string::npos) {
        scheme = path.substr(0, sch);
        rest = path.substr(sch + 3);
        if (scheme.empty()) scheme = "file";
    }
    // slice 2g v4b (m39 review): a RELATIVE local path is not an identity on its own -- `x.parquet` names a file only
    // together with the process's working directory, and under the plain rule it took the same key as the absolute
    // `/x.parquet`. It is resolved against the working directory here, so the two cannot collide. No normalisation is
    // done (`a/../x` is left alone): a lexical rewrite across a symlink would name a DIFFERENT file, and a key that
    // merely misses is safe while a key that matches the wrong file is not. If the working directory cannot be read the
    // path keeps its own namespace, `file-rel/`, which no absolute path can occupy.
    if (scheme == "file" && !rest.empty() && rest.front() != '/') {
        std::error_code ec;
        const std::filesystem::path cwd = std::filesystem::current_path(ec);
        if (ec) {
            scheme = "file-rel";
        } else {
            rest = cwd.string() + "/" + rest;
        }
    }
    while (!rest.empty() && rest.front() == '/') rest.erase(0, 1);
    return scheme + "/" + rest;
}

// Slice 2c: read through the scan's own FileSystem (null = the default one), so a sidecar next to
// the lake data (gs://...) is opened by the same connector that opened the data file. A missing
// object is ABSENT; any other open / read failure is UNUSABLE with the status message, so the scan
// stays complete and the counter shows it.
RapIndex::Result RapIndex::load(FileSystem* fs, const std::string& path, const Identity& expect) {
    if (fs == nullptr) fs = FileSystem::Default();
    // slice 2e (deployed check D3-c): the HDFS-backed remote filesystems open LAZILY -- a missing object is not
    // reported by new_random_access_file but by the first size/read, as a plain IOError (fs_hdfs.cpp getSize ->
    // hdfsGetPathInfo). Existence is therefore asked first. path_exists returns a Status: OK = present; NotFound =
    // absent OR undeterminable (fs_hdfs collapses every non-zero hdfsExists into NotFound, so a denied or
    // unreachable sidecar looks like absence there -- the scan proceeds unindexed and the reason names the step);
    // anything else = UNUSABLE with the step named.
    Status exists = fs->path_exists(path);
    if (!exists.ok()) {
        if (exists.is_not_found()) {
            Result r;
            r.state = State::ABSENT;
            r.reason = "exists: NotFound(" + path + ")";
            return r;
        }
        return unusable("exists: " + std::string(exists.message()));
    }
    // slice 4 (PRD-02): the byte ceiling is applied BEFORE the open where the filesystem can report a size; a filesystem
    // that cannot (fs_hdfs, fs_s3: NotSupported) is bounded below, on the opened stream, before the payload is touched
    auto size_or = fs->get_file_size(path);
    if (size_or.ok() && static_cast<int64_t>(*size_or) > config::rap_index_max_sidecar_bytes) {
        return unusable("size " + std::to_string(*size_or) + " above rap_index_max_sidecar_bytes " +
                        std::to_string(config::rap_index_max_sidecar_bytes));
    }
    auto file_or = fs->new_random_access_file(path);
    if (!file_or.ok()) {
        // slice 2d: the HDFS-backed remote filesystems (gs://, hdfs://) report a missing object as
        // REMOTE_FILE_NOT_FOUND, not NOT_FOUND; both mean "no sidecar here" -- ABSENT, never UNUSABLE
        if (file_or.status().is_not_found() || file_or.status().code() == TStatusCode::REMOTE_FILE_NOT_FOUND) {
            Result r;
            r.state = State::ABSENT;
            return r;
        }
        return unusable("open: " + std::string(file_or.status().message()));
    }
    // slice 4 fix-up 2 (m38 review, PRD-02): the HDFS-backed (gs://, hdfs://) and S3 filesystems answer get_file_size
    // with NotSupported, so the ceiling above never fired for a remote sidecar and read_all() sized its allocation from
    // the stream. The ceiling is applied to the OPENED stream's size as well, before any payload byte is allocated or
    // read; a stream whose size cannot be established is refused (UNUSABLE, step named) rather than read blind; the
    // read itself is bounded to the checked size.
    auto ssize_or = (*file_or)->get_size();
    if (!ssize_or.ok()) return unusable("size: " + std::string(ssize_or.status().message()));
    if (*ssize_or < 0 || *ssize_or > config::rap_index_max_sidecar_bytes) {
        return unusable("size " + std::to_string(*ssize_or) + " above rap_index_max_sidecar_bytes " +
                        std::to_string(config::rap_index_max_sidecar_bytes));
    }
    std::string bytes(static_cast<size_t>(*ssize_or), '\0');
    Status rs = (*file_or)->read_at_fully(0, bytes.data(), static_cast<int64_t>(bytes.size()));
    if (!rs.ok()) return unusable("read: " + std::string(rs.message()));
    return parse(bytes, expect);
}

RapIndex::Result RapIndex::parse(const std::string& b, const Identity& expect) {
    // trailer: u32 crc32c over [0, len-8) then magic
    if (b.size() < 4 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 8 + 4 + 4) return unusable("truncated");
    if (std::memcmp(b.data(), kMagic, 4) != 0) return unusable("magic (leading)");
    if (std::memcmp(b.data() + b.size() - 4, kMagic, 4) != 0) return unusable("magic (trailing)");
    uint32_t stored_crc = 0;
    std::memcpy(&stored_crc, b.data() + b.size() - 8, 4);
    const uint32_t crc = starrocks::crc32c::Value(b.data(), b.size() - 8);
    if (crc != stored_crc) return unusable("crc32c");
    const size_t body_end = b.size() - 8;

    Cursor c(b);
    std::string magic;
    c.read_bytes(4, &magic);
    uint32_t version = 0;
    uint64_t file_size = 0, file_rows = 0;
    if (!c.read(&version) || !c.read(&file_size) || !c.read(&file_rows)) return unusable("truncated header");
    if (version != kVersion && version != kVersionV2) return unusable("version " + std::to_string(version));
    const bool v2 = version == kVersionV2;
    uint32_t n = 0;
    std::string name, column;
    if (!c.read(&n) || n > body_end - c.offset() || !c.read_bytes(n, &name)) return unusable("truncated name");
    if (!c.read(&n) || n > body_end - c.offset() || !c.read_bytes(n, &column)) return unusable("truncated column");
    int32_t field_id = -1;
    uint8_t key_type = static_cast<uint8_t>(KeyType::STRING);
    uint32_t granularity = 0, n_values = 0, n_null_ranges = 0;
    uint64_t postings_offset = 0;
    if (!c.read(&field_id)) return unusable("truncated header");
    if (v2 && !c.read(&key_type)) return unusable("truncated header");
    if (!c.read(&granularity) || !c.read(&n_values)) return unusable("truncated header");
    if (v2 && !c.read(&n_null_ranges)) return unusable("truncated header");
    if (!c.read(&postings_offset)) return unusable("truncated header");
    if (postings_offset != c.offset()) return unusable("postings_offset");
    if (v2 && !valid_key_type(key_type)) return unusable("key_type " + std::to_string(key_type));
    // slice 4 (PRD-02): declared counts are checked against the ceiling and against the bytes left BEFORE any reserve
    if (static_cast<int64_t>(n_values) > config::rap_index_max_values) {
        return unusable("declared values " + std::to_string(n_values) + " above rap_index_max_values " +
                        std::to_string(config::rap_index_max_values));
    }
    const size_t remaining = body_end - c.offset();
    if (n_values > remaining / kMinValueBytes || static_cast<size_t>(n_null_ranges) > remaining / kRangeBytes) {
        return unusable("declared count exceeds body");
    }

    // identity gate -- the whole point
    if (name != expect.file_name) return unusable("file_name mismatch: index '" + name + "' vs file '" + expect.file_name + "'");
    if (file_size != expect.file_size) {
        return unusable("file_size mismatch: index " + std::to_string(file_size) + " vs file " + std::to_string(expect.file_size));
    }
    if (file_rows != expect.file_rows) {
        return unusable("file_rows mismatch: index " + std::to_string(file_rows) + " vs file " + std::to_string(expect.file_rows));
    }
    if (column != expect.column) return unusable("column mismatch: index '" + column + "' vs predicate '" + expect.column + "'");
    if (expect.field_id >= 0 && field_id >= 0 && field_id != expect.field_id) {
        return unusable("field_id mismatch: index " + std::to_string(field_id) + " vs schema " + std::to_string(expect.field_id));
    }

    auto idx = std::make_unique<RapIndex>();
    idx->_identity = Identity{name, file_size, file_rows, column, field_id};
    idx->_version = version;
    idx->_key_type = static_cast<KeyType>(key_type);
    idx->_granularity_rows = granularity;
    idx->_values.reserve(n_values);
    idx->_ranges.reserve(n_values);
    std::string prev;
    std::string why;
    for (uint32_t i = 0; i < n_values; ++i) {
        uint32_t vlen = 0;
        std::string v;
        if (!c.read(&vlen) || vlen > body_end - c.offset() || !c.read_bytes(vlen, &v)) return unusable("truncated value");
        if (i > 0 && !(prev < v)) return unusable("values not sorted");
        prev = v;
        uint32_t nr = 0;
        if (!c.read(&nr)) return unusable("truncated ranges");
        std::vector<RowRangeHint> rs;
        if (!read_ranges(&c, nr, file_rows, body_end, &rs, &why)) return unusable(why);
        idx->_values.push_back(std::move(v));
        idx->_ranges.push_back(std::move(rs));
    }
    if (v2 && !read_ranges(&c, n_null_ranges, file_rows, body_end, &idx->_null_ranges, &why)) return unusable("null posting: " + why);
    if (c.offset() != body_end) return unusable("trailing bytes");
    Result r;
    r.state = State::READY;
    r.index = std::move(idx);
    return r;
}

void RapIndex::encode_int64(int64_t v, std::string* out) {
    // sign bit flipped, big-endian: bytewise order == signed numeric order
    const uint64_t u = static_cast<uint64_t>(v) ^ (1ULL << 63);
    for (int i = 7; i >= 0; --i) out->push_back(static_cast<char>((u >> (8 * i)) & 0xff));
}

bool RapIndex::encode_literal(KeyType kt, LogicalType lt, const Datum& d, std::string* out) {
    if (d.is_null()) return false;
    out->clear();
    switch (kt) {
    case KeyType::STRING:
        if (lt != TYPE_VARCHAR && lt != TYPE_CHAR) return false;
        *out = d.get_slice().to_string();
        return true;
    case KeyType::INT64: {
        int64_t v = 0;
        switch (lt) {
        case TYPE_TINYINT:
            v = d.get_int8();
            break;
        case TYPE_SMALLINT:
            v = d.get_int16();
            break;
        case TYPE_INT:
            v = d.get_int32();
            break;
        case TYPE_BIGINT:
            v = d.get_int64();
            break;
        default:
            return false;
        }
        encode_int64(v, out);
        return true;
    }
    case KeyType::BOOLEAN:
        if (lt != TYPE_BOOLEAN) return false;
        out->push_back(d.get_uint8() ? 1 : 0);
        return true;
    case KeyType::DATE:
        if (lt != TYPE_DATE) return false;
        encode_int64(static_cast<int64_t>(d.get_date().julian()), out);
        return true;
    case KeyType::DATETIME:
        if (lt != TYPE_DATETIME) return false;
        encode_int64(static_cast<int64_t>(d.get_timestamp().timestamp()), out);
        return true;
    }
    return false;
}

std::vector<RowRangeHint> RapIndex::merge(std::vector<RowRangeHint> all) {
    std::sort(all.begin(), all.end(), [](const RowRangeHint& a, const RowRangeHint& b) {
        return a.start_row < b.start_row || (a.start_row == b.start_row && a.end_row < b.end_row);
    });
    std::vector<RowRangeHint> out;
    for (const auto& r : all) {
        if (!out.empty() && r.start_row <= out.back().end_row) {
            out.back().end_row = std::max(out.back().end_row, r.end_row);
        } else {
            out.push_back(r);
        }
    }
    return out;
}

std::vector<RowRangeHint> RapIndex::lookup(const std::vector<std::string>& values) const {
    std::vector<RowRangeHint> all;
    for (const auto& v : values) {
        auto it = std::lower_bound(_values.begin(), _values.end(), v);
        if (it == _values.end() || *it != v) continue;
        const auto& rs = _ranges[it - _values.begin()];
        all.insert(all.end(), rs.begin(), rs.end());
    }
    return merge(std::move(all));
}

std::vector<RowRangeHint> RapIndex::lookup_range(const std::string* lower, bool lower_inclusive, const std::string* upper,
                                                 bool upper_inclusive) const {
    // the keys are sorted bytewise and, for a typed sidecar, bytewise order is the type's order (canonical encodings);
    // every key inside the interval contributes its ranges -- an under-selection here would lose rows, so the bounds
    // follow SQL exactly: [lower, upper] when both inclusive, (lower, upper) when neither
    auto lo = _values.begin();
    if (lower != nullptr) lo = lower_inclusive ? std::lower_bound(_values.begin(), _values.end(), *lower)
                                               : std::upper_bound(_values.begin(), _values.end(), *lower);
    auto hi = _values.end();
    if (upper != nullptr) hi = upper_inclusive ? std::upper_bound(_values.begin(), _values.end(), *upper)
                                               : std::lower_bound(_values.begin(), _values.end(), *upper);
    std::vector<RowRangeHint> all;
    for (auto it = lo; it < hi; ++it) {
        const auto& rs = _ranges[it - _values.begin()];
        all.insert(all.end(), rs.begin(), rs.end());
    }
    return merge(std::move(all));
}

std::vector<RowRangeHint> RapIndex::null_ranges() const {
    return merge(_null_ranges);
}

std::vector<RowRangeHint> RapIndex::not_null_ranges() const {
    std::vector<RowRangeHint> all;
    for (const auto& rs : _ranges) all.insert(all.end(), rs.begin(), rs.end());
    return merge(std::move(all));
}

std::vector<RowRangeHint> RapIndex::intersect(const std::vector<RowRangeHint>& a, const std::vector<RowRangeHint>& b) {
    std::vector<RowRangeHint> out;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        const int64_t s = std::max(a[i].start_row, b[j].start_row);
        const int64_t e = std::min(a[i].end_row, b[j].end_row);
        if (s < e) out.push_back(RowRangeHint{s, e});
        if (a[i].end_row <= b[j].end_row) {
            ++i;
        } else {
            ++j;
        }
    }
    return out;
}

} // namespace starrocks::parquet
