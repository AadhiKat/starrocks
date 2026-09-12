// See rap_index.h. Format contract: harness/rap_index_build.py.
#include "formats/parquet/rap_index.h"

#include <algorithm>
#include <cstring>
#include <sstream>

#include "base/hash/crc32c.h"
#include "fs/fs.h"

namespace starrocks::parquet {

namespace {

constexpr char kMagic[4] = {'R', 'A', 'P', 'X'};

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

} // namespace

RapIndex::Result RapIndex::load(const std::string& path, const Identity& expect) {
    return load(nullptr, path, expect);
}

// Slice 2c: read through the scan's own FileSystem (null = the default one), so a sidecar next to
// the lake data (gs://...) is opened by the same connector that opened the data file. A missing
// object is ABSENT; any other open / read failure is UNUSABLE with the status message, so the scan
// stays complete and the counter shows it.
std::string RapIndex::cache_key(bool negative, const std::string& file_key, const std::string& column,
                                const std::string& generation, const std::string& directory) {
    auto field = [](const std::string& s) { return std::to_string(s.size()) + ":" + s; };
    std::string k = negative ? "rapneg|" : "rapidx|";
    k += field(file_key) + "|" + field(column) + "|" + field(generation);
    if (negative) k += "|" + field(directory); // a refusal is a fact about a directory; a READY index is not
    return k;
}

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
    auto bytes_or = (*file_or)->read_all();
    if (!bytes_or.ok()) return unusable("read: " + std::string(bytes_or.status().message()));
    return parse(*bytes_or, expect);
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

    Cursor c(b);
    std::string magic;
    c.read_bytes(4, &magic);
    uint32_t version = 0;
    uint64_t file_size = 0, file_rows = 0;
    if (!c.read(&version) || !c.read(&file_size) || !c.read(&file_rows)) return unusable("truncated header");
    if (version != kVersion) return unusable("version " + std::to_string(version));
    uint32_t n = 0;
    std::string name, column;
    if (!c.read(&n) || !c.read_bytes(n, &name)) return unusable("truncated name");
    if (!c.read(&n) || !c.read_bytes(n, &column)) return unusable("truncated column");
    int32_t field_id = -1;
    uint32_t granularity = 0, n_values = 0;
    uint64_t postings_offset = 0;
    if (!c.read(&field_id) || !c.read(&granularity) || !c.read(&n_values) || !c.read(&postings_offset)) {
        return unusable("truncated header");
    }
    if (postings_offset != c.offset()) return unusable("postings_offset");

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
    idx->_granularity_rows = granularity;
    idx->_values.reserve(n_values);
    idx->_ranges.reserve(n_values);
    std::string prev;
    for (uint32_t i = 0; i < n_values; ++i) {
        uint32_t vlen = 0;
        std::string v;
        if (!c.read(&vlen) || !c.read_bytes(vlen, &v)) return unusable("truncated value");
        if (i > 0 && !(prev < v)) return unusable("values not sorted");
        prev = v;
        uint32_t nr = 0;
        if (!c.read(&nr)) return unusable("truncated ranges");
        std::vector<RowRangeHint> rs;
        rs.reserve(nr);
        int64_t last_end = -1;
        for (uint32_t k = 0; k < nr; ++k) {
            int64_t s = 0, e = 0;
            if (!c.read(&s) || !c.read(&e)) return unusable("truncated range");
            if (s < 0 || s >= e || s < last_end) return unusable("malformed range");
            if (static_cast<uint64_t>(e) > file_rows) return unusable("range beyond file_rows");
            last_end = e;
            rs.push_back(RowRangeHint{s, e});
        }
        idx->_values.push_back(std::move(v));
        idx->_ranges.push_back(std::move(rs));
    }
    if (c.offset() != b.size() - 8) return unusable("trailing bytes");
    Result r;
    r.state = State::READY;
    r.index = std::move(idx);
    return r;
}

std::vector<RowRangeHint> RapIndex::lookup(const std::vector<std::string>& values) const {
    std::vector<RowRangeHint> all;
    for (const auto& v : values) {
        auto it = std::lower_bound(_values.begin(), _values.end(), v);
        if (it == _values.end() || *it != v) continue;
        const auto& rs = _ranges[it - _values.begin()];
        all.insert(all.end(), rs.begin(), rs.end());
    }
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
