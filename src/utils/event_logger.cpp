#include "utils/event_logger.h"

#include <chrono>
#include <cstdlib>
#include <cctype>
#include <cerrno>
#include <fcntl.h>
#include <initializer_list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <unistd.h>

namespace faas {
namespace utils {
namespace {

std::once_flag init_once;
std::mutex write_mu;
bool enabled = false;
std::string event_path;
std::string run_id;
std::unordered_set<std::string> allowed_events;

constexpr std::string_view kDefaultAllowedEvents =
    "invoke_end,state_read,state_write,txn_retry,txn_conflict,worker_start,worker_ready,scale_up_start,scale_up_end,scale_down_start,scale_down_end,placement_change";

std::string Trim(const std::string& value) {
    size_t begin = 0;
    while (begin < value.size()
           && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
        begin += 1;
    }
    size_t end = value.size();
    while (end > begin
           && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        end -= 1;
    }
    return value.substr(begin, end - begin);
}

std::string GetEnvFirstNonEmpty(std::initializer_list<const char*> names) {
    for (const char* name : names) {
        const char* value = std::getenv(name);
        if (value != nullptr) {
            std::string trimmed = Trim(value);
            if (!trimmed.empty()) {
                return trimmed;
            }
        }
    }
    return "";
}

void ParseAllowedEvents(const std::string& raw_value) {
    std::string raw = Trim(raw_value);
    if (raw.empty()) {
        raw = std::string(kDefaultAllowedEvents);
    }
    size_t start = 0;
    while (start <= raw.size()) {
        size_t end = raw.find(',', start);
        if (end == std::string::npos) {
            end = raw.size();
        }
        std::string token = Trim(raw.substr(start, end - start));
        if (!token.empty()) {
            allowed_events.insert(token);
        }
        if (end == raw.size()) {
            break;
        }
        start = end + 1;
    }
}

bool IsReservedField(std::string_view key) {
    return key == "schema_version"
        || key == "run_id"
        || key == "system"
        || key == "ts_ms"
        || key == "event_type"
        || key == "ok";
}

void Init() {
    event_path = GetEnvFirstNonEmpty({"BOKI_EVENTS_JSONL_PATH", "BOKI_EVENTS_JSONL"});
    run_id = GetEnvFirstNonEmpty({"BOKI_RUN_ID", "RUN_ID"});
    ParseAllowedEvents(GetEnvFirstNonEmpty({"BOKI_EVENT_TYPES"}));
    if (event_path.empty() || run_id.empty() || allowed_events.empty()) {
        enabled = false;
        return;
    }
    enabled = true;
}

int64_t NowTimestampMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

void EmitRuntimeEvent(std::string_view event_type, bool ok, const nlohmann::json& fields) {
    std::call_once(init_once, Init);
    if (!enabled) {
        return;
    }
    std::string type_str = Trim(std::string(event_type));
    if (type_str.empty() || allowed_events.find(type_str) == allowed_events.end()) {
        return;
    }

    nlohmann::json event = {
        {"schema_version", "1.0"},
        {"run_id", run_id},
        {"system", "boki"},
        {"ts_ms", NowTimestampMs()},
        {"event_type", type_str},
        {"ok", ok}
    };
    if (fields.is_object()) {
        for (const auto& [k, v] : fields.items()) {
            if (IsReservedField(k)) {
                continue;
            }
            event[k] = v;
        }
    }
    if (!event.contains("attributes")) {
        event["attributes"] = nlohmann::json::object();
    }

    std::string line = event.dump();
    line.push_back('\n');

    std::lock_guard<std::mutex> lk(write_mu);
    int fd = ::open(event_path.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        return;
    }
    const char* data = line.data();
    size_t remaining = line.size();
    while (remaining > 0) {
        ssize_t written = ::write(fd, data, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (written == 0) {
            break;
        }
        data += written;
        remaining -= static_cast<size_t>(written);
    }
    ::close(fd);
}

}  // namespace utils
}  // namespace faas
