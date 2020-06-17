#include "executor.h"
#include "error.h"
#include "client.pb.h"

#include "proto_bus.h"

//#include <spdlog/spdlog.h>

#include <json/reader.h>
#include <fstream>

#define ensure(condition) if (!(condition)) { throw std::logic_error("condition not met " #condition); }
#define verify(condition) if (!(condition)) { std::cerr << ("condition not met " #condition) << std::endl; std::terminate(); }

using duration = std::chrono::system_clock::duration;

duration parse_duration(const Json::Value& val) {
    ensure(!val.isNull());
    return std::chrono::duration_cast<duration>(std::chrono::duration<double>(val.asFloat()));
}

class Client : bus::ProtoBus {
private:
    enum {
        kClientReq = 3,
    };
public:
    Client(bus::ProtoBus::Options opts, bus::EndpointManager& manager, size_t members, duration timeout)
        : ProtoBus(opts, manager)
        , timeout_(timeout)
    {
        start();
    }

    bus::Future<ClientResponse> execute(ClientRequest req) {
        return bound_execute(req, leader_.load())
            .chain([=] (ClientResponse& resp) {
                    if (resp.should_retry()) {
                        leader_.store(resp.retry_to());
                        return bound_execute(req, resp.retry_to());
                    } else {
                        return bus::make_future(std::move(resp));
                    }
                });
    }

    bus::ErrorT<std::string> lookup(std::string key) {
        ClientRequest req;
        auto* op = req.add_operations();
        op->set_type(ClientRequest::Operation::READ);
        op->set_key(key);
        ClientResponse response = execute(req).wait();
        if (response.success() && response.entries_size() == 1) {
            return bus::ErrorT<std::string>::value(response.entries()[0].value());
        } else {
            return bus::ErrorT<std::string>::error("fetch failed");
        }
    }

    bus::Future<bool> async_write(std::string key, std::string value) {
        ClientRequest req;
        auto* op = req.add_operations();
        op->set_type(ClientRequest::Operation::WRITE);
        op->set_key(std::move(key));
        op->set_value(std::move(value));
        return execute(std::move(req)).map([](auto& err) { return err.success(); });
    }

    bool write(std::string key, std::string value) {
        return async_write(std::move(key), std::move(value)).wait();
    }

private:
    bus::Future<ClientResponse> bound_execute(ClientRequest req, size_t member) {
        return send<ClientRequest, ClientResponse>(req, member, kClientReq, timeout_)
            .map([](bus::ErrorT<ClientResponse>& resp) {
                    if (resp) {
                        return resp.unwrap();
                    } else {
                        ClientResponse response;
                        response.set_success(false);
                        return response;
                    }
                });
    }

private:
    duration timeout_;
    std::atomic<size_t> leader_ = 0;
};

template<typename F>
std::chrono::steady_clock::duration measure(F&& f, size_t repeats = 1) {
    auto pt = std::chrono::steady_clock::now();
    for (size_t i = 0; i < repeats; ++i) {
        f();
    }
    return (std::chrono::steady_clock::now() - pt) / repeats;
}

void print_statistics(std::vector<std::chrono::steady_clock::duration>& times, std::string header) {
    std::cout << "stats for " << header << std::endl;
    std::chrono::steady_clock::duration sum = std::chrono::steady_clock::duration::zero();
    std::sort(times.begin(), times.end());
    for (auto time : times) {
        sum += time;
    }
    std::cout << "avg " << std::chrono::duration_cast<std::chrono::nanoseconds>(sum/times.size()).count() << "ns" << std::endl;
    std::cout << "min " << std::chrono::duration_cast<std::chrono::nanoseconds>(times[0]).count() << "ns" << std::endl;
    std::cout << "max " << std::chrono::duration_cast<std::chrono::nanoseconds>(times.back()).count() << "ns" << std::endl;
    ssize_t q50 = std::min<ssize_t>(times.size() * 0.5, ssize_t(times.size()) - 1);
    std::cout << "q50 " << std::chrono::duration_cast<std::chrono::nanoseconds>(times[q50]).count() << "ns" << std::endl;
    ssize_t q90 = std::min<ssize_t>(times.size() * 0.9, ssize_t(times.size()) - 1);
    std::cout << "q90 " << std::chrono::duration_cast<std::chrono::nanoseconds>(times[q90]).count() << "ns" << std::endl;
}

void basic_workload(Client& client) {
    ensure(client.write("key", "value"));
    ensure(client.lookup("key").unwrap() == "value");
}

size_t maxinflight = 20;
void parallel_workload(Client& client) {
    constexpr size_t repeats = 5000;
    constexpr size_t mod = 10;
    bus::internal::Event event;
    bus::internal::ExclusiveWrapper<std::vector<std::chrono::steady_clock::duration>> times;
    std::atomic_uint64_t inflight = 0;
    for (size_t i = 0; i < repeats; ++i) {
        event.reset();
        std::string key = std::to_string(i % mod);
        std::string value = std::to_string(2 * i);
        auto pt = std::chrono::steady_clock::now();

        event.reset();
        while (inflight.load() >= maxinflight) {
            event.wait();
            event.reset();
        }

        inflight.fetch_add(1);
        client.async_write(std::move(key), std::move(value))
            .subscribe([&, pt] (auto& resp) {
                inflight.fetch_sub(1);
                auto time = std::chrono::steady_clock::now() - pt;
                verify(resp);
                event.notify();
                times.get()->push_back(time);
            });
    }
    event.reset();
    while (inflight > 0) {
        event.wait();
        event.reset();
    }

    print_statistics(*times.get(), "writes");
}

void counter(Client& client) {
    constexpr size_t repeats = 5000;
    for (size_t i = 0; i < repeats; ++i) {
        client.write("counter", std::to_string(i));
    }
}

void one_thread_latency(Client& client) {
    constexpr size_t N = 100;
    constexpr size_t mod = 10;
    std::vector<std::chrono::steady_clock::duration> writes, reads;
    for (size_t i = 0; i < N; ++i) {
        std::string key = std::to_string(i % mod);
        std::string value = std::to_string(2 * i);
        writes.push_back(measure([&] { ensure(client.write(key, value));}));
        std::cerr << std::chrono::duration_cast<std::chrono::microseconds>(writes.back()).count() << " taken client write" << std::endl;
    }
    std::cerr << "checking values" << std::endl;
    for (size_t i = N - mod; i < N; ++i) {
        std::string key = std::to_string(i % mod);
        std::string value = std::to_string(2 * i);
        reads.push_back(measure([&] { ensure(client.lookup(key).unwrap() == value); }));
    }
    print_statistics(writes, "writes");
    print_statistics(reads, "reads");
}

int main(int argc, char** argv) {
    ensure(argc == 2);
    Json::Value conf;
    std::ifstream(argv[1]) >> conf;

    bus::ProtoBus::Options opts;
    opts.batch_opts.max_batch = conf["max_batch"].asInt();
    opts.batch_opts.max_delay = parse_duration(conf["max_delay"]);
    opts.greeter = std::nullopt;
    opts.tcp_opts.port = conf["port"].asInt();
    opts.tcp_opts.fixed_pool_size = conf["pool_size"].asUInt64();
    opts.tcp_opts.max_message_size = conf["max_message"].asUInt64();

    bus::EndpointManager manager;
    auto members = conf["members"];
    for (size_t i = 0; i < members.size(); ++i) {
        auto member = members[Json::ArrayIndex(i)];
        manager.merge_to_endpoint(member["host"].asString(), member["port"].asInt(), i);
    }

    Client client(opts, manager, members.size(), parse_duration(conf["timeout"]));

    std::map<std::string, void(*)(Client&)> workloads;
    workloads["basic"] = &basic_workload;
    workloads["one_thread"] = &one_thread_latency;
    workloads["parallel"] = &parallel_workload;
    workloads["counter"] = &counter;

    workloads[conf["workload"].asString()](client);
}
