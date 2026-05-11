#include <mqtt/async_client.h>

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <cstring>
#include <cmath>

using namespace std::chrono;

struct Config {
    std::string broker = "tcp://localhost:1883";
    std::string topic = "/test";
    int qos = 1;
    int size = 100;
    int count = 100;
    int clients = 10;
    int wait_ms = 60000;
    int ramp_up = 0;
    int msg_interval = 0;
    bool quiet = false;
    int warmup_count = 10;  // Warmup messages
};

struct RunResults {
    int id = 0;
    int64_t successes = 0;
    int64_t failures = 0;

    double runtime = 0;

    double min = 0;
    double max = 0;
    double mean = 0;
    double stddev = 0;

    double msgs_per_sec = 0;
};

struct TotalResults {
    int64_t successes = 0;
    int64_t failures = 0;

    double total_runtime = 0;
    double avg_runtime = 0;

    double min = 0;
    double max = 0;
    double mean_avg = 0;
    double mean_std = 0;

    double total_mps = 0;
    double avg_mps = 0;
};

Config parse_args(int argc, char** argv) {
    Config c;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];

        auto next = [&](int& i) { return std::string(argv[++i]); };

        if (a == "--broker") c.broker = next(i);
        else if (a == "--topic") c.topic = next(i);
        else if (a == "--qos") c.qos = std::stoi(next(i));
        else if (a == "--size") c.size = std::stoi(next(i));
        else if (a == "--count") c.count = std::stoi(next(i));
        else if (a == "--clients") c.clients = std::stoi(next(i));
        else if (a == "--wait") c.wait_ms = std::stoi(next(i));
        else if (a == "--ramp-up-time") c.ramp_up = std::stoi(next(i));
        else if (a == "--message-interval") c.msg_interval = std::stoi(next(i));
        else if (a == "--quiet") c.quiet = true;
        else if (a == "--warmup") c.warmup_count = std::stoi(next(i));
    }

    return c;
}

double mean(const std::vector<double>& v) {
    if (v.empty()) return 0.0;
    return std::accumulate(v.begin(), v.end(), 0.0) / v.size();
}

double stddev(const std::vector<double>& v, double m) {
    if (v.size() <= 1) return 0.0;
    double acc = 0;
    for (auto x : v)
        acc += (x - m) * (x - m);
    return std::sqrt(acc / (v.size() - 1));
}

RunResults run_client(int id, const Config& cfg) {
    RunResults res;
    res.id = id;

    mqtt::async_client client(cfg.broker, "bench-" + std::to_string(id));

    mqtt::connect_options opts;
    opts.set_clean_start(true);
    opts.set_mqtt_version(MQTTVERSION_5);

    client.connect(opts)->wait();

    // Warmup phase - exclude from timing
    std::string payload(cfg.size, 'x');
    for (int i = 0; i < cfg.warmup_count; i++) {
        auto msg = mqtt::make_message(cfg.topic, payload);
        msg->set_qos(cfg.qos);
        client.publish(msg)->wait_for(milliseconds(cfg.wait_ms));
    }

    std::vector<double> times;
    times.reserve(cfg.count);  // Pre-allocate memory

    auto start = high_resolution_clock::now();

    for (int i = 0; i < cfg.count; i++) {
        auto t0 = high_resolution_clock::now();

        auto msg = mqtt::make_message(cfg.topic, payload);
        msg->set_qos(cfg.qos);

        auto tok = client.publish(msg);

        bool ok = tok->wait_for(milliseconds(cfg.wait_ms));

        if (!ok || tok->get_return_code() != mqtt::SUCCESS) {
            res.failures++;
        } else {
            auto t1 = high_resolution_clock::now();
            res.successes++;

            double ms = duration<double, std::milli>(t1 - t0).count();
            times.push_back(ms);
        }

        if (cfg.msg_interval > 0)
            std::this_thread::sleep_for(seconds(cfg.msg_interval));
    }

    auto end = high_resolution_clock::now();
    client.disconnect()->wait();

    res.runtime = duration<double>(end - start).count();

    if (!times.empty()) {
        std::sort(times.begin(), times.end());

        res.min = times.front();
        res.max = times.back();
        res.mean = mean(times);

        if (times.size() > 1)
            res.stddev = stddev(times, res.mean);
    }

    // Calculate rate based on actual measurement time
    res.msgs_per_sec = res.successes / res.runtime;

    return res;
}

TotalResults aggregate(const std::vector<RunResults>& r, double total_time) {
    TotalResults t;

    std::vector<double> means, mps, runtimes;

    // Initialize with extreme values
    t.min = std::numeric_limits<double>::max();
    t.max = 0;

    for (auto& x : r) {
        t.successes += x.successes;
        t.failures += x.failures;
        t.total_mps += x.msgs_per_sec;

        if (x.min < t.min) t.min = x.min;
        if (x.max > t.max) t.max = x.max;

        means.push_back(x.mean);
        mps.push_back(x.msgs_per_sec);
        runtimes.push_back(x.runtime);
    }

    t.total_runtime = total_time;
    t.avg_runtime = mean(runtimes);
    t.avg_mps = mean(mps);
    t.mean_avg = mean(means);

    if (means.size() > 1)
        t.mean_std = stddev(means, t.mean_avg);

    return t;
}

void print(const std::vector<RunResults>& r, const TotalResults& t) {
    for (auto& x : r) {
        std::cout << "======= CLIENT " << x.id << " =======\n";
        double total_attempts = x.successes + x.failures;
        std::cout << "Ratio: " << (total_attempts > 0 ? (double)x.successes / total_attempts : 0.0) << "\n";
        std::cout << "Runtime (s): " << x.runtime << "\n";
        std::cout << "Min (ms): " << x.min << "\n";
        std::cout << "Max (ms): " << x.max << "\n";
        std::cout << "Mean (ms): " << x.mean << "\n";
        std::cout << "Std (ms): " << x.stddev << "\n";
        std::cout << "Msg/s: " << x.msgs_per_sec << "\n\n";
    }

    std::cout << "========= TOTAL =========\n";
    std::cout << "Total runtime: " << t.total_runtime << "\n";
    std::cout << "Avg runtime: " << t.avg_runtime << "\n";
    std::cout << "Min: " << t.min << "\n";
    std::cout << "Max: " << t.max << "\n";
    std::cout << "Mean avg: " << t.mean_avg << "\n";
    std::cout << "Mean std: " << t.mean_std << "\n";
    std::cout << "Avg msg/s: " << t.avg_mps << "\n";
    std::cout << "Total msg/s: " << t.total_mps << "\n";
}

int main(int argc, char** argv) {
    Config cfg = parse_args(argc, argv);

    if (cfg.clients < 1 || cfg.count < 1) {
        std::cerr << "Invalid args\n";
        return 1;
    }

    std::vector<std::thread> threads;
    std::vector<RunResults> results(cfg.clients);

    auto start = high_resolution_clock::now();

    // Start all threads simultaneously for fair comparison
    for (int i = 0; i < cfg.clients; i++) {
        threads.emplace_back([&, i]() {
            results[i] = run_client(i, cfg);
        });
    }

    for (auto& t : threads) t.join();

    auto end = high_resolution_clock::now();

    double total_time = duration<double>(end - start).count();

    auto totals = aggregate(results, total_time);

    print(results, totals);
}