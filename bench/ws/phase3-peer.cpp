#include "App.h"
#include <array>
#include <fstream>
#include <iostream>

static uint64_t sent = 0, received = 0, count = 0;
static std::string report;
struct Data {};
using Socket = uWS::WebSocket<false, true, Data>;
static Socket *socket = nullptr;

static std::array<char, 256> payload(uint64_t sequence, unsigned direction) {
    std::array<char, 256> p{};
    p[0] = char(direction);
    for (unsigned i = 0; i < 8; ++i) p[1+i] = char(sequence >> (56-8*i));
    for (unsigned i = 9; i < p.size(); ++i)
        p[i] = char((sequence * 17 + i * 31 + direction * 7) & 255);
    return p;
}

[[noreturn]] static void finish(const char *failure) {
    std::ofstream out(report);
    out << "{\"sent\":" << sent << ",\"validated\":" << received
        << ",\"failure\":" << (failure ? std::string("\"") + failure + "\"" : "null") << "}";
    out.close();
    std::exit(failure ? 1 : 0);
}

static void produce() {
    if (!socket) return;
    for (unsigned turn = 0; turn < 256 && sent < count; ++turn) {
        if (socket->getBufferedAmount() + 270 > 2097152) break;
        auto p = payload(sent, 1);
        auto status = socket->send(std::string_view(p.data(), p.size()), uWS::BINARY, false);
        if (status == Socket::DROPPED) finish("unexpected rejection");
        ++sent;
        if (status == Socket::BACKPRESSURE) break;
    }
    if (sent == count && received == count && socket->getBufferedAmount() == 0) finish(nullptr);
}

int main(int argc, char **argv) {
    if (argc != 4) return 2;
    auto port = std::stoi(argv[1]); count = std::stoull(argv[2]); report = argv[3];
    uWS::App app;
    app.ws<Data>("/*", {
        .compression = uWS::DISABLED,
        .maxPayloadLength = 1048576,
        .idleTimeout = 0,
        .maxBackpressure = 2097152,
        .closeOnBackpressureLimit = false,
        .open = [](auto *ws) { if (socket) finish("second connection"); socket = ws; },
        .message = [](auto *, std::string_view bytes, uWS::OpCode opcode) {
            auto expected = payload(received, 0);
            if (received >= count || opcode != uWS::BINARY || bytes != std::string_view(expected.data(), expected.size()))
                finish("identity/order/payload");
            ++received;
        },
        .drain = [](auto *) { produce(); },
        .close = [](auto *, int, std::string_view) {
            socket = nullptr;
            if (sent != count || received != count) finish("premature close");
            finish(nullptr);
        }
    }).listen("127.0.0.1", port, [](auto *listener) {
        if (!listener) finish("listen");
        std::cout << "LISTENING" << std::endl;
    });
    auto *timer = us_create_timer(reinterpret_cast<us_loop_t *>(uWS::Loop::get()), 0, 0);
    us_timer_set(timer, [](us_timer_t *) { produce(); }, 1, 1);
    app.run();
    finish("event loop ended");
}
