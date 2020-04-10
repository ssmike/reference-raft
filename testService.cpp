#include "proto_bus.h"
#include "bus.h"
#include "util.h"

#include "messages.pb.h"


using namespace bus;

class SimpleService: ProtoBus {
public:
    SimpleService(EndpointManager& manager, BufferPool& pool, int port, bool receiver)
        : ProtoBus(TcpBus::Options{.port = port, .fixed_pool_size = 2}, manager, pool)
    {
        if (receiver) {
            register_handler<Operation, Operation>(1, [&](Operation op) -> Future<Operation> {
                op.set_key(op.key() + " - mirrored");
                return make_future(std::move(op));
            });
        }
    }

    void execute(int endpoint) {
        Operation op;
        op.set_key("key");
        op.set_data("value");

        send<Operation, Operation>(op, endpoint, 1, std::chrono::duration<double>(1))
            .subscribe([=](ErrorT<Operation>& op) {
                    assert(op);
                    assert(op.unwrap().key() == "key - mirrored");
                    assert(op.unwrap().data() == "data");
                });
    }

private:
};

int main() {
    BufferPool bufferPool{4098};
    EndpointManager manager;

    SimpleService second(manager, bufferPool, 4002, false);
    SimpleService first(manager, bufferPool, 4003, true);
    int receiver = manager.register_endpoint("::1", 4003);
    second.execute(receiver);
}
