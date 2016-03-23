/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "options.hpp"

#include "proton/io/socket.hpp"
#include "proton/event.hpp"
#include "proton/handler.hpp"
#include "proton/link.hpp"
#include "proton/url.hpp"
#include "proton/value.hpp"

#include <iostream>
#include <map>

#include "../fake_cpp11.hpp"

class direct_recv : public proton::handler {
  private:
    uint64_t expected;
    uint64_t received;

  public:
    direct_recv(int c) : expected(c), received(0) {}

    void on_message(proton::event &e) override {
        proton::message& msg = e.message();
        if (msg.id().get<uint64_t>() < received)
            return; // ignore duplicate
        if (expected == 0 || received < expected) {
            std::cout << msg.body() << std::endl;
            received++;
        }
        if (received == expected) {
            e.receiver().close();
            e.connection().close();
        }
    }
};

int main(int argc, char **argv) {
    // Command line options
    std::string address("127.0.0.1:5672/examples");
    int message_count = 100;
    options opts(argc, argv);
    opts.add_value(address, 'a', "address", "listen and receive on URL", "URL");
    opts.add_value(message_count, 'm', "messages", "receive COUNT messages", "COUNT");
    try {
        opts.parse();
        proton::url url(address);
        proton::io::socket::listener listener(url.host(), url.port());
        std::cout << "direct_recv listening on " << url << std::endl;
        direct_recv handler(message_count);
        proton::io::socket::engine(listener.accept(), handler).run();
        return 0;
    } catch (const bad_option& e) {
        std::cout << opts << std::endl << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return 1;
}
