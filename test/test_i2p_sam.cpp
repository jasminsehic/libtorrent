/*

Copyright (c) 2026, libtorrent contributors
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/session.hpp"
#include "libtorrent/settings_pack.hpp"
#include "test.hpp"
#include "settings.hpp"

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/write.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace lt;

#if TORRENT_USE_I2P

// A local SAM 3.1 bridge that closes the first incoming STREAM ACCEPT while
// leaving its control session established. The second accept stays open.
namespace {
class mock_sam_accept_bridge
{
public:
	mock_sam_accept_bridge()
		: m_acceptor(m_io, {boost::asio::ip::address_v4::loopback(), 0})
	{
		accept_next();
		m_worker = std::thread([this] { m_io.run(); });
	}

	~mock_sam_accept_bridge()
	{
		m_io.stop();
		m_worker.join();
	}

	int port() const { return m_acceptor.local_endpoint().port(); }
	int accept_count() const { return m_accept_count.load(); }

private:
	using tcp = boost::asio::ip::tcp;

	struct client : std::enable_shared_from_this<client>
	{
		explicit client(boost::asio::io_context& io, mock_sam_accept_bridge& bridge)
			: socket(io)
			, parent(bridge)
		{}

		void read()
		{
			auto self = shared_from_this();
			socket.async_read_some(
				boost::asio::buffer(&next_char, 1), [self](error_code const& ec, std::size_t n) {
					if (ec || n != 1)
						return;
					if (self->next_char == '\n')
					{
						std::string line = std::move(self->input);
						self->input.clear();
						self->on_line(line);
					}
					else if (self->input.size() < 4096)
					{
						self->input.push_back(self->next_char);
						self->read();
					}
				});
		}

		void reply(std::string response, bool const close_after = false)
		{
			auto self = shared_from_this();
			auto message = std::make_shared<std::string>(std::move(response));
			boost::asio::async_write(socket,
				boost::asio::buffer(*message),
				[self, message, close_after](error_code const& ec, std::size_t) {
					if (ec)
						return;
					if (close_after)
					{
						error_code ignored;
						self->socket.close(ignored);
					}
					else
						self->read();
				});
		}

		void on_line(std::string const& line)
		{
			if (line.compare(0, 6, "HELLO ") == 0)
				reply("HELLO REPLY RESULT=OK VERSION=3.1\n");
			else if (line.compare(0, 15, "SESSION CREATE ") == 0)
				reply("SESSION STATUS RESULT=OK DESTINATION=TRANSIENT\n");
			else if (line.compare(0, 14, "NAMING LOOKUP ") == 0)
				reply("NAMING REPLY RESULT=OK NAME=ME VALUE=testdestination\n");
			else if (line.compare(0, 14, "STREAM ACCEPT ") == 0)
			{
				int const count = ++parent.m_accept_count;
				reply("STREAM STATUS RESULT=OK\n", count == 1);
			}
		}

		tcp::socket socket;
		std::string input;
		char next_char = 0;
		mock_sam_accept_bridge& parent;
	};

	void accept_next()
	{
		auto c = std::make_shared<client>(m_io, *this);
		m_clients.push_back(c);
		m_acceptor.async_accept(c->socket, [this, c](error_code const& ec) {
			if (ec)
				return;
			c->read();
			accept_next();
		});
	}

	boost::asio::io_context m_io;
	tcp::acceptor m_acceptor;
	std::thread m_worker;
	std::atomic<int> m_accept_count{0};
	std::vector<std::shared_ptr<client>> m_clients;

public:
	// Close the SAM side of outstanding STREAM ACCEPT reads before the
	// libtorrent session shuts down. Keep all socket operations on the
	// mock bridge's io_context thread.
	void close_connections()
	{
		std::promise<void> completed;
		auto ready = completed.get_future();
		boost::asio::post(m_io, [this, &completed] {
			error_code ignored;
			m_acceptor.close(ignored);
			for (auto const& c : m_clients)
				c->socket.close(ignored);
			completed.set_value();
		});
		ready.wait();
	}
};
}
#endif // TORRENT_USE_I2P

TORRENT_TEST(i2p_acceptor_recovers_after_eof)
{
#if TORRENT_USE_I2P
	mock_sam_accept_bridge bridge;
	settings_pack p = settings();
	p.set_bool(settings_pack::enable_dht, false);
	p.set_str(settings_pack::i2p_hostname, "127.0.0.1");
	p.set_int(settings_pack::i2p_port, bridge.port());

	lt::session ses(p);
	// The SAM control session stays connected throughout. Verify that a
	// failed incoming accept triggers a new STREAM ACCEPT without a toggle.
	for (int i = 0; i < 200 && bridge.accept_count() < 2; ++i)
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	TEST_CHECK(bridge.accept_count() >= 2);
	// The mock must not leave an I2P accept socket waiting for data while
	// the session destructor attempts to cancel it (notably on Windows).
	bridge.close_connections();
#endif
}
