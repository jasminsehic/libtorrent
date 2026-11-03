/*

Copyright (c) 2026, Arvid Norberg
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"

#if TORRENT_USE_I2P

#include "libtorrent/i2p_stream.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/socket.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/span.hpp"
#include "libtorrent/session.hpp"
#include "libtorrent/settings_pack.hpp"

#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/write.hpp>

#include <array>
#include <chrono>
#include <functional>
#include <istream>
#include <memory>
#include <string>
#include <vector>

using namespace lt;
using namespace std::chrono_literals;

namespace {

struct lookup_result
{
	bool called = false;
	error_code ec;
};

auto record(std::shared_ptr<lookup_result> r)
{
	return [r](error_code const& ec, char const*) {
		r->called = true;
		r->ec = ec;
	};
}

// a hang must fail the test, not time out the whole test run
void run_to_completion(io_context& ios)
{
	ios.restart();
	ios.run_for(10s);
	TEST_CHECK(ios.stopped());
}

void run_until(io_context& ios, bool const& done)
{
	auto const deadline = std::chrono::steady_clock::now() + 10s;
	while (!done && ios.run_one_until(deadline))
	{}
	TEST_CHECK(done);
}

}

TORRENT_TEST(name_lookup_without_session)
{
	io_context ios;
	i2p_connection conn(ios);

	auto const r = std::make_shared<lookup_result>();
	conn.async_name_lookup("foo.i2p", record(r));
	TEST_CHECK(!r->called);
	run_to_completion(ios);
	TEST_CHECK(r->called);
	TEST_EQUAL(r->ec, error_code(errors::no_i2p_router));
	TEST_EQUAL(r.use_count(), 1);
}

TORRENT_TEST(close_aborts_queued_name_lookups)
{
	io_context ios;

	// a SAM bridge that never answers keeps the session connecting
	tcp::acceptor sam(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam_conn(ios);
	bool accepted = false;
	sam.async_accept(sam_conn, [&](error_code const& ec) { accepted = !ec; });

	i2p_connection conn(ios);
	error_code open_ec;
	conn.open(
		"127.0.0.1", sam.local_endpoint().port(), i2p_session_options{}, [&](error_code const& ec) {
			open_ec = ec;
		});

	auto const r1 = std::make_shared<lookup_result>();
	auto const r2 = std::make_shared<lookup_result>();
	conn.async_name_lookup("foo.i2p", record(r1));
	conn.async_name_lookup("bar.i2p", record(r2));

	run_until(ios, accepted);
	ios.run_for(200ms);
	TEST_CHECK(!conn.is_open());
	TEST_CHECK(!r1->called);
	TEST_CHECK(!r2->called);

	error_code ec;
	conn.close(ec);
	sam_conn.close(ec);
	run_to_completion(ios);

	TEST_CHECK(r1->called);
	TEST_CHECK(r2->called);
	TEST_EQUAL(r1->ec, error_code(boost::asio::error::operation_aborted));
	TEST_EQUAL(r2->ec, error_code(boost::asio::error::operation_aborted));
	TEST_EQUAL(open_ec, error_code(boost::asio::error::operation_aborted));
	TEST_EQUAL(r1.use_count(), 1);
	TEST_EQUAL(r2.use_count(), 1);

	// nothing would drain the queue after close()
	auto const r3 = std::make_shared<lookup_result>();
	conn.async_name_lookup("baz.i2p", record(r3));
	run_to_completion(ios);
	TEST_CHECK(r3->called);
	TEST_EQUAL(r3->ec, error_code(errors::no_i2p_router));
}

TORRENT_TEST(failed_sam_connect_aborts_queued_name_lookups)
{
	io_context ios;

	tcp::acceptor sam(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam_conn(ios);
	sam.async_accept(sam_conn, [&](error_code const&) {
		error_code ignore;
		sam_conn.close(ignore);
	});

	i2p_connection conn(ios);
	error_code open_ec;
	conn.open(
		"127.0.0.1", sam.local_endpoint().port(), i2p_session_options{}, [&](error_code const& ec) {
			open_ec = ec;
		});

	auto const r1 = std::make_shared<lookup_result>();
	auto const r2 = std::make_shared<lookup_result>();
	conn.async_name_lookup("foo.i2p", record(r1));
	conn.async_name_lookup("bar.i2p", record(r2));

	run_to_completion(ios);

	TEST_CHECK(open_ec);
	TEST_CHECK(!conn.is_open());
	TEST_CHECK(r1->called);
	TEST_CHECK(r2->called);
	TEST_EQUAL(r1->ec, open_ec);
	TEST_EQUAL(r2->ec, open_ec);
	TEST_EQUAL(r1.use_count(), 1);
	TEST_EQUAL(r2.use_count(), 1);
}

TORRENT_TEST(open_closes_replaced_session)
{
	io_context ios;

	// the first SAM bridge never answers, reading is only to detect the
	// client closing the connection
	tcp::acceptor sam1(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam1_conn(ios);
	std::array<char, 1024> buf;
	bool sam1_closed = false;
	std::function<void(error_code const&, std::size_t)> const drain = [&](error_code const& ec,
																		  std::size_t) {
		if (ec)
		{
			sam1_closed = true;
			return;
		}
		sam1_conn.async_read_some(boost::asio::buffer(buf), drain);
	};
	bool accepted = false;
	sam1.async_accept(sam1_conn, [&](error_code const& ec) {
		accepted = !ec;
		if (accepted)
			drain({}, 0);
	});

	i2p_connection conn(ios);
	error_code open1_ec;
	conn.open("127.0.0.1",
		sam1.local_endpoint().port(),
		i2p_session_options{},
		[&](error_code const& ec) { open1_ec = ec; });
	run_until(ios, accepted);

	tcp::acceptor sam2(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam2_conn(ios);
	sam2.async_accept(sam2_conn, [&](error_code const&) {
		error_code ignore;
		sam2_conn.close(ignore);
	});

	error_code open2_ec;
	conn.open("127.0.0.1",
		sam2.local_endpoint().port(),
		i2p_session_options{},
		[&](error_code const& ec) { open2_ec = ec; });

	run_to_completion(ios);

	TEST_EQUAL(open1_ec, error_code(boost::asio::error::operation_aborted));
	TEST_CHECK(open2_ec);
	TEST_CHECK(open2_ec != error_code(boost::asio::error::operation_aborted));
	TEST_CHECK(sam1_closed);
}

TORRENT_TEST(sam_error_reply_drops_session)
{
	io_context ios;

	// a SAM bridge that rejects HELLO but leaves its end of the connection
	// open, reading on is only to detect the client closing it
	tcp::acceptor sam(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam_conn(ios);
	std::array<char, 1024> buf;
	int accepted = 0;
	int closed = 0;
	std::function<void(error_code const&, std::size_t)> const drain = [&](error_code const& ec,
																		  std::size_t) {
		if (ec)
		{
			++closed;
			error_code ignore;
			sam_conn.close(ignore);
			return;
		}
		sam_conn.async_read_some(boost::asio::buffer(buf), drain);
	};
	std::string const reply = "HELLO REPLY RESULT=I2P_ERROR\n";
	auto const serve = [&] {
		sam.async_accept(sam_conn, [&](error_code const& ec) {
			if (ec)
				return;
			++accepted;
			// the client's HELLO is a single short write
			sam_conn.async_read_some(
				boost::asio::buffer(buf), [&](error_code const& e, std::size_t) {
					if (e)
						return drain(e, 0);
					boost::asio::async_write(sam_conn, boost::asio::buffer(reply), drain);
				});
		});
	};
	serve();

	i2p_connection conn(ios);
	error_code open_ec;
	auto const open = [&] {
		conn.open("127.0.0.1",
			sam.local_endpoint().port(),
			i2p_session_options{},
			[&](error_code const& ec) { open_ec = ec; });
	};
	open();

	auto const r1 = std::make_shared<lookup_result>();
	conn.async_name_lookup("foo.i2p", record(r1));

	run_to_completion(ios);

	error_code const sam_error(i2p_error::i2p_error, i2p_category());
	TEST_EQUAL(open_ec, sam_error);
	TEST_EQUAL(accepted, 1);
	TEST_EQUAL(closed, 1);
	TEST_CHECK(!conn.is_open());
	TEST_CHECK(r1->called);
	TEST_EQUAL(r1->ec, sam_error);
	TEST_EQUAL(r1.use_count(), 1);

	auto const r2 = std::make_shared<lookup_result>();
	conn.async_name_lookup("bar.i2p", record(r2));
	run_to_completion(ios);
	TEST_CHECK(r2->called);
	TEST_EQUAL(r2->ec, error_code(errors::no_i2p_router));

	// the same SAM bridge is retried
	open_ec.clear();
	serve();
	open();
	run_to_completion(ios);
	TEST_EQUAL(accepted, 2);
	TEST_EQUAL(closed, 2);
	TEST_EQUAL(open_ec, sam_error);
}

TORRENT_TEST(closed_sam_socket_fails_name_lookups)
{
	io_context ios;

	// a SAM bridge that sets up the session, then drops the connection on the
	// first name lookup
	tcp::acceptor sam(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam_conn(ios);
	std::array<char, 1024> buf;
	std::array<std::string, 3> const replies{{"HELLO REPLY RESULT=OK\n",
		"SESSION STATUS RESULT=OK\n",
		"NAMING REPLY RESULT=OK NAME=ME VALUE=local-dest\n"}};
	std::size_t next_reply = 0;
	std::string pending;
	std::function<void(error_code const&, std::size_t)> const serve = [&](error_code const& ec,
																		  std::size_t const len) {
		if (ec)
			return;
		pending.clear();
		for (char const c : span<char const>(buf.data(), int(len)))
		{
			if (c != '\n')
				continue;
			if (next_reply == replies.size())
			{
				error_code ignore;
				sam_conn.close(ignore);
				return;
			}
			pending += replies[next_reply++];
		}
		if (pending.empty())
		{
			sam_conn.async_read_some(boost::asio::buffer(buf), serve);
			return;
		}
		boost::asio::async_write(
			sam_conn, boost::asio::buffer(pending), [&](error_code const& e, std::size_t) {
				if (e)
					return serve(e, 0);
				sam_conn.async_read_some(boost::asio::buffer(buf), serve);
			});
	};
	sam.async_accept(sam_conn, [&](error_code const& ec) {
		if (ec)
			return;
		sam_conn.async_read_some(boost::asio::buffer(buf), serve);
	});

	i2p_connection conn(ios);
	error_code open_ec;
	conn.open(
		"127.0.0.1", sam.local_endpoint().port(), i2p_session_options{}, [&](error_code const& ec) {
			open_ec = ec;
		});

	auto const r1 = std::make_shared<lookup_result>();
	auto const r2 = std::make_shared<lookup_result>();
	conn.async_name_lookup("foo.i2p", record(r1));
	conn.async_name_lookup("bar.i2p", record(r2));

	run_to_completion(ios);

	TEST_EQUAL(next_reply, replies.size());
	TEST_CHECK(!open_ec);
	TEST_EQUAL(conn.local_endpoint(), "local-dest");
	TEST_CHECK(!conn.is_open());
	TEST_CHECK(r1->called);
	TEST_CHECK(r2->called);
	TEST_CHECK(r1->ec);
	TEST_EQUAL(r2->ec, r1->ec);
	TEST_EQUAL(r1.use_count(), 1);
	TEST_EQUAL(r2.use_count(), 1);

	auto const r3 = std::make_shared<lookup_result>();
	conn.async_name_lookup("baz.i2p", record(r3));
	run_to_completion(ios);
	TEST_CHECK(r3->called);
	TEST_EQUAL(r3->ec, error_code(errors::no_i2p_router));
	TEST_EQUAL(r3.use_count(), 1);
}

TORRENT_TEST(oversized_name_lookup_fails_queued_name_lookups)
{
	io_context ios;

	// a SAM bridge that sets up the session and then only reads, to detect
	// the client closing the connection
	tcp::acceptor sam(ios, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false);
	tcp::socket sam_conn(ios);
	std::array<char, 1024> buf;
	std::array<std::string, 3> const replies{{"HELLO REPLY RESULT=OK\n",
		"SESSION STATUS RESULT=OK\n",
		"NAMING REPLY RESULT=OK NAME=ME VALUE=local-dest\n"}};
	std::size_t next_reply = 0;
	std::string pending;
	std::function<void(error_code const&, std::size_t)> const serve = [&](error_code const& ec,
																		  std::size_t const len) {
		if (ec)
			return;
		pending.clear();
		for (char const c : span<char const>(buf.data(), int(len)))
		{
			if (c == '\n' && next_reply < replies.size())
				pending += replies[next_reply++];
		}
		if (pending.empty())
		{
			sam_conn.async_read_some(boost::asio::buffer(buf), serve);
			return;
		}
		boost::asio::async_write(
			sam_conn, boost::asio::buffer(pending), [&](error_code const& e, std::size_t) {
				if (e)
					return serve(e, 0);
				sam_conn.async_read_some(boost::asio::buffer(buf), serve);
			});
	};
	sam.async_accept(sam_conn, [&](error_code const& ec) {
		if (ec)
			return;
		sam_conn.async_read_some(boost::asio::buffer(buf), serve);
	});

	i2p_connection conn(ios);
	error_code open_ec;
	conn.open(
		"127.0.0.1", sam.local_endpoint().port(), i2p_session_options{}, [&](error_code const& ec) {
			open_ec = ec;
		});

	// doesn't fit in a SAM command, failing without any I/O
	std::string const long_name(2000, 'a');
	auto const r1 = std::make_shared<lookup_result>();
	auto const r2 = std::make_shared<lookup_result>();
	conn.async_name_lookup(long_name.c_str(), record(r1));
	conn.async_name_lookup("bar.i2p", record(r2));

	run_to_completion(ios);

	error_code const size_error(boost::asio::error::message_size);
	TEST_EQUAL(next_reply, replies.size());
	TEST_CHECK(!open_ec);
	TEST_CHECK(!conn.is_open());
	TEST_CHECK(r1->called);
	TEST_CHECK(r2->called);
	TEST_EQUAL(r1->ec, size_error);
	TEST_EQUAL(r2->ec, size_error);
	TEST_EQUAL(r1.use_count(), 1);
	TEST_EQUAL(r2.use_count(), 1);
}

namespace {

// In-process SAM bridge: the first STREAM ACCEPT loses its session ID; a
// second SESSION CREATE and STREAM ACCEPT demonstrate automatic recovery.
struct invalid_id_sam
{
	struct client : std::enable_shared_from_this<client>
	{
		explicit client(invalid_id_sam& s)
			: server(s), socket(s.ios) {}

		void read()
		{
			auto self = shared_from_this();
			boost::asio::async_read_until(socket, input, '\n'
				, [self](error_code const& ec, std::size_t) {
					if (ec) return;
					std::istream stream(&self->input);
					std::string cmd;
					std::getline(stream, cmd);
					self->command(cmd);
				});
		}

		void command(std::string const& cmd)
		{
			std::string reply;
			if (cmd.rfind("HELLO ", 0) == 0)
				reply = "HELLO REPLY RESULT=OK VERSION=3.1\n";
			else if (cmd.rfind("SESSION CREATE ", 0) == 0)
			{
				server.ids.push_back(session_id(cmd));
				reply = "SESSION STATUS RESULT=OK\n";
			}
			else if (cmd == "NAMING LOOKUP NAME=ME")
				reply = "NAMING REPLY RESULT=OK NAME=ME VALUE=local-dest\n";
			else if (cmd.rfind("STREAM ACCEPT ", 0) == 0)
			{
				++server.accepts;
				std::string const id = session_id(cmd);
				if (server.accepts == 1)
				{
					server.first_id_valid = server.ids.size() == 1
						&& !id.empty() && id == server.ids[0];
					reply = "STREAM STATUS RESULT=INVALID_ID\n";
				}
				else
				{
					server.recovered = server.first_id_valid
						&& server.ids.size() >= 2
						&& server.ids[0] != server.ids[1]
						&& id == server.ids[1];
					return;
				}
			}
			else
				return;

			auto message = std::make_shared<std::string>(std::move(reply));
			auto self = shared_from_this();
			boost::asio::async_write(socket, boost::asio::buffer(*message)
				, [self, message](error_code const& ec, std::size_t) {
					if (!ec) self->read();
				});
		}

		static std::string session_id(std::string const& cmd)
		{
			auto const pos = cmd.find("ID=");
			if (pos == std::string::npos) return {};
			auto const begin = pos + 3;
			auto const end = cmd.find_first_of(" \r", begin);
			return cmd.substr(begin, end - begin);
		}

		invalid_id_sam& server;
		tcp::socket socket;
		boost::asio::streambuf input;
	};

	explicit invalid_id_sam(io_context& io)
		: ios(io)
		, acceptor(io, tcp::endpoint(make_address_v4("127.0.0.1"), 0), false)
	{
		accept_next();
	}

	void accept_next()
	{
		auto c = std::make_shared<client>(*this);
		acceptor.async_accept(c->socket, [this, c](error_code const& ec) {
			if (ec) return;
			clients.push_back(c);
			c->read();
			accept_next();
		});
	}

	void close()
	{
		error_code ignore;
		acceptor.close(ignore);
		for (auto const& c : clients)
			c->socket.close(ignore);
	}

	io_context& ios;
	tcp::acceptor acceptor;
	std::vector<std::shared_ptr<client>> clients;
	std::vector<std::string> ids;
	int accepts = 0;
	bool first_id_valid = false;
	bool recovered = false;
};

} // anonymous namespace

TORRENT_TEST(invalid_id_accept_recreates_sam_session)
{
	io_context ios;
	invalid_id_sam sam(ios);
	settings_pack p;
	p.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");
	p.set_str(settings_pack::i2p_hostname, "127.0.0.1");
	p.set_int(settings_pack::i2p_port, int(sam.acceptor.local_endpoint().port()));
	p.set_bool(settings_pack::enable_dht, false);
	p.set_bool(settings_pack::enable_upnp, false);
	p.set_bool(settings_pack::enable_natpmp, false);

	{
		session ses(p);
		auto const deadline = std::chrono::steady_clock::now() + 10s;
		while (!sam.recovered && ios.run_one_until(deadline))
		{}
	}
	sam.close();

	TEST_CHECK(sam.first_id_valid);
	TEST_CHECK(sam.recovered);
	TEST_EQUAL(sam.accepts, 2);
	TEST_EQUAL(sam.ids.size(), std::size_t(2));
}

#else
TORRENT_TEST(dummy) {}
#endif
