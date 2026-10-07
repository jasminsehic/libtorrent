/*

Copyright (c) 2019 Paul-Louis Ageneau
Copyright (c) 2020-2021, Arvid Norberg
Copyright (c) 2020, Paul-Louis Ageneau
Copyright (c) 2021, Alden Torres
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_WEBSOCKET_TRACKER_CONNECTION_HPP_INCLUDED
#define TORRENT_WEBSOCKET_TRACKER_CONNECTION_HPP_INCLUDED

#include "libtorrent/config.hpp"

#if TORRENT_USE_RTC

#include "libtorrent/aux_/rtc_signaling.hpp" // for rtc_offer and rtc_answer
#include "libtorrent/aux_/websocket_stream.hpp"
#include "libtorrent/aux_/deadline_timer.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/peer_id.hpp"
#include "libtorrent/time.hpp"
#include "libtorrent/aux_/resolver_interface.hpp"
#include "libtorrent/aux_/tracker_manager.hpp" // for tracker_connection
#include "libtorrent/aux_/ssl.hpp"

#include <boost/beast/core/flat_buffer.hpp>

#include <deque>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <variant>
#include <optional>

namespace libtorrent::aux {

struct tracker_answer
{
	sha1_hash info_hash;
	peer_id pid;
	aux::rtc_answer answer;
};

struct TORRENT_EXTRA_EXPORT websocket_tracker_connection : tracker_connection
{
	friend class tracker_manager;

	// deadline is only set for an announce rescued from a dead connection,
	// see queue_rescued_request()
	websocket_tracker_connection(io_context& ios,
		tracker_manager& man,
		tracker_request const& req,
		std::weak_ptr<request_callback> cb,
		std::optional<time_point> deadline = std::nullopt);
	~websocket_tracker_connection() override = default;

	void start() override;
	void close() override;

	bool is_started() const;
	bool is_open() const;

	// aborts every non-stopped-event request on this connection (it may
	// multiplex several torrents' requests). Returns true if a
	// stopped-event request remains, meaning the connection must stay open.
	bool prune_non_stopped_requests();

	void queue_request(tracker_request req, std::weak_ptr<request_callback> cb);
	// queues an announce rescued from a dead connection, keeping its
	// original deadline. It won't be rescued again if this connection dies
	// too
	void queue_rescued_request(
		tracker_request req, std::weak_ptr<request_callback> cb, time_point deadline);
	void queue_answer(tracker_answer ans);

private:
	std::shared_ptr<websocket_tracker_connection> shared_from_this()
	{
		return std::static_pointer_cast<websocket_tracker_connection>(
			tracker_connection::shared_from_this());
	}

	void send_pending();
	void do_send(tracker_request const& req);
	void do_send(tracker_answer const& ans);
	void do_read();
	void on_timeout(error_code const& ec) override;
	void on_connect(error_code const& ec);
	void on_read(error_code ec, std::size_t bytes_read);
	void on_write(error_code const& ec, std::size_t bytes_written);
	// the time by which the tracker must have responded to req
	time_point request_deadline(tracker_request const& req) const;
	// the event sent to the tracker for req. paused is omitted for trackers
	// known not to support it
	event_t wire_event(tracker_request const& req) const;

	void update_announce_timer();
	void on_announce_timeout(error_code const& ec);
	// wraps tracker_connection::fail
	void fail(error_code const& ec, operation_t op);
	// does the actual work of close(); takes the real failure reason so
	// callers that just called fail(ec, op) can report it to every
	// multiplexed torrent, instead of the generic reason close() uses on
	// its own (e.g. session shutdown, where there is no specific error)
	void close(error_code const& ec, operation_t op);

	io_context& m_io_context;
	ssl::context* m_ssl_context;
	std::shared_ptr<aux::websocket_stream> m_websocket;
	boost::beast::flat_buffer m_read_buffer{RTC_MAX_MESSAGE_SIZE};
	std::string m_write_data;

	using tracker_message = std::variant<tracker_request, tracker_answer>;
	// messages not sent yet. For requests, the time_point is the deadline
	// by which the tracker must have responded (see request_deadline()),
	// which runs from the time the request is queued, so a request stuck
	// behind a connection attempt or write that never completes still
	// times out
	std::deque<std::tuple<tracker_message, std::weak_ptr<request_callback>, time_point>> m_pending;

	// pending is true from the point a request is sent until its first
	// tracker_response is delivered. close() reports an error for any
	// entry still pending, so a torrent multiplexed onto this connection
	// (i.e. not the most recently sent request) still gets notified when
	// the connection is torn down before it received a response. req is
	// the entry's own request, since it may not be the most recently sent
	// request on this connection (tracker_req()).
	struct callback_entry
	{
		std::weak_ptr<request_callback> cb;
		tracker_request req;
		bool pending = true;
		time_point deadline;
		// when the request was sent
		time_point sent;
	};
	// on_read() holds an iterator into m_callbacks across a reentrant call
	// into cb->on_rtc_offer()/on_rtc_answer(), which may indirectly insert
	// into this same map (e.g. by queuing another request on this
	// connection); this relies on stable iterators. Do not change this to
	// an unordered_map without also fixing on_read() to not hold an
	// iterator across that call.
	std::map<sha1_hash, callback_entry> m_callbacks;
	std::map<sha1_hash, int> m_offer_quota;

	// if the last message written was a paused announce that's still
	// outstanding, with no well-formed message received since, returns its
	// entry in m_callbacks, otherwise nullptr. A tracker reaction that
	// doesn't identify the request it's about, received in that state, is
	// most likely about it
	callback_entry* last_written_paused_announce();
	void mark_paused_unsupported(char const* reason);

	// when this connection dies, takes the announces still outstanding on it
	// (sent or not) that haven't timed out yet off it, to be sent again on a
	// new connection (see rescue()). Except for culprit, the announce
	// believed to have caused the connection to be closed, and announces
	// that were already rescued once. Those are left for close() to report
	std::vector<tracker_manager::websocket_rescued_request> take_rescuable_requests(
		std::optional<sha1_hash> culprit);
	// hands rescued announces to tracker_manager, to be sent again on a new
	// connection. Must be called after close(), so that a new connection is
	// used
	void rescue(std::vector<tracker_manager::websocket_rescued_request> reqs);
	// the announce believed to have caused the tracker to close the
	// connection: the last message written, if it was an announce still
	// outstanding and nothing well-formed was received since
	std::optional<sha1_hash> close_culprit() const;

	// info-hashes of announces on this connection that were rescued from a
	// dead connection. Each announce is only rescued once, so a tracker that
	// closes every connection can't make rescued announces bounce between
	// new connections until they time out
	std::set<sha1_hash> m_rescued_requests;

	deadline_timer m_announce_timer;

	// the last time a well-formed message was received on this connection
	// (or when it was established). Malformed messages deliberately don't
	// count: a tracker that only sends us messages we can't make sense of is
	// no more useful than a dead one, so it mustn't stop the connection from
	// being replaced. Used to tell a tracker that doesn't respond to a
	// request apart from a connection that's dead (see on_announce_timeout())
	time_point m_last_receive = min_time();

	// the last message written to the tracker. Used to attribute reactions
	// the tracker itself doesn't attribute to a request, i.e. closing the
	// connection or a failure reason without an info_hash, to the message
	// that most likely caused them
	struct written_message
	{
		// set for announces, empty for RTC answers
		std::optional<sha1_hash> info_hash;
		// the event actually sent
		event_t event = event_t::none;
		time_point time;
	};
	std::optional<written_message> m_last_written;

	// m_sending is true while the single outstanding WebSocket write is in
	// progress. m_sending_request identifies it when that write is a tracker
	// announce; an RTC answer leaves it empty.
	bool m_sending = false;
	std::optional<sha1_hash> m_sending_request;
};

struct websocket_tracker_response {
	sha1_hash info_hash;
	std::optional<tracker_response> resp;
	std::optional<aux::rtc_offer> offer;
	std::optional<aux::rtc_answer> answer;
	std::string failure_reason;
	// false for a failure reason without an info_hash, which trackers send
	// in response to requests they can't parse. info_hash is not valid then
	bool has_info_hash = true;
};

TORRENT_EXTRA_EXPORT std::variant<websocket_tracker_response, std::string>
	parse_websocket_tracker_response(span<char const> message, error_code &ec);

}

#endif // TORRENT_USE_RTC

#endif // TORRENT_WEBSOCKET_TRACKER_CONNECTION_HPP_INCLUDED
