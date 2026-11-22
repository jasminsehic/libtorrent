/*

Copyright (c) 2020-2021, Alden Torres
Copyright (c) 2020-2021, Arvid Norberg
Copyright (c) 2020, Paul-Louis Ageneau
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_RTC_SIGNALING_HPP_INCLUDED
#define TORRENT_RTC_SIGNALING_HPP_INCLUDED

#include "libtorrent/config.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/aux_/io_bytes.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/peer_id.hpp"
#include "libtorrent/span.hpp"
#include "libtorrent/time.hpp"
#include "libtorrent/aux_/deadline_timer.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <boost/functional/hash.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <utility>

namespace rtc {
	class PeerConnection;
	class DataChannel;
}

namespace libtorrent {

struct alert_manager;
struct torrent;

namespace aux {

struct rtc_stream_init;

constexpr int RTC_OFFER_ID_LEN = 20;
constexpr std::size_t RTC_MAX_SDP_SIZE = 64 * 1024;
constexpr std::size_t RTC_MAX_MESSAGE_SIZE = 2 * RTC_MAX_SDP_SIZE;

using rtc_offer_id = std::array<char, RTC_OFFER_ID_LEN>;

struct rtc_answer
{
	rtc_offer_id offer_id;
	peer_id pid;
	std::string sdp; // session description in SDP format
};

struct rtc_offer
{
	rtc_offer_id id;
	peer_id pid;
	std::string sdp; // session description in SDP format
	std::function<void(peer_id const& pid, rtc_answer const& answer)> answer_callback;
};

// Per-offer completion guard. Accessed only on the libtorrent network thread.
struct rtc_offer_completion_guard
{
	bool claim() noexcept
	{
		if (m_reported) return false;
		m_reported = true;
		return true;
	}

private:
	bool m_reported = false;
};

// Shared offer-batch accumulator used by rtc_signaling and its regression tests.
struct rtc_offer_batch
{
	using offers_handler = std::function<void(error_code const&, std::vector<rtc_offer> const&)>;

	rtc_offer_batch(int count, offers_handler handler)
		: m_count(count)
		, m_handler(std::move(handler))
	{
		if (m_count == 0) m_handler(error_code{}, {});
	}

	void add(error_code const& ec, rtc_offer offer)
	{
		if (!ec) m_offers.push_back(std::move(offer));
		else --m_count;

		if (is_complete())
			m_handler(m_offers.empty() ? ec : error_code(), m_offers);
	}

	bool is_complete() const
	{
		return int(m_offers.size()) == m_count;
	}

private:
	int m_count;
	offers_handler m_handler;
	std::vector<rtc_offer> m_offers;
};

struct rtc_offer_batch_queue
{
	using offers_handler = rtc_offer_batch::offers_handler;

	void emplace(int count, offers_handler handler)
	{
		m_batches.emplace(count, std::move(handler));
	}

	void add(error_code const& ec, rtc_offer offer)
	{
		while (!m_batches.empty() && m_batches.front().is_complete())
			m_batches.pop();

		if (!m_batches.empty())
			m_batches.front().add(ec, std::move(offer));
	}

private:
	std::queue<rtc_offer_batch> m_batches;
};

// This class handles client signaling for WebRTC DataChannels
struct TORRENT_EXTRA_EXPORT rtc_signaling final : std::enable_shared_from_this<rtc_signaling>
{
	using offers_handler = std::function<void(error_code const&, std::vector<rtc_offer> const&)>;
	using rtc_stream_handler = std::function<void(rtc_stream_init)>;
	explicit rtc_signaling(io_context& ioc, torrent* t, rtc_stream_handler handler);
	~rtc_signaling();
	rtc_signaling& operator=(rtc_signaling const&) = delete;
	rtc_signaling(rtc_signaling const&) = delete;
	rtc_signaling& operator=(rtc_signaling&&) noexcept = delete;
	rtc_signaling(rtc_signaling&&) noexcept = delete;

	alert_manager& alerts() const;

	void close();
	void generate_offers(int count, offers_handler handler);
	void process_offer(rtc_offer const& offer);
	void process_answer(rtc_answer const& answer);

#ifndef TORRENT_DISABLE_LOGGING
	bool should_log() const;
	void debug_log(const char* fmt, ...) const noexcept TORRENT_FORMAT(2,3);
#endif

private:
	using description_handler =
		std::function<void(error_code const&, std::string const& description)>;

	struct connection
	{
		explicit connection(io_context& ioc) : timer(ioc) {}

		std::shared_ptr<rtc::PeerConnection> peer_connection;
		std::shared_ptr<rtc::DataChannel> data_channel;
		std::optional<peer_id> pid;
		bool incoming = false;

		deadline_timer timer;
	};

	rtc_offer_id generate_offer_id() const;

	connection& create_connection(rtc_offer_id const& offer_id, description_handler handler);
	void on_generated_offer(error_code const& ec, rtc_offer offer);
	void on_generated_answer(error_code const& ec, rtc_answer answer, rtc_offer offer);
	void on_data_channel(error_code const& ec, rtc_offer_id offer_id, std::shared_ptr<rtc::DataChannel> dc);

	io_context& m_io_context;
	torrent* m_torrent;
	rtc_stream_handler m_rtc_stream_handler;

	std::unordered_map<rtc_offer_id, connection, boost::hash<rtc_offer_id>> m_connections;
	int m_num_incoming_connections = 0;
	std::queue<rtc_offer_id> m_queue;

	rtc_offer_batch_queue m_offer_batches;
};

}
}

#endif
