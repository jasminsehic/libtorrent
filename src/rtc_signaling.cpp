/*

Copyright (c) 2020-2021, Alden Torres
Copyright (c) 2020-2021, Arvid Norberg
Copyright (c) 2020-2021, Paul-Louis Ageneau
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/config.hpp"

#if TORRENT_USE_RTC

#include "libtorrent/alert.hpp"
#include "libtorrent/aux_/alert_manager.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/aux_/random.hpp"
#include "libtorrent/aux_/torrent.hpp"
#include "libtorrent/aux_/rtc_signaling.hpp"
#include "libtorrent/aux_/rtc_stream.hpp"
#include "libtorrent/aux_/session_interface.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <rtc/rtc.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <cstdarg>
#include <utility>
#include <sstream>
#include <mutex>
#include <algorithm>
#include <cctype>

// Always emit redacted protocol/direction hints for privacy diagnostics.
// Never forward raw libdatachannel messages (which may contain credentials, SDP
// or network addresses).
#define DEBUG_RTC 1

namespace libtorrent {
namespace aux {

namespace errc = boost::system::errc;

namespace {

template <class T> std::weak_ptr<T> make_weak_ptr(std::shared_ptr<T> ptr) { return ptr; }

#if DEBUG_RTC
class rtc_log_appender
{
public:
	// write() is called from libdatachannel's background threads; the lock
	// is held for its whole body (not just the m_ses read) so unset_session()
	// can't clear the pointer out from under an in-flight write(), which
	// would otherwise race with the session being destroyed right after
	void set_session(rtc_signaling* owner, aux::session_interface* ses)
	{
		std::lock_guard<std::mutex> l(m_mutex);
		m_owner = owner;
		m_ses = ses;
	}

	void unset_session(rtc_signaling* owner)
	{
		std::lock_guard<std::mutex> l(m_mutex);
		if (m_owner != owner)
			return;
		m_owner = nullptr;
		m_ses = nullptr;
	}

	void write(rtc::LogLevel level, rtc::string message)
	{
		std::lock_guard<std::mutex> l(m_mutex);
		if (!m_ses) return;

		auto &alerts = m_ses->alerts();
		if (!alerts.should_post<log_alert>()) return;

		(void)level;
		std::string event(message.begin(), message.end());
		std::transform(event.begin(), event.end(), event.begin(), [](unsigned char c) {
			return char(std::tolower(c));
		});
		char const* protocol = nullptr;
		if (event.find("stun") != std::string::npos)
			protocol = "stun";
		else if (event.find("turn") != std::string::npos)
			protocol = "turn";
		else if (event.find("ice") != std::string::npos)
			protocol = "ice";
		if (!protocol)
			return;
		char const* direction = "event";
		if (event.find("sending") != std::string::npos || event.find("sent") != std::string::npos
			|| event.find("send") != std::string::npos)
			direction = "send";
		else if (event.find("receiving") != std::string::npos
			|| event.find("received") != std::string::npos
			|| event.find("recv") != std::string::npos)
			direction = "receive";
		// Only report event classes. Raw library messages can contain
		// candidate IPs, TURN authentication fields, or sensitive SDP.
		char const* const label = protocol;
		std::string const safe = std::string("PRIVACY_TRACE RTC_STACK_HINT protocol=") + label
			+ " direction=" + direction + " raw=redacted";
		alerts.emplace_alert<log_alert>(safe.c_str());
	}

private:
	std::mutex m_mutex;
	// Identify the active signaling instance without dereferencing its torrent
	// during a delayed destructor (the torrent may already be gone).
	rtc_signaling* m_owner = nullptr;
	aux::session_interface* m_ses = nullptr;
};

static rtc_log_appender appender;
#endif

}

rtc_signaling::rtc_signaling(io_context& ioc,
	torrent* t,
	rtc_stream_handler handler,
	rtc_configuration_hook configuration_hook)
	: m_io_context(ioc)
	, m_torrent(t)
	, m_rtc_stream_handler(std::move(handler))
	, m_rtc_configuration_hook(std::move(configuration_hook))
{
#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling created");
#endif

	static std::once_flag flag;
#if DEBUG_RTC
	appender.set_session(this, &m_torrent->session());
	std::call_once(flag, []() {
		using namespace std::placeholders;
		rtc::InitLogger(
			rtc::LogLevel::Debug, std::bind(&rtc_log_appender::write, &appender, _1, _2));
	});
#else
	std::call_once(flag, []() {
		rtc::InitLogger(rtc::LogLevel::None, nullptr);
	});
#endif
}

rtc_signaling::~rtc_signaling() { close(); }

alert_manager& rtc_signaling::alerts() const
{
	return m_torrent->alerts();
}

void rtc_signaling::close()
{
	if (m_closed)
		return;
	m_closed = true;
#if DEBUG_RTC
	// RTC callbacks can keep this object alive after its torrent and session
	// are destroyed. Detach by owner identity, never by dereferencing m_torrent.
	appender.unset_session(this);
#endif
	m_connections.clear();
	m_num_incoming_connections = 0;
}

rtc_offer_id rtc_signaling::generate_offer_id() const
{
	rtc_offer_id id;
	aux::random_bytes({id.data(), int(id.size())});
	return id;
}

void rtc_signaling::generate_offers(int count, offers_handler handler)
{
#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling generating %d offers", count);
#endif
	m_offer_batches.emplace(count, std::move(handler));
	while (count--)
	{
		rtc_offer_id offer_id = generate_offer_id();
		peer_id pid = m_torrent->pid();

		// Both the description callback and the synchronous setup-failure path
		// may report this offer. All accesses to this flag run on the
		// libtorrent network thread (the RTC callbacks post to m_io_context).
		std::shared_ptr<bool> reported;
		try
		{
			reported = std::make_shared<bool>(false);
			description_handler const on_description =
				[weak_this = weak_from_this(), offer_id, pid, reported](
					error_code const& ec, std::string const& sdp) {
					auto self = weak_this.lock();
					if (!self)
						return;

					// Never report directly on a libdatachannel worker thread. The
					// failure path may have invalidated this offer before we run.
					// Capture the context before moving self into the callback;
					// function argument evaluation order is not guaranteed.
					auto& io_context = self->m_io_context;
					post(io_context,
						[signaling = std::move(self),
							reported,
							offer_id,
							pid,
							ec,
							description = std::string(sdp)]() mutable {
							if (std::exchange(*reported, true))
								return;
							signaling->on_generated_offer(
								ec, rtc_offer{offer_id, pid, std::move(description), {}});
						});
				};

			auto& conn = create_connection(offer_id, on_description);
			// createDataChannel() may throw during ICE gathering.
			auto dc = conn.peer_connection->createDataChannel("webtorrent");
			dc->onOpen([weak_this = weak_from_this(), offer_id, weak_dc = make_weak_ptr(dc)]() {
				// Warning: this is called from another thread
				auto self = weak_this.lock();
				auto dc_ = weak_dc.lock();
				if (!self || !dc_)
					return;

				auto& io_context = self->m_io_context;
				post(io_context,
					std::bind(&rtc_signaling::on_data_channel,
						std::move(self),
						error_code{},
						std::move(offer_id),
						std::move(dc_)));
			});

			// We need to maintain the DataChannel alive
			conn.data_channel = std::move(dc);
		}
		catch (std::exception const& e)
		{
			TORRENT_UNUSED(e);
			// Synchronous failure takes precedence over any description
			// already queued by libdatachannel for this offer.
			if (reported)
				*reported = true;
#ifndef TORRENT_DISABLE_LOGGING
			debug_log("*** RTC signaling could not create offer: %s", e.what());
#endif
			// Construction can fail before or after the connection is inserted.
			// In either case, account for the attempted offer.
			m_connections.erase(offer_id);
			post(m_io_context, [self = shared_from_this(), offer_id, pid] {
				self->on_generated_offer(
					errc::make_error_code(errc::resource_unavailable_try_again),
					rtc_offer{offer_id, pid, {}, {}});
			});
		}
	}
}

void rtc_signaling::process_offer(rtc_offer const& offer)
{
	if (m_connections.find(offer.id) != m_connections.end())
	{
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** RTC signaling ignored duplicate offer");
#endif
		// It seems the offer is from ourselves, ignore...
		return;
	}

	int const max_offers =
		std::max(m_torrent->settings().get_int(settings_pack::max_webtorrent_offers), 0);
	if (m_num_incoming_connections >= max_offers)
	{
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** RTC signaling rejected offer: connection limit reached");
#endif
		return;
	}

#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling processing remote offer");
#endif
	auto& conn = create_connection(offer.id, [weak_this = weak_from_this(), offer]
		(error_code const& ec, std::string sdp)
	{
		auto self = weak_this.lock();
		if (!self) return;

		rtc_answer answer{offer.id, offer.pid, std::move(sdp)};
		auto& io_context = self->m_io_context;
		post(io_context, std::bind(&rtc_signaling::on_generated_answer
			, std::move(self)
			, ec
			, std::move(answer)
			, std::move(offer)
		));
	});

	conn.pid = offer.pid;
	conn.incoming = true;
	++m_num_incoming_connections;

	try
	{
		conn.peer_connection->setRemoteDescription({offer.sdp, "offer"});
	}
	catch (std::exception const& e)
	{
		TORRENT_UNUSED(e);
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** Failed to set remote RTC offer: %s", e.what());
#endif
		auto const it = m_connections.find(offer.id);
		if (it != m_connections.end())
		{
			TORRENT_ASSERT(it->second.incoming);
			--m_num_incoming_connections;
			m_connections.erase(it);
		}
	}
}

void rtc_signaling::process_answer(rtc_answer const& answer)
{
	auto it = m_connections.find(answer.offer_id);
	if (it == m_connections.end())
	{
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** RTC signaling ignored answer for unknown offer");
#endif
		return;
	}

#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling processing remote answer");
#endif

	connection& conn = it->second;
	if (conn.pid)
	{
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** Local RTC offer already got an answer");
#endif
		return;
	}

	conn.pid = answer.pid;

	try
	{
		conn.peer_connection->setRemoteDescription({answer.sdp, "answer"});
	}
	catch (std::exception const& e)
	{
		TORRENT_UNUSED(e);
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** Failed to set remote RTC answer: %s", e.what());
#endif
		m_connections.erase(it);
	}
}

rtc_signaling::connection& rtc_signaling::create_connection(rtc_offer_id const& offer_id, description_handler handler)
{
	if (auto it = m_connections.find(offer_id); it != m_connections.end())
		return it->second;

#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling creating connection");
#endif

	rtc::Configuration config;
	bool const relay_only = m_torrent->settings().get_bool(settings_pack::webtorrent_relay_only);
	int accepted_stun = 0;
	int accepted_turn = 0;
	int rejected_servers = 0;

	// The legacy STUN setting also accepts multiple newline-delimited servers.
	// Only TURN URLs are used in relay-only mode, regardless of STUN settings.
	auto append_servers = [&](std::string const& servers,
							  rtc::IceServer::Type const expected_type) {
		std::istringstream input(servers);
		std::string url;
		while (std::getline(input, url))
		{
			auto const first = url.find_first_not_of(" \t\r");
			if (first == std::string::npos)
				continue;
			auto const last = url.find_last_not_of(" \t\r");
			url = url.substr(first, last - first + 1);
			if (expected_type == rtc::IceServer::Type::Stun && url.find(':') != std::string::npos
				&& url.compare(0, 5, "stun:") != 0 && url.compare(0, 6, "stuns:") != 0)
				url.insert(0, "stun:");
			try
			{
				rtc::IceServer server(url);
				if (server.type == expected_type)
				{
					if (expected_type == rtc::IceServer::Type::Turn)
						++accepted_turn;
					else
						++accepted_stun;
					config.iceServers.emplace_back(std::move(server));
				}
				else
					++rejected_servers;
			}
			catch (std::exception const&)
			{
				++rejected_servers;
				// A malformed ICE server must not prevent other servers from
				// being used. Do not log the URL: it may contain TURN secrets.
			}
		}
	};

	if (!relay_only)
		append_servers(m_torrent->settings().get_str(settings_pack::webtorrent_stun_server),
			rtc::IceServer::Type::Stun);
	append_servers(m_torrent->settings().get_str(settings_pack::webtorrent_turn_servers),
		rtc::IceServer::Type::Turn);

	if (relay_only)
		config.iceTransportPolicy = rtc::TransportPolicy::Relay;
	if (m_rtc_configuration_hook)
		m_rtc_configuration_hook(config, handler);
	if (relay_only)
	{
		// Enforce the policy even if a configuration hook modifies ICE servers.
		config.iceTransportPolicy = rtc::TransportPolicy::Relay;
		config.iceServers.erase(
			std::remove_if(config.iceServers.begin(),
				config.iceServers.end(),
				[](rtc::IceServer const& s) { return s.type != rtc::IceServer::Type::Turn; }),
			config.iceServers.end());
	}

#ifndef TORRENT_DISABLE_LOGGING
	debug_log("PRIVACY_TRACE ICE_CONFIG relay_only=%d stun=%d turn=%d rejected=%d policy=%s",
		int(relay_only),
		accepted_stun,
		accepted_turn,
		rejected_servers,
		config.iceTransportPolicy == rtc::TransportPolicy::Relay ? "relay" : "all");
	if (relay_only && config.iceServers.empty())
		debug_log("PRIVACY_TRACE ICE_NO_TURN fail_closed=1");
#else
	(void)accepted_stun;
	(void)accepted_turn;
	(void)rejected_servers;
#endif
	auto pc = std::make_shared<rtc::PeerConnection>(config);
#ifndef TORRENT_DISABLE_LOGGING
	pc->onLocalCandidate([weak_this = weak_from_this(), relay_only](rtc::Candidate candidate) {
		if (auto self = weak_this.lock())
			post(self->m_io_context, [weak_this, relay_only, type = candidate.type()] {
				auto self = weak_this.lock();
				if (!self || self->m_closed)
					return;
				self->debug_log(
					"PRIVACY_TRACE ICE_LOCAL_CANDIDATE type=%d relay_only=%d violation=%d",
					int(type),
					int(relay_only),
					int(relay_only && type != rtc::Candidate::Type::Relayed));
			});
	});
#endif
	pc->onStateChange([weak_this = weak_from_this(),
						  weak_pc = make_weak_ptr(pc),
						  offer_id,
						  handler,
						  relay_only](rtc::PeerConnection::State state) {
		// Warning: this is called from another thread
		auto self = weak_this.lock();
		auto pc_ = weak_pc.lock();
		if (!self || !pc_)
			return;

		post(self->m_io_context, [self, weak_pc, state, relay_only] {
			(void)relay_only;
			(void)state;
			if (self->m_closed)
				return;
			auto connected_pc = weak_pc.lock();
			if (!connected_pc)
				return;
#ifndef TORRENT_DISABLE_LOGGING
			self->debug_log(
				"PRIVACY_TRACE RTC_STATE state=%d relay_only=%d", int(state), int(relay_only));
			if (state == rtc::PeerConnection::State::Connected)
			{
				rtc::Candidate local;
				rtc::Candidate remote;
				bool const found = connected_pc->getSelectedCandidatePair(&local, &remote);
				bool const relayed = found && local.type() == rtc::Candidate::Type::Relayed;
				self->debug_log("PRIVACY_TRACE ICE_SELECTED_PAIR found=%d local_type=%d "
								"remote_type=%d relay_only=%d violation=%d",
					int(found),
					found ? int(local.type()) : -1,
					found ? int(remote.type()) : -1,
					int(relay_only),
					int(relay_only && found && !relayed));
			}
#endif
		});
		if (state == rtc::PeerConnection::State::Failed)
		{
			error_code const ec = boost::asio::error::connection_refused;
			auto& io_context = self->m_io_context;

			if(pc_->gatheringState() != rtc::PeerConnection::GatheringState::Complete)
				post(io_context, std::bind(std::move(handler), ec, ""));

			post(io_context, std::bind(&rtc_signaling::on_data_channel
				, std::move(self)
				, ec
				, std::move(offer_id)
				, nullptr
			));
		}
	});

	pc->onGatheringStateChange(
		[weak_this = weak_from_this(), weak_pc = make_weak_ptr(pc), handler_ = handler](
			rtc::PeerConnection::GatheringState state) {
			// Warning: this is called from another thread
			auto self = weak_this.lock();
			auto pc_ = weak_pc.lock();
			if (!self || !pc_)
				return;

			if (state == rtc::PeerConnection::GatheringState::Complete)
			{
				auto& io_context = self->m_io_context;
				auto description = *pc_->localDescription();
				post(io_context, std::bind(std::move(handler_), error_code{}, description));
			}
		});

	pc->onDataChannel([weak_this = weak_from_this(), offer_id]
		(std::shared_ptr<rtc::DataChannel> dc)
	{
		// Warning: this is called from another thread
		auto self = weak_this.lock();
		if (!self) return;

		auto& io_context = self->m_io_context;
		post(io_context, std::bind(&rtc_signaling::on_data_channel
			, std::move(self)
			, error_code{}
			, offer_id
			, dc
		));
	});

	int const connection_timeout = m_torrent->settings().get_int(settings_pack::webtorrent_connection_timeout);
	time_duration const timeout = seconds(std::max(connection_timeout, 1));
	connection conn(m_io_context);
	conn.peer_connection = std::move(pc);
	// Retain the description callback so an RTC timeout can complete an
	// offer that has not finished ICE gathering.
	conn.description_completion = std::move(handler);
	conn.timer.expires_after(timeout);
	conn.timer.async_wait(std::bind(&rtc_signaling::on_data_channel
		, shared_from_this()
		, boost::asio::error::timed_out
		, offer_id
		, nullptr
	));

	auto it = m_connections.emplace(offer_id, std::move(conn)).first;
	return it->second;
}

void rtc_signaling::on_generated_offer(error_code const& ec, rtc_offer offer)
{
	if (m_closed)
		return;
#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling generated offer");
#endif
	while (!m_offer_batches.empty() && m_offer_batches.front().is_complete())
		m_offer_batches.pop();

	if (!m_offer_batches.empty())
		m_offer_batches.front().add(ec, std::forward<rtc_offer>(offer));
}

void rtc_signaling::on_generated_answer(error_code const& ec, rtc_answer answer, rtc_offer offer)
{
	if (m_closed)
		return;
	if (ec)
	{
		// Ignore
		return;
	}
	if (m_connections.find(answer.offer_id) == m_connections.end())
		return;
#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC signaling generated answer");
#endif
	TORRENT_ASSERT(offer.answer_callback);
	offer.answer_callback(m_torrent->pid(), answer);
}

void rtc_signaling::on_data_channel(error_code const& ec
		, rtc_offer_id offer_id
		, std::shared_ptr<rtc::DataChannel> dc)
{
	if (m_closed)
		return;
	auto it = m_connections.find(offer_id);
	if (it == m_connections.end())
		return;

	if (it->second.incoming)
	{
		TORRENT_ASSERT(m_num_incoming_connections > 0);
		--m_num_incoming_connections;
	}

	connection conn = std::move(it->second);
	m_connections.erase(it);

	if (ec)
	{
		// A timeout or failed connection may occur before ICE gathering
		// produces a description. Report this outcome to the offer batch.
		// The completion guard in generate_offers() suppresses duplicates.
		if (conn.description_completion)
			conn.description_completion(ec, "");
#ifndef TORRENT_DISABLE_LOGGING
		debug_log("*** RTC negotiation failed");
#endif
		return;
	}

#ifndef TORRENT_DISABLE_LOGGING
	debug_log("*** RTC data channel open");
#endif

	TORRENT_ASSERT(dc);
	m_rtc_stream_handler(rtc_stream_init{conn.peer_connection, dc});
}

rtc_signaling::offer_batch::offer_batch(int count, rtc_signaling::offers_handler handler)
	: m_count(count)
	, m_handler(std::move(handler))
{
	if (m_count == 0) m_handler(error_code{}, {});
}

void rtc_signaling::offer_batch::add(error_code const& ec, rtc_offer offer)
{
	if (!ec) m_offers.push_back(std::move(offer));
	else --m_count;

	if (is_complete())
		m_handler(m_offers.empty() ? ec : error_code{}, m_offers);
}

bool rtc_signaling::offer_batch::is_complete() const
{
	return int(m_offers.size()) == m_count;
}

#ifndef TORRENT_DISABLE_LOGGING
bool rtc_signaling::should_log() const
{
	return !m_closed && alerts().should_post<torrent_log_alert>();
}

TORRENT_FORMAT(2,3)
void rtc_signaling::debug_log(char const* fmt, ...) const noexcept try
{
	if (m_closed || !alerts().should_post<torrent_log_alert>()) return;

	va_list v;
	va_start(v, fmt);
	alerts().emplace_alert<torrent_log_alert>(m_torrent->get_handle(), fmt, v);
	va_end(v);
}
catch (std::exception const&) {}
#endif

}
}

#endif
