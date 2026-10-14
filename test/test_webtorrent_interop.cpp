/*
Copyright (c) 2026, Arvid Norberg
All rights reserved.
BSD license; see LICENSE.
*/
#include "libtorrent/config.hpp"
#if !TORRENT_USE_RTC
#error "test_webtorrent_interop requires webtorrent=on"
#endif
#include "test.hpp"
#include "settings.hpp"
#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/create_torrent.hpp"
#include "libtorrent/bencode.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/load_torrent.hpp"
#include <cstdlib>
#include <fstream>
#include <string>

using namespace lt;
using namespace std::chrono_literals;

TORRENT_TEST(webtorrent_browser_interop)
{
	char const* const tracker = std::getenv("LIBTORRENT_INTEROP_TRACKER");
	char const* const torrent_path = std::getenv("LIBTORRENT_INTEROP_TORRENT");
	char const* const done_path = std::getenv("LIBTORRENT_INTEROP_DONE");
	char const* const log_path = std::getenv("LIBTORRENT_INTEROP_LOG");
	TEST_CHECK(tracker != nullptr);
	TEST_CHECK(torrent_path != nullptr);
	TEST_CHECK(done_path != nullptr);
	TEST_CHECK(log_path != nullptr);
	if (!tracker || !torrent_path || !done_path || !log_path) return;

	std::ofstream log(log_path, std::ios::out | std::ios::trunc);
	std::ofstream payload("interop-test.txt", std::ios::binary | std::ios::trunc);
	payload.write("Test", 4);
	payload.close();

	std::vector<create_file_entry> files;
	files.emplace_back("interop-test.txt", 4);
	create_torrent ct(std::move(files), 16 * 1024);
	ct.set_hash(piece_index_t{0}, hasher("Test", 4).final());
	std::vector<char> encoded;
	bencode(std::back_inserter(encoded), ct.generate());
	{
		std::ofstream torrent_file(torrent_path, std::ios::binary | std::ios::trunc);
		torrent_file.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
	}
	log << "Wrote 4-byte torrent; tracker=" << tracker << '\n';
	log.flush();

	settings_pack pack = settings();
	pack.set_int(settings_pack::alert_mask, alert::all_categories);
	session_params params(pack);
	session ses(params);

	add_torrent_params atp = load_torrent_buffer({encoded.data(), int(encoded.size())});
	atp.save_path = ".";
	atp.trackers.emplace_back(tracker);
	atp.flags &= ~torrent_flags::paused;
	atp.flags &= ~torrent_flags::auto_managed;
	torrent_handle tor = ses.add_torrent(std::move(atp));

	bool rtc_peer = false;
	auto const start = clock_type::now();
	while (clock_type::now() - start < 90s)
	{
		ses.wait_for_alert(200ms);
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert const* a : alerts)
		{
			log << "[alert:" << a->type() << "] " << a->message() << '\n';
			if (auto const* pc = alert_cast<peer_connect_alert>(a))
			{
				log << "[peer-connect] socket=" << static_cast<int>(pc->socket_type) << '\n';
				if (pc->socket_type == socket_type_t::rtc) rtc_peer = true;
			}
			if (auto const* pl = alert_cast<peer_log_alert>(a))
				log << "[peer-log][" << static_cast<int>(pl->event_type) << "] " << pl->log_message() << '\n';
		}
		log.flush();

		std::ifstream done(done_path);
		std::string result;
		if (done && std::getline(done, result))
		{
			log << "[browser-result] " << result << '\n';
			log.flush();
			TEST_CHECK(result == "PASS");
			TEST_CHECK(rtc_peer);
			TEST_CHECK(tor.status().is_seeding);
			session_proxy proxy = ses.abort();
			return;
		}
	}
	log << "Timed out waiting for browser result; rtc_peer=" << rtc_peer << '\n';
	log.flush();
	TEST_CHECK(false);
	session_proxy proxy = ses.abort();
}
