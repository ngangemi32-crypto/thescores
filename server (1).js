'use strict';
/*
 * Score Ticker server.
 * Polls ESPN's public scoreboard endpoints, normalizes the data with logos,
 * and streams it to browsers over Server-Sent Events. No dependencies; needs Node 18+.
 */
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

const ALL_LEAGUES = [
  { id: 'nba', label: 'NBA', short: 'NBA', path: 'basketball/nba', homeFirst: false },
  { id: 'ncaab', label: "NCAAB Men's", short: 'NCAAB', path: 'basketball/mens-college-basketball', extraQuery: 'groups=50', homeFirst: false },
  { id: 'nhl', label: 'NHL', short: 'NHL', path: 'hockey/nhl', homeFirst: false },
  { id: 'nfl', label: 'NFL', short: 'NFL', path: 'football/nfl', homeFirst: false },
  { id: 'ncaaf', label: 'NCAAF', short: 'NCAAF', path: 'football/college-football', homeFirst: false },
  { id: 'ufc', label: 'UFC', short: 'UFC', path: 'mma/ufc', homeFirst: true }
];

const DAY_MS = 86400000;

function etDate(d) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' })
    .format(d)
    .replaceAll('-', '');
}

function dateRange(daysBack, daysAhead, now = new Date()) {
  const from = etDate(new Date(now.getTime() - daysBack * DAY_MS));
  const to = etDate(new Date(now.getTime() + daysAhead * DAY_MS));
  return from === to ? from : `${from}-${to}`;
}

const toScore = (v) => {
  if (v == null || v === '') return null;
  const n = Number(typeof v === 'object' ? v.value ?? v.displayValue : v);
  return Number.isFinite(n) ? n : null;
};

function normalizeCompetition(ev, comp) {
  if (!comp || !Array.isArray(comp.competitors) || comp.competitors.length < 2) return null;

  const home = comp.competitors.find((c) => c.homeAway === 'home') || comp.competitors[0];
  const away = comp.competitors.find((c) => c.homeAway === 'away') || comp.competitors[1];
  if (!home || !away) return null;

  const status = comp.status || ev.status || {};
  const type = status.type || {};
  const name = String(type.name || '');
  let s = 'pre';
  if (type.state === 'in') s = 'live';
  else if (type.state === 'post') s = 'final';
  if (/POSTPONED|CANCELED|CANCELLED|FORFEIT/i.test(name)) s = 'other';

  const detail = String(type.shortDetail || type.detail || type.description || '');
  const entity = (c) => c.team || c.athlete || {};
  const abbr = (c) => {
    const e = entity(c);
    if (e.abbreviation) return String(e.abbreviation).slice(0, 6);
    const fullName = String(e.shortName || e.displayName || '?');
    return fullName.split(' ').pop().toUpperCase().slice(0, 8);
  };
  const full = (c) => {
    const e = entity(c);
    return String(e.displayName || e.fullName || e.shortDisplayName || abbr(c));
  };
  const logo = (c) => {
    const e = entity(c);
    if (e.logo) return String(e.logo);
    if (Array.isArray(e.logos) && e.logos[0]?.href) return String(e.logos[0].href);
    if (e.flag?.href) return String(e.flag.href);
    return '';
  };
  const shows = s === 'live' || s === 'final';

  return {
    id: String(comp.id || ev.id),
    start: comp.date || ev.date || '',
    s,
    detail,
    h: abbr(home),
    a: abbr(away),
    hn: full(home),
    an: full(away),
    hl: logo(home),
    al: logo(away),
    hw: Boolean(home.winner),
    aw: Boolean(away.winner),
    hs: shows ? toScore(home.score) : null,
    as: shows ? toScore(away.score) : null
  };
}

function normalizeEvent(ev, isCombat) {
  if (!ev || !Array.isArray(ev.competitions)) return [];
  if (isCombat) {
    return ev.competitions.map((comp) => normalizeCompetition(ev, comp)).filter(Boolean);
  }
  const single = normalizeCompetition(ev, ev.competitions[0]);
  return single ? [single] : [];
}

function sortGames(games) {
  return games.sort((x, y) => (Date.parse(x.start) || 0) - (Date.parse(y.start) || 0) || x.id.localeCompare(y.id));
}

function createApp(options = {}) {
  const env = process.env;
  const cfg = {
    port: options.port ?? (Number(env.PORT) || 3000),
    host: options.host ?? (env.HOST || '0.0.0.0'),
    espnBase: (options.espnBase || env.ESPN_BASE || 'https://site.api.espn.com/apis/site/v2/sports').replace(/\/$/, ''),
    pollLiveMs: options.pollLiveMs ?? (Number(env.POLL_LIVE_MS) || 20000),
    pollIdleMs: options.pollIdleMs ?? (Number(env.POLL_IDLE_MS) || 120000),
    daysBack: options.daysBack ?? (env.DAYS_BACK !== undefined ? Number(env.DAYS_BACK) : 1),
    daysAhead: options.daysAhead ?? (env.DAYS_AHEAD !== undefined ? Number(env.DAYS_AHEAD) : 2),
    timeoutMs: options.timeoutMs ?? 8000,
    metaMs: options.metaMs ?? 15000
  };
  const wanted = options.leagues || (env.LEAGUES ? env.LEAGUES.split(',').map((x) => x.trim().toLowerCase()) : null);
  const leagues = ALL_LEAGUES.filter((l) => !wanted || wanted.includes(l.id));

  const states = new Map(
    leagues.map((l) => [l.id, { league: l, games: [], key: '[]', updatedAt: 0, ok: false, error: null, failures: 0, pollMs: cfg.pollIdleMs, timer: null }])
  );
  const clients = new Set();
  let stopped = false;
  let metaTimer = null;
  let heartbeat = null;
  let server = null;

  const leagueView = (st, withGames) => {
    const v = {
      id: st.league.id, label: st.league.label, short: st.league.short, homeFirst: st.league.homeFirst,
      updatedAt: st.updatedAt, ok: st.ok, error: st.error, pollMs: st.pollMs
    };
    if (withGames) v.games = st.games;
    return v;
  };
  const snapshot = () => ({ serverTime: Date.now(), leagues: [...states.values()].map((st) => leagueView(st, true)) });
  const meta = () => ({ serverTime: Date.now(), leagues: [...states.values()].map((st) => leagueView(st, false)) });

  function broadcast(event, payload) {
    const msg = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const res of clients) {
      try { res.write(msg); } catch (_) { clients.delete(res); }
    }
  }

  const isActive = (st) => {
    const now = Date.now();
    return st.games.some((g) => g.s === 'live' || (g.s === 'pre' && Date.parse(g.start) - now < 15 * 60000));
  };

  function nextDelay(st) {
    let base = isActive(st) ? cfg.pollLiveMs : cfg.pollIdleMs;
    if (st.failures > 0) base = Math.min(300000, cfg.pollLiveMs * 2 ** Math.min(st.failures, 5));
    st.pollMs = base;
    return Math.round(base * (0.9 + Math.random() * 0.2));
  }

  async function fetchGames(league) {
    const extra = league.extraQuery ? `&${league.extraQuery}` : '';
    const url = `${cfg.espnBase}/${league.path}/scoreboard?dates=${dateRange(cfg.daysBack, cfg.daysAhead)}&limit=300${extra}`;
    const res = await fetch(url, {
      headers: { accept: 'application/json', 'user-agent': 'score-ticker/1.0' },
      signal: AbortSignal.timeout(cfg.timeoutMs)
    });
    if (!res.ok) throw new Error(`ESPN returned HTTP ${res.status}`);
    const json = await res.json();
    if (!json || !Array.isArray(json.events)) throw new Error('Unexpected ESPN response');
    const seen = new Set();
    const games = [];
    for (const ev of json.events) {
      for (const g of normalizeEvent(ev, league.id === 'ufc')) {
        if (g && !seen.has(g.id)) { seen.add(g.id); games.push(g); }
      }
    }
    return sortGames(games);
  }

  async function pollLeague(st) {
    if (stopped) return;
    const before = `${st.key}|${st.ok}|${st.error}`;
    try {
      const games = await fetchGames(st.league);
      st.games = games;
      st.key = JSON.stringify(games);
      st.ok = true;
      st.error = null;
      st.failures = 0;
      st.updatedAt = Date.now();
    } catch (err) {
      st.failures += 1;
      st.ok = false;
      st.error = err && err.name === 'TimeoutError' ? 'ESPN timed out' : String((err && err.message) || err);
    }
    if (stopped) return;
    if (before !== `${st.key}|${st.ok}|${st.error}`) broadcast('scores', snapshot());
    st.timer = setTimeout(() => pollLeague(st), nextDelay(st));
  }

  function serveIndex(req, res) {
    fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, buf) => {
      if (err) { res.writeHead(500); res.end('index.html missing'); return; }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(req.method === 'HEAD' ? undefined : buf);
    });
  }

  function handle(req, res) {
    if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405, { allow: 'GET, HEAD' }); res.end(); return; }
    const { pathname } = new URL(req.url, 'http://localhost');
    res.setHeader('x-content-type-options', 'nosniff');
    if (pathname === '/' || pathname === '/index.html') return serveIndex(req, res);
    if (pathname === '/api/scores') {
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify(snapshot()));
      return;
    }
    if (pathname === '/healthz') {
      const anyOk = [...states.values()].some((st) => st.ok);
      res.writeHead(anyOk ? 200 : 503, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: anyOk, leagues: meta().leagues }));
      return;
    }
    if (pathname === '/events') {
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
        'x-accel-buffering': 'no'
      });
      res.write(`event: scores\ndata: ${JSON.stringify(snapshot())}\n\n`);
      clients.add(res);
      req.on('close', () => clients.delete(res));
      return;
    }
    res.writeHead(404);
    res.end('Not found');
  }

  function start() {
    server = http.createServer(handle);
    for (const st of states.values()) pollLeague(st);
    metaTimer = setInterval(() => broadcast('meta', meta()), cfg.metaMs);
    heartbeat = setInterval(() => {
      for (const res of clients) {
        try { res.write(': ping\n\n'); } catch (_) { clients.delete(res); }
      }
    }, 25000);
    server.listen(cfg.port, cfg.host, () => {
      console.log(`Score Ticker running on http://${cfg.host}:${cfg.port}`);
    });
    return server;
  }

  return { start };
}

if (require.main === module) {
  createApp().start();
}

module.exports = { createApp };
