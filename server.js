// Staro relay server - zero dependencies (Node 18+).
//
//   OWNER_KEY=xxxx MEMBER_KEY=yyyy node server.js
//
// OWNER_KEY  : required. Used by the Owner's remote GUI and by the stands.
// MEMBER_KEY : optional. For members' remote GUI. Commands sent with it are
//              tagged role="member"; the stand refuses to let them act as the Owner.
// PORT       : default 8787 (hosting platforms set this automatically).
//
// Endpoints
//   POST /send   {room, key, sender, cmd}                 -> {ok:true, id}
//   GET  /poll?room=..&key=..&after=<id>[&wait=<seconds>] -> {epoch, latest, lp, commands:[...]}
//        With wait>0 the request is HELD (long-poll) until a command arrives or
//        the time runs out. This keeps the number of requests tiny (a few per
//        minute instead of ~170) and delivers commands faster.
//   GET  /                                                -> "ok"

const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8787;
const OWNER_KEY = process.env.OWNER_KEY || "";
const MEMBER_KEY = process.env.MEMBER_KEY || "";
if (!OWNER_KEY) {
  console.error("Set OWNER_KEY first, e.g.  OWNER_KEY=mysecret node server.js");
  process.exit(1);
}

const EPOCH = Date.now().toString(); // changes on restart so stands can resync
const MAX_BODY = 4096;
const MAX_CMD_LEN = 200;
const KEEP_MS = 60_000;
const KEEP_MAX = 100;
const MAX_WAIT_S = 25;
const MAX_WAITERS_PER_ROOM = 20;

const rooms = new Map();   // room -> { seq, cmds: [{id, t, sender, cmd, role}] }
const waiters = new Map(); // room -> Set of { res, after, timer }
const hits = new Map();    // ip -> [timestamps] (send rate limit)

const sha = (s) => crypto.createHash("sha256").update(String(s)).digest();
const same = (a, b) => b !== "" && crypto.timingSafeEqual(sha(a), sha(b));

function roleOf(key) {
  if (same(key, OWNER_KEY)) return "owner";
  if (MEMBER_KEY && same(key, MEMBER_KEY)) return "member";
  return null;
}

function getRoom(name) {
  let r = rooms.get(name);
  if (!r) {
    if (rooms.size >= 200) return null; // don't let anyone create unlimited rooms
    r = { seq: 0, cmds: [] };
    rooms.set(name, r);
  }
  return r;
}

function limited(ip) {
  const now = Date.now();
  const list = (hits.get(ip) || []).filter((t) => now - t < 10_000);
  list.push(now);
  hits.set(ip, list);
  return list.length > 40; // max 40 sends / 10 s per IP
}

function json(res, code, obj) {
  if (res.writableEnded || res.destroyed) return;
  const body = JSON.stringify(obj);
  res.writeHead(code, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}

const validRoom = (s) => typeof s === "string" && /^[\w.-]{1,40}$/.test(s);

function pollPayload(r, after) {
  const now = Date.now();
  r.cmds = r.cmds.filter((c) => now - c.t < KEEP_MS);
  const commands = Number.isFinite(after) && after >= 0
    ? r.cmds.filter((c) => c.id > after).map(({ id, sender, cmd, role }) => ({ id, sender, cmd, role }))
    : [];
  return { epoch: EPOCH, latest: r.seq, lp: true, commands };
}

function dropWaiter(room, w) {
  clearTimeout(w.timer);
  const set = waiters.get(room);
  if (set) {
    set.delete(w);
    if (set.size === 0) waiters.delete(room);
  }
}

function wake(room) {
  const set = waiters.get(room);
  const r = rooms.get(room);
  if (!set || !r) return;
  for (const w of [...set]) {
    const payload = pollPayload(r, w.after);
    if (payload.commands.length > 0) {
      dropWaiter(room, w);
      json(w.res, 200, payload);
    }
  }
}

const server = http.createServer((req, res) => {
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").toString().split(",")[0].trim();
  const url = new URL(req.url, "http://localhost");

  if (req.method === "GET" && url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }

  if (req.method === "GET" && url.pathname === "/poll") {
    const room = url.searchParams.get("room");
    const role = roleOf(url.searchParams.get("key") || "");
    if (!role) return json(res, 401, { error: "bad key" });
    if (!validRoom(room)) return json(res, 400, { error: "bad room" });
    const r = getRoom(room);
    if (!r) return json(res, 429, { error: "too many rooms" });
    const after = parseInt(url.searchParams.get("after"), 10);
    const wait = Math.min(Math.max(parseInt(url.searchParams.get("wait"), 10) || 0, 0), MAX_WAIT_S);

    const payload = pollPayload(r, after);
    const canHold = wait > 0 && Number.isFinite(after) && after >= 0;
    const set = waiters.get(room);
    if (payload.commands.length > 0 || !canHold || (set && set.size >= MAX_WAITERS_PER_ROOM)) {
      return json(res, 200, payload);
    }

    // long-poll: hold until a command arrives or the time is up
    const w = { res, after, timer: null };
    w.timer = setTimeout(() => {
      dropWaiter(room, w);
      json(res, 200, pollPayload(r, after));
    }, wait * 1000);
    if (!waiters.has(room)) waiters.set(room, new Set());
    waiters.get(room).add(w);
    res.on("close", () => dropWaiter(room, w));
    return;
  }

  if (req.method === "POST" && url.pathname === "/send") {
    if (limited(ip)) return json(res, 429, { error: "slow down" });
    let size = 0;
    const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > MAX_BODY) { req.destroy(); return; }
      chunks.push(c);
    });
    req.on("end", () => {
      let b;
      try { b = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { return json(res, 400, { error: "bad json" }); }
      const role = roleOf(String(b.key || ""));
      if (!role) return json(res, 401, { error: "bad key" });
      if (!validRoom(b.room)) return json(res, 400, { error: "bad room" });
      const sender = String(b.sender || "").slice(0, 40);
      const cmd = String(b.cmd || "").trim();
      if (!sender || !cmd || cmd.length > MAX_CMD_LEN) return json(res, 400, { error: "bad command" });
      const r = getRoom(b.room);
      if (!r) return json(res, 429, { error: "too many rooms" });
      const id = ++r.seq;
      r.cmds.push({ id, t: Date.now(), sender, cmd, role });
      if (r.cmds.length > KEEP_MAX) r.cmds.splice(0, r.cmds.length - KEEP_MAX);
      json(res, 200, { ok: true, id });
      wake(b.room);
    });
    return;
  }

  json(res, 404, { error: "not found" });
});

server.listen(PORT, () => console.log(`Staro relay listening on :${PORT}`));
