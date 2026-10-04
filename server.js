// Staro relay server - zero dependencies (Node 16+).
//
//   OWNER_KEY=xxxx MEMBER_KEY=yyyy node server.js
//
// OWNER_KEY  : required. Used by the Owner's remote GUI and by the stands.
// MEMBER_KEY : optional. For members' remote GUI. Commands sent with it are
//              tagged role="member"; the stand refuses to let them act as the Owner.
// PORT       : default 8787 (hosting platforms set this automatically).
//
// Endpoints
//   POST /send   {room, key, sender, cmd}      -> {ok:true, id}
//   GET  /poll?room=..&key=..&after=<id>       -> {epoch, latest, commands:[{id,sender,cmd,role}]}
//   GET  /                                     -> "ok"

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

const rooms = new Map(); // room -> { seq, cmds: [{id, t, sender, cmd, role}] }
const hits = new Map();  // ip -> [timestamps] (send rate limit)

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
  const body = JSON.stringify(obj);
  res.writeHead(code, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}

const validRoom = (s) => typeof s === "string" && /^[\w.-]{1,40}$/.test(s);

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
    const now = Date.now();
    r.cmds = r.cmds.filter((c) => now - c.t < KEEP_MS);
    const commands = Number.isFinite(after) && after >= 0
      ? r.cmds.filter((c) => c.id > after).map(({ id, sender, cmd, role }) => ({ id, sender, cmd, role }))
      : [];
    return json(res, 200, { epoch: EPOCH, latest: r.seq, commands });
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
    });
    return;
  }

  json(res, 404, { error: "not found" });
});

server.listen(PORT, () => console.log(`Staro relay listening on :${PORT}`));
