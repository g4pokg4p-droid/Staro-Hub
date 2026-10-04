// Staro relay server - zero dependencies (Node 18+).  PRIVATE-ROOM edition.
//
//   node server.js
//
// No OWNER_KEY / MEMBER_KEY any more. The ROOM NAME is the password:
// every user makes their own long random room name (16-64 characters) and puts
// the same one in the script on their main account and on their stand account.
// Nobody can read or command a room without knowing its exact name.
//
// PORT : default 8787 (hosting platforms like Render set this automatically).
//
// Endpoints
//   POST /send   {room, sender, cmd}                     -> {ok:true, id}
//   GET  /poll?after=<id>[&wait=<seconds>]   header  X-Room: <room>
//                                                         -> {epoch, latest, lp, commands:[...]}
//        (the room may also be given as ?room=... for old clients)
//        With wait>0 the request is HELD (long-poll) until a command arrives
//        or the time runs out.
//   GET  /                                               -> "ok"

const http = require("http");

const PORT = process.env.PORT || 8787;

const MAX_BODY = 4096;
const MAX_CMD_LEN = 200;
const KEEP_MS = 60_000;          // commands live this long
const KEEP_MAX = 100;            // max commands kept per room
const MAX_WAIT_S = 25;
const MAX_WAITERS_PER_ROOM = 20;
const MAX_WAITERS_TOTAL = 1500;  // total held (long-poll) connections on the whole server
const MAX_ROOMS = 5000;
const ROOM_IDLE_MS = 10 * 60_000; // a room nobody touched for this long is deleted
const SWEEP_MS = 60_000;

const ROOM_RE = /^[A-Za-z0-9._-]{16,64}$/; // too-short (guessable) rooms are refused

const rooms = new Map();    // room -> { seq, epoch, seen, cmds: [{id, t, sender, cmd}] }
const waiters = new Map();  // room -> Set of { res, after, timer }
let waiterTotal = 0;

// rate limit buckets: key -> [timestamps]
const bucketSendIp = new Map();
const bucketSendRoom = new Map();
const bucketPollIp = new Map();
const bucketNewRoomIp = new Map();

function hit(bucket, key, windowMs, max) {
  const now = Date.now();
  const list = (bucket.get(key) || []).filter((t) => now - t < windowMs);
  list.push(now);
  bucket.set(key, list);
  return list.length > max; // true = over the limit
}

function sweepBucket(bucket, windowMs) {
  const now = Date.now();
  for (const [k, list] of bucket) {
    if (!list.length || now - list[list.length - 1] > windowMs) bucket.delete(k);
  }
}

function newEpoch() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

// Returns the room, creating it if needed. null = refused (too many rooms).
function getRoom(name, ip) {
  let r = rooms.get(name);
  if (r) {
    r.seen = Date.now();
    return r;
  }
  if (rooms.size >= MAX_ROOMS) return null;
  if (hit(bucketNewRoomIp, ip, 60_000, 30)) return null; // max 30 new rooms / min / IP
  // each room has its OWN epoch: if a room is deleted and re-created, stands resync
  r = { seq: 0, epoch: newEpoch(), seen: Date.now(), cmds: [] };
  rooms.set(name, r);
  return r;
}

function json(res, code, obj) {
  if (res.writableEnded || res.destroyed) return;
  const body = JSON.stringify(obj);
  res.writeHead(code, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}

function pollPayload(r, after) {
  const now = Date.now();
  r.cmds = r.cmds.filter((c) => now - c.t < KEEP_MS);
  const commands = Number.isFinite(after) && after >= 0
    ? r.cmds.filter((c) => c.id > after).map(({ id, sender, cmd }) => ({ id, sender, cmd, role: "owner" }))
    : [];
  return { epoch: r.epoch, latest: r.seq, lp: true, commands };
}

function dropWaiter(room, w) {
  if (w.dropped) return;
  w.dropped = true;
  clearTimeout(w.timer);
  waiterTotal = Math.max(0, waiterTotal - 1);
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

// periodic cleanup: idle rooms + old rate-limit entries (keeps memory flat)
setInterval(() => {
  const now = Date.now();
  for (const [name, r] of rooms) {
    if (!waiters.has(name) && now - r.seen > ROOM_IDLE_MS) rooms.delete(name);
  }
  sweepBucket(bucketSendIp, 10_000);
  sweepBucket(bucketSendRoom, 10_000);
  sweepBucket(bucketPollIp, 10_000);
  sweepBucket(bucketNewRoomIp, 60_000);
}, SWEEP_MS).unref();

const server = http.createServer((req, res) => {
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").toString().split(",")[0].trim();
  const url = new URL(req.url, "http://localhost");

  if (req.method === "GET" && url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }

  if (req.method === "GET" && url.pathname === "/poll") {
    if (hit(bucketPollIp, ip, 10_000, 100)) return json(res, 429, { error: "slow down" });
    const room = String(req.headers["x-room"] || url.searchParams.get("room") || "");
    if (!ROOM_RE.test(room)) return json(res, 400, { error: "bad room (use 16-64 letters/numbers/-_.)" });
    const r = getRoom(room, ip);
    if (!r) return json(res, 429, { error: "too many rooms" });
    const after = parseInt(url.searchParams.get("after"), 10);
    const wait = Math.min(Math.max(parseInt(url.searchParams.get("wait"), 10) || 0, 0), MAX_WAIT_S);

    const payload = pollPayload(r, after);
    const canHold = wait > 0 && Number.isFinite(after) && after >= 0;
    const set = waiters.get(room);
    if (payload.commands.length > 0 || !canHold
        || (set && set.size >= MAX_WAITERS_PER_ROOM) || waiterTotal >= MAX_WAITERS_TOTAL) {
      return json(res, 200, payload);
    }

    // long-poll: hold until a command arrives or the time is up
    const w = { res, after, timer: null, dropped: false };
    w.timer = setTimeout(() => {
      dropWaiter(room, w);
      r.seen = Date.now();
      json(res, 200, pollPayload(r, after));
    }, wait * 1000);
    if (!waiters.has(room)) waiters.set(room, new Set());
    waiters.get(room).add(w);
    waiterTotal++;
    res.on("close", () => dropWaiter(room, w));
    return;
  }

  if (req.method === "POST" && url.pathname === "/send") {
    if (hit(bucketSendIp, ip, 10_000, 40)) return json(res, 429, { error: "slow down" });
    let size = 0;
    let tooBig = false;
    const chunks = [];
    req.on("data", (c) => {
      if (tooBig) return;
      size += c.length;
      if (size > MAX_BODY) {
        tooBig = true;
        json(res, 413, { error: "too big" });
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => {
      if (tooBig) return;
      let b;
      try { b = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { return json(res, 400, { error: "bad json" }); }
      if (!b || typeof b !== "object") return json(res, 400, { error: "bad json" });
      const room = String(b.room || "");
      if (!ROOM_RE.test(room)) return json(res, 400, { error: "bad room (use 16-64 letters/numbers/-_.)" });
      if (hit(bucketSendRoom, room, 10_000, 30)) return json(res, 429, { error: "slow down" }); // can't be dodged by faking the IP
      const sender = String(b.sender || "").slice(0, 40);
      const cmd = String(b.cmd || "").trim();
      if (!sender || !cmd || cmd.length > MAX_CMD_LEN) return json(res, 400, { error: "bad command" });
      const r = getRoom(room, ip);
      if (!r) return json(res, 429, { error: "too many rooms" });
      const id = ++r.seq;
      r.cmds.push({ id, t: Date.now(), sender, cmd });
      if (r.cmds.length > KEEP_MAX) r.cmds.splice(0, r.cmds.length - KEEP_MAX);
      json(res, 200, { ok: true, id });
      wake(room);
    });
    return;
  }

  json(res, 404, { error: "not found" });
});

server.listen(PORT, () => console.log(`Staro relay (private rooms) listening on :${PORT}`));
