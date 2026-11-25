// ToyVisual sync relay: tells ToyVisual players on the same Minecraft server which skin and emote each of them uses.
// Clients connect over WebSocket, say hello with their room (the Minecraft server address) and state, and get a
// snapshot of everyone else in that room plus live updates. Nothing is stored: state lives only while connected.
//
//   PORT=8787 node index.js
import http from "node:http";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 8787);
const SKINS = new Set(["", "verity", "dachshund"]);
const EMOTES = new Set([
  "", "verity_floss", "verity_griddy", "verity_noodle_wave", "verity_pogo", "verity_robot", "verity_twister",
  "dog_sit_up", "dog_zoomies",
]);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NAME_RE = /^[^\s"\\<>]{1,16}$/;
const MAX_MESSAGE = 512;
const MAX_PER_ROOM = 500;

/** room -> Map<uuid, client> */
const rooms = new Map();

function clean(msg) {
  const uuid = String(msg.uuid || "").toLowerCase();
  const name = String(msg.name || "");
  const skin = String(msg.skin || "");
  const emote = String(msg.emote || "");
  if (!UUID_RE.test(uuid) || !NAME_RE.test(name) || !SKINS.has(skin) || !EMOTES.has(emote)) return null;
  const age = Math.max(0, Math.min(3_600_000, Number(msg.age) || 0));
  const stopAge = Number(msg.stopAge);
  return { uuid, name, skin, emote, age, stopAge: Number.isFinite(stopAge) && stopAge >= 0 ? Math.min(60_000, stopAge) : -1, at: Date.now() };
}

/** A player's state as sent out now: emote ages move on while it sits on the server. */
function outgoing(state) {
  const passed = Date.now() - state.at;
  return {
    t: "state", uuid: state.uuid, name: state.name, skin: state.skin, emote: state.emote,
    age: state.emote ? state.age + passed : 0, stopAge: state.stopAge >= 0 ? state.stopAge + passed : -1,
  };
}

function broadcast(room, except, data) {
  const members = rooms.get(room);
  if (!members) return;
  const text = JSON.stringify(data);
  for (const c of members.values()) {
    if (c !== except && c.ws.readyState === c.ws.OPEN) c.ws.send(text);
  }
}

function leave(client) {
  if (!client.room || !client.state) return;
  const members = rooms.get(client.room);
  if (members && members.get(client.state.uuid) === client) {
    members.delete(client.state.uuid);
    broadcast(client.room, client, { t: "leave", uuid: client.state.uuid });
    if (members.size === 0) rooms.delete(client.room);
  }
}

const server = http.createServer((req, res) => {
  // health check for the host
  let players = 0;
  for (const m of rooms.values()) players += m.size;
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok: true, rooms: rooms.size, players }));
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE });

wss.on("connection", (ws) => {
  const client = { ws, room: null, state: null, tokens: 10, last: Date.now() };
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (data, isBinary) => {
    // simple token bucket: about 4 messages a second, bursts of 10
    const now = Date.now();
    client.tokens = Math.min(10, client.tokens + ((now - client.last) / 1000) * 4);
    client.last = now;
    if (isBinary || client.tokens < 1) return;
    client.tokens -= 1;
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }
    if (msg.t === "hello") {
      const room = String(msg.room || "").toLowerCase().slice(0, 100);
      const state = clean(msg);
      if (!room || !state || client.room) return;
      let members = rooms.get(room);
      if (!members) rooms.set(room, (members = new Map()));
      if (members.size >= MAX_PER_ROOM && !members.has(state.uuid)) return ws.close(1013, "room full");
      // one connection per player per room: a reconnect replaces the old one
      const old = members.get(state.uuid);
      if (old && old !== client) {
        old.room = null;
        old.ws.close(4000, "replaced");
      }
      client.room = room;
      client.state = state;
      members.set(state.uuid, client);
      ws.send(JSON.stringify({ t: "snapshot", players: [...members.values()].filter((c) => c !== client).map((c) => outgoing(c.state)) }));
      broadcast(room, client, outgoing(state));
    } else if (msg.t === "state" && client.room) {
      const state = clean(msg);
      if (!state || state.uuid !== client.state.uuid) return;
      client.state = state;
      broadcast(client.room, client, outgoing(state));
    }
  });

  ws.on("close", () => leave(client));
  ws.on("error", () => leave(client));
});

// drop dead connections
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

server.listen(PORT, () => console.log(`toyvisual sync listening on ${PORT}`));
