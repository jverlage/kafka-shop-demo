'use strict';

const MAX_CHIPS = 12;
const SERVICE_ICONS = { inventory: '📦', payment: '💳', analytics: '📊', shipping: '🚚', notification: '✉️' };
const CONSUMER_ORDER = ['inventory', 'analytics', 'payment', 'shipping', 'notification'];
const PRODUCER_OF = { orders: 'shop', payments: 'payment', shipments: 'shipping' };

const CAPTIONS = {
  1: '<strong>Fan-out:</strong> Der Shop schreibt jede Bestellung <strong>einmal</strong> nach Kafka – jeder Service liest sie unabhängig.',
  2: '<strong>Partitionen:</strong> Der Key (Kunde) bestimmt die Partition – so bleibt die Reihenfolge pro Kunde erhalten.',
  3: '<strong>Consumer Groups:</strong> Fällt ein Service aus, hebt Kafka die Nachrichten auf – nichts geht verloren.',
  4: '<strong>Event-Kette:</strong> Services reagieren auf Events und schreiben neue Events zurück nach Kafka.',
};

// Each arrow in the picture. Produce arrows point into Kafka, consume arrows point out of it.
const FLOWS = [
  { id: 'shop>orders', kind: 'produce', stage: 1, service: null, from: () => edge('#shop', 'right'), to: () => topicEdge('orders', 'left') },
  ...['inventory', 'analytics', 'payment'].map((s, i) => ({
    id: `orders>${s}`, kind: 'consume', stage: 1, service: s, from: () => topicEdge('orders', 'right', (i - 1) * 0.9), to: () => cardEdge(s, 'in'),
  })),
  { id: 'payment>payments', kind: 'produce', stage: 4, service: 'payment', from: () => cardEdge('payment', 'out'), to: () => topicEdge('payments', 'right', -0.45) },
  { id: 'payments>shipping', kind: 'consume', stage: 4, service: 'shipping', from: () => topicEdge('payments', 'right', 0.45), to: () => cardEdge('shipping', 'in') },
  { id: 'shipping>shipments', kind: 'produce', stage: 4, service: 'shipping', from: () => cardEdge('shipping', 'out'), to: () => topicEdge('shipments', 'right', -0.45) },
  { id: 'shipments>notification', kind: 'consume', stage: 4, service: 'notification', from: () => topicEdge('shipments', 'right', 0.45), to: () => cardEdge('notification', 'in') },
];

const state = {
  config: null,
  stage: 1,
  tempo: 1600,
  generation: 0,
  selectedCustomer: null,
  services: {},
  counts: {},
  last: {},
  lag: {},
  simulator: { enabled: false, rate: 2 },
  chips: { orders: [], payments: [], shipments: [] },
  freshChips: new Set(),
  keyPartitions: new Map(),
  resetting: false,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const esc = (value) => String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const customer = (id) => state.config?.customers.find((c) => c.id === id);
const colorFor = (customerId) => (state.stage >= 2 ? customer(customerId)?.color : undefined);
const chipKey = (topic, partition, offset) => `chip:${topic}:${partition}:${offset}`;
const storage = {
  get(key) { try { return localStorage.getItem(key); } catch { return null; } },
  set(key, value) { try { localStorage.setItem(key, value); } catch { /* storage unavailable */ } },
};
const stageEl = $('#stage');
const flowsSvg = $('#flows');

// ---------- Server ----------

function connect() {
  const socket = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
  socket.onopen = () => setConnected(true);
  socket.onclose = () => {
    setConnected(false);
    setTimeout(connect, 1000);
  };
  socket.onmessage = (event) => handleEvent(JSON.parse(event.data));
}

async function post(url, body = {}) {
  try {
    const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error ?? res.statusText);
    return data;
  } catch (err) {
    toast(err.message, true);
    return null;
  }
}

function handleEvent(event) {
  switch (event.type) {
    case 'snapshot': return applySnapshot(event);
    case 'message': onMessage(event, true); break;
    case 'processed': onProcessed(event, true); break;
    case 'status': state.services[event.service] = event; break;
    case 'lag': state.lag = event.lag; break;
    case 'simulator': state.simulator = { enabled: event.enabled, rate: event.rate }; renderSimulator(); break;
    case 'resetting': state.resetting = true; break;
  }
  scheduleRender();
}

function applySnapshot(snapshot) {
  const firstTime = !state.config;
  state.generation++;
  for (const token of $$('.token')) token.remove();
  lanes.clear();
  waits.clear();

  state.config = snapshot.config;
  state.resetting = snapshot.resetting;
  state.simulator = snapshot.simulator;
  state.lag = snapshot.lag ?? {};
  state.services = {};
  state.counts = {};
  for (const s of snapshot.services) {
    state.services[s.service] = s;
    state.counts[s.service] = s.processed ?? 0;
  }
  state.chips = { orders: [], payments: [], shipments: [] };
  state.freshChips.clear();
  state.last = {};
  state.keyPartitions = new Map();

  if (firstTime) buildLayout();
  for (const event of snapshot.recent) {
    if (event.type === 'message') onMessage(event, false);
    if (event.type === 'processed') onProcessed(event, false);
  }
  renderSimulator();
  scheduleRender();
}

// ---------- Visual pipeline ----------
// Real events arrive within milliseconds. The picture replays them in the order data actually
// flows: into the topic first, then out to the consumer, then (for a chain) back into Kafka.

const waits = new Map();
function waitEntry(key) {
  let entry = waits.get(key);
  if (!entry) {
    let resolve;
    const promise = new Promise((r) => (resolve = r));
    entry = { promise, resolve };
    waits.set(key, entry);
    if (waits.size > 3000) for (const k of [...waits.keys()].slice(0, 1000)) waits.delete(k);
  }
  return entry;
}
const signal = (key) => waitEntry(key).resolve();
const waitFor = (key) => Promise.race([waitEntry(key).promise, sleep(state.tempo * 6)]);

function onMessage(msg, live) {
  if (!state.chips[msg.topic]) return;
  if (msg.topic === 'orders') state.keyPartitions.set(msg.key, msg.partition);
  const key = chipKey(msg.topic, msg.partition, msg.offset);
  if (!live) {
    addChip(msg, false);
    signal(key);
    return;
  }
  const generation = state.generation;
  const producer = PRODUCER_OF[msg.topic];
  const ready = producer === 'shop' ? Promise.resolve() : waitFor(`svc:${producer}:${msg.value.orderId}`);
  ready
    .then(() => generation === state.generation && travel(`${producer}>${msg.topic}`, msg.value))
    .then(() => {
      if (generation !== state.generation) return;
      addChip(msg, true);
      signal(key);
    });
}

function onProcessed(event, live) {
  if (!live) {
    state.last[event.service] = event;
    return;
  }
  const generation = state.generation;
  waitFor(chipKey(event.topic, event.partition, event.offset))
    .then(() => sleep(Math.min(300, state.tempo / 5)))
    .then(() => generation === state.generation && travel(`${event.topic}>${event.service}`, event))
    .then(() => {
      if (generation !== state.generation) return;
      state.counts[event.service] = (state.counts[event.service] ?? 0) + 1;
      state.last[event.service] = event;
      hit(event.service);
      signal(`svc:${event.service}:${event.orderId}`);
      scheduleRender();
    });
}

function addChip(msg, fresh) {
  const list = state.chips[msg.topic];
  list.push(msg);
  if (list.length > 120) list.splice(0, list.length - 120);
  const id = chipKey(msg.topic, msg.partition, msg.offset);
  if (fresh) state.freshChips.add(id);
  scheduleRender();
}

// One queue per arrow: labels start with a minimum gap so they stay readable. When too many are
// waiting (simulator at high rate), single labels are skipped and only their effect is shown.
const lanes = new Map();
function travel(flowId, payload) {
  const flow = flowEls.get(flowId);
  if (!flow || flow.el.classList.contains('hidden')) return Promise.resolve();
  let lane = lanes.get(flowId);
  if (!lane) lanes.set(flowId, (lane = { nextStart: 0, inflight: 0 }));
  const now = performance.now();
  const start = Math.max(now, lane.nextStart);
  if (start - now > state.tempo * 2.5) {
    pulse(flow.el, lane);
    return Promise.resolve();
  }
  lane.nextStart = start + state.tempo * 0.55;
  return sleep(start - now).then(() => runToken(flow.el, lane, payload));
}

function runToken(path, lane, payload) {
  return new Promise((resolve) => {
    const generation = state.generation;
    const token = document.createElement('div');
    token.className = 'token';
    token.textContent = payload.orderId;
    const color = colorFor(payload.customerId);
    if (color) token.style.setProperty('--c', color);
    stageEl.append(token);
    const w = token.offsetWidth;
    const h = token.offsetHeight;
    lane.inflight++;
    path.classList.add('active');
    const duration = state.tempo;
    const t0 = performance.now();

    const finish = () => {
      token.remove();
      lane.inflight--;
      if (lane.inflight <= 0) path.classList.remove('active');
      resolve();
    };
    const step = (now) => {
      if (generation !== state.generation || !token.isConnected) return finish();
      const k = Math.min(1, (now - t0) / duration);
      const eased = k < 0.5 ? 2 * k * k : 1 - (-2 * k + 2) ** 2 / 2;
      try {
        const point = path.getPointAtLength(eased * path.getTotalLength());
        token.style.transform = `translate(${point.x - w / 2}px, ${point.y - h / 2}px)`;
      } catch {
        return finish();
      }
      if (k < 1) requestAnimationFrame(step);
      else finish();
    };
    requestAnimationFrame(step);
  });
}

function pulse(path, lane) {
  path.classList.add('active');
  clearTimeout(lane.pulse);
  lane.pulse = setTimeout(() => {
    if (lane.inflight <= 0) path.classList.remove('active');
  }, 500);
}

const hitTimers = {};
function hit(service) {
  const card = $(`#card-${service}`);
  if (!card) return;
  card.classList.add('hit');
  clearTimeout(hitTimers[service]);
  hitTimers[service] = setTimeout(() => card.classList.remove('hit'), 450);
}

// ---------- Geometry ----------

function rel(el) {
  const box = el.getBoundingClientRect();
  const origin = stageEl.getBoundingClientRect();
  const left = box.left - origin.left + stageEl.scrollLeft;
  const top = box.top - origin.top + stageEl.scrollTop;
  return { left, top, right: left + box.width, bottom: top + box.height, width: box.width, height: box.height };
}

function edge(selector, side) {
  const r = rel($(selector));
  return { x: side === 'right' ? r.right : r.left, y: r.top + r.height / 2 };
}

// offsetRem fans out arrows that leave the same topic, so their labels do not start on top of each other.
function topicEdge(topic, side, offsetRem = 0) {
  const kafka = rel($('#kafka'));
  const t = rel($(`.topic[data-topic="${topic}"]`));
  const rem = parseFloat(getComputedStyle(document.documentElement).fontSize);
  return { x: side === 'right' ? kafka.right : kafka.left, y: t.top + t.height / 2 + offsetRem * rem * 2 };
}

function cardEdge(service, direction) {
  const r = rel($(`#card-${service}`));
  const writesBack = state.stage >= 4 && (service === 'payment' || service === 'shipping');
  const ratio = direction === 'out' ? 0.72 : writesBack ? 0.32 : 0.5;
  return { x: r.left, y: r.top + r.height * ratio };
}

function curve(a, b) {
  const pad = 8;
  const forward = b.x > a.x;
  const x1 = a.x + (forward ? pad : -pad);
  const x2 = b.x + (forward ? -pad - 4 : pad + 4);
  const dx = Math.abs(x2 - x1) / 2;
  const c1 = forward ? x1 + dx : x1 - dx;
  const c2 = forward ? x2 - dx : x2 + dx;
  return `M ${x1} ${a.y} C ${c1} ${a.y}, ${c2} ${b.y}, ${x2} ${b.y}`;
}

const flowEls = new Map();
function buildFlows() {
  for (const flow of FLOWS) {
    const el = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    el.classList.add('flow', flow.kind);
    el.setAttribute('marker-end', `url(#head-${flow.kind})`);
    flowsSvg.append(el);
    flowEls.set(flow.id, { ...flow, el });
  }
}

let layoutQueued = false;
function scheduleLayout() {
  if (layoutQueued) return;
  layoutQueued = true;
  requestAnimationFrame(() => {
    layoutQueued = false;
    layoutFlows();
  });
}

function layoutFlows() {
  flowsSvg.setAttribute('width', stageEl.scrollWidth);
  flowsSvg.setAttribute('height', stageEl.scrollHeight);
  for (const flow of flowEls.values()) {
    const visible = state.stage >= flow.stage;
    flow.el.classList.toggle('hidden', !visible);
    flow.el.style.display = visible ? '' : 'none';
    if (visible) flow.el.setAttribute('d', curve(flow.from(), flow.to()));
  }
}

function updateFlowStates() {
  for (const flow of flowEls.values()) {
    const status = flow.service ? state.services[flow.service] : null;
    const off = status ? !status.online || !status.instances?.length : false;
    flow.el.classList.toggle('off', off);
    flow.el.setAttribute('marker-end', `url(#head-${off ? 'off' : flow.kind})`);
  }
}

// ---------- Layout ----------

function buildLayout() {
  const { customers, products, services } = state.config;
  $('#customers').innerHTML =
    `<button class="customer selected" data-customer=""><span class="swatch" style="--c: conic-gradient(${customers.map((c) => c.color).join(',')})"></span>Zufall</button>` +
    customers.map((c) => `<button class="customer" data-customer="${c.id}" style="--c:${c.color}"><span class="swatch"></span>${esc(c.name)}<span class="part"></span></button>`).join('');
  $('#products').innerHTML = products
    .map((p) => `<button class="product" data-product="${p.id}"><span class="icon">${p.icon}</span><span class="name">${esc(p.name)}</span><span class="price">${p.price} €</span></button>`)
    .join('');

  const byName = Object.fromEntries(services.map((s) => [s.name, s]));
  $('#consumers').innerHTML = CONSUMER_ORDER.map((name) => cardHtml(byName[name])).join('');
  buildFlows();

  const observer = new ResizeObserver(scheduleLayout);
  for (const el of [stageEl, $('#shop'), $('#kafka'), ...$$('.card')]) observer.observe(el);
  addEventListener('resize', scheduleLayout);
  applyStage(state.stage);
}

function cardHtml(def) {
  const chainOnly = def.name === 'shipping' || def.name === 'notification';
  return `
    <article class="card" id="card-${def.name}" data-service="${def.name}" ${chainOnly ? 'data-min-stage="4"' : ''}>
      <div class="card-top">
        <span class="card-icon">${SERVICE_ICONS[def.name]}</span>
        <span class="card-name">${esc(def.label)}</span>
        <span class="badge"></span>
        <span class="card-count">0</span>
      </div>
      <div class="card-last"></div>
      <div class="ops" data-min-stage="3">
        <div class="insts"></div>
        <div class="ops-row">
          <span class="lag ok">Lag 0</span>
          <button class="op" data-action="toggle"></button>
          <span class="op-label">Instanzen</span>
          <button class="op" data-action="scale-down" title="Instanz entfernen" aria-label="Instanz entfernen">−</button>
          <button class="op" data-action="scale-up" title="Instanz hinzufügen" aria-label="Instanz hinzufügen">+</button>
        </div>
      </div>
    </article>`;
}

function applyStage(stage) {
  state.stage = stage;
  storage.set('stage', String(stage));
  document.body.dataset.stage = String(stage);
  for (const el of $$('[data-min-stage]')) el.hidden = stage < Number(el.dataset.minStage);
  for (const btn of $$('.stages button')) btn.classList.toggle('active', Number(btn.dataset.stage) === stage);
  $('#caption').innerHTML = CAPTIONS[stage];
  render();
  scheduleLayout();
}

// ---------- Rendering ----------

let renderQueued = false;
function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => {
    renderQueued = false;
    render();
  });
}

function render() {
  if (!state.config) return;
  $('#overlay').hidden = !state.resetting;
  for (const topic of Object.keys(state.chips)) renderTopic(topic);
  for (const name of CONSUMER_ORDER) renderCard(name);
  renderCustomers();
  updateFlowStates();
}

function renderTopic(topic) {
  const container = $(`.topic[data-topic="${topic}"] .lanes`);
  if (!container || container.offsetParent === null) return;
  const chips = state.chips[topic];
  // Labels can land out of order when some animations are skipped, but a partition is an ordered log.
  const byOffset = (a, b) => Number(a.offset) - Number(b.offset);
  const byOrder = (a, b) => Number(a.value.orderId.slice(2)) - Number(b.value.orderId.slice(2));
  const lanes =
    state.stage < 2
      ? [{ label: '', items: [...chips].sort(byOrder).slice(-MAX_CHIPS) }]
      : [0, 1, 2].map((p) => ({ label: `P${p}`, items: chips.filter((m) => m.partition === p).sort(byOffset).slice(-MAX_CHIPS) }));
  container.innerHTML = lanes
    .map((lane) => {
      const html = lane.items
        .map((m) => {
          const id = chipKey(m.topic, m.partition, m.offset);
          const fresh = state.freshChips.delete(id) ? ' enter' : '';
          const color = colorFor(m.value.customerId);
          return `<span class="chip${fresh}"${color ? ` style="--c:${color}"` : ''}>${esc(m.value.orderId)}</span>`;
        })
        .join('');
      const label = lane.label ? `<span class="lane-label">${lane.label}</span>` : '';
      return `<div class="lane">${label}<div class="lane-track">${html}</div></div>`;
    })
    .join('');
}

function renderCard(name) {
  const card = $(`#card-${name}`);
  if (!card || card.hidden) return;
  const status = state.services[name] ?? { online: false, instances: [] };
  const instances = status.instances ?? [];
  const max = state.config.maxInstances;

  card.classList.toggle('offline', !status.online);
  card.classList.toggle('stopped', status.online && instances.length === 0);
  const badge = $('.badge', card);
  if (!status.online) setBadge(badge, 'bad', 'offline');
  else if (instances.length === 0) setBadge(badge, 'warn', 'gestoppt');
  else setBadge(badge, '', '');

  $('.card-count', card).textContent = state.counts[name] ?? 0;
  const last = state.last[name];
  $('.card-last', card).innerHTML = last ? `<b>${esc(last.orderId)}</b> · ${esc(last.info)}` : 'wartet auf Bestellungen …';

  if (state.stage !== 3) return;
  $('.insts', card).innerHTML = instances.length
    ? instances
        .map((inst, i) => {
          const parts = inst.partitions.length
            ? inst.partitions.map((p) => `<span class="p">P${p}</span>`).join('')
            : '<span class="idle">idle</span>';
          return `<span class="inst" title="${esc(inst.id)}"><span class="n">#${i + 1}</span>${parts}</span>`;
        })
        .join('')
    : `<span class="inst none">${status.online ? 'keine Instanz' : 'Container gestoppt'}</span>`;

  const lag = state.lag[name]?.total ?? 0;
  const lagEl = $('.lag', card);
  lagEl.textContent = `Lag ${lag}`;
  lagEl.className = `lag ${lag === 0 ? 'ok' : lag <= 20 ? 'warn' : 'bad'}`;

  const toggle = $('[data-action="toggle"]', card);
  toggle.textContent = instances.length ? '■ Stop' : '▶ Start';
  toggle.dataset.command = instances.length ? 'stop' : 'start';
  toggle.disabled = !status.online;
  $('[data-action="scale-down"]', card).disabled = !status.online || instances.length === 0;
  $('[data-action="scale-up"]', card).disabled = !status.online || instances.length >= max;
}

function setBadge(el, kind, text) {
  el.className = `badge ${kind}`;
  el.textContent = text;
}

function renderCustomers() {
  if (state.stage < 2) return;
  for (const btn of $$('.customer[data-customer]')) {
    const id = btn.dataset.customer;
    if (!id) continue;
    const p = state.keyPartitions.get(id);
    $('.part', btn).textContent = p === undefined ? '' : `→ P${p}`;
  }
}

function renderSimulator() {
  $('#sim-toggle').checked = state.simulator.enabled;
  $('#sim-rate').value = state.simulator.rate;
  $('#sim-rate-label').textContent = `${state.simulator.rate}/s`;
}

function renderTempo() {
  $('#tempo').value = state.tempo;
  $('#tempo-label').textContent = `${(state.tempo / 1000).toFixed(1).replace('.', ',')} s`;
}

function setConnected(connected) {
  $('#conn').classList.toggle('online', connected);
  $('.conn-label').textContent = connected ? 'live' : 'getrennt – verbinde neu …';
}

function toast(message, isError = false) {
  const el = document.createElement('div');
  el.className = `toast${isError ? ' error' : ''}`;
  el.textContent = message;
  $('#toasts').append(el);
  setTimeout(() => el.remove(), isError ? 6000 : 2500);
}

// ---------- Interaction ----------

function placeOrder(productId) {
  return post('/api/orders', { customerId: state.selectedCustomer ?? undefined, productId });
}

document.addEventListener('click', async (e) => {
  const stageBtn = e.target.closest('.stages button');
  if (stageBtn) return applyStage(Number(stageBtn.dataset.stage));

  const customerBtn = e.target.closest('[data-customer]');
  if (customerBtn) {
    state.selectedCustomer = customerBtn.dataset.customer || null;
    for (const b of $$('[data-customer]')) b.classList.toggle('selected', b === customerBtn);
    return;
  }

  const productBtn = e.target.closest('[data-product]');
  if (productBtn) return placeOrder(productBtn.dataset.product);

  const opBtn = e.target.closest('[data-action]');
  if (opBtn) {
    const service = opBtn.closest('[data-service]').dataset.service;
    const action = opBtn.dataset.action === 'toggle' ? opBtn.dataset.command : opBtn.dataset.action;
    opBtn.disabled = true;
    const result = await post(`/api/services/${service}/${action}`);
    if (result) state.services[service] = { ...state.services[service], online: true, instances: result.instances };
    scheduleRender();
  }
});

$('#burst').addEventListener('click', () => post('/api/orders/burst', { count: 20 }));
$('#reset').addEventListener('click', () => {
  if (confirm('Alle Bestellungen löschen und Demo zurücksetzen?')) post('/api/reset');
});
$('#sim-toggle').addEventListener('change', (e) => post('/api/simulator', { enabled: e.target.checked }));
$('#sim-rate').addEventListener('input', (e) => ($('#sim-rate-label').textContent = `${e.target.value}/s`));
$('#sim-rate').addEventListener('change', (e) => post('/api/simulator', { rate: Number(e.target.value) }));
$('#tempo').addEventListener('input', (e) => {
  state.tempo = Number(e.target.value);
  storage.set('tempo', String(state.tempo));
  renderTempo();
});

$('#theme-toggle').addEventListener('click', () => {
  const root = document.documentElement;
  const dark = root.dataset.theme ? root.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  root.dataset.theme = dark ? 'light' : 'dark';
  storage.set('theme', root.dataset.theme);
});

document.addEventListener('keydown', (e) => {
  if (e.metaKey || e.ctrlKey || e.altKey || e.target.matches('input[type="text"], textarea')) return;
  if (['1', '2', '3', '4'].includes(e.key)) applyStage(Number(e.key));
  else if (e.key === 'b') placeOrder();
  else if (e.key === 's') post('/api/simulator', { enabled: !state.simulator.enabled });
  else if (e.key === 'x') post('/api/orders/burst', { count: 20 });
});

// ---------- Start ----------

const savedTheme = storage.get('theme');
if (savedTheme) document.documentElement.dataset.theme = savedTheme;
state.tempo = Number(storage.get('tempo')) || state.tempo;
state.stage = Number(storage.get('stage')) || 1;
renderTempo();
connect();
