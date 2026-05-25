import { Elysia } from 'elysia';
import type { SyncEngine } from './engine/syncEngine';
import type { EngineActivity } from './engine/devtools';

export type SyncDevtoolsOptions = {
	/** The engine to inspect. */
	engine: SyncEngine;
	/** Route the dashboard is served from (its SSE feed is `<path>/stream`). Default `/sync/devtools`. */
	path?: string;
	/** Snapshot refresh interval (ms) — keeps subscription counts/version current. Default 2000. */
	snapshotMs?: number;
};

const dashboardHtml = (streamPath: string) => `<!doctype html>
<html lang="en"><head><meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>@absolutejs/sync devtools</title>
<style>
:root{color-scheme:dark}
body{margin:0;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;background:#0b0e14;color:#cdd6f4}
header{padding:12px 16px;border-bottom:1px solid #1c2230;display:flex;align-items:center;gap:12px}
header b{color:#89b4fa}
.ver{margin-left:auto;color:#a6e3a1}
main{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px}
section{background:#11151f;border:1px solid #1c2230;border-radius:8px;padding:12px;min-width:0}
h2{margin:0 0 8px;font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:#9399b2}
table{width:100%;border-collapse:collapse}
td,th{text-align:left;padding:4px 6px;border-bottom:1px solid #1c2230;vertical-align:top}
th{color:#7f849c;font-weight:600}
.kind{color:#f9e2af}.tables{color:#94e2d5}.subs{color:#89b4fa;text-align:right}
.log{grid-column:1/3;max-height:46vh;overflow:auto}
.row{display:flex;gap:10px;padding:3px 6px;border-bottom:1px solid #161b27}
.row time{color:#6c7086;flex:0 0 92px}
.t-change{color:#94e2d5}.t-mutation{color:#cba6f7}.err{color:#f38ba8}
.pill{padding:0 6px;border-radius:10px;background:#1c2230;flex:0 0 auto}
.empty{color:#6c7086;padding:6px}
</style></head>
<body>
<header><b>@absolutejs/sync</b> devtools <span id="status" class="empty">connecting…</span><span class="ver">v<span id="version">0</span></span></header>
<main>
<section><h2>Collections</h2><table><thead><tr><th>name</th><th>kind</th><th>tables</th><th class="subs">subs</th></tr></thead><tbody id="collections"></tbody></table></section>
<section><h2>Mutations · Schedules</h2><div id="ops"></div></section>
<section class="log"><h2>Activity</h2><div id="activity"><div class="empty">waiting for changes &amp; mutations…</div></div></section>
</main>
<script>
const $=(id)=>document.getElementById(id);
const esc=(s)=>String(s).replace(/[&<>]/g,(c)=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const renderSnapshot=(s)=>{
  $('version').textContent=s.version;
  $('collections').innerHTML=s.collections.length?s.collections.map((c)=>
    '<tr><td>'+esc(c.name)+'</td><td class="kind">'+esc(c.kind)+'</td><td class="tables">'+esc(c.tables.join(', ')||'—')+'</td><td class="subs">'+c.subscriptions+'</td></tr>').join(''):'<tr><td colspan="4" class="empty">none registered</td></tr>';
  $('ops').innerHTML=
    '<p><b>mutations</b> '+(s.mutations.map(esc).join(', ')||'<span class="empty">none</span>')+'</p>'+
    '<p><b>schedules</b> '+(s.schedules.map((x)=>esc(x.name)+' <span class="pill">'+esc(x.pattern)+'</span>').join(' ')||'<span class="empty">none</span>')+'</p>'+
    '<p><b>writers</b> '+(s.writers.map(esc).join(', ')||'<span class="empty">none</span>')+'</p>'+
    '<p><b>readers</b> '+(s.readers.map(esc).join(', ')||'<span class="empty">none</span>')+'</p>';
};
let logged=false;
const logActivity=(a)=>{
  const box=$('activity');
  if(!logged){box.innerHTML='';logged=true;}
  const t=new Date(a.at).toLocaleTimeString();
  const line=a.type==='change'
    ?'<span class="t-change">change</span><span class="pill">'+esc(a.table)+'</span>'+esc(a.op)+' <span class="empty">v'+a.version+'</span>'
    :'<span class="t-mutation '+(a.status==='error'?'err':'')+'">mutation</span><span class="pill">'+esc(a.name)+'</span>'+esc(a.status);
  const row=document.createElement('div');row.className='row';
  row.innerHTML='<time>'+t+'</time><div>'+line+'</div>';
  box.prepend(row);
  while(box.childNodes.length>200)box.removeChild(box.lastChild);
};
const src=new EventSource('${streamPath}');
src.addEventListener('open',()=>{$('status').textContent='live';$('status').className='';});
src.addEventListener('error',()=>{$('status').textContent='reconnecting…';$('status').className='empty';});
src.addEventListener('snapshot',(e)=>renderSnapshot(JSON.parse(e.data)));
src.addEventListener('activity',(e)=>logActivity(JSON.parse(e.data)));
</script></body></html>`;

/**
 * Elysia plugin: a live devtools dashboard for a {@link SyncEngine}. Mount it and
 * open `path` in a browser to watch registered collections (kind, source tables,
 * live subscription counts), mutations, schedules, readers/writers, the
 * change-feed version, and a streaming log of changes + mutation outcomes — over
 * Server-Sent Events. Read-only; safe to leave mounted in dev.
 */
export const syncDevtools = ({
	engine,
	path = '/sync/devtools',
	snapshotMs = 2000
}: SyncDevtoolsOptions) => {
	const streamPath = `${path}/stream`;

	return new Elysia({ name: '@absolutejs/sync/devtools' })
		.get(
			path,
			() =>
				new Response(dashboardHtml(streamPath), {
					headers: { 'content-type': 'text/html; charset=utf-8' }
				})
		)
		.get(streamPath, (context) => {
			const encoder = new TextEncoder();
			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					const write = (chunk: string) => {
						try {
							controller.enqueue(encoder.encode(chunk));
						} catch {
							// controller already closed by an abort race
						}
					};
					const sendSnapshot = () => {
						write(
							`event: snapshot\ndata: ${JSON.stringify(engine.inspect())}\n\n`
						);
					};
					const sendActivity = (event: EngineActivity) => {
						write(
							`event: activity\ndata: ${JSON.stringify(event)}\n\n`
						);
					};

					sendSnapshot();
					const unsubscribe = engine.onActivity(sendActivity);
					const snapshot = setInterval(sendSnapshot, snapshotMs);

					context.request.signal.addEventListener(
						'abort',
						() => {
							clearInterval(snapshot);
							unsubscribe();
							try {
								controller.close();
							} catch {
								// already closed
							}
						},
						{ once: true }
					);
				}
			});

			return new Response(stream, {
				headers: {
					'cache-control': 'no-cache, no-transform',
					connection: 'keep-alive',
					'content-type': 'text/event-stream'
				}
			});
		});
};
