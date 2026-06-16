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

const dashboardHtml = (
	streamPath: string,
	replayPath: string
) => `<!doctype html>
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
.replay{grid-column:1/3}
.replay-controls{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px}
.replay-controls input{background:#0b0e14;border:1px solid #1c2230;color:#cdd6f4;border-radius:4px;padding:4px 6px;font:inherit}
.replay-controls input[type="datetime-local"]{min-width:200px}
.replay-controls input[type="text"]{min-width:180px}
.replay-controls button{background:#89b4fa;border:none;color:#0b0e14;border-radius:4px;padding:5px 12px;font:inherit;font-weight:600;cursor:pointer}
.replay-controls button:hover{background:#74c7ec}
.replay-controls button:disabled{background:#1c2230;color:#6c7086;cursor:not-allowed}
.replay-controls label{color:#7f849c;display:flex;align-items:center;gap:6px}
.truncated{background:#311b1b;border:1px solid #f38ba8;color:#f38ba8;padding:6px 10px;border-radius:4px;margin-bottom:8px}
.replay-meta{color:#a6adc8;margin-bottom:8px}
.replay-meta b{color:#cdd6f4}
.replay-table{margin-top:10px}
.replay-table h3{margin:0 0 4px;color:#94e2d5;font-size:12px;font-weight:600}
.replay-rows pre{margin:0;font-size:11px;color:#a6adc8;background:#0b0e14;border:1px solid #1c2230;border-radius:4px;padding:6px;max-height:200px;overflow:auto;white-space:pre-wrap;word-break:break-word}
</style></head>
<body>
<header><b>@absolutejs/sync</b> devtools <span id="status" class="empty">connecting…</span><span class="ver">v<span id="version">0</span></span></header>
<main>
<section><h2>Collections</h2><table><thead><tr><th>name</th><th>kind</th><th>tables</th><th class="subs">subs</th></tr></thead><tbody id="collections"></tbody></table></section>
<section><h2>Mutations · Schedules</h2><div id="ops"></div></section>
<section class="replay"><h2>Point-in-time replay</h2>
<div class="replay-controls">
  <label>at <input type="datetime-local" id="replay-at" step="1" /></label>
  <label>tables <input type="text" id="replay-tables" placeholder="(all if blank — csv)" /></label>
  <label>max rows per table <input type="number" id="replay-max" value="10" min="1" max="500" style="width:64px" /></label>
  <button id="replay-go">Replay</button>
  <button id="replay-now" type="button" title="Set datetime to right now">Now</button>
</div>
<div id="replay-result"><div class="empty">Pick a date+time, optionally filter tables, then click Replay to reconstruct state at that point in the log window.</div></div>
</section>
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
const localToMs=(value)=>{
  if(!value)return null;
  const ms=new Date(value).getTime();
  return Number.isFinite(ms)?ms:null;
};
const msToLocal=(ms)=>{
  const d=new Date(ms);
  const pad=(n)=>String(n).padStart(2,'0');
  return d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate())+'T'+pad(d.getHours())+':'+pad(d.getMinutes())+':'+pad(d.getSeconds());
};
const renderReplay=(result, maxRows)=>{
  const box=$('replay-result');
  const tables=Object.keys(result.rows).sort();
  const trunc=result.truncated?'<div class="truncated">⚠ Replay truncated — log retention window doesn\\'t cover this timestamp. Result is best-effort, walked forward from the oldest retained entry.</div>':'';
  const meta='<div class="replay-meta">As of <b>version '+result.asOfVersion+'</b> at <b>'+(result.asOfAt?new Date(result.asOfAt).toLocaleString():'(no entries folded)')+'</b></div>';
  if(tables.length===0){
    box.innerHTML=trunc+meta+'<div class="empty">No rows in any table at this timestamp.</div>';
    return;
  }
  const sections=tables.map((t)=>{
    const rows=result.rows[t];
    const total=rows.length;
    const shown=rows.slice(0,maxRows);
    const more=total>maxRows?'<div class="empty">… '+(total-maxRows)+' more rows omitted</div>':'';
    return '<div class="replay-table"><h3>'+esc(t)+' <span class="empty">('+total+' row'+(total===1?'':'s')+')</span></h3><div class="replay-rows"><pre>'+esc(JSON.stringify(shown,null,2))+'</pre>'+more+'</div></div>';
  });
  box.innerHTML=trunc+meta+sections.join('');
};
const doReplay=async()=>{
  const at=localToMs($('replay-at').value);
  if(at===null){alert('Please pick a valid date+time');return;}
  const tables=$('replay-tables').value.split(',').map((s)=>s.trim()).filter(Boolean);
  const maxRows=Math.max(1,Math.min(500,parseInt($('replay-max').value)||10));
  const btn=$('replay-go');btn.disabled=true;btn.textContent='Replaying…';
  try{
    const params=new URLSearchParams();
    params.set('at',String(at));
    if(tables.length>0)params.set('tables',tables.join(','));
    const res=await fetch('${replayPath}?'+params.toString());
    if(!res.ok){throw new Error('HTTP '+res.status);}
    const result=await res.json();
    renderReplay(result,maxRows);
  }catch(e){
    $('replay-result').innerHTML='<div class="truncated">Replay failed: '+esc(e.message)+'</div>';
  }finally{
    btn.disabled=false;btn.textContent='Replay';
  }
};
$('replay-go').addEventListener('click',doReplay);
$('replay-now').addEventListener('click',()=>{$('replay-at').value=msToLocal(Date.now());});
$('replay-at').value=msToLocal(Date.now()-60*60*1000); // default: 1 hour ago
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
	const replayPath = `${path}/replay`;

	return new Elysia({ name: '@absolutejs/sync/devtools' })
		.get(
			path,
			() =>
				new Response(dashboardHtml(streamPath, replayPath), {
					headers: { 'content-type': 'text/html; charset=utf-8' }
				})
		)
		.get(replayPath, async (context) => {
			const url = new URL(context.request.url);
			const atRaw = url.searchParams.get('at');
			const atMs = atRaw === null ? NaN : Number(atRaw);
			if (!Number.isFinite(atMs)) {
				return new Response(
					JSON.stringify({
						error: 'invalid `at` — must be a numeric ms timestamp'
					}),
					{
						headers: {
							'content-type': 'application/json; charset=utf-8'
						},
						status: 400
					}
				);
			}
			const tablesRaw = url.searchParams.get('tables');
			const tables =
				tablesRaw === null || tablesRaw.length === 0
					? undefined
					: tablesRaw
							.split(',')
							.map((s) => s.trim())
							.filter((s) => s.length > 0);

			try {
				const result = await engine.replayTo(
					tables === undefined ? { at: atMs } : { at: atMs, tables }
				);
				return new Response(JSON.stringify(result), {
					headers: {
						'cache-control': 'no-store',
						'content-type': 'application/json; charset=utf-8'
					}
				});
			} catch (error) {
				return new Response(
					JSON.stringify({
						error:
							error instanceof Error
								? error.message
								: String(error)
					}),
					{
						headers: {
							'content-type': 'application/json; charset=utf-8'
						},
						status: 500
					}
				);
			}
		})
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
					'content-type': 'text/event-stream',
					// Tell nginx (and other reverse proxies) not to buffer the
					// stream — without this it holds chunks back and the SSE
					// connection tears (ERR_INCOMPLETE_CHUNKED_ENCODING).
					'x-accel-buffering': 'no'
				}
			});
		});
};
