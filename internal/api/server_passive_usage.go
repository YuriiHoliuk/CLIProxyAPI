package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// passiveUsage exposes only local snapshots. It cannot call a provider or expose tokens.
func (s *Server) passiveUsage(c *gin.Context) {
	accounts := s.handlers.AuthManager.List()
	loads := s.handlers.AuthManager.BalanceLoads()
	weights := map[string]int64{}
	for _, a := range accounts {
		d := auth.AccountBalance(a, time.Now(), 1, 0)
		if d.Weight > weights[a.Provider] {
			weights[a.Provider] = d.Weight
		}
	}
	entries := make([]gin.H, 0, len(accounts))
	now := time.Now()
	for _, a := range accounts {
		entries = append(entries, gin.H{"id": a.ID, "label": a.Label, "provider": a.Provider, "disabled": a.Disabled, "unavailable": a.Unavailable, "balance": auth.AccountBalance(a, now, weights[a.Provider], loads[a.ID])})
	}
	c.JSON(http.StatusOK, gin.H{"observed_at": now, "strategy": s.cfg.Routing.Strategy, "session_affinity": s.cfg.Routing.SessionAffinity, "accounts": entries})
}

const passiveDashboardHTML = `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Jetson proxy usage</title><style>body{font:16px system-ui;background:#101923;color:#e4eaf0;max-width:1100px;margin:40px auto;padding:0 20px}h1{font-size:28px}small,p{color:#adbcca}table{width:100%;border-collapse:collapse;background:#182430}th,td{text-align:left;padding:15px;border-bottom:1px solid #304151}progress{width:140px;accent-color:#64b5f6}button,input{padding:10px;background:#24384b;color:white;border:1px solid #567;border-radius:4px}#error{color:#ffbc9f}.window{margin-bottom:8px}a{color:#84c6ff}@media(max-width:650px){body{margin:24px auto}thead{display:none}table,tbody,tr,td{display:block;width:auto}tr{border-bottom:2px solid #567;padding:10px 0}td{padding:10px 15px;overflow-wrap:anywhere}td::before{content:attr(data-label);display:block;color:#adbcca;font-size:13px;margin-bottom:5px}progress{max-width:45%}}</style><h1>Jetson proxy usage</h1><p>Snapshots from proxied responses and local Claude limits. No provider usage requests.</p><p id="state">Loading…</p><details><summary>API key (only when configured on this server)</summary><input id="key" type="password" autocomplete="off"><button id="save">Apply</button></details><p id="error"></p><table><thead><tr><th>Account</th><th>Observed usage</th><th>Active sessions</th><th>Balanced score</th></tr></thead><tbody id="rows"></tbody></table><script>
const rows=document.querySelector('#rows'),state=document.querySelector('#state'),error=document.querySelector('#error');let key=sessionStorage.getItem('proxy-key')||'';document.querySelector('#key').value=key;document.querySelector('#save').onclick=()=>{key=document.querySelector('#key').value;sessionStorage.setItem('proxy-key',key);refresh()};
function cell(row,text){const td=document.createElement('td');td.dataset.label=['Account','Observed usage','Active sessions','Balanced score'][row.children.length];td.textContent=text;row.append(td);return td}
async function refresh(){try{const r=await fetch('/usage',{headers:key?{Authorization:'Bearer '+key}:{}});if(!r.ok)throw Error('Usage API returned '+r.status);const data=await r.json();rows.replaceChildren();state.textContent=data.strategy+' · session affinity '+(data.session_affinity?'on':'off')+' · updated '+new Date(data.observed_at).toLocaleTimeString();for(const a of data.accounts.sort((a,b)=>(a.provider+' '+(a.label||a.id)).localeCompare(b.provider+' '+(b.label||b.id)))){const row=document.createElement('tr');cell(row,(a.label||a.id)+' ('+a.provider+')'+(a.disabled?' · disabled':a.unavailable?' · cooldown':''));const td=cell(row,'');const windows=a.balance.windows||[];if(!windows.length)td.textContent='Unknown — no observation yet';for(const w of windows){const div=document.createElement('div');div.className='window';const progress=document.createElement('progress');progress.max=100;progress.value=w.used_percent;const text=document.createElement('span');const expired=Date.parse(w.resets_at)<=Date.parse(data.observed_at);text.textContent=' '+(w.window_seconds/3600)+'h: '+w.used_percent.toFixed(1)+'% used'+(expired?' · previous window':'');div.append(progress,text);const small=document.createElement('small');small.textContent=' · '+w.source+' · reported '+new Date(w.observed_at).toLocaleString()+' · resets '+new Date(w.resets_at).toLocaleString();div.append(document.createElement('br'),small);td.append(div)}cell(row,String(a.balance.busy));cell(row,a.balance.score.toFixed(3));rows.append(row)}error.textContent=''}catch(e){error.textContent=e.message}}
refresh();setInterval(refresh,15000);
</script></html>`

func (s *Server) passiveDashboard(c *gin.Context) {
	html := passiveDashboardHTML
	if len(s.cfg.APIKeys) == 0 {
		html = strings.Replace(html, "<details>", "<details hidden>", 1)
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
}
