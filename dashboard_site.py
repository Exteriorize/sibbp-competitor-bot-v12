from __future__ import annotations

import json
import os
import zipfile
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd

DATA_DIR = Path("data")
REPORTS_DIR = Path("reports")


def refresh_dashboard_history() -> None:
    """Make the website show the same fresh snapshot as the Telegram report."""
    if os.getenv("DASHBOARD_AUTO_REFRESH", "1").lower() in {"0", "false", "no"}:
        return
    try:
        from competitor_service import load_all_competitor_snapshots
        from history_store import upsert_weekly_snapshot
        snapshots = load_all_competitor_snapshots(sync_state=True)
        for snapshot in snapshots:
            competitor = snapshot.get("competitor", {})
            upsert_weekly_snapshot(
                str(competitor.get("code", "")),
                str(competitor.get("name", "")),
                snapshot.get("stats", {}),
                lifecycle=snapshot.get("lifecycle", {}),
                freshness=(snapshot.get("freshness", {}) or {}).get("freshness_label", ""),
                entity_role=str(competitor.get("entity_role") or "competitor"),
            )
    except Exception as exc:
        print(f"Dashboard refresh failed: {exc}")


def read_csv(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.DataFrame()


def num(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def normalize_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    for col in ["count", "total_area", "avg_price", "total_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0) if col in df.columns else 0
    for col in ["competitor_code", "competitor_name", "entity_role", "data_freshness", "snapshot_date", "snapshot_datetime"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("")
    return df


def apply_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    if df.empty or period in ("all", "") or "snapshot_date" not in df.columns:
        return df
    try:
        days = int(period)
    except Exception:
        return df
    tmp = df.copy()
    tmp["_date"] = pd.to_datetime(tmp["snapshot_date"], errors="coerce")
    max_date = tmp["_date"].max()
    if pd.isna(max_date):
        return df
    cutoff = max_date - timedelta(days=days)
    return tmp.loc[tmp["_date"] >= cutoff].drop(columns=["_date"])


def latest_by_company(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "competitor_code" not in df.columns:
        return pd.DataFrame()
    df = normalize_history(df)
    sort_cols = [c for c in ["snapshot_date", "snapshot_datetime"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)
    return pd.DataFrame([g.iloc[-1].to_dict() for _, g in df.groupby("competitor_code")])


def series_by_company(df: pd.DataFrame, metric: str, company: str = "") -> dict:
    if df.empty or metric not in df.columns or "snapshot_date" not in df.columns:
        return {"labels": [], "datasets": []}
    df = normalize_history(df)
    if company:
        df = df.loc[df["competitor_code"] == company].copy()
    dates = sorted([x for x in df["snapshot_date"].astype(str).unique().tolist() if x])
    datasets = []
    for name, group in df.groupby("competitor_name"):
        by_date = group.sort_values("snapshot_date").groupby("snapshot_date")[metric].last().to_dict()
        datasets.append({"label": str(name or "Без названия"), "data": [round(num(by_date.get(d)), 2) for d in dates]})
    return {"labels": dates, "datasets": datasets}


def category_series(cat_df: pd.DataFrame, category_code: str, metric: str, company: str = "") -> dict:
    if cat_df.empty or "category_code" not in cat_df.columns:
        return {"labels": [], "datasets": []}
    df = cat_df.copy()
    if company and "competitor_code" in df.columns:
        df = df.loc[df["competitor_code"] == company].copy()
    return series_by_company(df.loc[df["category_code"] == category_code].copy(), metric)


def market_share(latest: pd.DataFrame) -> list[dict]:
    if latest.empty:
        return []
    total = num(latest["total_area"].sum())
    if total <= 0:
        return []
    rows = []
    for _, row in latest.sort_values("total_area", ascending=False).iterrows():
        area = num(row.get("total_area"))
        rows.append({
            "name": str(row.get("competitor_name") or "Без названия"),
            "area": round(area, 2),
            "share": round(area / total * 100, 1),
            "role": str(row.get("entity_role") or "competitor"),
        })
    return rows


def changes(df: pd.DataFrame) -> list[dict]:
    if df.empty or "competitor_code" not in df.columns:
        return []
    df = normalize_history(df)
    sort_cols = [c for c in ["snapshot_date", "snapshot_datetime"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)
    alerts = []
    for _, group in df.groupby("competitor_code"):
        if len(group) < 2:
            continue
        prev, cur = group.iloc[-2], group.iloc[-1]
        for metric, title, threshold in [("total_area", "Площадь", 20), ("count", "Помещения", 20), ("avg_price", "Ставка", 15)]:
            old, new = num(prev.get(metric)), num(cur.get(metric))
            if old <= 0:
                continue
            diff = round((new - old) / old * 100, 1)
            if abs(diff) >= threshold:
                alerts.append({"company": str(cur.get("competitor_name") or ""), "metric": title, "diff": diff, "old": round(old, 2), "new": round(new, 2)})
    return alerts[:10]


def payload(refresh: bool = True, period: str = "all", company: str = "") -> dict:
    if refresh:
        refresh_dashboard_history()
    history_all = normalize_history(read_csv("history.csv"))
    categories_all = normalize_history(read_csv("category_history.csv"))
    history = apply_period(history_all, period)
    categories = apply_period(categories_all, period)
    if company and not history.empty:
        history = history.loc[history["competitor_code"] == company].copy()
    if company and not categories.empty and "competitor_code" in categories.columns:
        categories = categories.loc[categories["competitor_code"] == company].copy()

    latest = latest_by_company(history)
    companies = latest_by_company(history_all)
    company_options = companies[["competitor_code", "competitor_name"]].fillna("").to_dict("records") if not companies.empty else []

    if latest.empty:
        cards = {"total_count": 0, "total_area": 0, "avg_price": 0, "total_price": 0, "own_share": 0, "competitors": 0}
    else:
        total_count = int(num(latest["count"].sum()))
        total_area = round(num(latest["total_area"].sum()), 2)
        total_price = round(num(latest["total_price"].sum()), 2)
        avg_price = round(total_price / total_area, 2) if total_area > 0 and total_price > 0 else 0
        own = latest.loc[latest.get("entity_role", "") == "own_company"]
        own_area = num(own["total_area"].sum()) if not own.empty else 0
        own_share = round(own_area / total_area * 100, 1) if total_area > 0 else 0
        competitors = int(len(latest.loc[latest.get("entity_role", "") != "own_company"]))
        cards = {"total_count": total_count, "total_area": total_area, "avg_price": avg_price, "total_price": total_price, "own_share": own_share, "competitors": competitors}

    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filters": {"period": period, "company": company},
        "company_options": company_options,
        "cards": cards,
        "latest": latest.fillna("").to_dict("records") if not latest.empty else [],
        "market_share": market_share(latest),
        "top_area": latest.sort_values("total_area", ascending=False).head(8).fillna("").to_dict("records") if not latest.empty else [],
        "changes": changes(history_all),
        "charts": {
            "area": series_by_company(history, "total_area"),
            "price": series_by_company(history, "avg_price"),
            "count": series_by_company(history, "count"),
            "commercial_area": category_series(categories, "commercial", "total_area"),
            "industrial_area": category_series(categories, "industrial", "total_area"),
        },
    }


def url_with(base_path: str, query: dict) -> str:
    clean = {k: v for k, v in query.items() if v not in (None, "")}
    return base_path + ("?" + urlencode(clean) if clean else "")


def html(refresh: bool = True, query: dict | None = None) -> str:
    query = query or {}
    period = (query.get("period") or ["all"])[0]
    company = (query.get("company") or [""])[0]
    token = os.getenv("DASHBOARD_TOKEN", "").strip()
    base_q = {"key": token} if token else {}
    data = json.dumps(payload(refresh=refresh, period=period, company=company), ensure_ascii=False)
    bot_link = os.getenv("TELEGRAM_BOT_LINK", "https://t.me/")
    download_q = urlencode(base_q)
    api_url = url_with("/api/data", {**base_q, "refresh": "1", "period": period, "company": company})
    excel_url = url_with("/download/comparison", base_q)
    portfolio_url = url_with("/download/portfolio", base_q)
    backup_url = url_with("/download/database", base_q)
    refresh_url = url_with("/", {**base_q, "refresh": "1", "period": period, "company": company})

    template = r'''<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Ельцовка-1 | Аналитика</title><script src="https://cdn.jsdelivr.net/npm/chart.js"></script><style>
:root{--line:rgba(255,255,255,.09);--text:#eef5ff;--muted:#8fa4c0;--green:#56d8a6;--blue:#6aa7ff;--yellow:#ffd36a;--bg:#09111f}*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:radial-gradient(circle at 0 0,#1d3b63 0,#09111f 35%,#050912 100%);color:var(--text)}.wrap{max-width:1320px;margin:0 auto;padding:28px}.hero{display:flex;justify-content:space-between;gap:20px;margin-bottom:18px}.badge{display:inline-block;background:var(--green);color:#062015;font-weight:800;border-radius:999px;padding:8px 12px;font-size:13px}h1{font-size:36px;letter-spacing:-.04em;margin:14px 0 8px}p{margin:0;color:var(--muted);line-height:1.5}.actions{display:flex;flex-wrap:wrap;gap:10px;margin:18px 0}.btn{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);background:rgba(255,255,255,.06);color:var(--text);text-decoration:none;border-radius:14px;padding:11px 14px;font-weight:750}.btn.primary{background:var(--green);color:#062015}.btn.blue{background:rgba(106,167,255,.18);border-color:rgba(106,167,255,.38)}.filters{display:flex;flex-wrap:wrap;gap:10px;margin:0 0 18px}.chip,.select{border:1px solid var(--line);background:rgba(255,255,255,.05);color:var(--text);border-radius:999px;padding:9px 12px}.chip.active{background:var(--blue);color:#061629}.select{border-radius:14px}.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:14px}.card{background:linear-gradient(180deg,rgba(255,255,255,.065),rgba(255,255,255,.025));border:1px solid var(--line);border-radius:22px;padding:18px;box-shadow:0 20px 60px rgba(0,0,0,.25)}.kpi{min-height:118px}.label{color:var(--muted);font-size:13px}.value{font-size:27px;font-weight:900;letter-spacing:-.035em;margin-top:10px}.sub{color:#a8bbd3;font-size:12px;margin-top:8px}.s2{grid-column:span 2}.s3{grid-column:span 3}.s4{grid-column:span 4}.s6{grid-column:span 6}h2{font-size:18px;margin:0 0 14px}.chart{height:330px}table{width:100%;border-collapse:collapse}th,td{padding:12px 10px;border-bottom:1px solid var(--line);font-size:13px;text-align:left}th{color:#bfd0e5;background:rgba(255,255,255,.04)}.own{color:var(--green);font-weight:800}.comp{color:var(--blue);font-weight:800}.list{display:grid;gap:10px}.item{display:flex;justify-content:space-between;gap:12px;padding:12px;border-radius:16px;background:rgba(255,255,255,.045)}.item span{color:var(--muted);font-size:12px}.warn{border-left:4px solid var(--yellow)}@media(max-width:980px){.grid{grid-template-columns:1fr}.s2,.s3,.s4,.s6{grid-column:span 1}.hero{flex-direction:column}}
</style></head><body><div class="wrap"><section class="hero"><div><div class="badge">● Онлайн-дашборд</div><h1>Аналитика конкурентов Ельцовка-1</h1><p>Свободные площади, ставки, динамика, категории и доля рынка.</p></div><p>Обновлено: <b id="updated"></b></p></section>
<div class="actions"><a class="btn primary" href="__REFRESH_URL__">🔄 Обновить данные</a><a class="btn blue" href="__EXCEL_URL__">📊 Скачать Excel сравнение</a><a class="btn" href="__PORTFOLIO_URL__">📥 Excel вся база</a><a class="btn" href="__BACKUP_URL__">📦 Скачать базу</a><a class="btn" href="__API_URL__" target="_blank">{{}} API данные</a><a class="btn" href="__BOT_LINK__" target="_blank">💬 Открыть Telegram</a></div>
<div class="filters"><button class="chip" data-period="all">Все время</button><button class="chip" data-period="7">7 дней</button><button class="chip" data-period="14">14 дней</button><button class="chip" data-period="30">30 дней</button><select class="select" id="companySelect"><option value="">Все компании</option></select><button class="chip" id="resetFilters">Сбросить</button></div>
<section class="grid" id="cards"></section><section class="grid" style="margin-top:14px"><div class="card s3"><h2>Динамика площади</h2><div class="chart"><canvas id="areaChart"></canvas></div></div><div class="card s3"><h2>Динамика ставки</h2><div class="chart"><canvas id="priceChart"></canvas></div></div><div class="card s3"><h2>Офисы / торговые</h2><div class="chart"><canvas id="commercialChart"></canvas></div></div><div class="card s3"><h2>Склады / производства</h2><div class="chart"><canvas id="industrialChart"></canvas></div></div><div class="card s4"><h2>Доля рынка по площади</h2><div class="chart"><canvas id="shareChart"></canvas></div></div><div class="card s2"><h2>Топ по площади</h2><div class="list" id="topArea"></div></div><div class="card s6"><h2>Последний срез</h2><div style="overflow:auto"><table id="latestTable"></table></div></div><div class="card s6"><h2>Резкие изменения</h2><div class="list" id="changes"></div></div></section></div><script>
const data=__DATA_JSON__;const fmt=new Intl.NumberFormat('ru-RU');const money=v=>fmt.format(Math.round(v||0))+' ₽';const area=v=>fmt.format(Math.round((v||0)*10)/10)+' м²';const price=v=>fmt.format(Math.round((v||0)*100)/100)+' ₽/м²';const colors=['#56d8a6','#6aa7ff','#ffd36a','#ff6b6b','#b589ff','#4dd8ff','#ff9fb3','#b7f36b'];function ds(a){return a.map((x,i)=>({...x,borderColor:colors[i%colors.length],backgroundColor:colors[i%colors.length]+'33',tension:.35,pointRadius:4}))}function line(id,p,y){new Chart(document.getElementById(id),{type:'line',data:{labels:p.labels,datasets:ds(p.datasets)},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#dbe7f7'}}},scales:{x:{ticks:{color:'#9fb0c7'},grid:{color:'rgba(255,255,255,.06)'}},y:{title:{display:true,text:y,color:'#9fb0c7'},ticks:{color:'#9fb0c7'},grid:{color:'rgba(255,255,255,.06)'}}}}})}function doughnut(id,rows){new Chart(document.getElementById(id),{type:'doughnut',data:{labels:rows.map(x=>x.name),datasets:[{data:rows.map(x=>x.share),backgroundColor:colors}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{color:'#dbe7f7'}}}}})}function go(params){const u=new URL(location.href);Object.entries(params).forEach(([k,v])=>{if(v){u.searchParams.set(k,v)}else{u.searchParams.delete(k)}});u.searchParams.set('refresh','1');location.href=u.toString()}document.querySelectorAll('[data-period]').forEach(b=>{b.onclick=()=>go({period:b.dataset.period});if((data.filters.period||'all')===b.dataset.period)b.classList.add('active')});const sel=document.getElementById('companySelect');data.company_options.forEach(x=>{const o=document.createElement('option');o.value=x.competitor_code;o.textContent=x.competitor_name;sel.appendChild(o)});sel.value=data.filters.company||'';sel.onchange=()=>go({company:sel.value});document.getElementById('resetFilters').onclick=()=>go({period:'all',company:''});document.getElementById('updated').textContent=data.updated_at;document.getElementById('cards').innerHTML=`<div class="card kpi"><div class="label">Всего помещений</div><div class="value">${fmt.format(data.cards.total_count)}</div><div class="sub">актуальный срез</div></div><div class="card kpi"><div class="label">Свободная площадь</div><div class="value">${area(data.cards.total_area)}</div><div class="sub">по рынку</div></div><div class="card kpi"><div class="label">Средняя ставка</div><div class="value">${price(data.cards.avg_price)}</div><div class="sub">средневзвешенная</div></div><div class="card kpi"><div class="label">Суммарная стоимость</div><div class="value">${money(data.cards.total_price)}</div><div class="sub">оценка месяца</div></div><div class="card kpi"><div class="label">Доля Ельцовки-1</div><div class="value">${data.cards.own_share}%</div><div class="sub">по площади</div></div><div class="card kpi"><div class="label">Конкурентов</div><div class="value">${data.cards.competitors}</div><div class="sub">в срезе</div></div>`;line('areaChart',data.charts.area,'Площадь, м²');line('priceChart',data.charts.price,'₽/м²');line('commercialChart',data.charts.commercial_area,'Площадь, м²');line('industrialChart',data.charts.industrial_area,'Площадь, м²');doughnut('shareChart',data.market_share);document.getElementById('topArea').innerHTML=data.top_area.length?data.top_area.map((r,i)=>`<div class="item"><div><b>${i+1}. ${r.competitor_name||'Без названия'}</b><br><span>${fmt.format(r.count||0)} помещ.</span></div><b>${area(r.total_area||0)}</b></div>`).join(''):'<p>Нет данных</p>';document.getElementById('latestTable').innerHTML=`<thead><tr><th>Компания</th><th>Роль</th><th>Помещений</th><th>Площадь</th><th>Ставка</th><th>Стоимость</th><th>Свежесть</th></tr></thead><tbody>${data.latest.map(r=>`<tr><td><b>${r.competitor_name||'Без названия'}</b></td><td class="${r.entity_role==='own_company'?'own':'comp'}">${r.entity_role==='own_company'?'Моя компания':'Конкурент'}</td><td>${fmt.format(r.count||0)}</td><td>${area(r.total_area||0)}</td><td>${price(r.avg_price||0)}</td><td>${money(r.total_price||0)}</td><td>${r.data_freshness||'—'}</td></tr>`).join('')}</tbody>`;document.getElementById('changes').innerHTML=data.changes.length?data.changes.map(x=>`<div class="item warn"><div><b>${x.company}</b><br><span>${x.metric}: ${x.old} → ${x.new}</span></div><b>${x.diff>0?'+':''}${x.diff}%</b></div>`).join(''):'<p>Сильных изменений пока нет.</p>';
</script></body></html>'''
    return (template
        .replace("__DATA_JSON__", data)
        .replace("__REFRESH_URL__", refresh_url)
        .replace("__EXCEL_URL__", excel_url)
        .replace("__PORTFOLIO_URL__", portfolio_url)
        .replace("__BACKUP_URL__", backup_url)
        .replace("__API_URL__", api_url)
        .replace("__BOT_LINK__", bot_link))


def make_database_zip() -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    path = REPORTS_DIR / f"dashboard_database_{datetime.now():%Y-%m-%d_%H-%M}.zip"
    files = [
        DATA_DIR / "manual_competitors.json",
        DATA_DIR / "manual_items.json",
        DATA_DIR / "manual_records.json",
        DATA_DIR / "history.csv",
        DATA_DIR / "category_history.csv",
        DATA_DIR / "item_registry.csv",
        DATA_DIR / "change_log.csv",
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            if file.exists():
                zf.write(file, file.as_posix())
    return path


class DashboardHandler(BaseHTTPRequestHandler):
    def send_body(self, status: int, body: bytes, content_type: str = "text/html; charset=utf-8", filename: str = "") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(body)

    def check_token(self, query: dict) -> bool:
        token = os.getenv("DASHBOARD_TOKEN", "").strip()
        return not token or (query.get("key") or [""])[0] == token

    def send_file(self, path: Path, content_type: str) -> None:
        self.send_body(200, path.read_bytes(), content_type, path.name)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path in ("/health", "/healthz"):
            self.send_body(200, b"OK", "text/plain; charset=utf-8")
            return
        if not self.check_token(query):
            self.send_body(403, "Доступ закрыт. Нужна корректная ссылка с ключом.".encode("utf-8"), "text/plain; charset=utf-8")
            return
        if parsed.path == "/api/data":
            self.send_body(200, json.dumps(payload(refresh=(query.get("refresh") or ["1"])[0] != "0", period=(query.get("period") or ["all"])[0], company=(query.get("company") or [""])[0]), ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return
        if parsed.path == "/download/comparison":
            refresh_dashboard_history()
            from dynamics_report import create_role_comparison_report
            from history_store import get_role_comparison_history
            self.send_file(Path(create_role_comparison_report(get_role_comparison_history())), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            return
        if parsed.path == "/download/portfolio":
            refresh_dashboard_history()
            from competitor_service import load_all_competitor_snapshots
            from portfolio_report import create_portfolio_report
            self.send_file(Path(create_portfolio_report(load_all_competitor_snapshots(sync_state=True))), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            return
        if parsed.path == "/download/database":
            self.send_file(make_database_zip(), "application/zip")
            return
        if parsed.path not in ("/", "/dashboard"):
            self.send_body(404, b"Not found", "text/plain; charset=utf-8")
            return
        self.send_body(200, html(refresh=(query.get("refresh") or ["1"])[0] != "0", query=query).encode("utf-8"))

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        return
