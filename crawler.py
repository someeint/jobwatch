#!/usr/bin/env python3
"""
jobwatch - watches company career sites and LinkedIn for roles that fit a profile
and pushes matches to your phone with ntfy (https://ntfy.sh).

Usage
  python crawler.py run                 normal scheduled run (respects per-source cadence)
  python crawler.py run --all           run every source regardless of cadence
  python crawler.py run --dry-run       do everything except notify / save state
  python crawler.py selftest            hit every source, report health + sample matches
  python crawler.py test-notify         send a test push to your phone

Environment
  NTFY_TOPIC    (required to get alerts) your private ntfy topic name
  NTFY_SERVER   default https://ntfy.sh
  NTFY_TOKEN    optional access token if you self-host / reserve the topic
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import requests
import yaml
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.yaml"
STATE_PATH = ROOT / "state" / "seen.json"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
NOW = datetime.now(timezone.utc)


# --------------------------------------------------------------------------- model
@dataclass
class Job:
    source: str
    company: str
    job_id: str
    title: str
    url: str
    locations: list[str] = field(default_factory=list)
    posted: Optional[datetime] = None
    description: str = ""
    remote: bool = False
    enrich: Optional[Callable[["Job"], None]] = None   # lazy detail fetch (Workday)
    needs_enrich: bool = False

    @property
    def key(self) -> str:
        return f"{self.source}:{self.job_id}"

    @property
    def fingerprint(self) -> str:
        """Cross-source identity so a role found on Microsoft.com AND LinkedIn alerts once."""
        co = re.sub(r"[^a-z0-9]", "", (self.company.split() or [""])[0].lower())
        ti = re.sub(r"[^a-z0-9]", "", self.title.lower())
        city = re.sub(r"[^a-z]", "", (self.locations[0].split(",")[0] if self.locations else "").lower())
        return hashlib.sha1(f"{co}|{ti}|{city}".encode()).hexdigest()[:16]


class SourceError(Exception):
    pass


# --------------------------------------------------------------------------- http
class Http:
    def __init__(self) -> None:
        self.s = requests.Session()
        retry = Retry(total=2, backoff_factor=1.5, status_forcelist=[500, 502, 503, 504],
                      allowed_methods=None, raise_on_status=False)
        self.s.mount("https://", HTTPAdapter(max_retries=retry))
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

    def _check(self, r: requests.Response, url: str) -> requests.Response:
        if r.status_code in (429, 999):
            raise SourceError(f"rate-limited/blocked (HTTP {r.status_code}) by {url.split('/')[2]}")
        if r.status_code >= 400:
            raise SourceError(f"HTTP {r.status_code} from {url.split('?')[0]}")
        return r

    def get(self, url: str, **kw) -> requests.Response:
        kw.setdefault("timeout", 25)
        return self._check(self.s.get(url, **kw), url)

    def post(self, url: str, **kw) -> requests.Response:
        kw.setdefault("timeout", 25)
        return self._check(self.s.post(url, **kw), url)


# --------------------------------------------------------------------------- helpers
def from_epoch(v) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def from_iso(v: str | None) -> Optional[datetime]:
    if not v:
        return None
    try:
        d = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def from_workday(text: str | None) -> Optional[datetime]:
    t = (text or "").lower()
    if "today" in t:
        return NOW
    if "yesterday" in t:
        return NOW - timedelta(days=1)
    m = re.search(r"(\d+)\+?\s*days?", t)
    if m:
        return NOW - timedelta(days=int(m.group(1)) + (1 if "+" in t else 0))
    return None


def strip_html(s: str | None, limit: int = 4000) -> str:
    if not s:
        return ""
    return BeautifulSoup(html.unescape(s), "html.parser").get_text(" ", strip=True)[:limit]


def slugify(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


# --------------------------------------------------------------------------- adapters
def fetch_pcsx(src: dict, http: Http) -> list[Job]:
    """Eightfold 'PCSX' careers API (Microsoft, Starbucks)."""
    out: dict[str, Job] = {}
    for loc in src.get("locations", [""]):
        for q in src.get("queries", [""]):
            for page in range(src.get("max_pages", 3)):
                r = http.get(f"https://{src['host']}/api/pcsx/search", params={
                    "domain": src["domain"], "query": q, "location": loc,
                    "start": page * 10, "sort_by": "timestamp"})
                data = (r.json().get("data") or {})
                positions = data.get("positions") or []
                for p in positions:
                    jid = str(p.get("id"))
                    if jid in out:
                        continue
                    locs = list(dict.fromkeys((p.get("standardizedLocations") or []) + (p.get("locations") or [])))
                    remote = str(p.get("workLocationOption", "")).lower() == "remote"
                    out[jid] = Job(
                        source=src["name"], company=src["name"], job_id=jid,
                        title=p.get("name", "").strip(),
                        url=f"https://{src['host']}{p.get('positionUrl') or '/careers/job/' + jid}",
                        locations=locs + (["Remote"] if remote else []),
                        posted=from_epoch(p.get("postedTs") or p.get("creationTs")),
                        description=p.get("department") or "", remote=remote)
                if len(positions) < 10:
                    break
    return list(out.values())


def fetch_jobsyn(src: dict, http: Http) -> list[Job]:
    """DirectEmployers/Jobsyn search API behind careers.alaskaair.com."""
    origin = src["origin"]
    headers = {"x-origin": origin, "Origin": f"https://{origin}", "Referer": f"https://{origin}/",
               "Accept": "application/json"}
    out: dict[str, Job] = {}
    for page in range(1, src.get("max_pages", 12) + 1):
        r = http.get("https://prod-search-api.jobsyn.org/api/v1/solr/search",
                     params={"page": page, "num_items": 10, "sort": "date"}, headers=headers)
        data = r.json()
        for j in (data.get("featured_jobs") or []) + (data.get("jobs") or []):
            guid = j.get("guid")
            if not guid or guid in out:
                continue
            loc = j.get("location_exact") or ""
            out[guid] = Job(
                source=src["name"], company=j.get("company_exact") or "Alaska Airlines", job_id=guid,
                title=(j.get("title_exact") or "").strip(),
                url=f"{src['site_url']}/{slugify(loc)}/{j.get('title_slug', 'job')}/{guid}/job/",
                locations=[loc] if loc else [], posted=from_iso(j.get("date_added")),
                description=strip_html(j.get("description"), 3000))
        if not (data.get("pagination") or {}).get("has_more_pages"):
            break
    return list(out.values())


def fetch_amazon(src: dict, http: Http) -> list[Job]:
    out: dict[str, Job] = {}
    for loc in src.get("locations", [""]):
        for q in src.get("queries", [""]):
            r = http.get("https://www.amazon.jobs/en/search.json", params=[
                ("base_query", q), ("normalized_location[]", loc), ("result_limit", 100), ("sort", "recent")])
            for j in r.json().get("jobs") or []:
                jid = str(j.get("id_icims") or j.get("id"))
                if jid in out:
                    continue
                posted = None
                try:
                    posted = datetime.strptime(j.get("posted_date", ""), "%B %d, %Y").replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
                locs = [j.get("location") or "", j.get("normalized_location") or ""]
                for raw in j.get("locations") or []:       # multi-location postings: JSON strings
                    try:
                        locs.append(json.loads(raw).get("normalizedLocation") or "")
                    except (ValueError, AttributeError):
                        pass
                locs = list(dict.fromkeys(x for x in locs if x))
                out[jid] = Job(
                    source=src["name"], company="Amazon", job_id=jid, title=(j.get("title") or "").strip(),
                    url="https://www.amazon.jobs" + (j.get("job_path") or f"/en/jobs/{jid}"),
                    locations=locs, posted=posted,
                    description=" ".join(filter(None, [j.get("job_category"), j.get("job_family"),
                                                        strip_html(j.get("description_short"), 800)])))
    return list(out.values())


def fetch_workday(src: dict, http: Http) -> list[Job]:
    base = f"https://{src['host']}/wday/cxs/{src['tenant']}/{src['site']}"
    out: dict[str, Job] = {}

    def enrich(job: Job, path: str) -> None:
        d = http.get(f"{base}{path}", headers={"Accept": "application/json"}).json().get("jobPostingInfo") or {}
        locs = [d.get("location")] + (d.get("additionalLocations") or [])
        job.locations = [x for x in locs if x] or job.locations
        job.description = strip_html(d.get("jobDescription"), 4000) or job.description
        if str(d.get("remoteType", "")).lower().startswith("remote"):
            job.remote = True
            job.locations.append("Remote")

    for page in range(src.get("max_pages", 6)):
        r = http.post(f"{base}/jobs", json={"appliedFacets": {}, "limit": 20, "offset": page * 20,
                                             "searchText": src.get("search_text", "")},
                      headers={"Accept": "application/json", "Content-Type": "application/json"})
        postings = r.json().get("jobPostings") or []
        for p in postings:
            path = p.get("externalPath") or ""
            jid = (p.get("bulletFields") or [path])[0]
            if jid in out:
                continue
            loc_text = p.get("locationsText") or ""
            multi = bool(re.match(r"^\d+\s+Locations?$", loc_text))
            job = Job(source=src["name"], company=src["name"], job_id=jid, title=(p.get("title") or "").strip(),
                      url=f"https://{src['host']}/en-US/{src['site']}{path}",
                      locations=[] if multi else [loc_text], posted=from_workday(p.get("postedOn")),
                      needs_enrich=multi)
            job.enrich = (lambda j, pth=path: enrich(j, pth))
            out[jid] = job
        if len(postings) < 20:
            break
    return list(out.values())


def fetch_greenhouse(src: dict, http: Http) -> list[Job]:
    r = http.get(f"https://boards-api.greenhouse.io/v1/boards/{src['board']}/jobs")
    out = []
    for j in r.json().get("jobs") or []:
        loc = (j.get("location") or {}).get("name") or ""
        out.append(Job(source=src["name"], company=src["name"], job_id=str(j["id"]),
                       title=(j.get("title") or "").strip(), url=j.get("absolute_url") or "",
                       locations=[loc] if loc else [],
                       posted=from_iso(j.get("first_published") or j.get("updated_at"))))
    return out


def fetch_linkedin(src: dict, http: Http) -> list[Job]:
    """LinkedIn's public (logged-out) job-search fragments. Gentle: few queries, 1s apart."""
    out: dict[str, Job] = {}
    blocked: Optional[str] = None
    tpr = f"r{int(src.get('within_seconds', 172800))}"
    for s in src.get("searches", []):
        for loc in s.get("locations", []):
            for start in (0, 10):
                time.sleep(1.0)
                try:
                    r = http.get("https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search", params={
                        "keywords": s["keywords"], "location": loc, "f_TPR": tpr, "sortBy": "DD", "start": start})
                except SourceError as e:
                    blocked = str(e)
                    break
                cards = BeautifulSoup(r.text, "html.parser").find_all("li")
                for li in cards:
                    urn = li.find(attrs={"data-entity-urn": True})
                    a = li.select_one("a.base-card__full-link")
                    t = li.select_one(".base-search-card__title")
                    if not (urn and a and t):
                        continue
                    jid = urn["data-entity-urn"].split(":")[-1]
                    if jid in out:
                        continue
                    lo = li.select_one(".job-search-card__location")
                    co = li.select_one(".base-search-card__subtitle")
                    tm = li.find("time")
                    lo_text = lo.get_text(strip=True) if lo else ""
                    out[jid] = Job(source=src["name"], company=co.get_text(strip=True) if co else "",
                                   job_id=jid, title=t.get_text(strip=True), url=a["href"].split("?")[0],
                                   locations=[lo_text] if lo_text else [],
                                   posted=from_iso(tm.get("datetime")) if tm else None,
                                   remote="remote" in lo_text.lower())
                if len(cards) < 10:
                    break
            if blocked:
                break
        if blocked:
            break
    if blocked and not out:
        raise SourceError(blocked)
    return list(out.values())


ADAPTERS = {"pcsx": fetch_pcsx, "jobsyn": fetch_jobsyn, "amazon": fetch_amazon, "workday": fetch_workday,
            "greenhouse": fetch_greenhouse, "linkedin": fetch_linkedin}


# --------------------------------------------------------------------------- scoring
class Matcher:
    def __init__(self, cfg: dict) -> None:
        p = cfg["profile"]
        self.p = p
        self.title_patterns = [(re.compile(rx, re.I), w) for rx, w in p["title_patterns"]]
        self.seniority = [(re.compile(rx, re.I), w) for rx, w in p["seniority_bonus"]]
        self.penalties = [(re.compile(rx, re.I), w) for rx, w in p["penalties"]]
        self.reject = re.compile(p["reject_title"], re.I)
        self.domain = [(kw, re.compile(rf"\b{re.escape(kw)}\b", re.I)) for kw in p["domain_bonus"]["keywords"]]
        self.priority = [c.lower() for c in p.get("priority_companies", [])]
        self.allow = re.compile(cfg["locations"]["allow_regex"], re.I)
        self.deny = re.compile(cfg["locations"]["deny_regex"], re.I)
        self.remote_title = re.compile(cfg["locations"].get("remote_title_regex", r"$^"), re.I)
        self.max_age = timedelta(days=p.get("max_age_days", 30))

    def title_gate(self, job: Job) -> Optional[tuple[float, list[str]]]:
        """Cheap first pass: does the TITLE look like a fit at all?"""
        t = job.title
        if not t or self.reject.search(t):
            return None
        hits = sorted(((w, m.group(0)) for rx, w in self.title_patterns if (m := rx.search(t))), reverse=True)
        if not hits:
            return None
        base = min(60.0, hits[0][0] + 0.3 * sum(w for w, _ in hits[1:]))
        reasons = [h[1].lower() for h in hits[:2]]
        sen = [(w, m.group(0)) for rx, w in self.seniority if (m := rx.search(t))]
        if sen:
            w, txt = max(sen)
            base += w
            if w >= 12:
                reasons.append(txt.lower())
        base += sum(w for rx, w in self.penalties if rx.search(t))
        return base, reasons

    def location_ok(self, job: Job) -> bool:
        cands = list(job.locations) + (["Remote"] if job.remote else [])
        if self.remote_title.search(job.title):
            job.remote = True
            return not self.deny.search(job.title)
        return any(self.allow.search(l) and not self.deny.search(l) for l in cands if l)

    def score(self, job: Job) -> Optional[tuple[int, list[str]]]:
        g = self.title_gate(job)
        if g is None:
            return None
        s, reasons = g
        if any(c in job.company.lower() for c in self.priority):
            s += 8
        hay = f"{job.title} {job.description}"
        dom = [kw for kw, rx in self.domain if rx.search(hay)]
        if dom:
            s += min(15, 3 * len(dom))
            reasons.append("/".join(dom[:3]))
        return max(0, min(100, round(s))), reasons


# --------------------------------------------------------------------------- state
def load_state() -> dict:
    try:
        st = json.loads(STATE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        st = {}
    st.setdefault("seen", {})       # alerted job keys + fingerprints -> iso time
    st.setdefault("skip", {})       # rejected after detail lookup (avoid refetching)
    st.setdefault("health", {})
    st.setdefault("alerts", [])
    st.setdefault("initialized", False)
    return st


def save_state(st: dict) -> None:
    cutoff = (NOW - timedelta(days=365)).isoformat()
    st["seen"] = {k: v for k, v in st["seen"].items() if v >= cutoff}
    st["skip"] = {k: v for k, v in st["skip"].items() if v >= (NOW - timedelta(days=60)).isoformat()}
    st["alerts"] = st["alerts"][-200:]
    STATE_PATH.parent.mkdir(exist_ok=True)
    STATE_PATH.write_text(json.dumps(st, indent=1, sort_keys=True) + "\n")


# --------------------------------------------------------------------------- notify
class Notifier:
    def __init__(self) -> None:
        self.topic = os.environ.get("NTFY_TOPIC", "").strip()
        self.server = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
        self.token = os.environ.get("NTFY_TOKEN", "").strip()

    @property
    def enabled(self) -> bool:
        return bool(self.topic)

    def send(self, title: str, message: str, url: str = "", priority: int = 3, tags: list[str] | None = None) -> bool:
        if not self.enabled:
            print(f"[notify:disabled] {title} | {message}")
            return False
        body: dict = {"topic": self.topic, "title": title[:250], "message": message[:1800],
                      "priority": priority, "tags": tags or ["briefcase"]}
        if url:
            body["click"] = url
            body["actions"] = [{"action": "view", "label": "Open job", "url": url}]
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        try:
            r = requests.post(self.server, json=body, headers=headers, timeout=20)
            if r.status_code >= 400:
                print(f"[notify] ntfy error {r.status_code}: {r.text[:200]}", file=sys.stderr)
                return False
            return True
        except requests.RequestException as e:
            print(f"[notify] failed: {e}", file=sys.stderr)
            return False


def describe(job: Job, score: int, reasons: list[str]) -> tuple[str, str]:
    loc = next((l for l in job.locations if l), "") or ("Remote" if job.remote else "")
    when = job.posted.strftime("%b %d") if job.posted else "recent"
    title = f"{job.title} - {job.company}"
    msg = f"Fit {score}/100 | {loc} | posted {when}\nWhy: {', '.join(r for r in reasons if r)}\nvia {job.source}"
    return title, msg


# --------------------------------------------------------------------------- engine
def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text())


def due(src: dict, force: bool) -> bool:
    if src.get("enabled", True) is False:
        return False
    n = int(src.get("every_n_runs", 1))
    return force or n <= 1 or (int(time.time() // 600) % n == 0)


def collect(cfg: dict, force: bool, only: Optional[str] = None) -> tuple[list[Job], dict[str, str], list[str]]:
    http = Http()
    sources = [s for s in cfg["sources"] if due(s, force) and (not only or s["name"] == only)]
    errors: dict[str, str] = {}
    ran: list[str] = []

    def work(src: dict):
        try:
            return src, ADAPTERS[src["type"]](src, http), None
        except SourceError as e:
            return src, [], str(e)
        except Exception as e:  # network errors, JSON changes, etc.
            return src, [], f"{type(e).__name__}: {e}"

    jobs: list[Job] = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for src, found, err in ex.map(work, sources):
            ran.append(src["name"])
            if err:
                errors[src["name"]] = err
            jobs.extend(found)
            print(f"  {src['name']:<18} {len(found):>4} jobs" + (f"   ERROR: {err}" if err else ""))
    # direct company sources first so the best link wins over the LinkedIn duplicate
    jobs.sort(key=lambda j: j.source == "LinkedIn")
    return jobs, errors, ran


def evaluate(cfg: dict, jobs: list[Job], state: dict, matcher: Matcher
             ) -> list[tuple[int, Job, list[str]]]:
    """Return NEW matching jobs as (score, job, reasons), best first."""
    threshold = cfg["profile"]["notify_threshold"]
    seen, skip = state["seen"], state["skip"]
    found: list[tuple[int, Job, list[str]]] = []
    batch_fp: set[str] = set()
    for job in jobs:
        if job.key in seen or job.key in skip:
            continue
        if matcher.title_gate(job) is None:
            continue
        if job.posted and NOW - job.posted > matcher.max_age:
            continue
        if job.needs_enrich and job.enrich:
            try:
                job.enrich(job)
            except Exception as e:
                print(f"    detail lookup failed for {job.key}: {e}", file=sys.stderr)
        if not matcher.location_ok(job):
            skip[job.key] = NOW.isoformat()
            continue
        res = matcher.score(job)
        if not res or res[0] < threshold:
            continue
        fp = job.fingerprint
        if fp in seen or fp in batch_fp:
            seen[job.key] = NOW.isoformat()      # same role already alerted via another source
            continue
        batch_fp.add(fp)
        found.append((res[0], job, res[1]))
    found.sort(key=lambda t: (t[0], t[1].posted or NOW), reverse=True)
    return found


def cmd_run(args) -> int:
    cfg = load_config()
    state = load_state()
    notifier = Notifier()
    matcher = Matcher(cfg)
    print(f"jobwatch run @ {NOW:%Y-%m-%d %H:%M}Z  (notifications {'ON' if notifier.enabled else 'OFF'})")
    if not notifier.enabled and not args.dry_run:
        # Without a topic nobody would be told about anything, so do not mark jobs as seen.
        print("NTFY_TOPIC is not set: add it as a repository secret. Skipping this run (nothing saved).")
        return 0
    jobs, errors, ran = collect(cfg, force=args.all or args.dry_run)

    # ---- health tracking (alert once when a source has been down for a while)
    hcfg = cfg["health"]
    newly_down: list[str] = []
    for name in ran:
        h = state["health"].setdefault(name, {})
        if name in errors:
            h["fails"] = h.get("fails", 0) + 1
            h["last_error"] = errors[name][:200]
            if h["fails"] == hcfg["down_after_failures"]:
                newly_down.append(name)
        else:
            state["health"].pop(name, None)
    if newly_down and not args.dry_run:     # one combined alert, even if everything is down at once
        detail = "; ".join(f"{n}: {errors[n][:90]}" for n in newly_down[:3])
        more = f" (+{len(newly_down) - 3} more)" if len(newly_down) > 3 else ""
        notifier.send(f"jobwatch: {len(newly_down)} source(s) failing",
                      f"Down for about an hour: {', '.join(newly_down)}. {detail}{more}",
                      priority=2, tags=["warning"])

    new = evaluate(cfg, jobs, state, matcher)
    first_run = not state["initialized"]
    strong = cfg["profile"]["strong_threshold"]
    print(f"  -> {len(jobs)} postings scanned, {len(new)} new fits" + ("  (first run: baselining)" if first_run else ""))

    to_send = new[: (cfg["profile"]["first_run_notify"] if first_run else 6)]
    rest = new[len(to_send):]
    sent_keys = set()
    for score, job, reasons in to_send:
        title, msg = describe(job, score, reasons)
        print(f"    [{score:>3}] {title}  ({job.locations[:1]})  {job.url}")
        if args.dry_run:
            continue
        if notifier.send(title, msg, job.url, priority=4 if score >= strong else 3,
                         tags=["star"] if score >= strong else ["briefcase"]):
            sent_keys.add(job.key)
            state["alerts"].append({"t": NOW.isoformat(), "s": score})
        elif not notifier.enabled:
            sent_keys.add(job.key)        # no topic configured: don't loop forever
        time.sleep(0.4)

    if rest and not args.dry_run:
        lines = "\n".join(f"{s} {j.title} - {j.company}" for s, j, _ in rest[:8])
        label = "More current matches" if first_run else "More new matches"
        if notifier.send(f"jobwatch: {len(rest)} {label.lower()}", lines, priority=3, tags=["memo"]):
            sent_keys.update(j.key for _, j, _ in rest)
        elif not notifier.enabled:
            sent_keys.update(j.key for _, j, _ in rest)

    if not args.dry_run:
        for score, job, _ in new:
            if job.key in sent_keys:
                state["seen"][job.key] = NOW.isoformat()
                state["seen"][job.fingerprint] = NOW.isoformat()
        state["initialized"] = True
        # weekly heartbeat so silence never means "broken"
        if (NOW.weekday() == hcfg["heartbeat_weekday"] and NOW.hour == hcfg["heartbeat_hour_utc"] and NOW.minute < 10):
            week = sum(1 for a in state["alerts"] if a["t"] >= (NOW - timedelta(days=7)).isoformat())
            bad = [n for n in state["health"] if state["health"][n].get("fails")]
            notifier.send("jobwatch is running",
                          f"{week} alerts in the last 7 days. " + (f"Trouble with: {', '.join(bad)}." if bad else "All sources healthy."),
                          priority=1, tags=["white_check_mark"])
        save_state(state)
    return 0


def cmd_selftest(args) -> int:
    cfg = load_config()
    matcher = Matcher(cfg)
    print("jobwatch self-test (no alerts sent, nothing saved)\n")
    jobs, errors, _ = collect(cfg, force=True, only=args.only)
    scratch = {"seen": {}, "skip": {}}
    fits = evaluate(cfg, jobs, scratch, matcher)
    print(f"\n{len(jobs)} postings scanned -> {len(fits)} fit the profile. Top 15:")
    for score, job, reasons in fits[:15]:
        print(f"  [{score:>3}] {job.title} - {job.company} | {(job.locations or [''])[0]} | {job.source}")
    if errors:
        print("\nSources with problems:")
        for n, e in errors.items():
            print(f"  {n}: {e}")
    return 1 if errors and len(errors) == len([s for s in cfg['sources'] if s.get('enabled', True)]) else 0


def cmd_test_notify(_args) -> int:
    n = Notifier()
    if not n.enabled:
        print("NTFY_TOPIC is not set.")
        return 1
    ok = n.send("jobwatch test", "If you can read this on your phone, alerts are working.",
                url="https://careers.microsoft.com", priority=3, tags=["tada"])
    print("sent" if ok else "FAILED to send")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--all", action="store_true", help="ignore per-source cadence")
    r.add_argument("--dry-run", action="store_true")
    r.set_defaults(fn=cmd_run)
    s = sub.add_parser("selftest")
    s.add_argument("--only", help="test a single source by name")
    s.set_defaults(fn=cmd_selftest)
    t = sub.add_parser("test-notify")
    t.set_defaults(fn=cmd_test_notify)
    a = ap.parse_args()
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
