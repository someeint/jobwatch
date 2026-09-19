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
    matched_location: str = ""        # the office/remote option that satisfied the location filter
    fetch_detail: Optional[Callable[["Job"], None]] = None   # lazy: load the full job description

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


def longest_text(obj, hint: str = "description") -> str:
    """Longest string stored under a key containing `hint` anywhere in a JSON document."""
    best = ""

    def walk(o, hinted=False):
        nonlocal best
        if isinstance(o, dict):
            for k, v in o.items():
                walk(v, hinted or hint in str(k).lower())
        elif isinstance(o, list):
            for v in o:
                walk(v, hinted)
        elif isinstance(o, str) and hinted and len(o) > len(best):
            best = o

    walk(obj)
    return strip_html(best, 5000)


def slugify(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


# --------------------------------------------------------------------------- adapters
def fetch_pcsx(src: dict, http: Http) -> list[Job]:
    """Eightfold 'PCSX' careers API (Microsoft, Starbucks)."""
    out: dict[str, Job] = {}

    def detail(job: Job, pid: str) -> None:
        r = http.get(f"https://{src['host']}/api/pcsx/position_details",
                     params={"position_id": pid, "domain": src["domain"], "hl": "en"})
        job.description = longest_text(r.json()) or job.description

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
                    out[jid].fetch_detail = (lambda j, pid=jid: detail(j, pid))
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

    def detail(job: Job) -> None:
        time.sleep(1.0)
        r = http.get(f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job.job_id}")
        soup = BeautifulSoup(r.text, "html.parser")
        node = soup.select_one(".show-more-less-html__markup") or soup.select_one(".description__text")
        job.description = node.get_text(" ", strip=True)[:5000] if node else job.description

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
                                   remote="remote" in lo_text.lower(), fetch_detail=detail)
                if len(cards) < 10:
                    break
            if blocked:
                break
        if blocked:
            break
    if blocked and not out:
        raise SourceError(blocked)
    return list(out.values())


def parse_indeed(html_text: str, src_name: str) -> list[Job]:
    """Indeed search page -> jobs. Reads the embedded 'mosaic' job-card JSON, falls back to card anchors."""
    out: dict[str, Job] = {}
    m = re.search(r'window\.mosaic\.providerData\["mosaic-provider-jobcards"\]\s*=\s*(\{.*?\});\s*(?:window\.|</script>)',
                  html_text, re.S)
    rows: list[dict] = []
    if m:
        try:
            rows = (((json.loads(m.group(1)).get("metaData") or {}).get("mosaicProviderJobCardsModel") or {})
                    .get("results") or [])
        except json.JSONDecodeError:
            rows = []
    for r in rows:
        jk = r.get("jobkey")
        if not jk or jk in out:
            continue
        loc = r.get("formattedLocation") or ""
        out[jk] = Job(source=src_name, company=(r.get("company") or "").strip(), job_id=jk,
                      title=(r.get("displayTitle") or r.get("title") or "").strip(),
                      url=f"https://www.indeed.com/viewjob?jk={jk}",
                      locations=[loc] if loc else [], posted=from_epoch((r.get("pubDate") or 0) / 1000 or None),
                      description=strip_html(r.get("snippet"), 600),
                      remote=bool(r.get("remoteLocation")) or "remote" in loc.lower())
    if not out:                                            # fallback: plain result cards
        soup = BeautifulSoup(html_text, "html.parser")
        for a in soup.select("a[data-jk]"):
            jk = a["data-jk"]
            t = a.select_one("span[title]") or a.select_one("h2")
            card = a.find_parent(class_=re.compile("job_seen_beacon|result")) or a
            co = card.select_one("[data-testid='company-name']")
            lo = card.select_one("[data-testid='text-location']")
            if jk in out or not t:
                continue
            lo_text = lo.get_text(strip=True) if lo else ""
            out[jk] = Job(source=src_name, company=co.get_text(strip=True) if co else "", job_id=jk,
                          title=t.get_text(strip=True), url=f"https://www.indeed.com/viewjob?jk={jk}",
                          locations=[lo_text] if lo_text else [], remote="remote" in lo_text.lower())
    return list(out.values())


def fetch_indeed(src: dict, http: Http) -> list[Job]:
    """Indeed public search pages. Best effort: Indeed often blocks cloud servers, so this source is
    marked `optional` in the config (failures are logged, never alerted)."""
    out: dict[str, Job] = {}
    blocked: Optional[str] = None

    def detail(job: Job) -> None:
        time.sleep(1.5)
        node = BeautifulSoup(http.get(job.url).text, "html.parser").select_one("#jobDescriptionText")
        job.description = node.get_text(" ", strip=True)[:5000] if node else job.description

    for q in src.get("searches", []):
        time.sleep(2.0)
        try:
            r = http.get("https://www.indeed.com/jobs", params={
                "q": q["q"], "l": q.get("l", "Seattle, WA"), "radius": q.get("radius", 35),
                "fromage": src.get("fromage", 2), "sort": "date"})
        except SourceError as e:
            blocked = str(e)
            break
        for j in parse_indeed(r.text, src["name"]):
            j.fetch_detail = detail
            out.setdefault(j.job_id, j)
    if blocked and not out:
        raise SourceError(blocked)
    return list(out.values())


PAY_RX = re.compile(r"\$\s?([\d,]+(?:\.\d+)?)\s*-\s*\$\s?([\d,]+(?:\.\d+)?)\s*(Annually|Hourly|Monthly|Biweekly|Bi-weekly|Weekly)?", re.I)
PAY_FACTOR = {"annually": 1, "hourly": 2080, "monthly": 12, "biweekly": 26, "bi-weekly": 26, "weekly": 52}


def parse_pay(text: str) -> Optional[tuple[float, float]]:
    """'$131,007.34 - $166,615.70 Annually' -> (131007.34, 166615.70) as yearly amounts."""
    m = PAY_RX.search(text or "")
    if not m:
        return None
    k = PAY_FACTOR.get((m.group(3) or "annually").lower(), 1)
    return float(m.group(1).replace(",", "")) * k, float(m.group(2).replace(",", "")) * k


def parse_neogov(html_text: str, src: dict) -> list[Job]:
    """NEOGOV / governmentjobs.com listing fragment -> jobs (verified live for King County, Seattle,
    Snohomish County, Tacoma). Keeps only postings whose top pay reaches `min_salary` a year."""
    agency, min_salary = src["agency"], float(src.get("min_salary", 0))
    reject = re.compile(src["title_reject"], re.I) if src.get("title_reject") else None
    out: list[Job] = []
    for li in BeautifulSoup(html_text, "html.parser").select("li.list-item"):
        a = li.select_one("a.item-details-link")
        if not a or not li.get("data-job-id"):
            continue
        title = a.get_text(" ", strip=True)
        if reject and reject.search(title):
            continue
        metas = [x.get_text(" ", strip=True) for x in li.select("ul.list-meta > li")]
        pay_line = next((m for m in metas if "$" in m), "")
        pay = parse_pay(pay_line)
        if not pay or pay[1] < min_salary:
            continue
        loc = next((m for m in metas if "$" not in m and not re.match(r"^(Category|Department|Division):", m)), "")
        loc = loc or src.get("default_location", "")
        posted_txt = li.get_text(" ", strip=True)
        pm = re.search(r"Posted\s+(\d+)\s+(day|week|month)s?\s+ago|Posted\s+(today|yesterday)", posted_txt, re.I)
        posted = None
        if pm:
            if pm.group(3):
                posted = NOW - timedelta(days=1 if pm.group(3).lower() == "yesterday" else 0)
            else:
                posted = NOW - timedelta(days=int(pm.group(1)) * {"day": 1, "week": 7, "month": 30}[pm.group(2).lower()])
        summary = li.select_one(".list-entry")
        out.append(Job(
            source=src["name"], company=src.get("company", src["name"]), job_id=li["data-job-id"], title=title,
            url=f"https://www.governmentjobs.com{a['href']}", locations=[loc] if loc else [], posted=posted,
            description=f"Pay ${pay[0]:,.0f} - ${pay[1]:,.0f} a year. " + (summary.get_text(" ", strip=True)[:300] if summary else "")))
    return out


def fetch_neogov(src: dict, http: Http) -> list[Job]:
    """governmentjobs.com (NEOGOV): city / county / agency jobs in Washington. Government postings always
    list their pay, so `min_salary` (default $150k a year, top of range) can be enforced here."""
    out: dict[str, Job] = {}

    def detail(job: Job) -> None:
        time.sleep(0.5)
        node = BeautifulSoup(http.get(job.url).text, "html.parser").select_one("#details-info")
        if node:
            job.description = job.description.split(". ", 1)[0] + ". " + node.get_text(" ", strip=True)[:5000]

    for page in range(1, src.get("max_pages", 15) + 1):
        r = http.get("https://www.governmentjobs.com/careers/home/index", params={
            "agency": src["agency"], "sort": "PostingDate", "isDescendingSort": "true", "page": page},
            headers={"X-Requested-With": "XMLHttpRequest"})
        raw = BeautifulSoup(r.text, "html.parser").select("li.list-item")
        for j in parse_neogov(r.text, src):
            j.fetch_detail = detail
            out.setdefault(j.job_id, j)
        if len(raw) < 10:
            break
        time.sleep(0.5)
    return list(out.values())


ADAPTERS = {"pcsx": fetch_pcsx, "jobsyn": fetch_jobsyn, "amazon": fetch_amazon, "workday": fetch_workday,
            "greenhouse": fetch_greenhouse, "linkedin": fetch_linkedin, "indeed": fetch_indeed, "neogov": fetch_neogov}


# --------------------------------------------------------------------------- scoring
class Matcher:
    def __init__(self, cfg: dict) -> None:
        p = cfg["profile"]
        self.p = p
        self.title_patterns = [(re.compile(rx, re.I), w) for rx, w in p["title_patterns"]]
        self.seniority = [(re.compile(rx, re.I), w) for rx, w in p["seniority_bonus"]]
        self.penalties = [(re.compile(rx, re.I), w) for rx, w in p["penalties"]]
        self.reject = re.compile(p["reject_title"], re.I)
        self.allow = re.compile(cfg["locations"]["allow_regex"], re.I)
        self.deny = re.compile(cfg["locations"]["deny_regex"], re.I)
        self.remote_title = re.compile(cfg["locations"].get("remote_title_regex", r"$^"), re.I)
        self.max_age = timedelta(days=p.get("max_age_days", 30))
        # Dan's real experience: [label, regex, points]. Evidence in the job text is what validates the match.
        self.experience = [(lab, re.compile(rx, re.I), pts) for lab, rx, pts in p.get("experience", [])]
        self.desc_penalties = [(re.compile(rx, re.I), w) for rx, w in p.get("description_penalties", [])]
        self.min_desc = int(p.get("min_description_chars", 200))
        # a job we could only judge by title can still gain this many points once its description is read
        self.headroom = 30 - 8

    def _parts(self, job: Job) -> Optional[tuple[float, float, float, list[str]]]:
        t = job.title
        if not t or self.reject.search(t):
            return None
        hits = sorted(((w, m.group(0)) for rx, w in self.title_patterns if (m := rx.search(t))), reverse=True)
        if not hits:
            return None
        base = min(60.0, hits[0][0] + 0.3 * sum(w for w, _ in hits[1:]))
        role = min(50.0, base * 50 / 45)                       # 0-50: is this the kind of role Dan does?
        reasons = [h[1].lower() for h in hits[:2]]
        level = 0.0                                             # 0-15: is it at his level?
        sen = [(w, m.group(0)) for rx, w in self.seniority if (m := rx.search(t))]
        if sen:
            level, txt = max(sen)
            if level >= 12:
                reasons.append(txt.lower())
        pen = float(sum(w for rx, w in self.penalties if rx.search(t)))
        return role, level, pen, reasons

    def title_gate(self, job: Job) -> Optional[tuple[float, list[str]]]:
        """Cheap first pass: does the TITLE look like a fit at all?"""
        p = self._parts(job)
        if p is None:
            return None
        role, level, pen, reasons = p
        return role + level + pen, reasons

    def location_ok(self, job: Job) -> bool:
        cands = list(job.locations) + (["Remote"] if job.remote else [])
        if self.remote_title.search(job.title):
            job.remote = True
            job.matched_location = "Remote"
            return not self.deny.search(job.title)
        for l in cands:
            if l and self.allow.search(l) and not self.deny.search(l):
                job.matched_location = l
                return True
        return False

    def score(self, job: Job) -> Optional[tuple[int, list[str]]]:
        """Match % against Dan's profile: role fit (0-50) + level fit (0-15) + evidence of his
        actual experience in the job text (0-30, only when the description was read; otherwise a
        capped benefit of the doubt) - mismatch penalties."""
        parts = self._parts(job)
        if parts is None:
            return None
        role, level, pen, reasons = parts
        verified = len(job.description) >= self.min_desc
        hay = f"{job.title} {job.description}"
        ev = sorted(((pts, lab) for lab, rx, pts in self.experience if rx.search(hay)), reverse=True)
        ev_pts = float(sum(p for p, _ in ev))
        exp = min(30.0, ev_pts) if verified else min(20.0, 8.0 + ev_pts)
        dpen = float(sum(w for rx, w in self.desc_penalties if rx.search(job.description))) if verified else 0.0
        if ev:
            reasons.append(" / ".join(lab for _, lab in ev[:3]))
        if not verified:
            reasons.append("title only")
        return max(0, min(100, round(role + level + pen + exp + dpen))), reasons


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
        # optional second topic for postings that have been open for a while ("days long")
        self.topic_older = os.environ.get("NTFY_TOPIC_OLDER", "").strip() or self.topic
        self.server = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
        self.token = os.environ.get("NTFY_TOKEN", "").strip()

    @property
    def enabled(self) -> bool:
        return bool(self.topic)

    def topic_for(self, job: Job) -> str:
        return self.topic if is_fresh(job) else self.topic_older

    def send(self, title: str, message: str, url: str = "", priority: int = 3, tags: list[str] | None = None,
             topic: str = "") -> bool:
        if not self.enabled:
            print(f"[notify:disabled] {title} | {message}")
            return False
        body: dict = {"topic": topic or self.topic, "title": title[:250], "message": message[:1800],
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


SALARY_RX = re.compile(r"\$\s?(\d{2,3}(?:,\d{3})+|\d{2,3}\s?[kK])\s*(?:-|\u2013|\u2014|to|and)\s*\$?\s?(\d{2,3}(?:,\d{3})+|\d{2,3}\s?[kK])")


def salary_text(job: Job) -> str:
    """Pay range quoted in the posting (Washington law requires one), e.g. '$140,000-$185,000'."""
    m = SALARY_RX.search(job.description or "")
    if not m:
        return ""

    def fmt(v: str) -> str:
        v = v.replace(" ", "")
        return f"${int(v[:-1]) * 1000:,}" if v[-1] in "kK" else f"${v}"
    lo, hi = fmt(m.group(1)), fmt(m.group(2))
    return f"{lo}-{hi}"


FRESH_HOURS = 24


def is_fresh(job: Job) -> bool:
    """Just posted (last 24 hours, or date unknown) vs open for days."""
    return not job.posted or NOW - job.posted <= timedelta(hours=FRESH_HOURS)


def age_label(job: Job) -> str:
    if not job.posted:
        return "recently posted"
    days = (NOW - job.posted).days
    if days < 1:
        return "posted today"
    return "posted yesterday" if days == 1 else f"open {days} days"


def describe(job: Job, score: int, reasons: list[str]) -> tuple[str, str]:
    loc = job.matched_location or next((l for l in job.locations if l), "") or ("Remote" if job.remote else "")
    title = f"{job.title} - {job.company}"
    pay = salary_text(job)
    msg = f"{score}% match | {loc} | {age_label(job)}" + (f" | pay {pay}" if pay else "") + f"\nWhy: {', '.join(r for r in reasons if r)}\nvia {job.source}"
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


def evaluate(cfg: dict, jobs: list[Job], state: dict, matcher: Matcher, detail_budget: int = 60
             ) -> list[tuple[int, Job, list[str]]]:
    """Return NEW jobs whose match % clears the threshold, as (score, job, reasons), best first.

    Pass 1 judges by title/location and keeps only jobs that could still reach the threshold.
    Pass 2 reads the full description of the best candidates (when the source can supply it) so the
    final percentage is checked against Dan's experience, not just the title."""
    threshold = cfg["profile"]["notify_threshold"]
    seen, skip = state["seen"], state["skip"]
    cands: list[tuple[int, Job]] = []
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
        if res and res[0] + matcher.headroom >= threshold:
            cands.append((res[0], job))
    cands.sort(key=lambda t: t[0], reverse=True)

    found: list[tuple[int, Job, list[str]]] = []
    batch_fp: set[str] = set()
    for _, job in cands:
        if len(job.description) < matcher.min_desc and job.fetch_detail and detail_budget > 0:
            detail_budget -= 1
            try:
                job.fetch_detail(job)
            except Exception as e:
                print(f"    description lookup failed for {job.key}: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(0.3)
        res = matcher.score(job)
        if not res:
            continue
        if res[0] < threshold:
            if len(job.description) >= matcher.min_desc:      # judged on the full text: no need to look again
                skip[job.key] = NOW.isoformat()
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
            optional = any(x["name"] == name and x.get("optional") for x in cfg["sources"])
            if h["fails"] == hcfg["down_after_failures"] and not optional:
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
        print(f"    [{score:>3}] {title}  ({job.matched_location or job.locations[:1]})  {job.url}")
        if args.dry_run:
            continue
        fresh = is_fresh(job)
        if notifier.send(("NEW: " if fresh else "") + title, msg, job.url,
                         priority=4 if score >= strong else 3,
                         tags=["star"] if score >= strong else (["briefcase"] if fresh else ["hourglass"]),
                         topic=notifier.topic_for(job)):
            sent_keys.add(job.key)
            state["alerts"].append({"t": NOW.isoformat(), "s": score})
        elif not notifier.enabled:
            sent_keys.add(job.key)        # no topic configured: don't loop forever
        time.sleep(0.4)

    if rest and not args.dry_run:
        label = "more current matches" if first_run else "more new matches"
        for is_new, group in ((True, [t for t in rest if is_fresh(t[1])]), (False, [t for t in rest if not is_fresh(t[1])])):
            if not group:
                continue
            lines = "\n".join(f"{sc}% {j.title} - {j.company} ({age_label(j)})" for sc, j, _ in group[:8])
            tag = "just posted" if is_new else "open for days"
            if notifier.send(f"jobwatch: {len(group)} {label} ({tag})", lines, priority=3, tags=["memo"],
                             topic=notifier.topic if is_new else notifier.topic_older):
                sent_keys.update(j.key for _, j, _ in group)
            elif not notifier.enabled:
                sent_keys.update(j.key for _, j, _ in group)

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
    print(f"\n{len(jobs)} postings scanned -> {len(fits)} fit the profile (match >= {cfg['profile']['notify_threshold']}%):")
    for score, job, reasons in fits:
        print(f"  [{score:>3}] {job.title} - {job.company} | {job.matched_location or (job.locations or [''])[0]} | "
              f"{job.source} | {'JUST POSTED' if is_fresh(job) else age_label(job)} | {job.url}")
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
    ok = n.send("jobwatch test: just posted", "If you can read this on your phone, alerts are working.",
                url="https://careers.microsoft.com", priority=3, tags=["tada"])
    if n.topic_older != n.topic:
        ok = n.send("jobwatch test: open for days", "This topic will receive jobs that have been open for a few days.",
                    url="https://careers.microsoft.com", priority=3, tags=["tada"], topic=n.topic_older) and ok
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
