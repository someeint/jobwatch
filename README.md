# jobwatch

Checks career sites every ~10 minutes and pushes a notification to your phone when a **new** role fits.
It runs for free on GitHub's servers, so your computer can be off.

**Watches:** Microsoft, Starbucks, Alaska Air Group (Alaska / Horizon / Hawaiian), Amazon, T-Mobile,
Salesforce, Adobe, Nordstrom, Zillow, Boeing, CrowdStrike, Okta, Airbnb, plus **LinkedIn** search
(which also catches Google, Meta, Apple, Costco, REI, Delta, Verizon, Seattle startups, VCs and studios).

**Looks for:** senior program leadership (not day-to-day project management), business operations and
strategy, vendor/partner strategy, and startup/venture/corporate-development roles, in the Seattle-Bellevue
area or explicitly remote. Everything is tunable in `config.yaml`.

**Only strong matches get through.** Every job gets a *match %* against Dan's real background (senior program / delivery
management, vendor and stakeholder work, Zero Trust and cloud rollouts, Salesforce, operations, startup experience). It is
built from role fit (0-50), level fit (0-15) and evidence of his experience in the job description (0-30), minus penalties
for technical or engineering-heavy roles and for levels above his track record. Only **75% and higher** is sent to your phone.
The description is read before alerting; if it cannot be loaded the alert says "title only".

Each alert looks like: *Senior Program Manager - Microsoft* / *82% match | Redmond, WA | posted Sep 18 | pay $150,000-$190,000*,
and tapping it opens the job posting.

**Also watched:** Indeed (via a Cowork task that runs a real browser on Olia's computer every hour, since Indeed blocks GitHub's own servers outright - see data/indeed_raw.json), and government jobs paying
$150k+ a year at the top of the range (King County, City of Seattle, Snohomish County, City of Tacoma via governmentjobs.com).

---

## One-time setup (about 10 minutes)

### 1. Get the phone app
Install **ntfy** (free; iPhone App Store or Google Play). Tap **+**, and subscribe to a topic name that
nobody could guess, for example `dan-jobs-k7f2q9x4m1` (make up your own). The topic name is effectively the
password, so do not share it. Anyone who should get the alerts (you and Dan) subscribes to the same topic.

### 2. Put the code on GitHub
1. Make a free account at github.com and create a new repository named `jobwatch`, set to **Public**
   (see "Public vs private" below for why).
2. Upload everything in this folder. The easiest way: on the new repo page click **uploading an existing file**
   and drag in the folder contents. If the hidden `.github` folder does not upload, click **Add file > Create new file**,
   type `.github/workflows/jobwatch.yml` as the name, and paste in that file's contents.

### 3. Add your topic as a secret
Repo **Settings > Secrets and variables > Actions > New repository secret**
Name: `NTFY_TOPIC`   Value: the topic you subscribed to in step 1.

### 4. Switch it on and test
Open the **Actions** tab (click the green button to enable workflows if asked), choose **jobwatch** on the left,
then **Run workflow**, and run these in order:

1. mode `test-notify`: your phone should buzz within seconds.
2. mode `selftest`: shows every source's health and the top matches right now (no alerts, nothing saved).
3. mode `run`: the first real run. It sends the ~8 best current openings plus one summary, then remembers
   everything it has seen. From then on the schedule takes over and you only hear about **new** matches.

## What you will and will not get

* You are alerted **once per role**, even when it shows up on both the company site and LinkedIn.
* 85% and up arrives as high priority; 75-84% as normal. Change either number in `config.yaml`.
* Expect only a handful of alerts a week. That is deliberate: 75% is a high bar. Too quiet? Lower `notify_threshold` to 70.
* If a source breaks (site changed, blocked), you get one combined "sources failing" alert after about an hour instead of silence.
* Every Monday morning a quiet "jobwatch is running" message confirms it is alive.

## Things to know

* **How fast is "every 10 minutes"?** GitHub's scheduler often runs a few minutes late, so expect roughly 10-15 minutes
  between checks. It is not a hard guarantee.
* **LinkedIn** is checked every ~30 minutes, gently, using its public search. LinkedIn sometimes blocks cloud servers;
  if that happens you will get the "source is failing" alert, and everything else keeps working. LinkedIn's public
  pages also do not reliably label remote jobs, so only postings that literally say "Remote" count as remote.
* **Verified vs. not yet verified sources.** Microsoft, Starbucks, Alaska, Amazon, T-Mobile, Okta and LinkedIn were
  tested against the live sites while building this. Salesforce, Adobe, Nordstrom, Zillow, Boeing and CrowdStrike use the
  same technology, but I could not confirm their exact site names. If one is wrong, the `selftest` run says so and you
  can correct or delete its block in `config.yaml`.
* **Idle repos get paused.** GitHub switches off scheduled runs after 60 days with no repository activity. The bot's
  state commits usually count as activity; if alerts ever stop, open the Actions tab and click enable.
* **Companies without their own adapter** (Google, Meta, Apple, Costco, Uber and so on) are only covered through LinkedIn
  search. To add a direct feed, copy any block in `config.yaml`; if the company uses Workday or Greenhouse it is a
  three-line change.

## Public vs. private repo

Public repos get unlimited free Actions minutes. A **private** repo only gets 2,000 free minutes a month, and a
10-minute schedule uses about 4,300. The repo contains no name, resume, or contact details, only job-title keywords and
a list of companies, and your topic stays hidden in the secret. If you prefer private, change the cron line in
`.github/workflows/jobwatch.yml` to `*/30 * * * *`.

## Tuning (`config.yaml`)

| To change...                         | Edit                                                                 |
|--------------------------------------|----------------------------------------------------------------------|
| How picky the alerts are             | `profile.notify_threshold` (lower = more alerts)                      |
| What counts as Dan's experience      | `profile.experience` (label, regex, points)                           |
| What titles count as a fit           | `profile.title_patterns` (regex and weight)                           |
| Titles to never show                 | `profile.reject_title`                                                |
| Seniority preference                 | `profile.seniority_bonus`, `profile.penalties`                        |
| Cities / remote rules                | `locations.allow_regex`, `locations.deny_regex`                       |
| Companies watched                    | `sources`                                                             |
| Government pay floor                 | `min_salary` on each `neogov` source                                  |

## Running it yourself (optional)

```
pip install -r requirements.txt
export NTFY_TOPIC=your-topic
python crawler.py selftest        # health check + top matches, no alerts
python crawler.py run --dry-run   # full pipeline, no alerts, nothing saved
python -m unittest discover -s tests
```
