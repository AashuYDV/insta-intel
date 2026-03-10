# 🚀 LeapScholar · Insta Intel

**Competitor Instagram Reel Monitoring + Content Inspiration Engine**

An internal tool for LeapScholar's content team. Automatically scrapes competitor Instagram reels, analyses them with AI, and surfaces trends in a clean dashboard.

---

## 📁 Project Structure

```
insta_intel/
├── scraper/
│   └── apify_scraper.py        ← Apify Instagram profile scraper
├── processing/
│   ├── download_reel.py        ← yt-dlp video downloader
│   ├── extract_audio.py        ← ffmpeg audio extraction
│   ├── transcribe.py           ← OpenAI Whisper transcription
│   └── ai_analysis.py          ← OpenAI GPT metadata extraction
├── database/
│   └── mongo_client.py         ← MongoDB Atlas client + all queries
├── pipeline/
│   └── run_pipeline.py         ← Master pipeline orchestrator
├── dashboard/
│   └── app.py                  ← Streamlit dashboard (2 tabs)
├── config/
│   ├── settings.py             ← All config + env vars
│   └── accounts.json           ← Competitor account lists
├── utils/
│   └── helpers.py              ← Shared utilities
├── requirements.txt
├── .env.example
└── README.md
```

---

## ⚡ Quick Start

### 1. Clone and install
```bash
git clone <repo>
cd insta_intel
pip install -r requirements.txt
```

### 2. Install system dependencies
```bash
# macOS
brew install ffmpeg

# Ubuntu / Debian
apt install ffmpeg -y
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env with your API keys
```

### 4. Get your API keys

| Key | Where to get |
|-----|-------------|
| `APIFY_API_TOKEN` | [console.apify.com](https://console.apify.com) → Settings → Integrations |
| `OPENAI_API_KEY` | [platform.openai.com](https://platform.openai.com) → API Keys |
| `MONGO_URI` | [cloud.mongodb.com](https://cloud.mongodb.com) → Connect → Drivers |

### 5. Launch dashboard (demo mode — no keys needed)
```bash
cd dashboard
streamlit run app.py
```

### 6. Run the full pipeline
```bash
python -m pipeline.run_pipeline
```

---

## 🔄 Pipeline Steps

```
1. Scrape        → Apify scrapes all 27 accounts, pulls latest reels
2. Store         → Raw reels saved to MongoDB (duplicates auto-skipped)
3. Download      → yt-dlp downloads reel videos to /tmp
4. Extract Audio → ffmpeg converts video to 16kHz mono MP3
5. Transcribe    → OpenAI Whisper transcribes speech to text
6. AI Analysis   → GPT-4o-mini extracts hook, topic, CTA, format, summary
7. Update DB     → MongoDB updated with transcript + AI analysis
```

### Run individual steps
```bash
# Skip scraping, only run AI analysis
python -m pipeline.run_pipeline --skip-scrape --skip-transcribe

# Only scrape, skip everything else
python -m pipeline.run_pipeline --skip-transcribe --skip-ai
```

---

## ⏰ Cron Schedule (every 2 days at 6 AM)

```bash
# Add to crontab: crontab -e
0 6 */2 * * cd /path/to/insta_intel && python -m pipeline.run_pipeline >> logs/pipeline.log 2>&1
```

---

## 📊 Dashboard Features

### Tab 1 — Competitor Reels Database
- All scraped reels with views, likes, comments, engagement rate
- AI-extracted hook, topic, CTA, format, summary per reel
- Filters: competitor, account type, topic, views, date range
- Search across captions, hooks, topics
- One-click open on Instagram
- Sortable data table + CSV export

### Tab 2 — Trend Explorer
- **Trending Hooks** — ranked list of opening lines that work
- **Trending Topics** — what subjects are getting the most views
- **Trending CTAs** — which calls-to-action are used most
- **Trending Formats** — Storytime vs Listicle vs Talking Head etc.
- **Trending Audio** — most used sounds across competitor reels
- Avg Views by Topic chart
- Engagement Rate by Format chart
- Company vs Creator performance comparison

---

## 🏢 Accounts Monitored

**15 Company accounts:** yocketapp, leverageedu, ambitiohq, upgradabroad, edvoyglobal, tcglobalofficial, idp.india, applyboard, studyportals, aeccglobal, siukindia, gradright, stoodnt, shiksha_studyabroad, collegedekhoindia

**12 Creator accounts:** sheenamgautam, shreyamahendru_, studyabroadkar, searcheduindia, parthvijayvergiya, harnoor.studyabroad, indianstudentabroad, studyinukguide, mastersabroaddiaries, abroadpathway, moveabroadsimplified, visastories

---

## 💰 Cost Estimate (Monthly)

| Service | Cost |
|---------|------|
| Apify Starter | ~$29/month (12,600 results) |
| OpenAI GPT-4o-mini | ~$2–5/month (1000 reels/month) |
| OpenAI Whisper | ~$3–6/month (1000 reels × avg 60s) |
| MongoDB Atlas M0 | **Free** (512MB) |
| **Total** | **~$34–40/month** |

---

## 🗄️ MongoDB Schema

**Collection: `reels`**
```json
{
  "competitor":       "yocketapp",
  "account_type":     "company",
  "reel_url":         "https://instagram.com/reel/xyz/",
  "shortcode":        "xyz",
  "caption":          "...",
  "views":            120000,
  "likes":            4500,
  "comments":         230,
  "engagement_rate":  0.039,
  "audio":            "Original sound - yocketapp",
  "transcript":       "Hey guys, today I want to talk about...",
  "ai_analysis": {
    "hook":    "Nobody tells you this about Canada visa",
    "topic":   "Canada Visa",
    "cta":     "Save this reel",
    "format":  "Talking Head",
    "summary": "Reveals 3 hidden Canada visa mistakes Indian students make."
  },
  "date":       "2026-03-05",
  "scraped_at": "2026-03-05"
}
```

---

## 🛠️ Troubleshooting

**Dashboard shows no data**
→ Run the pipeline first, or check MongoDB connection in `.env`

**Apify actor fails**
→ Verify `APIFY_API_TOKEN` and check credits at console.apify.com

**ffmpeg not found**
→ Install: `brew install ffmpeg` (Mac) or `apt install ffmpeg` (Linux)

**OpenAI rate limit**
→ The pipeline auto-retries with backoff. Reduce batch size in `run_pipeline.py`

**MongoDB connection timeout**
→ Whitelist your IP in MongoDB Atlas → Network Access → Add IP Address
