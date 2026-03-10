"""
insta_intel/dashboard/app.py
LeapScholar · Insta Intel — Streamlit Dashboard
Two tabs: Competitor Reels Database + Trend Explorer
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
from datetime import datetime, timedelta
from utils.helpers import fmt_number, truncate
from config.settings import DASHBOARD_TITLE, DASHBOARD_ICON

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title = DASHBOARD_TITLE,
    page_icon  = DASHBOARD_ICON,
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

# ── CSS — Clean white SaaS style (inspired by PolicyPilot + BizLink reference) ──
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Syne:wght@700;800&display=swap');

/* ── Reset & Base ── */
html, body, [class*="css"], .stApp {
    font-family: 'Inter', sans-serif !important;
    background-color: #f5f6fa !important;
    color: #1a1d23 !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e8eaed !important;
    padding-top: 0 !important;
}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSlider label {
    color: #6b7280 !important;
    font-size: 12px !important;
    font-weight: 500 !important;
}

/* ── Top nav bar ── */
.top-nav {
    background: #ffffff;
    border-bottom: 1px solid #e8eaed;
    padding: 14px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 28px;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.nav-logo {
    font-family: 'Syne', sans-serif;
    font-size: 20px;
    font-weight: 800;
    color: #1a1d23;
    letter-spacing: -0.5px;
}
.nav-logo span { color: #2563eb; }
.nav-badge {
    background: #eff6ff;
    color: #2563eb;
    font-size: 11px;
    font-weight: 600;
    padding: 4px 12px;
    border-radius: 20px;
    border: 1px solid #bfdbfe;
    letter-spacing: 0.5px;
}
.nav-right {
    display: flex;
    align-items: center;
    gap: 12px;
}

/* ── KPI Cards (PolicyPilot style) ── */
.kpi-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 24px;
}
.kpi-card {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 12px;
    padding: 22px 24px;
    position: relative;
    transition: box-shadow 0.2s, border-color 0.2s;
    cursor: default;
}
.kpi-card:hover {
    box-shadow: 0 4px 16px rgba(37,99,235,0.08);
    border-color: #bfdbfe;
}
.kpi-icon {
    width: 36px; height: 36px;
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 16px;
    margin-bottom: 14px;
}
.kpi-icon.blue   { background: #eff6ff; }
.kpi-icon.green  { background: #f0fdf4; }
.kpi-icon.purple { background: #faf5ff; }
.kpi-icon.amber  { background: #fffbeb; }
.kpi-value {
    font-family: 'Syne', sans-serif;
    font-size: 30px;
    font-weight: 800;
    color: #1a1d23;
    line-height: 1;
    margin-bottom: 4px;
}
.kpi-label {
    font-size: 13px;
    color: #6b7280;
    font-weight: 500;
}
.kpi-delta {
    position: absolute;
    top: 20px; right: 20px;
    font-size: 11px;
    font-weight: 600;
    color: #10b981;
    background: #f0fdf4;
    border-radius: 6px;
    padding: 2px 8px;
}

/* ── Section title ── */
.section-title {
    font-family: 'Syne', sans-serif;
    font-size: 17px;
    font-weight: 700;
    color: #1a1d23;
    margin: 4px 0 16px 0;
    display: flex;
    align-items: center;
    gap: 8px;
}
.section-title .pill {
    background: #eff6ff;
    color: #2563eb;
    font-size: 11px;
    font-weight: 600;
    padding: 2px 10px;
    border-radius: 20px;
    font-family: 'Inter', sans-serif;
}

/* ── Data table card ── */
.table-card {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    margin-bottom: 20px;
}

/* ── Reel row card ── */
.reel-card {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 10px;
    display: flex;
    align-items: flex-start;
    gap: 16px;
    transition: all 0.15s;
    cursor: pointer;
}
.reel-card:hover {
    border-color: #2563eb;
    box-shadow: 0 2px 12px rgba(37,99,235,0.08);
}
.reel-title {
    font-size: 14px;
    font-weight: 600;
    color: #1a1d23;
    margin-bottom: 4px;
    line-height: 1.4;
}
.reel-sub {
    font-size: 12px;
    color: #9ca3af;
    margin-bottom: 10px;
}
.reel-stats {
    display: flex;
    gap: 20px;
    flex-wrap: wrap;
}
.reel-stat {
    font-size: 12px;
    color: #6b7280;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 4px;
}
.reel-stat.primary { color: #2563eb; font-weight: 600; }

/* ── Badges ── */
.badge {
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 20px;
    white-space: nowrap;
}
.badge-company  { background: #eff6ff; color: #2563eb; }
.badge-creator  { background: #fdf4ff; color: #9333ea; }
.badge-topic    { background: #f0fdf4; color: #16a34a; }
.badge-format   { background: #fff7ed; color: #ea580c; }
.badge-hot      { background: #fef2f2; color: #dc2626; }
.badge-warm     { background: #fffbeb; color: #d97706; }
.badge-normal   { background: #f9fafb; color: #6b7280; }

/* ── Trend cards ── */
.trend-section {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 12px;
    padding: 22px 24px;
    height: 100%;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.trend-section-title {
    font-family: 'Syne', sans-serif;
    font-size: 14px;
    font-weight: 700;
    color: #1a1d23;
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 1px solid #f3f4f6;
    display: flex;
    align-items: center;
    gap: 8px;
}
.trend-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 0;
    border-bottom: 1px solid #f9fafb;
    gap: 10px;
}
.trend-item:last-child { border-bottom: none; }
.trend-rank {
    font-family: 'Syne', sans-serif;
    font-size: 18px;
    font-weight: 800;
    color: #e5e7eb;
    min-width: 28px;
}
.trend-rank.top { color: #2563eb; }
.trend-name {
    font-size: 13px;
    font-weight: 500;
    color: #374151;
    flex: 1;
}
.trend-count {
    font-size: 12px;
    font-weight: 600;
    color: #2563eb;
    background: #eff6ff;
    padding: 2px 8px;
    border-radius: 6px;
    white-space: nowrap;
}
.trend-bar-bg {
    height: 4px;
    background: #f3f4f6;
    border-radius: 2px;
    margin-top: 4px;
}
.trend-bar-fill {
    height: 4px;
    background: linear-gradient(90deg, #2563eb, #60a5fa);
    border-radius: 2px;
}

/* ── Filter bar ── */
.filter-bar {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}

/* ── Empty state ── */
.empty-state {
    text-align: center;
    padding: 60px 20px;
    color: #9ca3af;
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 12px;
}
.empty-state .icon { font-size: 48px; margin-bottom: 16px; }
.empty-state .title { font-size: 16px; font-weight: 600; color: #374151; margin-bottom: 8px; }
.empty-state .sub   { font-size: 13px; }

/* ── Streamlit overrides ── */
.stTabs [data-baseweb="tab-list"] {
    background: #ffffff;
    border-bottom: 2px solid #e8eaed;
    padding: 0;
    gap: 0;
    border-radius: 0;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #6b7280;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
    font-size: 14px;
    padding: 14px 24px;
    border-bottom: 2px solid transparent;
    margin-bottom: -2px;
    border-radius: 0;
}
.stTabs [aria-selected="true"] {
    color: #2563eb !important;
    border-bottom: 2px solid #2563eb !important;
    background: transparent !important;
    font-weight: 600 !important;
}
.stTabs [data-baseweb="tab-panel"] { padding-top: 24px; }

.stSelectbox > div > div,
.stMultiselect > div > div {
    background: #ffffff !important;
    border: 1px solid #e8eaed !important;
    border-radius: 8px !important;
    font-size: 13px !important;
}
.stTextInput input {
    background: #ffffff !important;
    border: 1px solid #e8eaed !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    color: #1a1d23 !important;
}
.stTextInput input:focus {
    border-color: #2563eb !important;
    box-shadow: 0 0 0 3px rgba(37,99,235,0.1) !important;
}
.stButton button {
    background: #2563eb !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    padding: 8px 20px !important;
    font-family: 'Inter', sans-serif !important;
}
.stButton button:hover {
    background: #1d4ed8 !important;
}
.stDownloadButton button {
    background: #f9fafb !important;
    color: #374151 !important;
    border: 1px solid #e8eaed !important;
    border-radius: 8px !important;
    font-size: 13px !important;
}
.stSlider [data-baseweb="slider"] { color: #2563eb !important; }
div[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════
#  DATA LAYER — load from MongoDB or demo
# ═══════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def load_data() -> pd.DataFrame:
    try:
        from database.mongo_client import get_all_reels, get_stats
        reels = get_all_reels(limit=1000)
        if reels:
            return pd.DataFrame(reels), get_stats()
    except Exception:
        pass
    return _demo_dataframe(), _demo_stats()

def _demo_dataframe() -> pd.DataFrame:
    data = [
        {"competitor":"yocketapp","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC001/","caption":"3 mistakes every Indian student makes before applying for a Canada student visa. Watch till the end! #studyabroad #canadavisa","views":980000,"likes":42000,"comments":3100,"engagement_rate":0.046,"audio":"Original sound - yocketapp","date":"2026-02-28","ai_analysis":{"hook":"3 mistakes every Indian student makes","topic":"Canada Visa","cta":"Watch till the end","format":"Listicle","summary":"Highlights 3 common visa mistakes Indian students make when applying to Canada."}},
        {"competitor":"leverageedu","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC002/","caption":"Nobody tells you THIS about UK student visa rejections 👀 Save this before you apply! #ukvisa #studyabroad","views":760000,"likes":31000,"comments":2800,"engagement_rate":0.044,"audio":"Trending audio - 2026","date":"2026-02-27","ai_analysis":{"hook":"Nobody tells you THIS about UK student visa rejections","topic":"UK Visa","cta":"Save this before you apply","format":"Talking Head","summary":"Exposes hidden reasons for UK visa rejections that most applicants are unaware of."}},
        {"competitor":"sheenamgautam","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC003/","caption":"How I got a fully funded scholarship to the UK 🇬🇧 Comment 'SCHOLAR' for the guide! #scholarship #studyinuk","views":1240000,"likes":67000,"comments":8900,"engagement_rate":0.061,"audio":"Original sound - sheenamgautam","date":"2026-02-26","ai_analysis":{"hook":"How I got a fully funded scholarship to the UK","topic":"Scholarships","cta":"Comment 'SCHOLAR' for the guide","format":"Storytime","summary":"Personal story of securing a fully funded UK scholarship, resonates deeply with aspirational students."}},
        {"competitor":"ambitiohq","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC004/","caption":"IELTS band 7 in 30 days? Here's my exact study plan 📚 Save this reel! #ielts #studyabroad","views":540000,"likes":22000,"comments":1900,"engagement_rate":0.044,"audio":"Lo-fi study beats","date":"2026-02-25","ai_analysis":{"hook":"IELTS band 7 in 30 days?","topic":"IELTS Tips","cta":"Save this reel","format":"Tips & Tricks","summary":"Actionable 30-day IELTS study plan that promises Band 7 results for motivated students."}},
        {"competitor":"shreyamahendru_","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC005/","caption":"Stop wasting money on education loans! Here's what banks don't tell you 💰 Follow for more finance tips #educationloan","views":890000,"likes":38000,"comments":5200,"engagement_rate":0.048,"audio":"Original sound - shreyamahendru_","date":"2026-02-24","ai_analysis":{"hook":"Stop wasting money on education loans","topic":"Education Loan","cta":"Follow for more finance tips","format":"Talking Head","summary":"Reveals hidden charges and smarter alternatives to traditional education loans for study abroad."}},
        {"competitor":"studyabroadkar","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC006/","caption":"Day in my life studying in Germany 🇩🇪 Cost breakdown inside! #studyingermany #studentlife","views":420000,"likes":18500,"comments":2200,"engagement_rate":0.049,"audio":"Aesthetic vlog music","date":"2026-02-23","ai_analysis":{"hook":"Day in my life studying in Germany","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Authentic vlog showing daily life and actual costs of studying in Germany for Indian students."}},
        {"competitor":"gradright","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC007/","caption":"Australia vs Canada: Which is better for Indian students in 2026? Drop your vote below 👇 #studyabroad","views":670000,"likes":29000,"comments":7800,"engagement_rate":0.055,"audio":"Trending sound - debate","date":"2026-02-22","ai_analysis":{"hook":"Australia vs Canada: Which is better for Indian students?","topic":"General Study Abroad","cta":"Drop your vote below","format":"Myth vs Fact","summary":"Compelling comparison between Australia and Canada that drives high comment engagement through voting."}},
        {"competitor":"harnoor.studyabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC008/","caption":"My SOP got rejected 4 times. Here's what I changed to finally get into Oxford 📝 DM me 'SOP' #oxford #sop","views":1100000,"likes":59000,"comments":11200,"engagement_rate":0.064,"audio":"Original sound - harnoor.studyabroad","date":"2026-02-21","ai_analysis":{"hook":"My SOP got rejected 4 times","topic":"SOP/LOR","cta":"DM me 'SOP'","format":"Storytime","summary":"Relatable failure-to-success SOP story that builds trust and drives DMs for Oxford admission guidance."}},
        {"competitor":"upgradabroad","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC009/","caption":"Part-time jobs in Canada for Indian students — everything you need to know! Share this with your friends #canada","views":380000,"likes":15000,"comments":1400,"engagement_rate":0.043,"audio":"Upbeat background music","date":"2026-02-20","ai_analysis":{"hook":"Part-time jobs in Canada for Indian students — everything you need to know","topic":"Canada Visa","cta":"Share this with your friends","format":"Tips & Tricks","summary":"Practical guide to legal part-time work options for Indian students studying in Canada."}},
        {"competitor":"searcheduindia","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC010/","caption":"Germany student visa rejected? Watch this before reapplying 🇩🇪 #germanyvisa #studyindia","views":310000,"likes":13500,"comments":1800,"engagement_rate":0.049,"audio":"Original sound - searcheduindia","date":"2026-02-19","ai_analysis":{"hook":"Germany student visa rejected? Watch this before reapplying","topic":"Germany Visa","cta":"None","format":"Talking Head","summary":"Expert advice on recovering from German student visa rejection with a higher success strategy."}},
        {"competitor":"indianstudentabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC011/","caption":"The real cost of studying in the UK in 2026 😮 Not what universities tell you! #studyinuk #ukcost","views":720000,"likes":33000,"comments":4100,"engagement_rate":0.051,"audio":"Dramatic reveal sound","date":"2026-02-18","ai_analysis":{"hook":"The real cost of studying in the UK in 2026","topic":"UK Visa","cta":"None","format":"Voiceover + Text","summary":"Reveals hidden costs of UK education that universities don't advertise, creating urgency and shares."}},
        {"competitor":"mastersabroaddiaries","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC012/","caption":"POV: You finally got your Australia PR after studying there 🇦🇺🎉 Follow my journey #australiapr #studyinaustralia","views":560000,"likes":27000,"comments":3900,"engagement_rate":0.055,"audio":"Emotional background music","date":"2026-02-17","ai_analysis":{"hook":"POV: You finally got your Australia PR after studying there","topic":"Australia Visa","cta":"Follow my journey","format":"Storytime","summary":"Aspirational PR success story that motivates students to pursue Australia as their study destination."}},
        {"competitor":"idp.india","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC013/","caption":"Top 5 IELTS speaking mistakes that cost you marks. Link in bio for free mock test! #ielts #ieltsspeaking","views":445000,"likes":19500,"comments":2300,"engagement_rate":0.049,"audio":"Original sound - idp.india","date":"2026-02-16","ai_analysis":{"hook":"Top 5 IELTS speaking mistakes that cost you marks","topic":"IELTS Tips","cta":"Link in bio for free mock test","format":"Listicle","summary":"Identifies 5 specific speaking mistakes with actionable fixes, driving traffic to their platform."}},
        {"competitor":"abroadpathway","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC014/","caption":"USA F1 visa interview questions that ACTUALLY get asked in 2026 👀 Save this! #f1visa #usavisa","views":690000,"likes":30500,"comments":4700,"engagement_rate":0.051,"audio":"Suspenseful background","date":"2026-02-15","ai_analysis":{"hook":"USA F1 visa interview questions that ACTUALLY get asked in 2026","topic":"USA Visa","cta":"Save this","format":"Tips & Tricks","summary":"High-value interview prep content with real 2026 F1 visa questions that drives saves and shares."}},
        {"competitor":"moveabroadsimplified","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC015/","caption":"My first month expenses in Canada as an Indian student 🇨🇦 Was it what I expected? #canadastudent","views":490000,"likes":21000,"comments":2800,"engagement_rate":0.048,"audio":"Vlog background music","date":"2026-02-14","ai_analysis":{"hook":"My first month expenses in Canada as an Indian student","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Transparent first-month financial breakdown in Canada that builds authenticity and drives saves."}},
    ]
    df = pd.DataFrame(data)
    df["hook"]    = df["ai_analysis"].apply(lambda x: x.get("hook",   "") if isinstance(x,dict) else "")
    df["topic"]   = df["ai_analysis"].apply(lambda x: x.get("topic",  "") if isinstance(x,dict) else "")
    df["cta"]     = df["ai_analysis"].apply(lambda x: x.get("cta",    "") if isinstance(x,dict) else "")
    df["format"]  = df["ai_analysis"].apply(lambda x: x.get("format", "") if isinstance(x,dict) else "")
    df["summary"] = df["ai_analysis"].apply(lambda x: x.get("summary","") if isinstance(x,dict) else "")
    return df

def _demo_stats():
    return {
        "total_reels": 15, "avg_views": 633000,
        "avg_eng": 0.051, "total_views": 9500000,
        "competitors": ["yocketapp","leverageedu","sheenamgautam","ambitiohq",
                        "shreyamahendru_","gradright","harnoor.studyabroad"],
    }

# ── Load ──────────────────────────────────────────────────────
try:
    result = load_data()
    if isinstance(result, tuple):
        df_raw, stats = result
    else:
        df_raw, stats = result, _demo_stats()
except Exception:
    df_raw, stats = _demo_dataframe(), _demo_stats()

# Ensure flat columns exist
for col in ["hook","topic","cta","format","summary"]:
    if col not in df_raw.columns:
        df_raw[col] = df_raw.get("ai_analysis", pd.Series([{}]*len(df_raw))).apply(
            lambda x: x.get(col,"") if isinstance(x,dict) else ""
        )

# ═══════════════════════════════════════════════
#  SIDEBAR — Filters
# ═══════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style='padding:20px 4px 16px 4px; border-bottom:1px solid #f3f4f6; margin-bottom:16px;'>
        <div style='font-family:Syne,sans-serif;font-size:18px;font-weight:800;color:#1a1d23;'>⚡ Insta Intel</div>
        <div style='font-size:11px;color:#9ca3af;margin-top:2px;letter-spacing:1px;text-transform:uppercase;'>LeapScholar Content Team</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**🔍 Filters**")

    all_competitors = ["All"] + sorted(df_raw["competitor"].unique().tolist())
    sel_competitor  = st.selectbox("Competitor", all_competitors)

    all_types = ["All", "company", "creator"]
    sel_type  = st.selectbox("Account Type", all_types)

    all_topics = ["All"] + sorted(df_raw["topic"].dropna().unique().tolist()) if "topic" in df_raw.columns else ["All"]
    sel_topic  = st.selectbox("Topic", all_topics)

    min_views_k = st.slider("Min Views (K)", 0, 1000, 0, step=10)
    min_views   = min_views_k * 1000

    st.markdown("<hr style='border-color:#f3f4f6;margin:16px 0;'>", unsafe_allow_html=True)
    st.markdown("**📅 Date Range**")
    date_from = st.date_input("From", value=datetime.now()-timedelta(days=30))
    date_to   = st.date_input("To",   value=datetime.now())

    st.markdown("<hr style='border-color:#f3f4f6;margin:16px 0;'>", unsafe_allow_html=True)
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style='margin-top:24px;padding:12px;background:#f9fafb;border-radius:8px;border:1px solid #f3f4f6;'>
        <div style='font-size:11px;color:#6b7280;line-height:1.8;'>
        <b style='color:#374151;'>Pipeline Status</b><br>
        🟢 Scraper: Active<br>
        🟢 AI Analysis: Active<br>
        🟡 Whisper: On demand<br>
        ⏱ Next run: in 2 days
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── Apply filters ────────────────────────────────────────────
df = df_raw.copy()
if sel_competitor != "All": df = df[df["competitor"] == sel_competitor]
if sel_type       != "All": df = df[df["account_type"] == sel_type]
if sel_topic      != "All" and "topic" in df.columns:
    df = df[df["topic"] == sel_topic]
if min_views > 0:           df = df[df["views"] >= min_views]
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[
        (df["date"] >= pd.Timestamp(date_from)) &
        (df["date"] <= pd.Timestamp(date_to))
    ]

# ═══════════════════════════════════════════════
#  TOP NAV
# ═══════════════════════════════════════════════
st.markdown(f"""
<div class='top-nav'>
    <div class='nav-logo'>Leap<span>Scholar</span> · Insta Intel</div>
    <div class='nav-right'>
        <span style='font-size:12px;color:#9ca3af;'>
            {len(df)} reels &nbsp;|&nbsp; {len(df['competitor'].unique())} accounts
        </span>
        <span class='nav-badge'>🟢 LIVE</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════
#  KPI CARDS
# ═══════════════════════════════════════════════
total_views   = df["views"].sum()
avg_eng       = df["engagement_rate"].mean() if len(df) else 0
top_reel_views = df["views"].max() if len(df) else 0
n_creators    = len(df[df["account_type"]=="creator"]["competitor"].unique()) if len(df) else 0

st.markdown(f"""
<div class='kpi-row'>
    <div class='kpi-card'>
        <div class='kpi-icon blue'>📊</div>
        <div class='kpi-value'>{len(df)}</div>
        <div class='kpi-label'>Total Reels Tracked</div>
        <div class='kpi-delta'>+{len(df)} total</div>
    </div>
    <div class='kpi-card'>
        <div class='kpi-icon green'>👁</div>
        <div class='kpi-value'>{fmt_number(total_views)}</div>
        <div class='kpi-label'>Total Views</div>
    </div>
    <div class='kpi-card'>
        <div class='kpi-icon purple'>📈</div>
        <div class='kpi-value'>{round(avg_eng*100,1)}%</div>
        <div class='kpi-label'>Avg Engagement Rate</div>
    </div>
    <div class='kpi-card'>
        <div class='kpi-icon amber'>🏆</div>
        <div class='kpi-value'>{fmt_number(top_reel_views)}</div>
        <div class='kpi-label'>Top Reel Views</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════
#  TABS
# ═══════════════════════════════════════════════
tab1, tab2 = st.tabs(["📋  Competitor Reels Database", "🔥  Trend Explorer"])

# ════════════════════════════════════════════════
#  TAB 1 — COMPETITOR REELS DATABASE
# ════════════════════════════════════════════════
with tab1:
    col_left, col_right = st.columns([3, 1])
    with col_left:
        st.markdown(f"""
        <div class='section-title'>
            Competitor Reels
            <span class='pill'>{len(df)} results</span>
        </div>
        """, unsafe_allow_html=True)
    with col_right:
        sort_col = st.selectbox("Sort by", ["views","engagement_rate","likes","comments","date"], label_visibility="collapsed")

    df_sorted = df.sort_values(sort_col, ascending=False).reset_index(drop=True)

    # ── Search bar ──
    search = st.text_input("🔎  Search captions, hooks, topics...", placeholder="e.g. Canada visa, IELTS, scholarship")
    if search:
        mask = df_sorted.apply(
            lambda r: search.lower() in str(r.get("caption","")).lower()
                   or search.lower() in str(r.get("hook","")).lower()
                   or search.lower() in str(r.get("topic","")).lower()
                   or search.lower() in str(r.get("competitor","")).lower(),
            axis=1
        )
        df_sorted = df_sorted[mask]

    if df_sorted.empty:
        st.markdown("""
        <div class='empty-state'>
            <div class='icon'>🔍</div>
            <div class='title'>No reels found</div>
            <div class='sub'>Try adjusting filters or running the pipeline to scrape new data.</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        # ── Reel cards ──
        for _, row in df_sorted.iterrows():
            eng  = row.get("engagement_rate", 0)
            views = row.get("views", 0)
            hook  = row.get("hook",   "")
            topic = row.get("topic",  "")
            fmt   = row.get("format", "")
            cta   = row.get("cta",    "")
            summ  = row.get("summary","")
            acc_type = row.get("account_type","")

            if   views >= 800000: badge_cls, badge_lbl = "badge-hot",  "🔥 Viral"
            elif views >= 400000: badge_cls, badge_lbl = "badge-warm", "⚡ Hot"
            else:                 badge_cls, badge_lbl = "badge-normal","📈 Active"

            date_str = str(row.get("date",""))[:10]

            st.markdown(f"""
            <div class='reel-card'>
                <div style='flex:1;'>
                    <div style='display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap;'>
                        <span style='font-family:Syne,sans-serif;font-size:14px;font-weight:700;color:#1a1d23;'>
                            @{row.get("competitor","")}
                        </span>
                        <span class='badge {"badge-company" if acc_type=="company" else "badge-creator"}'>{acc_type}</span>
                        <span class='badge {badge_cls}'>{badge_lbl}</span>
                        {f"<span class='badge badge-topic'>{topic}</span>" if topic else ""}
                        {f"<span class='badge badge-format'>{fmt}</span>" if fmt else ""}
                    </div>

                    {f"<div style='font-size:13px;font-weight:600;color:#374151;margin-bottom:4px;'>💡 Hook: {truncate(hook, 100)}</div>" if hook else ""}
                    {f"<div style='font-size:12px;color:#6b7280;margin-bottom:8px;font-style:italic;'>{truncate(summ, 130)}</div>" if summ else ""}

                    <div class='reel-stats'>
                        <span class='reel-stat primary'>👁 {fmt_number(views)}</span>
                        <span class='reel-stat'>❤️ {fmt_number(row.get("likes",0))}</span>
                        <span class='reel-stat'>💬 {fmt_number(row.get("comments",0))}</span>
                        <span class='reel-stat'>📊 {round(eng*100,1)}% eng</span>
                        <span class='reel-stat'>🎵 {truncate(str(row.get("audio","")),40)}</span>
                        <span class='reel-stat'>📅 {date_str}</span>
                        {f'<span class="reel-stat">📣 {truncate(cta,50)}</span>' if cta and cta != "None" else ""}
                    </div>

                    <div style='margin-top:10px;font-size:12px;color:#9ca3af;line-height:1.5;'>
                        {truncate(row.get("caption",""), 150)}
                    </div>
                </div>
                <div style='padding-left:8px;padding-top:4px;'>
                    <a href='{row.get("reel_url","#")}' target='_blank'
                       style='background:#eff6ff;color:#2563eb;padding:7px 14px;border-radius:8px;
                              font-size:12px;font-weight:600;text-decoration:none;
                              border:1px solid #bfdbfe;white-space:nowrap;'>
                       ▶ Open
                    </a>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<hr style='border-color:#f3f4f6;margin:20px 0;'>", unsafe_allow_html=True)

    # ── Full data table ──
    with st.expander("📊 View as Data Table + Export"):
        display_cols = ["competitor","account_type","views","likes","comments",
                        "engagement_rate","topic","format","hook","cta","audio","date","reel_url"]
        available = [c for c in display_cols if c in df_sorted.columns]
        df_display = df_sorted[available].copy()
        if "engagement_rate" in df_display.columns:
            df_display["engagement_rate"] = (df_display["engagement_rate"]*100).round(2).astype(str) + "%"

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=400,
            column_config={
                "reel_url":        st.column_config.LinkColumn("Open Reel"),
                "views":           st.column_config.NumberColumn("Views",    format="%d"),
                "likes":           st.column_config.NumberColumn("Likes",    format="%d"),
                "comments":        st.column_config.NumberColumn("Comments", format="%d"),
                "engagement_rate": st.column_config.TextColumn("Eng Rate"),
            }
        )
        csv = df_sorted.drop(columns=["ai_analysis"], errors="ignore").to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️  Export CSV",
            data=csv,
            file_name=f"insta_intel_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    # ── Charts row ──
    st.markdown("<div class='section-title' style='margin-top:8px;'>📊 Performance Charts</div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)

    with c1:
        if "competitor" in df.columns and not df.empty:
            top_acc = df.groupby("competitor")["views"].sum().sort_values(ascending=False).head(10).reset_index()
            fig = px.bar(top_acc, x="views", y="competitor", orientation="h",
                         color="views", color_continuous_scale=["#bfdbfe","#2563eb"],
                         labels={"views":"Total Views","competitor":""})
            fig.update_layout(
                title=dict(text="Top Accounts by Views", font=dict(family="Syne", size=14, color="#1a1d23")),
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
                font=dict(family="Inter", color="#6b7280"),
                margin=dict(l=0,r=0,t=36,b=0), height=280,
                coloraxis_showscale=False,
                xaxis=dict(gridcolor="#f3f4f6"),
                yaxis=dict(gridcolor="rgba(0,0,0,0)"),
            )
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        if "views" in df.columns and "engagement_rate" in df.columns and not df.empty:
            fig2 = px.scatter(
                df, x="views", y=df["engagement_rate"]*100,
                color="account_type", hover_name="competitor",
                color_discrete_map={"company":"#2563eb","creator":"#9333ea"},
                labels={"views":"Views","y":"Engagement Rate %","account_type":"Type"},
            )
            fig2.update_layout(
                title=dict(text="Views vs Engagement Rate", font=dict(family="Syne", size=14, color="#1a1d23")),
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
                font=dict(family="Inter", color="#6b7280"),
                margin=dict(l=0,r=0,t=36,b=0), height=280,
                xaxis=dict(gridcolor="#f3f4f6"),
                yaxis=dict(gridcolor="#f3f4f6"),
                legend=dict(font=dict(size=11)),
            )
            st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════
#  TAB 2 — TREND EXPLORER
# ════════════════════════════════════════════════
with tab2:
    st.markdown("""
    <div class='section-title'>
        Trend Explorer
        <span class='pill'>AI-powered insights</span>
    </div>
    """, unsafe_allow_html=True)

    def rank_list_html(items: list, top_n: int = 8, color: str = "#2563eb") -> str:
        if not items:
            return "<div style='color:#9ca3af;font-size:13px;padding:20px 0;'>No data yet</div>"
        max_count = items[0][1] if items else 1
        html = ""
        for i, (name, count) in enumerate(items[:top_n], 1):
            pct = int((count / max_count) * 100)
            rank_cls = "top" if i <= 3 else ""
            html += f"""
            <div class='trend-item'>
                <div class='trend-rank {rank_cls}'>{i}</div>
                <div style='flex:1;'>
                    <div class='trend-name'>{name}</div>
                    <div class='trend-bar-bg'>
                        <div class='trend-bar-fill' style='width:{pct}%;background:linear-gradient(90deg,{color},{color}99);'></div>
                    </div>
                </div>
                <div class='trend-count'>{count} reels</div>
            </div>"""
        return html

    # Aggregate trend data
    hooks_data   = Counter(df["hook"].dropna().tolist())   if "hook"   in df.columns else Counter()
    topics_data  = Counter(df["topic"].dropna().tolist())  if "topic"  in df.columns else Counter()
    cta_data     = Counter(df["cta"].dropna().tolist())    if "cta"    in df.columns else Counter()
    formats_data = Counter(df["format"].dropna().tolist()) if "format" in df.columns else Counter()
    audio_data   = Counter(df["audio"].dropna().tolist())  if "audio"  in df.columns else Counter()

    # Remove blanks and "None"
    for d in [hooks_data, topics_data, cta_data, formats_data, audio_data]:
        for bad in ["", "None", "none", "N/A"]:
            d.pop(bad, None)

    # ── Row 1: Hooks + Topics ──
    r1c1, r1c2 = st.columns(2)

    with r1c1:
        st.markdown(f"""
        <div class='trend-section'>
            <div class='trend-section-title'>💡 Trending Hooks</div>
            {rank_list_html(hooks_data.most_common(8), color="#2563eb")}
        </div>
        """, unsafe_allow_html=True)

    with r1c2:
        st.markdown(f"""
        <div class='trend-section'>
            <div class='trend-section-title'>🏷️ Trending Topics</div>
            {rank_list_html(topics_data.most_common(8), color="#16a34a")}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)

    # ── Row 2: CTA + Format ──
    r2c1, r2c2 = st.columns(2)

    with r2c1:
        st.markdown(f"""
        <div class='trend-section'>
            <div class='trend-section-title'>📣 Trending CTAs</div>
            {rank_list_html(cta_data.most_common(8), color="#ea580c")}
        </div>
        """, unsafe_allow_html=True)

    with r2c2:
        st.markdown(f"""
        <div class='trend-section'>
            <div class='trend-section-title'>🎬 Trending Formats</div>
            {rank_list_html(formats_data.most_common(8), color="#9333ea")}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)

    # ── Row 3: Audio + Charts ──
    r3c1, r3c2 = st.columns([1, 2])

    with r3c1:
        st.markdown(f"""
        <div class='trend-section'>
            <div class='trend-section-title'>🎵 Trending Audio</div>
            {rank_list_html(audio_data.most_common(8), color="#0891b2")}
        </div>
        """, unsafe_allow_html=True)

    with r3c2:
        if not df.empty and "topic" in df.columns and "views" in df.columns:
            # Topic performance chart
            topic_perf = (
                df.groupby("topic")
                  .agg(avg_views=("views","mean"), count=("views","count"))
                  .reset_index()
                  .sort_values("avg_views", ascending=False)
                  .head(10)
            )
            fig3 = px.bar(
                topic_perf, x="topic", y="avg_views",
                color="count",
                color_continuous_scale=["#bfdbfe","#1d4ed8"],
                text="count",
                labels={"avg_views":"Avg Views","topic":"Topic","count":"# Reels"},
            )
            fig3.update_traces(texttemplate="%{text} reels", textposition="outside", textfont_size=10)
            fig3.update_layout(
                title=dict(text="Avg Views by Topic", font=dict(family="Syne", size=14, color="#1a1d23")),
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
                font=dict(family="Inter", color="#6b7280"),
                margin=dict(l=0,r=0,t=36,b=80), height=320,
                coloraxis_showscale=False,
                xaxis=dict(gridcolor="rgba(0,0,0,0)", tickangle=-30),
                yaxis=dict(gridcolor="#f3f4f6"),
            )
            st.plotly_chart(fig3, use_container_width=True)

    st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)

    # ── Engagement by Format ──
    if not df.empty and "format" in df.columns and "engagement_rate" in df.columns:
        st.markdown("<div class='section-title'>📊 Engagement Rate by Content Format</div>", unsafe_allow_html=True)
        fmt_eng = (
            df.groupby("format")["engagement_rate"]
              .mean()
              .reset_index()
              .sort_values("engagement_rate", ascending=False)
        )
        fmt_eng["engagement_pct"] = (fmt_eng["engagement_rate"] * 100).round(2)
        fig4 = px.bar(
            fmt_eng, x="format", y="engagement_pct",
            color="engagement_pct",
            color_continuous_scale=["#bfdbfe","#1d4ed8"],
            labels={"format":"Format","engagement_pct":"Avg Engagement %"},
            text="engagement_pct",
        )
        fig4.update_traces(texttemplate="%{text}%", textposition="outside", textfont_size=11)
        fig4.update_layout(
            paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
            font=dict(family="Inter", color="#6b7280"),
            margin=dict(l=0,r=0,t=10,b=0), height=280,
            coloraxis_showscale=False,
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(gridcolor="#f3f4f6"),
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ── Company vs Creator performance ──
    if not df.empty and "account_type" in df.columns:
        st.markdown("<div class='section-title'>🏢 Company vs Creator Performance</div>", unsafe_allow_html=True)
        cc1, cc2, cc3 = st.columns(3)
        for acc_type, col in [("company", cc1), ("creator", cc2)]:
            subset = df[df["account_type"] == acc_type]
            if not subset.empty:
                with col:
                    avg_v = fmt_number(int(subset["views"].mean()))
                    avg_e = f"{round(subset['engagement_rate'].mean()*100,1)}%"
                    clr   = "#2563eb" if acc_type == "company" else "#9333ea"
                    st.markdown(f"""
                    <div style='background:#ffffff;border:1px solid #e8eaed;border-radius:12px;
                                padding:20px 24px;text-align:center;'>
                        <div style='font-size:12px;color:#9ca3af;font-weight:600;
                                    text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;'>
                            {"🏢 Company" if acc_type=="company" else "👤 Creator"} Accounts
                        </div>
                        <div style='font-family:Syne,sans-serif;font-size:28px;font-weight:800;color:{clr};'>
                            {avg_v}
                        </div>
                        <div style='font-size:12px;color:#6b7280;margin:4px 0 12px 0;'>avg views</div>
                        <div style='font-size:20px;font-weight:700;color:#1a1d23;'>{avg_e}</div>
                        <div style='font-size:12px;color:#6b7280;'>avg engagement</div>
                        <div style='font-size:11px;color:#9ca3af;margin-top:8px;'>{len(subset)} reels tracked</div>
                    </div>
                    """, unsafe_allow_html=True)
        with cc3:
            if not df.empty:
                best = df.loc[df["views"].idxmax()]
                st.markdown(f"""
                <div style='background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;
                            padding:20px 24px;'>
                    <div style='font-size:12px;color:#2563eb;font-weight:600;
                                text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;'>
                        🏆 Top Reel
                    </div>
                    <div style='font-size:13px;font-weight:600;color:#1a1d23;margin-bottom:6px;'>
                        @{best.get("competitor","")}
                    </div>
                    <div style='font-size:12px;color:#6b7280;margin-bottom:8px;'>
                        {truncate(str(best.get("hook",""))[:80], 80)}
                    </div>
                    <div style='font-family:Syne,sans-serif;font-size:24px;font-weight:800;color:#2563eb;'>
                        {fmt_number(int(best.get("views",0)))}
                    </div>
                    <div style='font-size:12px;color:#6b7280;margin-bottom:10px;'>views</div>
                    <a href='{best.get("reel_url","#")}' target='_blank'
                       style='background:#2563eb;color:#fff;padding:7px 16px;border-radius:8px;
                              font-size:12px;font-weight:600;text-decoration:none;'>
                       ▶ Watch Reel
                    </a>
                </div>
                """, unsafe_allow_html=True)
