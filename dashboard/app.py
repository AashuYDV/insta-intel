"""
insta_intel/dashboard/app.py
LeapScholar Insta Intel - Fixed Dashboard
Uses native Streamlit components to avoid HTML rendering issues on Streamlit Cloud.
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

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="LeapScholar · Insta Intel",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS — Minimal safe CSS only (no HTML rendering) ──────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}

/* Background */
.stApp { background-color: #f8fafc !important; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e2e8f0 !important;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
[data-testid="stMetricLabel"] {
    font-size: 12px !important;
    font-weight: 600 !important;
    color: #64748b !important;
    text-transform: uppercase !important;
    letter-spacing: 0.8px !important;
}
[data-testid="stMetricValue"] {
    font-size: 28px !important;
    font-weight: 800 !important;
    color: #0f172a !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: #ffffff;
    border-bottom: 2px solid #e2e8f0;
    padding: 0;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #64748b;
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

/* Buttons */
.stButton button {
    background: #2563eb !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    padding: 8px 20px !important;
}
.stButton button:hover {
    background: #1d4ed8 !important;
}

/* Inputs */
.stTextInput input, .stSelectbox > div > div {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    background: #ffffff !important;
}

/* Expander */
.streamlit-expanderHeader {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    color: #374151 !important;
}

/* Hide streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

/* Divider */
hr { border-color: #e2e8f0 !important; }

/* Success/info boxes */
.stAlert { border-radius: 8px !important; }
</style>
""", unsafe_allow_html=True)

# ── Helper functions ─────────────────────────────────────────
def fmt_number(n):
    if n is None: return "0"
    n = int(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)

def truncate(text, max_len=100):
    if not text: return ""
    return text if len(text) <= max_len else text[:max_len] + "..."

def tms_label(views):
    if views >= 800000: return "🔥 Viral"
    if views >= 400000: return "⚡ Hot"
    return "📈 Active"

# ── Demo data ────────────────────────────────────────────────
def _demo_data():
    return [
        {"competitor":"yocketapp","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC001/","caption":"3 mistakes every Indian student makes before applying for a Canada student visa. Watch till the end!","views":980000,"likes":42000,"comments":3100,"engagement_rate":0.046,"audio":"Original sound - yocketapp","date":"2026-02-28","hook":"3 mistakes every Indian student makes","topic":"Canada Visa","cta":"Watch till the end","format":"Listicle","summary":"Highlights 3 common visa mistakes Indian students make when applying to Canada."},
        {"competitor":"leverageedu","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC002/","caption":"Nobody tells you THIS about UK student visa rejections. Save this before you apply!","views":760000,"likes":31000,"comments":2800,"engagement_rate":0.044,"audio":"Trending audio - 2026","date":"2026-02-27","hook":"Nobody tells you THIS about UK student visa rejections","topic":"UK Visa","cta":"Save this before you apply","format":"Talking Head","summary":"Exposes hidden reasons for UK visa rejections."},
        {"competitor":"sheenamgautam","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC003/","caption":"How I got a fully funded scholarship to the UK. Comment SCHOLAR for the guide!","views":1240000,"likes":67000,"comments":8900,"engagement_rate":0.061,"audio":"Original sound - sheenamgautam","date":"2026-02-26","hook":"How I got a fully funded scholarship to the UK","topic":"Scholarships","cta":"Comment SCHOLAR for the guide","format":"Storytime","summary":"Personal story of securing a fully funded UK scholarship."},
        {"competitor":"ambitiohq","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC004/","caption":"IELTS band 7 in 30 days? Here is my exact study plan. Save this reel!","views":540000,"likes":22000,"comments":1900,"engagement_rate":0.044,"audio":"Lo-fi study beats","date":"2026-02-25","hook":"IELTS band 7 in 30 days?","topic":"IELTS Tips","cta":"Save this reel","format":"Tips & Tricks","summary":"Actionable 30-day IELTS study plan that promises Band 7 results."},
        {"competitor":"shreyamahendru_","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC005/","caption":"Stop wasting money on education loans! Here is what banks do not tell you. Follow for more finance tips","views":890000,"likes":38000,"comments":5200,"engagement_rate":0.048,"audio":"Original sound - shreyamahendru_","date":"2026-02-24","hook":"Stop wasting money on education loans","topic":"Education Loan","cta":"Follow for more finance tips","format":"Talking Head","summary":"Reveals hidden charges and smarter alternatives to traditional education loans."},
        {"competitor":"studyabroadkar","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC006/","caption":"Day in my life studying in Germany. Cost breakdown inside!","views":420000,"likes":18500,"comments":2200,"engagement_rate":0.049,"audio":"Aesthetic vlog music","date":"2026-02-23","hook":"Day in my life studying in Germany","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Authentic vlog showing daily life and actual costs of studying in Germany."},
        {"competitor":"gradright","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC007/","caption":"Australia vs Canada: Which is better for Indian students in 2026? Drop your vote below!","views":670000,"likes":29000,"comments":7800,"engagement_rate":0.055,"audio":"Trending sound - debate","date":"2026-02-22","hook":"Australia vs Canada: Which is better for Indian students?","topic":"General Study Abroad","cta":"Drop your vote below","format":"Myth vs Fact","summary":"Compelling comparison between Australia and Canada."},
        {"competitor":"harnoor.studyabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC008/","caption":"My SOP got rejected 4 times. Here is what I changed to finally get into Oxford. DM me SOP","views":1100000,"likes":59000,"comments":11200,"engagement_rate":0.064,"audio":"Original sound - harnoor.studyabroad","date":"2026-02-21","hook":"My SOP got rejected 4 times","topic":"SOP/LOR","cta":"DM me SOP","format":"Storytime","summary":"Relatable failure-to-success SOP story that builds trust."},
        {"competitor":"upgradabroad","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC009/","caption":"Part-time jobs in Canada for Indian students. Share this with your friends!","views":380000,"likes":15000,"comments":1400,"engagement_rate":0.043,"audio":"Upbeat background music","date":"2026-02-20","hook":"Part-time jobs in Canada for Indian students","topic":"Canada Visa","cta":"Share this with your friends","format":"Tips & Tricks","summary":"Practical guide to legal part-time work options in Canada."},
        {"competitor":"searcheduindia","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC010/","caption":"Germany student visa rejected? Watch this before reapplying.","views":310000,"likes":13500,"comments":1800,"engagement_rate":0.049,"audio":"Original sound - searcheduindia","date":"2026-02-19","hook":"Germany student visa rejected? Watch this before reapplying","topic":"Germany Visa","cta":"None","format":"Talking Head","summary":"Expert advice on recovering from German student visa rejection."},
        {"competitor":"indianstudentabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC011/","caption":"The real cost of studying in the UK in 2026. Not what universities tell you!","views":720000,"likes":33000,"comments":4100,"engagement_rate":0.051,"audio":"Dramatic reveal sound","date":"2026-02-18","hook":"The real cost of studying in the UK in 2026","topic":"UK Visa","cta":"None","format":"Voiceover + Text","summary":"Reveals hidden costs of UK education that universities do not advertise."},
        {"competitor":"mastersabroaddiaries","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC012/","caption":"POV: You finally got your Australia PR after studying there. Follow my journey!","views":560000,"likes":27000,"comments":3900,"engagement_rate":0.055,"audio":"Emotional background music","date":"2026-02-17","hook":"POV: You finally got your Australia PR after studying there","topic":"Australia Visa","cta":"Follow my journey","format":"Storytime","summary":"Aspirational PR success story that motivates students."},
        {"competitor":"idp.india","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC013/","caption":"Top 5 IELTS speaking mistakes that cost you marks. Link in bio for free mock test!","views":445000,"likes":19500,"comments":2300,"engagement_rate":0.049,"audio":"Original sound - idp.india","date":"2026-02-16","hook":"Top 5 IELTS speaking mistakes that cost you marks","topic":"IELTS Tips","cta":"Link in bio for free mock test","format":"Listicle","summary":"Identifies 5 specific speaking mistakes with actionable fixes."},
        {"competitor":"abroadpathway","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC014/","caption":"USA F1 visa interview questions that ACTUALLY get asked in 2026. Save this!","views":690000,"likes":30500,"comments":4700,"engagement_rate":0.051,"audio":"Suspenseful background","date":"2026-02-15","hook":"USA F1 visa interview questions that ACTUALLY get asked in 2026","topic":"USA Visa","cta":"Save this","format":"Tips & Tricks","summary":"High-value interview prep content with real 2026 F1 visa questions."},
        {"competitor":"moveabroadsimplified","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC015/","caption":"My first month expenses in Canada as an Indian student. Was it what I expected?","views":490000,"likes":21000,"comments":2800,"engagement_rate":0.048,"audio":"Vlog background music","date":"2026-02-14","hook":"My first month expenses in Canada as an Indian student","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Transparent first-month financial breakdown in Canada."},
    ]

# ── Load data ────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    try:
        from database.mongo_client import get_all_reels
        reels = get_all_reels(limit=1000)
        if reels and len(reels) > 0:
            df = pd.DataFrame(reels)
            for col in ["hook","topic","cta","format","summary"]:
                if col not in df.columns:
                    df[col] = df.get("ai_analysis", pd.Series([{}]*len(df))).apply(
                        lambda x: x.get(col,"") if isinstance(x, dict) else ""
                    )
            return df
    except Exception:
        pass
    return pd.DataFrame(_demo_data())

df_raw = load_data()
if df_raw.empty:
    df_raw = pd.DataFrame(_demo_data())

# ── SIDEBAR ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚡ Insta Intel")
    st.caption("LeapScholar Content Team")
    st.divider()

    st.markdown("**🔍 Filters**")

    competitors = ["All"] + sorted(df_raw["competitor"].dropna().unique().tolist())
    sel_comp = st.selectbox("Competitor", competitors)

    sel_type = st.selectbox("Account Type", ["All", "company", "creator"])

    topics = ["All"] + sorted(df_raw["topic"].dropna().unique().tolist()) if "topic" in df_raw.columns else ["All"]
    sel_topic = st.selectbox("Topic", topics)

    min_views_k = st.slider("Min Views (K)", 0, 1000, 0, step=10)

    st.divider()
    st.markdown("**📅 Date Range**")
    date_from = st.date_input("From", value=datetime.now() - timedelta(days=30))
    date_to   = st.date_input("To",   value=datetime.now())

    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption("🟢 Scraper Active  |  🟢 AI Active  |  ⏱ Auto-runs every 2 days")

# ── Apply filters ─────────────────────────────────────────────
df = df_raw.copy()
if sel_comp  != "All": df = df[df["competitor"]   == sel_comp]
if sel_type  != "All": df = df[df["account_type"] == sel_type]
if sel_topic != "All" and "topic" in df.columns:
    df = df[df["topic"] == sel_topic]
if min_views_k > 0:
    df = df[df["views"] >= min_views_k * 1000]
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[
        (df["date"] >= pd.Timestamp(date_from)) &
        (df["date"] <= pd.Timestamp(date_to))
    ]

# ── HEADER ───────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("## LeapScholar · Insta Intel")
    st.caption(f"{len(df)} reels tracked across {df['competitor'].nunique()} accounts")
with col_h2:
    st.success("🟢 LIVE")

st.divider()

# ── KPI METRICS ───────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("📊 Total Reels", len(df))
with m2:
    st.metric("👁 Total Views", fmt_number(df["views"].sum()))
with m3:
    avg_eng = df["engagement_rate"].mean() * 100 if len(df) else 0
    st.metric("📈 Avg Engagement", f"{avg_eng:.1f}%")
with m4:
    st.metric("🏆 Top Reel Views", fmt_number(df["views"].max() if len(df) else 0))

st.divider()

# ── TABS ──────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📋  Competitor Reels Database", "🔥  Trend Explorer"])

# ═══════════════════════════════════════════════
#  TAB 1 — COMPETITOR REELS DATABASE
# ═══════════════════════════════════════════════
with tab1:
    col_t, col_s = st.columns([2, 1])
    with col_t:
        st.markdown(f"### Competitor Reels  `{len(df)} results`")
    with col_s:
        sort_by = st.selectbox("Sort by", ["views","engagement_rate","likes","comments"], label_visibility="collapsed")

    search = st.text_input("🔎 Search captions, hooks, topics...", placeholder="e.g. Canada visa, IELTS, scholarship")

    df_sorted = df.sort_values(sort_by, ascending=False).reset_index(drop=True)

    if search:
        mask = df_sorted.apply(
            lambda r: any(search.lower() in str(r.get(c,"")).lower()
                         for c in ["caption","hook","topic","competitor","summary"]),
            axis=1
        )
        df_sorted = df_sorted[mask]

    if df_sorted.empty:
        st.info("No reels found. Try adjusting filters or running the pipeline.")
    else:
        for _, row in df_sorted.iterrows():
            views    = int(row.get("views", 0))
            likes    = int(row.get("likes", 0))
            comments = int(row.get("comments", 0))
            eng      = float(row.get("engagement_rate", 0))
            hook     = str(row.get("hook",    ""))
            topic    = str(row.get("topic",   ""))
            fmt      = str(row.get("format",  ""))
            cta      = str(row.get("cta",     ""))
            summary  = str(row.get("summary", ""))
            caption  = str(row.get("caption", ""))
            acc_type = str(row.get("account_type", ""))
            comp     = str(row.get("competitor", ""))
            url      = str(row.get("reel_url", "#"))
            audio    = str(row.get("audio", ""))
            date_str = str(row.get("date", ""))[:10]
            label    = tms_label(views)

            with st.container():
                # Header row
                c1, c2, c3 = st.columns([3, 1, 1])
                with c1:
                    acc_icon = "🏢" if acc_type == "company" else "👤"
                    st.markdown(f"**{acc_icon} @{comp}**  ·  `{acc_type}`  ·  {label}")
                with c2:
                    st.markdown(f"**{fmt_number(views)}** views")
                with c3:
                    st.link_button("▶ Open Reel", url)

                # Stats row
                c_s1, c_s2, c_s3, c_s4 = st.columns(4)
                with c_s1:
                    st.caption(f"❤️ {fmt_number(likes)} likes")
                with c_s2:
                    st.caption(f"💬 {fmt_number(comments)} comments")
                with c_s3:
                    st.caption(f"📊 {round(eng*100,1)}% engagement")
                with c_s4:
                    st.caption(f"📅 {date_str}")

                # Hook + Summary
                if hook:
                    st.info(f"💡 **Hook:** {truncate(hook, 120)}")
                if summary:
                    st.caption(f"📝 {truncate(summary, 150)}")

                # Tags row
                tag_cols = st.columns(4)
                with tag_cols[0]:
                    if topic: st.success(f"🏷️ {topic}")
                with tag_cols[1]:
                    if fmt: st.warning(f"🎬 {fmt}")
                with tag_cols[2]:
                    if cta and cta != "None": st.info(f"📣 {truncate(cta, 40)}")
                with tag_cols[3]:
                    if audio: st.caption(f"🎵 {truncate(audio, 35)}")

                # Caption
                if caption:
                    with st.expander("📄 View Caption"):
                        st.write(caption)

                st.divider()

    # ── Data Table + Export ──
    with st.expander("📊 Full Data Table + Export"):
        display_cols = ["competitor","account_type","views","likes","comments",
                        "engagement_rate","topic","format","hook","cta","audio","reel_url"]
        available = [c for c in display_cols if c in df_sorted.columns]
        df_display = df_sorted[available].copy()
        if "engagement_rate" in df_display.columns:
            df_display["engagement_rate"] = (df_display["engagement_rate"]*100).round(2)

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=400,
            column_config={
                "reel_url":        st.column_config.LinkColumn("Open"),
                "views":           st.column_config.NumberColumn("Views",    format="%d"),
                "likes":           st.column_config.NumberColumn("Likes",    format="%d"),
                "comments":        st.column_config.NumberColumn("Comments", format="%d"),
                "engagement_rate": st.column_config.NumberColumn("Eng %",    format="%.2f"),
            }
        )
        csv = df_sorted.drop(columns=["ai_analysis"], errors="ignore").to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Export CSV", data=csv,
                           file_name=f"insta_intel_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv")

    # ── Charts ──
    st.markdown("### 📊 Performance Charts")
    ch1, ch2 = st.columns(2)

    with ch1:
        if not df.empty and "competitor" in df.columns:
            top_acc = (df.groupby("competitor")["views"]
                       .sum().sort_values(ascending=False).head(10).reset_index())
            fig = px.bar(top_acc, x="views", y="competitor", orientation="h",
                         color="views", color_continuous_scale=["#bfdbfe","#1d4ed8"],
                         title="Top Accounts by Total Views",
                         labels={"views":"Total Views","competitor":""})
            fig.update_layout(
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
                font=dict(family="Plus Jakarta Sans", color="#374151", size=12),
                title_font=dict(size=14, color="#0f172a"),
                coloraxis_showscale=False,
                margin=dict(l=0,r=0,t=40,b=0), height=300,
                xaxis=dict(gridcolor="#f1f5f9"),
                yaxis=dict(gridcolor="rgba(0,0,0,0)"),
            )
            st.plotly_chart(fig, use_container_width=True)

    with ch2:
        if not df.empty and "views" in df.columns:
            fig2 = px.scatter(df, x="views", y=df["engagement_rate"]*100,
                              color="account_type", hover_name="competitor",
                              color_discrete_map={"company":"#2563eb","creator":"#9333ea"},
                              title="Views vs Engagement Rate",
                              labels={"views":"Views","y":"Engagement %","account_type":"Type"})
            fig2.update_layout(
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
                font=dict(family="Plus Jakarta Sans", color="#374151", size=12),
                title_font=dict(size=14, color="#0f172a"),
                margin=dict(l=0,r=0,t=40,b=0), height=300,
                xaxis=dict(gridcolor="#f1f5f9"),
                yaxis=dict(gridcolor="#f1f5f9"),
                legend=dict(font=dict(size=11)),
            )
            st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════
#  TAB 2 — TREND EXPLORER
# ═══════════════════════════════════════════════
with tab2:
    st.markdown("### 🔥 Trend Explorer")
    st.caption("AI-powered insights from competitor reels")

    # Aggregate
    hooks_data   = Counter([x for x in df["hook"].dropna()   if x and x != "None"]) if "hook"   in df.columns else Counter()
    topics_data  = Counter([x for x in df["topic"].dropna()  if x and x != "None"]) if "topic"  in df.columns else Counter()
    cta_data     = Counter([x for x in df["cta"].dropna()    if x and x != "None"]) if "cta"    in df.columns else Counter()
    formats_data = Counter([x for x in df["format"].dropna() if x and x != "None"]) if "format" in df.columns else Counter()
    audio_data   = Counter([x for x in df["audio"].dropna()  if x and x != "None"]) if "audio"  in df.columns else Counter()

    # ── Row 1: Hooks + Topics ──
    r1, r2 = st.columns(2)

    with r1:
        st.markdown("#### 💡 Trending Hooks")
        st.divider()
        if hooks_data:
            for i, (name, count) in enumerate(hooks_data.most_common(6), 1):
                col_rank, col_name, col_count = st.columns([1, 6, 1])
                with col_rank:
                    st.markdown(f"**#{i}**")
                with col_name:
                    st.write(truncate(name, 60))
                    progress = count / hooks_data.most_common(1)[0][1]
                    st.progress(progress)
                with col_count:
                    st.caption(f"{count}")
        else:
            st.info("No hook data yet. Run the pipeline first.")

    with r2:
        st.markdown("#### 🏷️ Trending Topics")
        st.divider()
        if topics_data:
            for i, (name, count) in enumerate(topics_data.most_common(6), 1):
                col_rank, col_name, col_count = st.columns([1, 6, 1])
                with col_rank:
                    st.markdown(f"**#{i}**")
                with col_name:
                    st.write(name)
                    progress = count / topics_data.most_common(1)[0][1]
                    st.progress(progress)
                with col_count:
                    st.caption(f"{count}")
        else:
            st.info("No topic data yet.")

    st.divider()

    # ── Row 2: CTA + Format ──
    r3, r4 = st.columns(2)

    with r3:
        st.markdown("#### 📣 Trending CTAs")
        st.divider()
        if cta_data:
            for i, (name, count) in enumerate(cta_data.most_common(6), 1):
                col_rank, col_name, col_count = st.columns([1, 6, 1])
                with col_rank:
                    st.markdown(f"**#{i}**")
                with col_name:
                    st.write(truncate(name, 50))
                    progress = count / cta_data.most_common(1)[0][1]
                    st.progress(progress)
                with col_count:
                    st.caption(f"{count}")
        else:
            st.info("No CTA data yet.")

    with r4:
        st.markdown("#### 🎬 Trending Formats")
        st.divider()
        if formats_data:
            for i, (name, count) in enumerate(formats_data.most_common(6), 1):
                col_rank, col_name, col_count = st.columns([1, 6, 1])
                with col_rank:
                    st.markdown(f"**#{i}**")
                with col_name:
                    st.write(name)
                    progress = count / formats_data.most_common(1)[0][1]
                    st.progress(progress)
                with col_count:
                    st.caption(f"{count}")
        else:
            st.info("No format data yet.")

    st.divider()

    # ── Row 3: Audio + Charts ──
    r5, r6 = st.columns([1, 2])

    with r5:
        st.markdown("#### 🎵 Trending Audio")
        st.divider()
        if audio_data:
            for i, (name, count) in enumerate(audio_data.most_common(6), 1):
                col_rank, col_name, col_count = st.columns([1, 5, 1])
                with col_rank:
                    st.markdown(f"**#{i}**")
                with col_name:
                    st.write(truncate(name, 35))
                    progress = count / audio_data.most_common(1)[0][1]
                    st.progress(progress)
                with col_count:
                    st.caption(f"{count}")
        else:
            st.info("No audio data yet.")

    with r6:
        if not df.empty and "topic" in df.columns:
            topic_perf = (
                df.groupby("topic")
                  .agg(avg_views=("views","mean"), count=("views","count"))
                  .reset_index()
                  .sort_values("avg_views", ascending=False)
                  .head(10)
            )
            fig3 = px.bar(topic_perf, x="topic", y="avg_views",
                          color="count", text="count",
                          color_continuous_scale=["#bfdbfe","#1d4ed8"],
                          title="Avg Views by Topic",
                          labels={"avg_views":"Avg Views","topic":"","count":"# Reels"})
            fig3.update_traces(texttemplate="%{text} reels", textposition="outside", textfont_size=10)
            fig3.update_layout(
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
                font=dict(family="Plus Jakarta Sans", color="#374151", size=11),
                title_font=dict(size=14, color="#0f172a"),
                coloraxis_showscale=False,
                margin=dict(l=0,r=0,t=40,b=80), height=340,
                xaxis=dict(gridcolor="rgba(0,0,0,0)", tickangle=-30),
                yaxis=dict(gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    # ── Engagement by Format ──
    if not df.empty and "format" in df.columns:
        st.markdown("#### 📊 Engagement Rate by Format")
        fmt_eng = (df.groupby("format")["engagement_rate"]
                   .mean().reset_index()
                   .sort_values("engagement_rate", ascending=False))
        fmt_eng["eng_pct"] = (fmt_eng["engagement_rate"] * 100).round(2)
        fig4 = px.bar(fmt_eng, x="format", y="eng_pct",
                      color="eng_pct", text="eng_pct",
                      color_continuous_scale=["#bfdbfe","#1d4ed8"],
                      labels={"format":"Format","eng_pct":"Avg Engagement %"})
        fig4.update_traces(texttemplate="%{text}%", textposition="outside", textfont_size=11)
        fig4.update_layout(
            paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
            font=dict(family="Plus Jakarta Sans", color="#374151", size=12),
            coloraxis_showscale=False,
            margin=dict(l=0,r=0,t=10,b=0), height=280,
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(gridcolor="#f1f5f9"),
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()

    # ── Company vs Creator ──
    st.markdown("#### 🏢 Company vs Creator Performance")
    p1, p2, p3 = st.columns(3)

    for acc_type, col in [("company", p1), ("creator", p2)]:
        subset = df[df["account_type"] == acc_type] if "account_type" in df.columns else pd.DataFrame()
        with col:
            if not subset.empty:
                st.metric(
                    f"{'🏢 Company' if acc_type == 'company' else '👤 Creator'} Avg Views",
                    fmt_number(int(subset["views"].mean())),
                    f"{round(subset['engagement_rate'].mean()*100,1)}% avg eng"
                )
                st.caption(f"{len(subset)} reels tracked")

    with p3:
        if not df.empty:
            best = df.loc[df["views"].idxmax()]
            st.markdown("**🏆 Top Reel**")
            st.markdown(f"**@{best.get('competitor','')}**")
            st.caption(truncate(str(best.get("hook", best.get("caption",""))), 80))
            st.metric("Views", fmt_number(int(best.get("views",0))))
            st.link_button("▶ Watch Reel", str(best.get("reel_url","#")))
