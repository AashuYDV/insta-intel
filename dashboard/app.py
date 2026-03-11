"""
insta_intel/dashboard/app.py
LeapScholar Insta Intel - Content Intelligence Dashboard
Tabs: Competitor Reels | Trend Explorer | Weekly Digest | Scorecard | Content Gap
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
from collections import Counter
from datetime import datetime, timedelta
import re

st.set_page_config(
    page_title="LeapScholar - Insta Intel",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
.stDeployButton { display: none; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt(n):
    if not n: return "0"
    n = int(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)

def trunc(text, n=100):
    if not text: return ""
    s = str(text)
    return s if len(s) <= n else s[:n] + "..."

def viral_score(views, engagement_rate):
    """Combined score: 60% views weight + 40% engagement weight."""
    v = int(views or 0)
    e = float(engagement_rate or 0)
    return round((v * 0.6) + (e * 100 * 10000 * 0.4))

def viral_badge(views):
    v = int(views or 0)
    if v >= 800_000: return "🔥 Viral"
    if v >= 400_000: return "⚡ Hot"
    if v >= 100_000: return "📈 Trending"
    return "Active"

def hook_pattern(hook):
    """Classify hook into a reusable pattern."""
    if not hook: return "Other"
    h = str(hook).lower()
    if re.search(r"^\d+\s", h):                         return "Numbered List"
    if h.startswith("pov"):                              return "POV"
    if any(h.startswith(w) for w in ["nobody", "no one", "they don't", "they never"]): return "Secret/Hidden Info"
    if "?" in h[:60]:                                   return "Question Hook"
    if any(w in h[:60] for w in ["stop", "don't", "never", "avoid"]): return "Warning/Mistake"
    if any(w in h[:60] for w in ["how i", "how to", "here's how"]): return "How I/How To"
    if any(w in h[:60] for w in ["my ", "i got", "i made", "i failed"]): return "Personal Story"
    if any(w in h[:60] for w in ["real", "truth", "honest", "actual"]): return "Reality Check"
    return "Other"


# ── Demo Data ─────────────────────────────────────────────────────────────────

DEMO = [
    {"competitor":"sheenamgautam","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC003/","caption":"How I got a fully funded scholarship to the UK. Comment SCHOLAR for the guide!","views":1240000,"likes":67000,"comments":8900,"engagement_rate":0.061,"audio":"Original sound - sheenamgautam","date":"2026-02-26","hook":"How I got a fully funded scholarship to the UK","topic":"Scholarships","cta":"Comment SCHOLAR for the guide","format":"Storytime","summary":"Personal story of securing a fully funded UK scholarship."},
    {"competitor":"harnoor.studyabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC008/","caption":"My SOP got rejected 4 times. Here is what I changed to finally get into Oxford. DM me SOP","views":1100000,"likes":59000,"comments":11200,"engagement_rate":0.064,"audio":"Original sound - harnoor","date":"2026-02-21","hook":"My SOP got rejected 4 times","topic":"SOP/LOR","cta":"DM me SOP","format":"Storytime","summary":"Failure to success SOP story that builds trust."},
    {"competitor":"yocketapp","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC001/","caption":"3 mistakes every Indian student makes before applying for a Canada student visa.","views":980000,"likes":42000,"comments":3100,"engagement_rate":0.046,"audio":"Original sound - yocketapp","date":"2026-02-28","hook":"3 mistakes every Indian student makes","topic":"Canada Visa","cta":"Watch till the end","format":"Listicle","summary":"Highlights 3 common visa mistakes Indian students make."},
    {"competitor":"leverageedu","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC002/","caption":"Nobody tells you THIS about UK student visa rejections. Save this before you apply!","views":760000,"likes":31000,"comments":2800,"engagement_rate":0.044,"audio":"Trending audio","date":"2026-02-27","hook":"Nobody tells you THIS about UK student visa rejections","topic":"UK Visa","cta":"Save this before you apply","format":"Talking Head","summary":"Exposes hidden reasons for UK visa rejections."},
    {"competitor":"shreyamahendru_","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC005/","caption":"Stop wasting money on education loans! Here is what banks do not tell you.","views":890000,"likes":38000,"comments":5200,"engagement_rate":0.048,"audio":"Original sound","date":"2026-02-24","hook":"Stop wasting money on education loans","topic":"Education Loan","cta":"Follow for more finance tips","format":"Talking Head","summary":"Reveals hidden charges in education loans."},
    {"competitor":"gradright","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC007/","caption":"Australia vs Canada: Which is better for Indian students in 2026?","views":670000,"likes":29000,"comments":7800,"engagement_rate":0.055,"audio":"Trending debate sound","date":"2026-02-22","hook":"Australia vs Canada: Which is better?","topic":"General Study Abroad","cta":"Drop your vote below","format":"Myth vs Fact","summary":"Comparison between Australia and Canada for Indian students."},
    {"competitor":"indianstudentabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC011/","caption":"The real cost of studying in the UK in 2026. Not what universities tell you!","views":720000,"likes":33000,"comments":4100,"engagement_rate":0.051,"audio":"Dramatic reveal sound","date":"2026-02-18","hook":"The real cost of studying in the UK in 2026","topic":"Cost of Living","cta":"None","format":"Voiceover + Text","summary":"Reveals hidden costs of UK education."},
    {"competitor":"abroadpathway","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC014/","caption":"USA F1 visa interview questions that ACTUALLY get asked in 2026. Save this!","views":690000,"likes":30500,"comments":4700,"engagement_rate":0.051,"audio":"Suspenseful background","date":"2026-02-15","hook":"USA F1 visa interview questions in 2026","topic":"USA Visa","cta":"Save this","format":"Tips & Tricks","summary":"Real F1 visa interview questions for 2026."},
    {"competitor":"mastersabroaddiaries","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC012/","caption":"POV: You finally got your Australia PR after studying there. Follow my journey!","views":560000,"likes":27000,"comments":3900,"engagement_rate":0.055,"audio":"Emotional background music","date":"2026-02-17","hook":"POV: You finally got your Australia PR","topic":"Australia Visa","cta":"Follow my journey","format":"Storytime","summary":"Aspirational Australia PR success story."},
    {"competitor":"ambitiohq","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC004/","caption":"IELTS band 7 in 30 days? Here is my exact study plan. Save this reel!","views":540000,"likes":22000,"comments":1900,"engagement_rate":0.044,"audio":"Lo-fi study beats","date":"2026-02-25","hook":"IELTS band 7 in 30 days?","topic":"IELTS Tips","cta":"Save this reel","format":"Tips & Tricks","summary":"30-day IELTS study plan for Band 7."},
    {"competitor":"idp.india","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC013/","caption":"Top 5 IELTS speaking mistakes that cost you marks. Link in bio for free mock test!","views":445000,"likes":19500,"comments":2300,"engagement_rate":0.049,"audio":"Original sound - idp","date":"2026-02-16","hook":"Top 5 IELTS speaking mistakes","topic":"IELTS Tips","cta":"Link in bio for free mock test","format":"Listicle","summary":"5 IELTS speaking mistakes with fixes."},
    {"competitor":"moveabroadsimplified","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC015/","caption":"My first month expenses in Canada as an Indian student. Was it what I expected?","views":490000,"likes":21000,"comments":2800,"engagement_rate":0.048,"audio":"Vlog background music","date":"2026-02-14","hook":"My first month expenses in Canada","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Transparent first-month financial breakdown in Canada."},
    {"competitor":"studyabroadkar","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC006/","caption":"Day in my life studying in Germany. Cost breakdown inside!","views":420000,"likes":18500,"comments":2200,"engagement_rate":0.049,"audio":"Aesthetic vlog music","date":"2026-02-23","hook":"Day in my life studying in Germany","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Daily life and costs of studying in Germany."},
    {"competitor":"upgradabroad","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC009/","caption":"Part-time jobs in Canada for Indian students. Share this with your friends!","views":380000,"likes":15000,"comments":1400,"engagement_rate":0.043,"audio":"Upbeat background","date":"2026-02-20","hook":"Part-time jobs in Canada for Indian students","topic":"Part-time Jobs","cta":"Share this with your friends","format":"Tips & Tricks","summary":"Guide to legal part-time work in Canada."},
    {"competitor":"searcheduindia","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC010/","caption":"Germany student visa rejected? Watch this before reapplying.","views":310000,"likes":13500,"comments":1800,"engagement_rate":0.049,"audio":"Original sound","date":"2026-02-19","hook":"Germany student visa rejected?","topic":"Germany Visa","cta":"None","format":"Talking Head","summary":"Advice on recovering from German visa rejection."},
]

LEAPSCHOLAR_TOPICS = {
    "UK Visa", "Canada Visa", "Australia Visa", "USA Visa", "Germany Visa",
    "IELTS Tips", "Scholarships", "Education Loan", "University Admissions",
    "General Study Abroad"
}


# ── Data Loader ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    try:
        from config.settings import MONGO_URI, MONGO_DB
        if not MONGO_URI:
            st.sidebar.error("MONGO_URI is empty — check Streamlit secrets")
            return pd.DataFrame(DEMO), False

        from pymongo import MongoClient
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=8000,
            tls=True,
            tlsAllowInvalidCertificates=True,
        )
        db   = client[MONGO_DB]
        data = list(db["reels"].find({}, {"_id": 0}).limit(1000))

        if not data:
            st.sidebar.warning("MongoDB connected but reels collection is empty")
            return pd.DataFrame(DEMO), False

        df = pd.DataFrame(data)
        if "ai_analysis" in df.columns:
            for field in ["hook", "topic", "cta", "format", "summary"]:
                if field not in df.columns:
                    df[field] = df["ai_analysis"].apply(
                        lambda x: x.get(field, "") if isinstance(x, dict) else ""
                    )
        return df, True

    except Exception as e:
        st.sidebar.error(f"MongoDB error: {e}")
        return pd.DataFrame(DEMO), False


df_raw, is_live = load_data()
if df_raw.empty:
    df_raw = pd.DataFrame(DEMO)
    is_live = False

for col in ["hook", "topic", "cta", "format", "summary", "audio"]:
    if col not in df_raw.columns:
        df_raw[col] = ""

# Add viral score column
df_raw["viral_score"] = df_raw.apply(
    lambda r: viral_score(r.get("views", 0), r.get("engagement_rate", 0)), axis=1
)

# Add hook pattern column
df_raw["hook_pattern"] = df_raw["hook"].apply(hook_pattern)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🚀 Insta Intel")
    st.caption("LeapScholar Content Team")
    if is_live:
        st.success("✅ Connected to MongoDB")
    else:
        st.warning("⚠️ Showing demo data")
    st.divider()

    st.subheader("Filters")
    competitors = ["All"] + sorted(df_raw["competitor"].dropna().unique().tolist())
    sel_comp = st.selectbox("Competitor", competitors)

    sel_type = st.selectbox("Account Type", ["All", "company", "creator"])

    all_topics = sorted(df_raw["topic"].dropna().replace("", pd.NA).dropna().unique().tolist())
    sel_topic = st.selectbox("Topic", ["All"] + all_topics)

    min_views_k = st.slider("Min Views (K)", 0, 1000, 0, step=10)

    st.divider()
    st.subheader("Date Range")
    date_from = st.date_input("From", value=datetime.now() - timedelta(days=365))
    date_to   = st.date_input("To",   value=datetime.now())

    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Pipeline auto-runs every 2 days")


# ── Apply Filters ─────────────────────────────────────────────────────────────

df = df_raw.copy()
if sel_comp  != "All": df = df[df["competitor"]   == sel_comp]
if sel_type  != "All": df = df[df["account_type"] == sel_type]
if sel_topic != "All": df = df[df["topic"]        == sel_topic]
if min_views_k > 0:    df = df[df["views"]        >= min_views_k * 1000]
if "date" in df.columns:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[
        (df["date"] >= pd.Timestamp(date_from)) &
        (df["date"] <= pd.Timestamp(date_to))
    ]
df = df.sort_values("views", ascending=False).reset_index(drop=True)


# ── Header ────────────────────────────────────────────────────────────────────

st.title("🚀 LeapScholar — Insta Intel")
st.caption(
    f"Tracking {len(df)} reels across {df['competitor'].nunique()} accounts"
    + ("  |  🟢 LIVE DATA" if is_live else "  |  🟡 Demo Mode")
)
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Reels",     str(len(df)))
c2.metric("Total Views",     fmt(df["views"].sum()))
c3.metric("Avg Engagement",  f"{round(df['engagement_rate'].mean()*100, 1)}%")
c4.metric("Top Reel Views",  fmt(df["views"].max() if len(df) else 0))
c5.metric("Accounts",        str(df["competitor"].nunique()))
st.divider()


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 Competitor Reels",
    "📊 Trend Explorer",
    "📅 Weekly Digest",
    "🏆 Scorecard",
    "💡 Content Gap",
])


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — COMPETITOR REELS DATABASE
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    col_hd, col_sort = st.columns([3, 1])
    with col_hd:
        st.subheader(f"Competitor Reels  ({len(df)} results)")
    with col_sort:
        sort_by = st.selectbox(
            "Sort by",
            ["viral_score", "views", "engagement_rate", "likes", "comments"],
            label_visibility="collapsed"
        )

    search = st.text_input("🔍 Search captions, hooks, topics, competitors...", key="search_box")
    df_view = df.sort_values(sort_by, ascending=False).reset_index(drop=True)

    if search:
        mask = df_view.apply(
            lambda r: any(
                search.lower() in str(r.get(c, "")).lower()
                for c in ["caption", "hook", "topic", "competitor", "summary", "cta"]
            ),
            axis=1
        )
        df_view = df_view[mask]
        st.caption(f"Found {len(df_view)} results for '{search}'")

    if df_view.empty:
        st.info("No reels match your filters.")
    else:
        for _, row in df_view.iterrows():
            comp     = str(row.get("competitor",     "unknown"))
            acc_type = str(row.get("account_type",   ""))
            views    = int(row.get("views",          0))
            likes    = int(row.get("likes",          0))
            comments = int(row.get("comments",       0))
            eng      = float(row.get("engagement_rate", 0))
            hook     = str(row.get("hook",    "") or "")
            topic    = str(row.get("topic",   "") or "")
            fmt_val  = str(row.get("format",  "") or "")
            cta_val  = str(row.get("cta",     "") or "")
            summary  = str(row.get("summary", "") or "")
            caption  = str(row.get("caption", "") or "")
            audio    = str(row.get("audio",   "") or "")
            url      = str(row.get("reel_url", "#"))
            date_val = str(row.get("date",    ""))[:10]
            badge    = viral_badge(views)
            vscore   = int(row.get("viral_score", 0))
            icon     = "🏢 Company" if acc_type == "company" else "👤 Creator"

            with st.container(border=True):
                h1, h2, h3, h4 = st.columns([3, 2, 1, 1])
                with h1:
                    st.markdown(f"**@{comp}**  |  {icon}  |  {badge}")
                with h2:
                    st.markdown(f"**{fmt(views)} views**")
                with h3:
                    st.caption(f"Score: {fmt(vscore)}")
                with h4:
                    st.link_button("Open Reel ↗", url)

                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Likes",      fmt(likes))
                s2.metric("Comments",   fmt(comments))
                s3.metric("Engagement", f"{round(eng*100, 1)}%")
                s4.metric("Date",       date_val)

                if hook and hook not in ("", "None"):
                    st.info(f"🎣 Hook: {trunc(hook, 150)}")

                if summary and summary not in ("", "None"):
                    st.caption(trunc(summary, 180))

                tag_cols = st.columns(4)
                with tag_cols[0]:
                    if topic and topic not in ("", "None"):
                        st.success(topic)
                with tag_cols[1]:
                    if fmt_val and fmt_val not in ("", "None"):
                        st.warning(fmt_val)
                with tag_cols[2]:
                    if cta_val and cta_val not in ("", "None"):
                        st.info(trunc(cta_val, 50))
                with tag_cols[3]:
                    if audio and audio not in ("", "None"):
                        st.caption(f"🎵 {trunc(audio, 40)}")

                if caption and caption not in ("", "None"):
                    with st.expander("View Caption"):
                        st.write(caption)

    st.divider()
    st.subheader("Performance Charts")

    ch1, ch2 = st.columns(2)
    with ch1:
        top = (df.groupby("competitor")["views"]
               .sum().sort_values(ascending=False).head(10).reset_index())
        fig = px.bar(
            top, x="views", y="competitor", orientation="h",
            title="Top Accounts by Total Views",
            color="views",
            color_continuous_scale=["#93c5fd", "#1d4ed8"],
            labels={"views": "Total Views", "competitor": ""},
        )
        fig.update_layout(
            height=350, coloraxis_showscale=False,
            paper_bgcolor="white", plot_bgcolor="white",
            margin=dict(l=0, r=10, t=40, b=0),
            yaxis=dict(autorange="reversed"),
            xaxis=dict(gridcolor="#f1f5f9"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with ch2:
        fig2 = px.scatter(
            df, x="views", y=df["engagement_rate"] * 100,
            color="account_type", hover_name="competitor",
            color_discrete_map={"company": "#2563eb", "creator": "#9333ea"},
            title="Views vs Engagement Rate",
            labels={"views": "Views", "y": "Engagement %", "account_type": "Type"},
        )
        fig2.update_layout(
            height=350,
            paper_bgcolor="white", plot_bgcolor="white",
            margin=dict(l=0, r=10, t=40, b=0),
            xaxis=dict(gridcolor="#f1f5f9"),
            yaxis=dict(gridcolor="#f1f5f9"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("📥 Export Data"):
        drop_cols = [c for c in ["ai_analysis", "_id", "transcript"] if c in df.columns]
        export_df = df.drop(columns=drop_cols, errors="ignore")
        st.dataframe(export_df.head(20), use_container_width=True, hide_index=True)
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv,
            file_name=f"insta_intel_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 2 — TREND EXPLORER
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.subheader("Trend Explorer")
    st.caption("AI-powered analysis of what is working across competitors")
    st.divider()

    def build_counter(col_name):
        if col_name not in df.columns:
            return Counter()
        vals = df[col_name].dropna().astype(str)
        vals = vals[~vals.isin(["", "None", "nan"])]
        return Counter(vals.tolist())

    def show_trend_list(counter, title, top_n=6):
        st.markdown(f"**{title}**")
        if not counter:
            st.caption("No data yet.")
            return
        top = counter.most_common(top_n)
        max_c = top[0][1] if top else 1
        for i, (name, count) in enumerate(top, 1):
            a, b, c, d = st.columns([0.5, 4, 3, 0.8])
            a.markdown(f"**{i}**")
            b.write(trunc(name, 55))
            c.progress(count / max_c)
            d.caption(str(count))

    r1, r2 = st.columns(2)
    with r1:
        show_trend_list(build_counter("hook"),   "🎣 Trending Hooks")
    with r2:
        show_trend_list(build_counter("topic"),  "📌 Trending Topics")

    st.divider()

    r3, r4 = st.columns(2)
    with r3:
        show_trend_list(build_counter("cta"),    "📢 Trending CTAs")
    with r4:
        show_trend_list(build_counter("format"), "🎬 Trending Formats")

    st.divider()

    r5, r6 = st.columns([1, 2])
    with r5:
        show_trend_list(build_counter("audio"),  "🎵 Trending Audio")
    with r6:
        topic_df = df[df["topic"].notna() & (df["topic"].astype(str) != "")]
        if not topic_df.empty:
            tp = (topic_df.groupby("topic")
                  .agg(avg_views=("views", "mean"), count=("views", "count"))
                  .reset_index()
                  .sort_values("avg_views", ascending=False)
                  .head(10))
            fig3 = px.bar(
                tp, x="topic", y="avg_views",
                color="count", text="count",
                color_continuous_scale=["#93c5fd", "#1d4ed8"],
                title="Avg Views by Topic",
                labels={"avg_views": "Avg Views", "topic": "", "count": "Reels"},
            )
            fig3.update_traces(texttemplate="%{text} reels", textposition="outside", textfont_size=10)
            fig3.update_layout(
                height=350, coloraxis_showscale=False,
                paper_bgcolor="white", plot_bgcolor="white",
                margin=dict(l=0, r=10, t=40, b=80),
                xaxis=dict(tickangle=-35, gridcolor="rgba(0,0,0,0)"),
                yaxis=dict(gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    fmt_df = df[df["format"].notna() & (df["format"].astype(str) != "")]
    if not fmt_df.empty:
        st.subheader("Engagement Rate by Format")
        fdata = (fmt_df.groupby("format")["engagement_rate"]
                 .mean().reset_index()
                 .sort_values("engagement_rate", ascending=False))
        fdata["eng_pct"] = (fdata["engagement_rate"] * 100).round(2)
        fig4 = px.bar(
            fdata, x="format", y="eng_pct",
            color="eng_pct", text="eng_pct",
            color_continuous_scale=["#93c5fd", "#1d4ed8"],
            labels={"format": "Format", "eng_pct": "Avg Engagement %"},
        )
        fig4.update_traces(texttemplate="%{text}%", textposition="outside")
        fig4.update_layout(
            height=300, coloraxis_showscale=False,
            paper_bgcolor="white", plot_bgcolor="white",
            margin=dict(l=0, r=10, t=20, b=0),
            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
            yaxis=dict(gridcolor="#f1f5f9"),
        )
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()
    st.subheader("Company vs Creator Performance")
    p1, p2, p3 = st.columns(3)

    comp_df    = df[df["account_type"] == "company"]
    creator_df = df[df["account_type"] == "creator"]

    with p1:
        with st.container(border=True):
            st.markdown("**🏢 Company Accounts**")
            if not comp_df.empty:
                st.metric("Avg Views",      fmt(int(comp_df["views"].mean())))
                st.metric("Avg Engagement", f"{round(comp_df['engagement_rate'].mean()*100, 1)}%")
                st.caption(f"{len(comp_df)} reels tracked")
            else:
                st.caption("No data")

    with p2:
        with st.container(border=True):
            st.markdown("**👤 Creator Accounts**")
            if not creator_df.empty:
                st.metric("Avg Views",      fmt(int(creator_df["views"].mean())))
                st.metric("Avg Engagement", f"{round(creator_df['engagement_rate'].mean()*100, 1)}%")
                st.caption(f"{len(creator_df)} reels tracked")
            else:
                st.caption("No data")

    with p3:
        with st.container(border=True):
            st.markdown("**🏆 Top Performing Reel**")
            if not df.empty:
                best = df.loc[df["viral_score"].idxmax()]
                st.markdown(f"**@{best.get('competitor', '')}**")
                best_hook = best.get("hook", "") or best.get("caption", "")
                st.write(trunc(str(best_hook), 80))
                st.metric("Views", fmt(int(best.get("views", 0))))
                st.link_button("Watch Reel ↗", str(best.get("reel_url", "#")))


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 3 — WEEKLY DIGEST
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.subheader("📅 Weekly Digest")
    st.caption("What competitors posted this week — sorted by viral score. Open this every Monday.")
    st.divider()

    # Always show last 7 days regardless of sidebar filters
    df_all = df_raw.copy()
    df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
    week_ago = pd.Timestamp(datetime.now() - timedelta(days=7))
    df_week  = df_all[df_all["date"] >= week_ago].sort_values("viral_score", ascending=False).reset_index(drop=True)

    if df_week.empty:
        st.info("No reels found from the last 7 days. Run the pipeline to get fresh data.")
    else:
        w1, w2, w3, w4 = st.columns(4)
        w1.metric("New This Week",   str(len(df_week)))
        w2.metric("Total Views",     fmt(df_week["views"].sum()))
        w3.metric("Accounts Active", str(df_week["competitor"].nunique()))
        w4.metric("Avg Engagement",  f"{round(df_week['engagement_rate'].mean()*100, 1)}%")
        st.divider()

        # Top reel of the week
        if not df_week.empty:
            best_week = df_week.iloc[0]
            st.markdown("### 🏆 Top Reel This Week")
            with st.container(border=True):
                bw1, bw2, bw3 = st.columns([3, 2, 1])
                with bw1:
                    acc = "🏢 Company" if best_week.get("account_type") == "company" else "👤 Creator"
                    st.markdown(f"**@{best_week.get('competitor', '')}**  |  {acc}")
                with bw2:
                    st.markdown(f"**{fmt(best_week.get('views', 0))} views**")
                with bw3:
                    st.link_button("Open Reel ↗", str(best_week.get("reel_url", "#")))

                hook_w = best_week.get("hook", "") or ""
                if hook_w:
                    st.info(f"🎣 {trunc(hook_w, 200)}")

                bm1, bm2, bm3 = st.columns(3)
                bm1.metric("Likes",      fmt(best_week.get("likes", 0)))
                bm2.metric("Engagement", f"{round(float(best_week.get('engagement_rate', 0))*100, 1)}%")
                bm3.metric("Topic",      str(best_week.get("topic", "") or "—"))

        st.divider()
        st.markdown("### All Reels This Week")

        for _, row in df_week.iterrows():
            comp     = str(row.get("competitor", "unknown"))
            acc_type = str(row.get("account_type", ""))
            views    = int(row.get("views", 0))
            eng      = float(row.get("engagement_rate", 0))
            hook     = str(row.get("hook", "") or "")
            topic    = str(row.get("topic", "") or "")
            fmt_val  = str(row.get("format", "") or "")
            url      = str(row.get("reel_url", "#"))
            date_val = str(row.get("date", ""))[:10]
            badge    = viral_badge(views)
            icon     = "🏢" if acc_type == "company" else "👤"

            with st.container(border=True):
                wc1, wc2, wc3, wc4 = st.columns([3, 2, 2, 1])
                with wc1:
                    st.markdown(f"{icon} **@{comp}**  |  {badge}")
                with wc2:
                    st.markdown(f"**{fmt(views)} views**  |  {round(eng*100, 1)}% eng")
                with wc3:
                    parts = [x for x in [topic, fmt_val] if x and x != "None"]
                    st.caption("  ·  ".join(parts) if parts else "")
                with wc4:
                    st.link_button("↗", url)

                if hook and hook not in ("", "None"):
                    st.caption(f"🎣 {trunc(hook, 120)}")


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 4 — COMPETITOR SCORECARD
# ══════════════════════════════════════════════════════════════════════════════

with tab4:
    st.subheader("🏆 Competitor Scorecard")
    st.caption("Performance summary for every account being tracked.")
    st.divider()

    if df.empty:
        st.info("No data available.")
    else:
        # Build scorecard
        scorecard_rows = []
        for competitor, grp in df.groupby("competitor"):
            acc_type    = grp["account_type"].iloc[0]
            total_reels = len(grp)
            avg_views   = int(grp["views"].mean())
            avg_eng     = round(grp["engagement_rate"].mean() * 100, 2)
            total_views = int(grp["views"].sum())
            top_topic   = (grp["topic"].dropna()
                           .replace("", pd.NA).dropna()
                           .value_counts().index[0]
                           if not grp["topic"].dropna().replace("", pd.NA).dropna().empty
                           else "—")
            top_format  = (grp["format"].dropna()
                           .replace("", pd.NA).dropna()
                           .value_counts().index[0]
                           if not grp["format"].dropna().replace("", pd.NA).dropna().empty
                           else "—")
            avg_score   = int(grp["viral_score"].mean())

            scorecard_rows.append({
                "Account":       f"@{competitor}",
                "Type":          acc_type.capitalize(),
                "Reels":         total_reels,
                "Avg Views":     avg_views,
                "Total Views":   total_views,
                "Avg Eng %":     avg_eng,
                "Viral Score":   avg_score,
                "Top Topic":     top_topic,
                "Top Format":    top_format,
            })

        sc_df = pd.DataFrame(scorecard_rows).sort_values("Avg Views", ascending=False).reset_index(drop=True)

        # Sort control
        sort_col = st.selectbox(
            "Sort scorecard by",
            ["Avg Views", "Viral Score", "Avg Eng %", "Total Views", "Reels"],
            key="scorecard_sort"
        )
        sc_df = sc_df.sort_values(sort_col, ascending=False).reset_index(drop=True)
        sc_df.index = sc_df.index + 1

        st.dataframe(
            sc_df,
            use_container_width=True,
            column_config={
                "Avg Views":   st.column_config.NumberColumn(format="%d"),
                "Total Views": st.column_config.NumberColumn(format="%d"),
                "Avg Eng %":   st.column_config.NumberColumn(format="%.2f%%"),
                "Viral Score": st.column_config.NumberColumn(format="%d"),
            }
        )

        st.divider()

        # Visual comparison
        sc1, sc2 = st.columns(2)
        with sc1:
            fig_sc1 = px.bar(
                sc_df.head(12),
                x="Account", y="Avg Views",
                color="Type",
                color_discrete_map={"Company": "#2563eb", "Creator": "#9333ea"},
                title="Avg Views per Reel by Account",
            )
            fig_sc1.update_layout(
                height=350, paper_bgcolor="white", plot_bgcolor="white",
                margin=dict(l=0, r=10, t=40, b=80),
                xaxis=dict(tickangle=-35, gridcolor="rgba(0,0,0,0)"),
                yaxis=dict(gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig_sc1, use_container_width=True)

        with sc2:
            fig_sc2 = px.bar(
                sc_df.head(12),
                x="Account", y="Avg Eng %",
                color="Type",
                color_discrete_map={"Company": "#2563eb", "Creator": "#9333ea"},
                title="Avg Engagement % by Account",
            )
            fig_sc2.update_layout(
                height=350, paper_bgcolor="white", plot_bgcolor="white",
                margin=dict(l=0, r=10, t=40, b=80),
                xaxis=dict(tickangle=-35, gridcolor="rgba(0,0,0,0)"),
                yaxis=dict(gridcolor="#f1f5f9"),
            )
            st.plotly_chart(fig_sc2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 5 — CONTENT GAP
# ══════════════════════════════════════════════════════════════════════════════

with tab5:
    st.subheader("💡 Content Gap Analysis")
    st.caption("Topics and patterns competitors are using that LeapScholar can capitalise on.")
    st.divider()

    if df.empty:
        st.info("No data available.")
    else:
        # ── Section 1: Topic gaps ─────────────────────────────────────────
        st.markdown("### 📌 Topic Coverage by Competitors")

        topic_counts = (df[df["topic"].notna() & (df["topic"].astype(str) != "")]
                        .groupby("topic")
                        .agg(
                            reels       = ("reel_url", "count"),
                            avg_views   = ("views",    "mean"),
                            avg_eng     = ("engagement_rate", "mean"),
                            avg_score   = ("viral_score", "mean"),
                        )
                        .reset_index()
                        .sort_values("avg_views", ascending=False))

        topic_counts["avg_views_fmt"] = topic_counts["avg_views"].apply(lambda x: fmt(int(x)))
        topic_counts["avg_eng_fmt"]   = topic_counts["avg_eng"].apply(lambda x: f"{round(x*100,1)}%")
        topic_counts["opportunity"]   = topic_counts["topic"].apply(
            lambda t: "🟢 Covered" if t in LEAPSCHOLAR_TOPICS else "🔴 GAP"
        )

        for _, row in topic_counts.iterrows():
            opp   = row["opportunity"]
            topic = row["topic"]
            reels = int(row["reels"])
            avv   = row["avg_views_fmt"]
            ave   = row["avg_eng_fmt"]

            with st.container(border=True):
                gc1, gc2, gc3, gc4, gc5 = st.columns([3, 1, 1, 1, 1])
                with gc1:
                    st.markdown(f"{opp}  **{topic}**")
                with gc2:
                    st.caption(f"{reels} reels")
                with gc3:
                    st.caption(f"Avg {avv} views")
                with gc4:
                    st.caption(f"{ave} eng")
                with gc5:
                    if opp == "🔴 GAP":
                        st.error("Create content")
                    else:
                        st.success("Active")

        st.divider()

        # ── Section 2: Hook patterns ──────────────────────────────────────
        st.markdown("### 🎣 Hook Patterns Being Used")
        st.caption("Reusable hook structures — not the exact hooks, but the patterns behind them.")

        pattern_df = (df[df["hook_pattern"].notna()]
                      .groupby("hook_pattern")
                      .agg(
                          count     = ("reel_url",        "count"),
                          avg_views = ("views",           "mean"),
                          avg_eng   = ("engagement_rate", "mean"),
                      )
                      .reset_index()
                      .sort_values("avg_views", ascending=False))

        for _, row in pattern_df.iterrows():
            pattern   = row["hook_pattern"]
            count     = int(row["count"])
            avg_v     = fmt(int(row["avg_views"]))
            avg_e     = f"{round(row['avg_eng']*100,1)}%"

            # Show example hooks for this pattern
            examples = (df[df["hook_pattern"] == pattern]["hook"]
                        .dropna().replace("", pd.NA).dropna()
                        .head(2).tolist())

            with st.container(border=True):
                pc1, pc2, pc3, pc4 = st.columns([3, 1, 1, 1])
                with pc1:
                    st.markdown(f"**{pattern}**")
                with pc2:
                    st.caption(f"{count} reels")
                with pc3:
                    st.caption(f"Avg {avg_v} views")
                with pc4:
                    st.caption(f"{avg_e} eng")

                if examples:
                    for ex in examples:
                        st.caption(f"  e.g. \"{trunc(ex, 100)}\"")

        st.divider()

        # ── Section 3: Underserved topics with high views ─────────────────
        st.markdown("### 🚀 Highest Opportunity Topics")
        st.caption("Topics with high avg views but few reels = low competition, high demand.")

        if not topic_counts.empty:
            opp_df = topic_counts.copy()
            opp_df["opportunity_score"] = (
                opp_df["avg_views"] / (opp_df["reels"] * 10000)
            ).round(2)
            opp_df = opp_df.sort_values("opportunity_score", ascending=False).head(5)

            for i, (_, row) in enumerate(opp_df.iterrows(), 1):
                with st.container(border=True):
                    oc1, oc2, oc3 = st.columns([4, 2, 2])
                    with oc1:
                        st.markdown(f"**#{i}  {row['topic']}**")
                    with oc2:
                        st.metric("Avg Views", row["avg_views_fmt"])
                    with oc3:
                        st.metric("Competitor Reels", int(row["reels"]))
