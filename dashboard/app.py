"""
insta_intel/dashboard/app.py
LeapScholar Insta Intel - Clean Dashboard
Minimal CSS, native Streamlit components only, no rendering issues.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
from collections import Counter
from datetime import datetime, timedelta

st.set_page_config(
    page_title="LeapScholar - Insta Intel",
    page_icon="rocket",
    layout="wide",
    initial_sidebar_state="expanded",
)

# SAFE CSS - only targets specific elements, never overrides text color globally
st.markdown("""
<style>
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
.stDeployButton { display: none; }
</style>
""", unsafe_allow_html=True)


def fmt(n):
    if not n: return "0"
    n = int(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(n)

def trunc(text, n=100):
    if not text: return ""
    s = str(text)
    return s if len(s) <= n else s[:n] + "..."

def viral_badge(views):
    v = int(views or 0)
    if v >= 800000: return "Viral"
    if v >= 400000: return "Hot"
    return "Active"

DEMO = [
    {"competitor":"sheenamgautam","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC003/","caption":"How I got a fully funded scholarship to the UK. Comment SCHOLAR for the guide!","views":1240000,"likes":67000,"comments":8900,"engagement_rate":0.061,"audio":"Original sound - sheenamgautam","date":"2026-02-26","hook":"How I got a fully funded scholarship to the UK","topic":"Scholarships","cta":"Comment SCHOLAR for the guide","format":"Storytime","summary":"Personal story of securing a fully funded UK scholarship."},
    {"competitor":"harnoor.studyabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC008/","caption":"My SOP got rejected 4 times. Here is what I changed to finally get into Oxford. DM me SOP","views":1100000,"likes":59000,"comments":11200,"engagement_rate":0.064,"audio":"Original sound - harnoor","date":"2026-02-21","hook":"My SOP got rejected 4 times","topic":"SOP/LOR","cta":"DM me SOP","format":"Storytime","summary":"Failure to success SOP story that builds trust."},
    {"competitor":"yocketapp","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC001/","caption":"3 mistakes every Indian student makes before applying for a Canada student visa.","views":980000,"likes":42000,"comments":3100,"engagement_rate":0.046,"audio":"Original sound - yocketapp","date":"2026-02-28","hook":"3 mistakes every Indian student makes","topic":"Canada Visa","cta":"Watch till the end","format":"Listicle","summary":"Highlights 3 common visa mistakes Indian students make."},
    {"competitor":"leverageedu","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC002/","caption":"Nobody tells you THIS about UK student visa rejections. Save this before you apply!","views":760000,"likes":31000,"comments":2800,"engagement_rate":0.044,"audio":"Trending audio","date":"2026-02-27","hook":"Nobody tells you THIS about UK student visa rejections","topic":"UK Visa","cta":"Save this before you apply","format":"Talking Head","summary":"Exposes hidden reasons for UK visa rejections."},
    {"competitor":"shreyamahendru_","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC005/","caption":"Stop wasting money on education loans! Here is what banks do not tell you.","views":890000,"likes":38000,"comments":5200,"engagement_rate":0.048,"audio":"Original sound","date":"2026-02-24","hook":"Stop wasting money on education loans","topic":"Education Loan","cta":"Follow for more finance tips","format":"Talking Head","summary":"Reveals hidden charges in education loans."},
    {"competitor":"gradright","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC007/","caption":"Australia vs Canada: Which is better for Indian students in 2026?","views":670000,"likes":29000,"comments":7800,"engagement_rate":0.055,"audio":"Trending debate sound","date":"2026-02-22","hook":"Australia vs Canada: Which is better?","topic":"General Study Abroad","cta":"Drop your vote below","format":"Myth vs Fact","summary":"Comparison between Australia and Canada for Indian students."},
    {"competitor":"indianstudentabroad","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC011/","caption":"The real cost of studying in the UK in 2026. Not what universities tell you!","views":720000,"likes":33000,"comments":4100,"engagement_rate":0.051,"audio":"Dramatic reveal sound","date":"2026-02-18","hook":"The real cost of studying in the UK in 2026","topic":"UK Visa","cta":"None","format":"Voiceover + Text","summary":"Reveals hidden costs of UK education."},
    {"competitor":"abroadpathway","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC014/","caption":"USA F1 visa interview questions that ACTUALLY get asked in 2026. Save this!","views":690000,"likes":30500,"comments":4700,"engagement_rate":0.051,"audio":"Suspenseful background","date":"2026-02-15","hook":"USA F1 visa interview questions in 2026","topic":"USA Visa","cta":"Save this","format":"Tips and Tricks","summary":"Real F1 visa interview questions for 2026."},
    {"competitor":"mastersabroaddiaries","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC012/","caption":"POV: You finally got your Australia PR after studying there. Follow my journey!","views":560000,"likes":27000,"comments":3900,"engagement_rate":0.055,"audio":"Emotional background music","date":"2026-02-17","hook":"POV: You finally got your Australia PR","topic":"Australia Visa","cta":"Follow my journey","format":"Storytime","summary":"Aspirational Australia PR success story."},
    {"competitor":"ambitiohq","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC004/","caption":"IELTS band 7 in 30 days? Here is my exact study plan. Save this reel!","views":540000,"likes":22000,"comments":1900,"engagement_rate":0.044,"audio":"Lo-fi study beats","date":"2026-02-25","hook":"IELTS band 7 in 30 days?","topic":"IELTS Tips","cta":"Save this reel","format":"Tips and Tricks","summary":"30-day IELTS study plan for Band 7."},
    {"competitor":"idp.india","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC013/","caption":"Top 5 IELTS speaking mistakes that cost you marks. Link in bio for free mock test!","views":445000,"likes":19500,"comments":2300,"engagement_rate":0.049,"audio":"Original sound - idp","date":"2026-02-16","hook":"Top 5 IELTS speaking mistakes","topic":"IELTS Tips","cta":"Link in bio for free mock test","format":"Listicle","summary":"5 IELTS speaking mistakes with fixes."},
    {"competitor":"moveabroadsimplified","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC015/","caption":"My first month expenses in Canada as an Indian student. Was it what I expected?","views":490000,"likes":21000,"comments":2800,"engagement_rate":0.048,"audio":"Vlog background music","date":"2026-02-14","hook":"My first month expenses in Canada","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Transparent first-month financial breakdown in Canada."},
    {"competitor":"studyabroadkar","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC006/","caption":"Day in my life studying in Germany. Cost breakdown inside!","views":420000,"likes":18500,"comments":2200,"engagement_rate":0.049,"audio":"Aesthetic vlog music","date":"2026-02-23","hook":"Day in my life studying in Germany","topic":"Student Life","cta":"None","format":"Day in My Life","summary":"Daily life and costs of studying in Germany."},
    {"competitor":"upgradabroad","account_type":"company","reel_url":"https://www.instagram.com/reel/ABC009/","caption":"Part-time jobs in Canada for Indian students. Share this with your friends!","views":380000,"likes":15000,"comments":1400,"engagement_rate":0.043,"audio":"Upbeat background","date":"2026-02-20","hook":"Part-time jobs in Canada for Indian students","topic":"Canada Visa","cta":"Share this with your friends","format":"Tips and Tricks","summary":"Guide to legal part-time work in Canada."},
    {"competitor":"searcheduindia","account_type":"creator","reel_url":"https://www.instagram.com/reel/ABC010/","caption":"Germany student visa rejected? Watch this before reapplying.","views":310000,"likes":13500,"comments":1800,"engagement_rate":0.049,"audio":"Original sound","date":"2026-02-19","hook":"Germany student visa rejected?","topic":"Germany Visa","cta":"None","format":"Talking Head","summary":"Advice on recovering from German visa rejection."},
]


@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    try:
        from database.mongo_client import get_all_reels
        reels = get_all_reels(limit=1000)
        if reels and len(reels) > 0:
            df = pd.DataFrame(reels)
            if "ai_analysis" in df.columns:
                for field in ["hook", "topic", "cta", "format", "summary"]:
                    if field not in df.columns:
                        df[field] = df["ai_analysis"].apply(
                            lambda x: x.get(field, "") if isinstance(x, dict) else ""
                        )
            return df, True
    except Exception:
        pass
    return pd.DataFrame(DEMO), False


df_raw, is_live = load_data()
if df_raw.empty:
    df_raw = pd.DataFrame(DEMO)
    is_live = False

for col in ["hook", "topic", "cta", "format", "summary", "audio"]:
    if col not in df_raw.columns:
        df_raw[col] = ""


# ── SIDEBAR ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Insta Intel")
    st.caption("LeapScholar Content Team")
    if is_live:
        st.success("Connected to MongoDB")
    else:
        st.warning("Showing demo data")
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
    date_from = st.date_input("From", value=datetime.now() - timedelta(days=30))
    date_to   = st.date_input("To",   value=datetime.now())

    st.divider()
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Pipeline auto-runs every 2 days")


# ── Filters ───────────────────────────────────────────────────
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


# ── HEADER ────────────────────────────────────────────────────
st.title("LeapScholar - Insta Intel")
st.caption(
    f"Tracking {len(df)} reels across {df['competitor'].nunique()} accounts"
    + ("  |  LIVE DATA" if is_live else "  |  Demo Mode")
)
st.divider()

# ── KPI ───────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Reels",     str(len(df)))
c2.metric("Total Views",     fmt(df["views"].sum()))
c3.metric("Avg Engagement",  f"{round(df['engagement_rate'].mean()*100, 1)}%")
c4.metric("Top Reel Views",  fmt(df["views"].max() if len(df) else 0))
st.divider()


# ── TABS ──────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["Competitor Reels Database", "Trend Explorer"])


# ══════════════════════════════════════════
#  TAB 1
# ══════════════════════════════════════════
with tab1:
    col_hd, col_sort = st.columns([3, 1])
    with col_hd:
        st.subheader(f"Competitor Reels  ({len(df)} results)")
    with col_sort:
        sort_by = st.selectbox(
            "Sort by",
            ["views", "engagement_rate", "likes", "comments"],
            label_visibility="collapsed"
        )

    search = st.text_input("Search captions, hooks, topics, competitors...", key="search_box")
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
            icon     = "Company" if acc_type == "company" else "Creator"

            with st.container(border=True):
                # Header
                h1, h2, h3 = st.columns([3, 2, 1])
                with h1:
                    st.markdown(f"**@{comp}**  |  {icon}  |  {badge}")
                with h2:
                    st.markdown(f"**{fmt(views)} views**")
                with h3:
                    st.link_button("Open Reel", url)

                # Stats
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Likes",        fmt(likes))
                s2.metric("Comments",     fmt(comments))
                s3.metric("Engagement",   f"{round(eng*100, 1)}%")
                s4.metric("Date",         date_val)

                # Hook
                if hook and hook not in ("", "None"):
                    st.info(f"Hook: {trunc(hook, 150)}")

                # Summary
                if summary and summary not in ("", "None"):
                    st.caption(trunc(summary, 180))

                # Tags
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
                        st.caption(trunc(audio, 40))

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

    with st.expander("Export Data"):
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


# ══════════════════════════════════════════
#  TAB 2 — TREND EXPLORER
# ══════════════════════════════════════════
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
        show_trend_list(build_counter("hook"),   "Trending Hooks")
    with r2:
        show_trend_list(build_counter("topic"),  "Trending Topics")

    st.divider()

    r3, r4 = st.columns(2)
    with r3:
        show_trend_list(build_counter("cta"),    "Trending CTAs")
    with r4:
        show_trend_list(build_counter("format"), "Trending Formats")

    st.divider()

    r5, r6 = st.columns([1, 2])
    with r5:
        show_trend_list(build_counter("audio"),  "Trending Audio")
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
            st.markdown("**Company Accounts**")
            if not comp_df.empty:
                st.metric("Avg Views",      fmt(int(comp_df["views"].mean())))
                st.metric("Avg Engagement", f"{round(comp_df['engagement_rate'].mean()*100, 1)}%")
                st.caption(f"{len(comp_df)} reels tracked")
            else:
                st.caption("No data")

    with p2:
        with st.container(border=True):
            st.markdown("**Creator Accounts**")
            if not creator_df.empty:
                st.metric("Avg Views",      fmt(int(creator_df["views"].mean())))
                st.metric("Avg Engagement", f"{round(creator_df['engagement_rate'].mean()*100, 1)}%")
                st.caption(f"{len(creator_df)} reels tracked")
            else:
                st.caption("No data")

    with p3:
        with st.container(border=True):
            st.markdown("**Top Performing Reel**")
            if not df.empty:
                best = df.loc[df["views"].idxmax()]
                st.markdown(f"**@{best.get('competitor', '')}**")
                best_hook = best.get("hook", "") or best.get("caption", "")
                st.write(trunc(str(best_hook), 80))
                st.metric("Views", fmt(int(best.get("views", 0))))
                st.link_button("Watch Reel", str(best.get("reel_url", "#")))
