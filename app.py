import streamlit as st
import pandas as pd
import re
import uuid
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone

# --- IMPORT MODULES ---
from constants import ALL_COUNTRY_CODES, LANGUAGE_CODES
from tracker import log_usage, get_logs, estimate_daily_usage

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="ContextLab - Placement Finder",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)


# --- CSS LOADER ---
def load_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


# --- HELPER FUNCTIONS ---
def parse_duration(duration_iso):
    if not duration_iso: return 0
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
    if not match: return 0
    hours, minutes, seconds = match.groups()
    total_seconds = 0
    if hours: total_seconds += int(hours) * 3600
    if minutes: total_seconds += int(minutes) * 60
    if seconds: total_seconds += int(seconds)
    return total_seconds


def format_duration(seconds):
    if not seconds: return "0:00"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def format_time_ago(date_str):
    try:
        pub_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - pub_date
        if diff.days == 0:
            return "Today"
        elif diff.days == 1:
            return "Yesterday"
        elif diff.days < 30:
            return f"{diff.days} days ago"
        elif diff.days < 365:
            months = diff.days // 30; return f"{months} mo ago"
        else:
            years = diff.days // 365; return f"{years} yr ago"
    except:
        return date_str


def format_big_number(num):
    if num >= 1_000_000: return f"{num / 1_000_000:.1f}M"
    if num >= 1_000: return f"{num / 1_000:.0f}k"
    return str(num)


# --- CACHED DATA FETCHING ---
@st.cache_data(show_spinner=False)
def get_category_map(_youtube_service, region_code="US"):
    category_map = {}
    try:
        request = _youtube_service.videoCategories().list(part="snippet", regionCode=region_code)
        response = request.execute()
        for item in response.get("items", []):
            category_map[item["id"]] = item["snippet"]["title"]
    except HttpError:
        pass
    return category_map


@st.cache_data(show_spinner=False, ttl=3600)
def get_channel_stats(_youtube, channel_ids):
    """
    Returns (channel_stats_map, quota_cost)
    """
    if not channel_ids: return {}, 0

    channel_stats_map = {}
    unique_ids_list = list(channel_ids)
    quota_cost = 0

    # Batch calls (50 IDs per call)
    for i in range(0, len(unique_ids_list), 50):
        batch_ids = unique_ids_list[i:i + 50]
        try:
            # API Call: channels.list costs 1 unit
            request = _youtube.channels().list(part="statistics,snippet", id=",".join(batch_ids))
            response = request.execute()
            quota_cost += 1

            for item in response.get("items", []):
                channel_id = item["id"]
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})

                sub_count = stats.get("subscriberCount", "N/A")
                view_count = int(stats.get("viewCount", 0))

                thumbs = snippet.get("thumbnails", {})
                thumb_url = thumbs.get("default", {}).get("url") or \
                            thumbs.get("medium", {}).get("url") or \
                            thumbs.get("high", {}).get("url") or \
                            "https://cdn-icons-png.flaticon.com/512/847/847969.png"

                channel_stats_map[channel_id] = {
                    "subscriberCount": sub_count,
                    "videoCount": stats.get("videoCount", 0),
                    "totalChannelViews": view_count,
                    "thumbnail": thumb_url
                }
        except HttpError:
            pass

    return channel_stats_map, quota_cost


@st.cache_data(show_spinner=False, ttl=3600)
def search_videos(_youtube, query, include_cat_ids, exclude_cat_ids, exclude_words_list, target_total, year,
                  region_code, sort_order, relevance_language, video_duration, video_type):
    """
    Returns (all_videos, quota_cost)
    """
    all_videos = []
    next_page_token = None
    max_per_page = 50
    max_pages_to_fetch = (target_total // 5) + 5

    quota_cost = 0

    try:
        for i in range(max_pages_to_fetch):
            search_params = {
                'part': 'snippet', 'q': query, 'type': 'video', 'order': sort_order,
                'maxResults': max_per_page, 'pageToken': next_page_token, 'regionCode': region_code
            }
            if relevance_language: search_params['relevanceLanguage'] = relevance_language
            if video_duration and video_duration != "any": search_params['videoDuration'] = video_duration
            if video_type and video_type != "any": search_params['videoType'] = video_type
            if year:
                search_params['publishedAfter'] = f"{year}-01-01T00:00:00Z"
                search_params['publishedBefore'] = f"{int(year) + 1}-01-01T00:00:00Z"

            if len(include_cat_ids) == 1:
                search_params['videoCategoryId'] = include_cat_ids[0]

            # API Call: search.list costs 100 units
            search_response = _youtube.search().list(**search_params).execute()
            quota_cost += 100

            video_ids = []
            for item in search_response.get("items", []):
                if "id" in item and "videoId" in item["id"] and item["id"]["videoId"]:
                    video_ids.append(item["id"]["videoId"])

            if not video_ids: break

            # API Call: videos.list costs 1 unit
            video_response = _youtube.videos().list(part="snippet,statistics,contentDetails",
                                                    id=",".join(video_ids)).execute()
            quota_cost += 1

            fetched_videos = video_response.get("items", [])

            for video in fetched_videos:
                # --- Strict Filtering ---
                title = video['snippet']['title'].lower()
                description = video['snippet'].get('description', '').lower()
                is_excluded_text = False

                for word in exclude_words_list:
                    if word.lower() in title or word.lower() in description:
                        is_excluded_text = True
                        break
                if is_excluded_text: continue

                vid_cat = video['snippet'].get('categoryId')
                if vid_cat in exclude_cat_ids: continue
                if include_cat_ids and vid_cat not in include_cat_ids: continue

                all_videos.append(video)
                if len(all_videos) >= target_total: return all_videos[:target_total], quota_cost

            next_page_token = search_response.get("nextPageToken")
            if not next_page_token: break

    except HttpError as e:
        st.error(f"API Error: {e}")

    return all_videos, quota_cost


# --- MAIN UI ---
def main():
    try:
        load_css("style.css")
    except FileNotFoundError:
        st.error("style.css not found. Please create the file.")

    if 'visit_id' not in st.session_state:
        st.session_state.visit_id = str(uuid.uuid4())
        log_usage(st.session_state.visit_id, "New Visit", quota_units=0)

    st.title("🎯 ContextLab - Placement Finder")
    st.markdown("""
    Relevance is key. Show your ads to the right audience, on the right videos, at the right time. 
    Use this tool to generate highly targeted placement lists for your YouTube campaigns and stop wasting budget on irrelevant placements.
    """)

    with st.sidebar:
        st.header("Credentials")

        use_shared_key = st.checkbox("Use Shared Key (Free)", value=False, help="Check this to use the hosted API key.")
        api_key = ""

        if use_shared_key:
            if "YOUTUBE_API_KEY" in st.secrets:
                api_key = st.secrets["YOUTUBE_API_KEY"]

                # --- QUOTA DISPLAY ---
                usage_pct, used_units = estimate_daily_usage()
                st.info(f"ℹ️ Shared Quota: {int(usage_pct * 100)}% Used ({format_big_number(used_units)} units)")
                st.progress(usage_pct)
                if usage_pct >= 1.0:
                    st.error("⚠️ Shared quota exhausted. Please use your own key.")
            else:
                st.error("🚨 Shared key not found.")

            if "api_key" in st.query_params: del st.query_params["api_key"]

        else:
            st.markdown("👉 [**How to get an API Key?**](https://developers.google.com/youtube/v3/getting-started) 🔑")
            url_key = st.query_params.get("api_key", "")
            user_input_key = st.text_input("Your YouTube API Key", value=url_key, type="password",
                                           help="Enter your own key.")

            if user_input_key:
                api_key = user_input_key
                st.query_params["api_key"] = user_input_key
            else:
                if "api_key" in st.query_params: del st.query_params["api_key"]
                st.warning("👈 Please enter your API Key or check 'Use Shared Key'.")

        st.divider()
        st.header("Targeting and Filtering")

        search_topic = st.text_input("Search Query", placeholder="", help="The main subject of the videos.")
        is_broad = st.checkbox("Enable Broad Match", value=False,
                               help="Uncheck for Exact Match (Recommended). Check to allow broader results.")
        exclude_words_input = st.text_input("Exclude Queries", placeholder="",
                                            help="Words to exclude from results (e.g. 'dropshipping').")
        exclude_words_list = [w.strip() for w in exclude_words_input.split() if w.strip()]

        final_query_parts = []
        if search_topic: final_query_parts.append(search_topic if is_broad else f'"{search_topic}"')
        if exclude_words_input: final_query_parts.append(" ".join([f"-{w}" for w in exclude_words_list]))
        final_query_string = " ".join(final_query_parts)

        country_names = sorted(list(ALL_COUNTRY_CODES.keys()))
        default_idx = country_names.index("United States") if "United States" in country_names else 0
        selected_country = st.selectbox("Target Location", options=country_names, index=default_idx,
                                        help="Restricts results to videos viewable or trending in this country.")
        selected_region_code = ALL_COUNTRY_CODES[selected_country]

        selected_lang_label = st.selectbox("Language Bias", options=list(LANGUAGE_CODES.keys()), index=0,
                                           help="Tells YouTube to prioritize results in this language.")
        selected_lang_code = LANGUAGE_CODES[selected_lang_label]

        try:
            youtube_temp = build("youtube", "v3", developerKey=api_key) if api_key else None
            cat_map = get_category_map(youtube_temp, region_code=selected_region_code) if youtube_temp else {}
        except:
            cat_map = {}

        name_to_id = {v: k for k, v in cat_map.items()}
        clean_cat_options = sorted([k for k in name_to_id.keys()])

        selected_cat_names = st.multiselect("Include Categories", options=clean_cat_options, default=[],
                                            help="Leave empty to search ALL categories.")
        include_cat_ids = [name_to_id[n] for n in selected_cat_names]

        exclude_cat_names = st.multiselect("Exclude Categories", options=clean_cat_options, default=[],
                                           help="Videos from these categories will be removed.")
        exclude_cat_ids = [name_to_id[n] for n in exclude_cat_names]

        duration_options = {"Any": "any", "Short (<4m)": "short", "Medium (4-20m)": "medium", "Long (>20m)": "long"}
        selected_duration_label = st.selectbox("Duration", options=list(duration_options.keys()), index=0,
                                               help="Filter by video length.")
        selected_duration_code = duration_options[selected_duration_label]

        year_input = st.text_input("Publish Year", value="", placeholder="All time (e.g. 2025)",
                                   help="Filter for videos published within a specific year.")
        year = int(year_input) if year_input.strip() and year_input.isdigit() else None
        target_total = st.number_input("Max Results", min_value=1, max_value=1000, value=20,
                                       help="Maximum number of videos to retrieve.")

    linkedin_url = "https://www.linkedin.com/in/milan-van-der-gronde-online-marketing-google-ads/"
    coffee_url = "https://buymeacoffee.com/youtubeplacementfinder"
    st.markdown(f"""
        <div class="creator-footer">
            <span>Created by <b>Milan van der Gronde</b></span> • 
            <span><a href="{linkedin_url}" target="_blank">Let's Connect on LinkedIn 🔗</a></span>
            <span style="margin-left: 15px; padding-left: 15px; border-left: 1px solid #dadce0;">
                <a href="{coffee_url}" target="_blank" style="text-decoration: none;">☕ Buy me a coffee</a>
            </span>
        </div>
    """, unsafe_allow_html=True)

    if not api_key:
        st.info("👈 Please enter your YouTube API Key (or check 'Use Shared Key') in the sidebar to start.")
        return

    try:
        youtube = build("youtube", "v3", developerKey=api_key)

        if st.button("🚀 Run Search", type="primary"):
            if not final_query_string.strip():
                st.warning("Please enter a Search Query.")
                return

            if final_query_string.strip() == '"admin_view_logs"':
                log_df = get_logs()
                if log_df is not None:
                    st.success("Admin Access Granted: Viewing Logs")
                    csv_logs = log_df.to_csv(index=False).encode('utf-8')
                    st.download_button(label="📥 Download Log File", data=csv_logs, file_name="server_logs.csv",
                                       mime="text/csv")
                    st.dataframe(log_df, use_container_width=True)
                    st.stop()
                else:
                    st.warning("No logs found yet.")
                    st.stop()

            display_year = year if year else "All Time"
            lang_msg = f"in {selected_lang_label}" if selected_lang_code else ""
            with st.spinner(f"Searching for '{final_query_string}' in {selected_country} {lang_msg}..."):

                # --- 1. SEARCH VIDEOS & COUNT COST ---
                raw_videos, search_cost = search_videos(
                    youtube, final_query_string,
                    include_cat_ids, exclude_cat_ids,
                    exclude_words_list,
                    target_total, year, selected_region_code, 'relevance',
                    selected_lang_code, selected_duration_code, 'any'
                )

                if not raw_videos:
                    log_usage(st.session_state.visit_id, "Search (No Results)", query=final_query_string,
                              country=selected_country, quota_units=search_cost)
                    st.warning("No videos found matching criteria.")
                    return

                # --- 2. CHANNEL STATS & COUNT COST ---
                unique_channel_ids = {v['snippet']['channelId'] for v in raw_videos if 'snippet' in v}
                channel_stats, channel_cost = get_channel_stats(youtube, unique_channel_ids)

                # --- 3. LOG TOTAL COST ---
                total_quota_cost = search_cost + channel_cost
                log_usage(st.session_state.visit_id, "Search Run", query=final_query_string, country=selected_country,
                          result_count=len(raw_videos), quota_units=total_quota_cost)

                processed_data = []
                rank_counter = 1
                for video in raw_videos:
                    snippet = video["snippet"]
                    stats = video.get("statistics", {})
                    details = video.get("contentDetails", {})
                    views = int(stats.get("viewCount", 0))
                    likes = int(stats.get("likeCount", 0))
                    comments = int(stats.get("commentCount", 0))
                    duration_sec = parse_duration(details.get("duration", "PT0S"))
                    published_at = snippet.get("publishedAt", "")
                    like_ratio = (likes / views * 100) if views > 0 else 0
                    comment_ratio = (comments / views * 100) if views > 0 else 0
                    avg_daily_views = 0
                    if published_at:
                        pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                        today_dt = datetime.now(timezone.utc)
                        days_live = (today_dt - pub_dt).days
                        avg_daily_views = views / days_live if days_live > 0 else views

                    cid = snippet.get("channelId")
                    c_stats = channel_stats.get(cid, {})
                    cat_id = snippet.get("categoryId")
                    current_cat_name = cat_map.get(cat_id, str(cat_id))

                    processed_data.append({
                        "Rank": rank_counter,
                        "Thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                        "Title": snippet.get("title"),
                        "Channel": snippet.get("channelTitle"),
                        "Channel ID": cid,
                        "Channel Subscribers": c_stats.get("subscriberCount", "N/A"),
                        "Channel Total Views": c_stats.get("totalChannelViews", 0),
                        "Channel Logo": c_stats.get("thumbnail", ""),
                        "Channel Video Count": c_stats.get("videoCount", 0),
                        "Views": views, "Likes": likes, "Comments": comments,
                        "Avg Views per Day": round(avg_daily_views, 2),
                        "Published Date": published_at.split("T")[0],
                        "Like-to-View Ratio (%)": round(like_ratio, 2),
                        "Comment-to-View Ratio (%)": round(comment_ratio, 2),
                        "Duration (Seconds)": duration_sec,
                        "Spoken Language": snippet.get("defaultAudioLanguage", "N/A"),
                        "Text Language": snippet.get("defaultLanguage", "N/A"),
                        "Video Category": current_cat_name,
                        "Tags": "|".join(snippet.get("tags", [])),
                        "URL": f"https://www.youtube.com/watch?v={video['id']}"
                    })
                    rank_counter += 1

                st.session_state['df_full'] = pd.DataFrame(processed_data)
                safe_topic = re.sub(r'[^a-zA-Z0-9]', '_', search_topic)
                st.session_state['search_meta'] = f"{safe_topic}_{selected_region_code}_{display_year}"

        if 'df_full' in st.session_state:
            df_full = st.session_state['df_full']
            meta_name = st.session_state.get('search_meta', 'results')
            st.divider()

            total_vids = len(df_full)
            total_views = int(df_full['Views'].sum())
            total_daily = int(df_full['Avg Views per Day'].sum())
            csv_data = df_full.to_csv(index=False).encode('utf-8')

            tab_videos, tab_channels = st.tabs(["📹 Video Results", "📢 Channel Insights"])

            with tab_videos:
                with st.container():
                    st.markdown(f"""
                    <div class="overViewDiv">
                        <div class="overViewDivHeader">🔍 Overview for search: {final_query_string}</div>
                        <div class="overViewDivMetrics">
                            <div><b>{total_vids}</b> Videos</div>
                            <div><b>{format_big_number(total_views)}</b> Views</div>
                            <div><b>{format_big_number(total_daily)}</b> Daily Views</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    c1, c2 = st.columns([3, 1])
                    with c1:
                        count_high = len(df_full[df_full['Views'] > 10000])
                        count_mid = len(df_full[(df_full['Views'] >= 1000) & (df_full['Views'] <= 10000)])
                        count_low = len(df_full[df_full['Views'] < 1000])

                        segment_filter = st.radio(
                            "Filter Results by View Count:",
                            options=["All", "> 10k Views", "1k - 10k Views", "< 1k Views"],
                            format_func=lambda x: {
                                "All": f"Show All ({total_vids})",
                                "> 10k Views": f"> 10k Views ({count_high})",
                                "1k - 10k Views": f"1k - 10k Views ({count_mid})",
                                "< 1k Views": f"< 1k Views ({count_low})"
                            }[x],
                            horizontal=True,
                            label_visibility="collapsed"
                        )
                    with c2:
                        def on_dl_click():
                            log_usage(st.session_state.visit_id, "Data Export", query=final_query_string,
                                      country=selected_country, result_count=len(df_full), quota_units=0)

                        st.download_button(
                            "📥 Download Results (CSV)", csv_data, f"youtube_{meta_name}.csv", "text/csv",
                            type="primary", use_container_width=True, on_click=on_dl_click
                        )

                filtered_df = df_full.copy()
                if "> 10k" in segment_filter:
                    filtered_df = filtered_df[filtered_df['Views'] > 10000]
                elif "1k - 10k" in segment_filter:
                    filtered_df = filtered_df[(filtered_df['Views'] >= 1000) & (filtered_df['Views'] <= 10000)]
                elif "< 1k" in segment_filter:
                    filtered_df = filtered_df[filtered_df['Views'] < 1000]

                sort_col1, _ = st.columns([2, 2])
                with sort_col1:
                    local_sort = st.selectbox("Sort Preview By",
                                              ["Relevance (Default)", "Engagement (High)", "Views (High)",
                                               "Daily Views (High)", "Newest First"], index=0, key="local_sort")

                preview_df = filtered_df.copy()
                if "Engagement" in local_sort:
                    preview_df = preview_df.sort_values("Like-to-View Ratio (%)", ascending=False)
                elif "Views" in local_sort and "Daily" not in local_sort:
                    preview_df = preview_df.sort_values("Views", ascending=False)
                elif "Daily" in local_sort:
                    preview_df = preview_df.sort_values("Avg Views per Day", ascending=False)
                elif "Newest" in local_sort:
                    preview_df = preview_df.sort_values("Published Date", ascending=False)

                # VIDEO GRID RENDER
                grid_html = '<div class="video-grid">'
                for row in preview_df.head(20).to_dict('records'):
                    views_fmt = format_big_number(row['Views'])
                    daily_fmt = format_big_number(row['Avg Views per Day'])
                    likes_fmt = format_big_number(row['Likes'])
                    comments_fmt = format_big_number(row['Comments'])
                    dur_fmt = format_duration(row['Duration (Seconds)'])
                    time_ago = format_time_ago(row['Published Date'])
                    eng = row['Like-to-View Ratio (%)']

                    eng_badge = f'<span class="engagement-badge tooltip" data-tooltip="View to Like Ratio" style="background:#e6f4ea; color:#137333;">★ V/L: {eng}%</span>' if eng > 5 else f'<span class="tooltip" data-tooltip="View to Like Ratio" style="color:#70757a; font-size:11px;">V/L: {eng}%</span>'
                    lang_badge = f'<div style="background:rgba(0,0,0,0.7); color:white; padding:2px 6px; border-radius:4px; font-size:10px; font-weight:bold;">{row["Spoken Language"].upper()}</div>' if \
                    row['Spoken Language'] != "N/A" else ""
                    cat_badge = f'<div style="background:rgba(0,0,0,0.7); color:white; padding:2px 6px; border-radius:4px; font-size:10px; font-weight:bold;">{row["Video Category"]}</div>'

                    grid_html += f"""
<div class="video-card">
    <a href="{row['URL']}" target="_blank" class="thumbnail-container">
        <img src="{row['Thumbnail']}" alt="{row['Title']}">
        <div style="position:absolute; top:8px; right:8px; display:flex; flex-direction:column; gap:4px; align-items:flex-end; z-index:10;">
            {lang_badge}
            {cat_badge}
        </div>
        <div class="rank-badge">#{row['Rank']}</div>
        <div class="duration-badge">{dur_fmt}</div>
    </a>
    <div class="card-content">
        <a href="{row['URL']}" target="_blank" class="video-title" title="{row['Title']}">{row['Title']}</a>
        <div class="channel-row"><span>{row['Channel']}</span><span>{time_ago}</span></div>
    </div>
    <div class="stats-container">
        <div style="display:flex; gap:12px;">
            <div title="Total Views">👁️ {views_fmt}</div>
            <div style="font-size:11px; color:#70757a;" title="Daily Views">🔥 {daily_fmt}/d</div>
            <div style="font-size:11px; color:#70757a;" title="Likes">👍 {likes_fmt}</div>
            <div style="font-size:11px; color:#70757a;" title="Comments">💬 {comments_fmt}</div>
        </div>
        <div>{eng_badge}</div>
    </div>
</div>"""
                grid_html += "</div>"
                st.markdown(grid_html, unsafe_allow_html=True)

            with tab_channels:
                channel_groups = df_full.groupby('Channel ID')
                channel_data = []

                grand_total_res_views = df_full['Views'].sum()
                grand_total_global_views = 0

                for cid, group in channel_groups:
                    first = group.iloc[0]
                    grand_total_global_views += first['Channel Total Views']

                for cid, group in channel_groups:
                    first = group.iloc[0]
                    total_res_views = int(group['Views'].sum())
                    avg_like_ratio = group['Like-to-View Ratio (%)'].mean()

                    subs = first['Channel Subscribers']
                    sub_val = int(subs) if isinstance(subs, str) and subs.isdigit() else 0

                    global_views = first['Channel Total Views']

                    sov_res = (total_res_views / grand_total_res_views * 100) if grand_total_res_views > 0 else 0
                    sov_glob = (global_views / grand_total_global_views * 100) if grand_total_global_views > 0 else 0

                    sorted_group = group.sort_values(by="Views", ascending=False)

                    channel_data.append({
                        "Channel": first['Channel'],
                        "ID": cid,
                        "Logo": first['Channel Logo'],
                        "Subscribers": subs,
                        "Sub_Val": sub_val,
                        "Global_Views": global_views,
                        "Result_Views": total_res_views,
                        "Avg_Like_Ratio": avg_like_ratio,
                        "Videos Found": len(group),
                        "SoV Results": round(sov_res, 2),
                        "SoV Global": round(sov_glob, 2),
                        "Video List": sorted_group.to_dict('records')
                    })

                cdf = pd.DataFrame(channel_data)

                c_total = len(cdf)
                c_subs_est = cdf["Sub_Val"].sum()
                c_lifetime_views = cdf["Global_Views"].sum()

                st.markdown(f"""
                <div class="overViewDiv" style="background-color: #fce8e6; color: #c5221f;">
                    <div style="font-size: 1.1rem; font-weight: 500; margin-bottom: 10px;">📢 Channel Overview</div>
                    <div class="overViewDivChannelMetrics" style="display: flex; gap: 30px; align-items: center; flex-wrap: wrap;">
                        <div><b>{c_total}</b> Unique Channels</div>
                        <div style="border-left: 1px solid #c5221f; padding-left: 20px;"><b>{format_big_number(c_lifetime_views)}</b> Total Channel Views (Lifetime)</div>
                        <div style="border-left: 1px solid #c5221f; padding-left: 20px;"><b>{format_big_number(c_subs_est)}+</b> Total Subscribers</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                sort_col, _ = st.columns([2, 3])
                with sort_col:
                    chan_sort = st.selectbox(
                        "Sort Channels By",
                        ["Share of Voice (Results)", "Share of Voice (Global)", "Total Subscribers", "Lifetime Views",
                         "Videos Found"],
                        index=0,
                        key="channel_sort"
                    )

                if "Results" in chan_sort:
                    cdf = cdf.sort_values("Result_Views", ascending=False)
                elif "Global" in chan_sort:
                    cdf = cdf.sort_values("Global_Views", ascending=False)
                elif "Subscribers" in chan_sort:
                    cdf = cdf.sort_values("Sub_Val", ascending=False)
                elif "Lifetime" in chan_sort:
                    cdf = cdf.sort_values("Global_Views", ascending=False)
                elif "Videos" in chan_sort:
                    cdf = cdf.sort_values("Videos Found", ascending=False)

                for _, row in cdf.iterrows():
                    mini_grid_html = "".join([
                                                 f'<a href="{v["URL"]}" target="_blank" title="{v["Title"]}" style="flex: 0 0 160px; text-decoration:none;"><img src="{v["Thumbnail"]}" style="width:100%; border-radius:8px; aspect-ratio:16/9; object-fit:cover; border:1px solid #eee; transition: transform 0.2s;"></a>'
                                                 for v in row['Video List']])

                    logo = row['Logo'] if row['Logo'] else "https://cdn-icons-png.flaticon.com/512/847/847969.png"

                    # Channel Card Render
                    card_html = f"""
<div class="channelResultsGrid">
    <div>
        <img src="{logo}" style="width:50px; height:50px; border-radius:50%; object-fit:cover; border:1px solid #eee;">
        <a href="https://www.youtube.com/channel/{row['ID']}" target="_blank" style="font-size:18px; font-weight:600; color:#202124; text-decoration:none;">{row['Channel']}</a> 
        <div style="flex-grow:1;">
            <div class="channelCard">
                <span><b>{row['Subscribers']}</b> Subs</span>
                <span style="color:#dadce0;">|</span>
                <span><b>{format_big_number(row['Global_Views'])}</b> Ch. Views</span>
                <span style="color:#dadce0;">|</span>
                <span><b>{row['Videos Found']}</b> Vids in Results</span>
                <span style="color:#dadce0;">|</span>
                <span><b>{format_big_number(row['Result_Views'])}</b> Result Views</span>
                <span style="color:#dadce0;">|</span>
                <span class="tooltip" data-tooltip="Average View-to-Like Ratio for videos in this search."><b>{row['Avg_Like_Ratio']:.1f}%</b> Avg V/L</span>
                <span style="color:#dadce0;">|</span>
                <span class="tooltip" data-tooltip="This channel's share of the total views found in this specific search result." style="color:#1a73e8; background:#e8f0fe; padding:1px 6px; border-radius:4px; cursor:help;"><b>{row['SoV Results']}%</b> SoV (Res)</span>
                <span style="color:#dadce0;">|</span>
                <span class="tooltip" data-tooltip="This channel's share of the combined lifetime views of all channels found in this search." style="color:#137333; background:#e6f4ea; padding:1px 6px; border-radius:4px; cursor:help;"><b>{row['SoV Global']}%</b> SoV (Glob)</span>
            </div>
        </div>
    </div>
    <div style="display: flex; gap: 12px; overflow-x: auto; padding-bottom: 8px; scrollbar-width: thin;">{mini_grid_html}</div>
</div>"""
                    st.markdown(card_html, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
