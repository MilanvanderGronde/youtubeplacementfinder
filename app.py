import streamlit as st
import pandas as pd
import re
import textwrap
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone
# --- IMPORT CONSTANTS ---
from constants import ALL_COUNTRY_CODES, LANGUAGE_CODES

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
    if not channel_ids: return {}
    channel_stats_map = {}
    unique_ids_list = list(channel_ids)
    for i in range(0, len(unique_ids_list), 50):
        batch_ids = unique_ids_list[i:i + 50]
        try:
            request = _youtube.channels().list(part="statistics", id=",".join(batch_ids))
            response = request.execute()
            for item in response.get("items", []):
                channel_id = item["id"]
                stats = item.get("statistics", {})
                sub_count = stats.get("subscriberCount", "N/A")
                if stats.get("hiddenSubscriberCount", False): sub_count = "Hidden"
                channel_stats_map[channel_id] = {
                    "subscriberCount": sub_count,
                    "videoCount": stats.get("videoCount", 0)
                }
        except HttpError:
            pass
    return channel_stats_map


@st.cache_data(show_spinner=False, ttl=3600)
def search_videos(_youtube, query, category_id, target_total, year, region_code, sort_order, relevance_language,
                  video_duration, video_type):
    all_videos = []
    next_page_token = None
    max_per_page = 50
    pages_to_fetch = (target_total + max_per_page - 1) // max_per_page

    try:
        for i in range(pages_to_fetch):
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
            if category_id and category_id != "All": search_params['videoCategoryId'] = category_id

            search_response = _youtube.search().list(**search_params).execute()
            video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]
            if not video_ids: break

            video_response = _youtube.videos().list(part="snippet,statistics,contentDetails",
                                                    id=",".join(video_ids)).execute()
            fetched_videos = video_response.get("items", [])
            for video in fetched_videos:
                all_videos.append(video)
                if len(all_videos) >= target_total: return all_videos[:target_total]
            next_page_token = search_response.get("nextPageToken")
            if not next_page_token: break
    except HttpError as e:
        st.error(f"API Error: {e}")
    return all_videos


# --- MAIN UI ---
def main():
    # Load External CSS
    try:
        load_css("style.css")
    except FileNotFoundError:
        st.error("style.css not found. Please create the file.")

    st.title("🎯 ContextLab - Placement Finder")
    st.markdown("""
    **Relevance is key.** Show your ads to the right audience, on the right videos, at the right time. 
    Use this tool to generate highly targeted placement lists for your YouTube campaigns and stop wasting budget on irrelevant placements.
    """)

    # 1. Sidebar: Configuration
    with st.sidebar:
        # Check for key in Secrets or URL
        default_key = ""
        if "YOUTUBE_API_KEY" in st.secrets:
            default_key = st.secrets["YOUTUBE_API_KEY"]
        elif "api_key" in st.query_params:
            default_key = st.query_params["api_key"]

        # LOGIC: Only show the input field if NO key was found
        if not default_key:
            st.header("1. Credentials")
            st.markdown("👉 [**How to get an API Key?**](https://developers.google.com/youtube/v3/getting-started) 🔑")

            api_key = st.text_input("YouTube API Key", type="password",
                                    help="Enter your YouTube Data API v3 Key from Google Cloud Console.")
            st.divider()

            # If user enters a key manually, save it to URL so they can bookmark it
            if api_key:
                st.query_params["api_key"] = api_key
        else:
            # If key is found, use it silently and skip the UI
            api_key = default_key
            # Optional: Small indicator that it's working
            #st.caption("✅ API Key loaded automatically")

       # st.divider()
        st.header("2. Targeting")

        query = st.text_input("Search Query", value="",
                              help="Main topic (e.g. 'Colosseum tours'). Use | for OR, - for NOT.")

        country_names = sorted(list(ALL_COUNTRY_CODES.keys()))
        default_idx = country_names.index("United States") if "United States" in country_names else 0
        selected_country = st.selectbox("Target Location", options=country_names, index=default_idx,
                                        help="Restricts results to videos viewable or trending in this country.")
        selected_region_code = ALL_COUNTRY_CODES[selected_country]

        selected_lang_label = st.selectbox("Language Bias", options=list(LANGUAGE_CODES.keys()), index=0,
                                           help="Tells YouTube to prioritize results in this language.")
        selected_lang_code = LANGUAGE_CODES[selected_lang_label]

        # Fetch Categories
        try:
            youtube_temp = build("youtube", "v3", developerKey=api_key) if api_key else None
            cat_map = get_category_map(youtube_temp, region_code=selected_region_code) if youtube_temp else {}
        except:
            cat_map = {}

        name_to_id = {v: k for k, v in cat_map.items()}
        name_to_id["All Categories"] = "All"
        cat_options = ["All Categories"] + sorted([k for k in name_to_id.keys() if k != "All Categories"])

        selected_cat_name = st.selectbox("Category", options=cat_options, index=0,
                                         help="Filter results to a specific YouTube category (e.g. Travel, Gaming).")
        selected_cat_id = name_to_id.get(selected_cat_name, "All")

        st.divider()
        st.header("3. Filters & Sort")

        c1, c2 = st.columns(2)
        with c1:
            duration_options = {"Any": "any", "Short (<4m)": "short", "Medium (4-20m)": "medium", "Long (>20m)": "long"}
            selected_duration_label = st.selectbox("Duration", options=list(duration_options.keys()), index=0,
                                                   help="Filter by video length.")
            selected_duration_code = duration_options[selected_duration_label]
        with c2:
            type_options = {"All": "any", "Movie": "movie", "Episode": "episode"}
            selected_type_label = st.selectbox("Type", options=list(type_options.keys()), index=0,
                                               help="Restrict content type.")
            selected_type_code = type_options[selected_type_label]

        sort_options = {"Relevance": "relevance", "View Count": "viewCount", "Date": "date", "Rating": "rating"}
        selected_sort_label = st.selectbox("Sort By", options=list(sort_options.keys()), index=0,
                                           help="Determines how YouTube ranks the results.")
        selected_sort_order = sort_options[selected_sort_label]

        year_input = st.text_input("Publish Year", value="2025",
                                   help="Filter for videos published within a specific year.")
        year = int(year_input) if year_input.strip() and year_input.isdigit() else None

        target_total = st.number_input("Max Results", min_value=1, max_value=1000, value=20,
                                       help="Maximum number of videos to retrieve.")

    # --- CREATOR FOOTER ---
    linkedin_url = "https://www.linkedin.com/in/milan-van-der-gronde/"
    buymeacoffee_url = "https://buymeacoffee.com/youtubeplacementfinder"
    st.markdown(f"""
            <div class="creator-footer">
                Created by <b>Milan van der Gronde</b> • 
                <a href="{linkedin_url}" target="_blank">Let's Connect on LinkedIn 🔗</a>
                <a href="{buymeacoffee_url}" target="_blank">
                <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" width="160">
            </a>
            </div>
        """, unsafe_allow_html=True)

    # 2. Main Area: Logic
    if not api_key:
        st.info("👈 Please enter your YouTube API Key in the sidebar to start.")
        return

    try:
        # Pass _youtube (with underscore) to cached functions
        youtube = build("youtube", "v3", developerKey=api_key)

        if st.button("🚀 Run Search", type="primary"):
            display_year = year if year else "All Time"
            lang_msg = f"in {selected_lang_label}" if selected_lang_code else ""
            with st.spinner(f"Searching for '{query}' in {selected_country} {lang_msg}..."):

                raw_videos = search_videos(
                    youtube, query, selected_cat_id, target_total, year,
                    selected_region_code, selected_sort_order, selected_lang_code,
                    selected_duration_code, selected_type_code
                )

                if not raw_videos:
                    st.warning("No videos found matching criteria.")
                    return

                unique_channel_ids = {v['snippet']['channelId'] for v in raw_videos if 'snippet' in v}
                channel_stats = get_channel_stats(youtube, unique_channel_ids)

                processed_data = []
                today = datetime.now(timezone.utc)
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
                        days_live = (today - pub_dt).days
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
                        "Channel Subscribers": c_stats.get("subscriberCount", "N/A"),
                        "Channel Total Videos": c_stats.get("videoCount", 0),
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
                st.session_state['search_meta'] = f"{query}_{selected_region_code}_{display_year}"

        # --- DISPLAY RESULTS ---
        if 'df_full' in st.session_state:
            df_full = st.session_state['df_full']
            meta_name = st.session_state.get('search_meta', 'results')

            st.divider()
            col1, col2 = st.columns([3, 1])
            with col1:
                st.success(f"✅ Search Complete! Found {len(df_full)} videos.")
            with col2:
                csv = df_full.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"youtube_{meta_name}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            # Local Sort Controls
            st.subheader("Results Preview")
            sort_col1, _ = st.columns([2, 2])
            with sort_col1:
                local_sort = st.selectbox(
                    "Sort Preview By",
                    ["API Default", "Engagement (High)", "Views (High)", "Daily Views (High)", "Newest First"],
                    index=0,
                    key="local_sort",
                    help="Re-order the preview cards below without re-running the search."
                )

            preview_df = df_full.copy()
            if "Engagement" in local_sort:
                preview_df = preview_df.sort_values("Like-to-View Ratio (%)", ascending=False)
            elif "Views" in local_sort and "Daily" not in local_sort:
                preview_df = preview_df.sort_values("Views", ascending=False)
            elif "Daily" in local_sort:
                preview_df = preview_df.sort_values("Avg Views per Day", ascending=False)
            elif "Newest" in local_sort:
                preview_df = preview_df.sort_values("Published Date", ascending=False)

            # Grid Render
            grid_html = textwrap.dedent("""<div class="video-grid">""")

            for row in preview_df.head(10).to_dict('records'):
                views_fmt = format_big_number(row['Views'])
                daily_fmt = format_big_number(row['Avg Views per Day'])
                dur_fmt = format_duration(row['Duration (Seconds)'])
                time_ago = format_time_ago(row['Published Date'])
                eng = row['Like-to-View Ratio (%)']

                eng_style = ""
                if eng > 5.0:
                    eng_badge = f'<span class="engagement-badge tooltip" style="background:#e6f4ea; color:#137333; border-color:#ceead6;" data-tooltip="High: {eng}%">★ {eng}%</span>'
                else:
                    eng_badge = f'<span class="tooltip" style="color:#70757a; font-size:11px;" data-tooltip="Engagement: {eng}%">{eng}%</span>'

                lang_html = ""
                if row['Spoken Language'] != "N/A":
                    lang_html = f'<span class="lang-tag tooltip" data-tooltip="{row["Spoken Language"].upper()}">{row["Spoken Language"].upper()}</span>'

                grid_html += textwrap.dedent(f"""
                <div class="video-card">
                    <a href="{row['URL']}" target="_blank" class="thumbnail-container">
                        <img src="{row['Thumbnail']}" alt="{row['Title']}">
                        <div class="rank-badge tooltip" data-tooltip="Relevancy Rank">#{row['Rank']}</div>
                        <div class="duration-badge">{dur_fmt}</div>
                    </a>
                    <div class="card-content">
                        <a href="{row['URL']}" target="_blank" class="video-title tooltip" data-tooltip="{row['Title']}">{row['Title']}</a>
                        <div class="channel-row">
                            <span>{row['Channel']}</span>
                            <span class="tooltip" data-tooltip="Published: {row['Published Date']}">{time_ago}</span>
                        </div>
                    </div>
                    <div class="stats-container">
                        <div>
                            <div class="tooltip" data-tooltip="Total Views: {row['Views']:,}" style="margin-bottom:2px;">👁️ {views_fmt}</div>
                            <div class="tooltip" data-tooltip="Avg Daily Views: {row['Avg Views per Day']:.1f}" style="font-size:11px; color:#70757a;">🔥 {daily_fmt}/d</div>
                        </div>
                        <div style="text-align:right;">{eng_badge}<div>{lang_html}</div></div>
                    </div>
                </div>""")

            grid_html += "</div>"
            st.markdown(grid_html, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
