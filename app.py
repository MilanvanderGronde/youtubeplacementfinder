import streamlit as st
import pandas as pd
import re
import textwrap
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timezone
# --- IMPORT FROM CONSTANTS.PY ---
from constants import ALL_COUNTRY_CODES, LANGUAGE_CODES

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="YouTube Placement Finder",
    page_icon="🎯",
    layout="wide"
)


# --- HELPER FUNCTIONS ---

def parse_duration(duration_iso):
    """Converts YouTube's ISO 8601 duration format into seconds."""
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
    """Converts seconds (e.g. 125) to '2:05' string."""
    if not seconds: return "0:00"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def format_time_ago(date_str):
    """Converts YYYY-MM-DD to '2 days ago'."""
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
            months = diff.days // 30
            return f"{months} mo ago"
        else:
            years = diff.days // 365
            return f"{years} yr ago"
    except:
        return date_str


def format_big_number(num):
    """Formats 1,200,000 -> 1.2M and 35,000 -> 35k"""
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    if num >= 1_000:
        return f"{num / 1_000:.0f}k"
    return str(num)


@st.cache_data(show_spinner=False)
def get_category_map(_youtube_service, region_code="US"):
    """Fetches category list for a specific region. Cached."""
    category_map = {}
    try:
        request = _youtube_service.videoCategories().list(
            part="snippet",
            regionCode=region_code
        )
        response = request.execute()
        for item in response.get("items", []):
            category_map[item["id"]] = item["snippet"]["title"]
    except HttpError as e:
        st.error(f"Could not fetch categories: {e}")
    return category_map


def get_channel_stats(youtube, channel_ids):
    """Fetches statistics for a batch of channel IDs."""
    if not channel_ids: return {}
    channel_stats_map = {}
    unique_ids_list = list(channel_ids)

    progress_bar = st.progress(0, text="Fetching channel stats...")

    for i in range(0, len(unique_ids_list), 50):
        batch_ids = unique_ids_list[i:i + 50]
        try:
            request = youtube.channels().list(part="statistics", id=",".join(batch_ids))
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
        progress_bar.progress(min((i + 50) / len(unique_ids_list), 1.0))

    progress_bar.empty()
    return channel_stats_map


def search_videos(youtube, query, category_id, target_total, year, region_code, sort_order, relevance_language,
                  video_duration, video_type):
    """Main search logic refactored to return a list of dicts. Includes new filters."""
    all_videos = []
    next_page_token = None

    max_per_page = 50
    pages_to_fetch = (target_total + max_per_page - 1) // max_per_page

    try:
        for i in range(pages_to_fetch):
            search_params = {
                'part': 'snippet',
                'q': query,
                'type': 'video',
                'order': sort_order,
                'maxResults': max_per_page,
                'pageToken': next_page_token,
                'regionCode': region_code
            }

            # Optional Language Filter
            if relevance_language:
                search_params['relevanceLanguage'] = relevance_language

            # Optional Duration Filter
            if video_duration and video_duration != "any":
                search_params['videoDuration'] = video_duration

            # Optional Type Filter
            if video_type and video_type != "any":
                search_params['videoType'] = video_type

            if year:
                search_params['publishedAfter'] = f"{year}-01-01T00:00:00Z"
                search_params['publishedBefore'] = f"{int(year) + 1}-01-01T00:00:00Z"

            if category_id and category_id != "All":
                search_params['videoCategoryId'] = category_id

            search_response = youtube.search().list(**search_params).execute()
            video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]

            if not video_ids: break

            video_response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids)
            ).execute()

            fetched_videos = video_response.get("items", [])

            for video in fetched_videos:
                all_videos.append(video)
                if len(all_videos) >= target_total:
                    return all_videos[:target_total]

            next_page_token = search_response.get("nextPageToken")
            if not next_page_token: break

    except HttpError as e:
        st.error(f"API Error: {e}")

    return all_videos


# --- MAIN UI ---
def main():
    st.title("🎯 YouTube Placement Finder")
    st.markdown("Generate targeted video lists for your marketing campaigns.")

    # 1. Sidebar: Configuration
    with st.sidebar:
        st.header("1. API Configuration")
        st.markdown("👉 [**How to get an API Key?**](https://developers.google.com/youtube/v3/getting-started) 🔑")

        query_params = st.query_params
        default_key = query_params.get("api_key", "")

        if not default_key and "YOUTUBE_API_KEY" in st.secrets:
            default_key = st.secrets["YOUTUBE_API_KEY"]

        api_key = st.text_input("Enter YouTube API Key", value=default_key, type="password")

        if api_key:
            if api_key == default_key:
                st.caption("✅ Key loaded automatically")
            if api_key != query_params.get("api_key", ""):
                if st.button("🔗 Generate Bookmark Link"):
                    st.query_params["api_key"] = api_key
                    st.success("Link updated! Bookmark this page.")

        st.divider()
        st.header("2. Search Filters")
        query = st.text_input("Search Query", value="Colosseum tours")

        # --- LOCATION FILTER ---
        country_names = sorted(list(ALL_COUNTRY_CODES.keys()))
        default_country_index = country_names.index("United States") if "United States" in country_names else 0

        selected_country = st.selectbox(
            "Target Location",
            options=country_names,
            index=default_country_index
        )
        selected_region_code = ALL_COUNTRY_CODES[selected_country]

        # --- LANGUAGE FILTER ---
        selected_lang_label = st.selectbox(
            "Language Bias",
            options=list(LANGUAGE_CODES.keys()),
            index=0,
            help="Tells YouTube to prioritize results in this language."
        )
        selected_lang_code = LANGUAGE_CODES[selected_lang_label]

        # --- DURATION FILTER ---
        duration_options = {
            "Any Duration": "any",
            "Short (< 4 min)": "short",
            "Medium (4 - 20 min)": "medium",
            "Long (> 20 min)": "long"
        }
        selected_duration_label = st.selectbox("Video Duration", options=list(duration_options.keys()), index=0)
        selected_duration_code = duration_options[selected_duration_label]

        # --- TYPE FILTER ---
        type_options = {
            "All Videos": "any",
            "Movie": "movie",
            "Episode": "episode"
        }
        selected_type_label = st.selectbox("Video Type", options=list(type_options.keys()), index=0)
        selected_type_code = type_options[selected_type_label]

        # --- ORDER FILTER ---
        sort_options = {
            "Relevance (Default)": "relevance",
            "View Count (Highest)": "viewCount",
            "Date (Newest)": "date",
            "Rating (Highest)": "rating",
            "Title (A-Z)": "title"
        }
        selected_sort_label = st.selectbox("Sort Order (API)", options=list(sort_options.keys()), index=0)
        selected_sort_order = sort_options[selected_sort_label]

        year_input = st.text_input("Year (Leave blank for all time)", value="2025")
        year = int(year_input) if year_input.strip() and year_input.isdigit() else None

        target_total = st.number_input("Total Videos to Fetch (for CSV)", min_value=1, max_value=100, value=20)

    # --- FLOATING BUTTON ---
    buymeacoffee_url = "https://buymeacoffee.com/youtubeplacementfinder"
    st.markdown(
        f"""
        <style>
            .fixed-bottom-right {{
                position: fixed;
                top: 5rem;
                right: 20px;
                z-index: 9999;
                background-color: transparent;
                text-align: right;
            }}
            .fixed-bottom-right img {{
                transition: transform 0.2s;
                border-radius: 8px;
                box-shadow: 0px 2px 10px rgba(0,0,0,0.2);
            }}
            .fixed-bottom-right img:hover {{
                transform: scale(1.05);
            }}
        </style>
        <div class="fixed-bottom-right">
            <a href="{buymeacoffee_url}" target="_blank">
                <img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" width="160">
            </a>
        </div>
        """,
        unsafe_allow_html=True
    )

    # 2. Main Area: Logic
    if not api_key:
        st.info("👈 Please enter your YouTube API Key in the sidebar to start.")
        return

    try:
        youtube = build("youtube", "v3", developerKey=api_key)

        cat_map = get_category_map(youtube, region_code=selected_region_code)
        name_to_id = {v: k for k, v in cat_map.items()}
        name_to_id["All Categories"] = "All"
        cat_options = ["All Categories"] + sorted([k for k in name_to_id.keys() if k != "All Categories"])

        col1, col2 = st.columns([1, 1])
        with col1:
            selected_cat_name = st.selectbox("Select Category", options=cat_options, index=0)

        selected_cat_id = name_to_id[selected_cat_name]

        if st.button("🚀 Run Search", type="primary"):
            display_year = year if year else "All Time"
            lang_msg = f"in {selected_lang_label}" if selected_lang_code else ""
            with st.spinner(f"Searching for '{query}' in {selected_country} {lang_msg}..."):

                raw_videos = search_videos(
                    youtube,
                    query,
                    selected_cat_id,
                    target_total,
                    year,
                    selected_region_code,
                    selected_sort_order,
                    selected_lang_code,
                    selected_duration_code,
                    selected_type_code
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

                    # --- Extract Data ---
                    views = int(stats.get("viewCount", 0))
                    likes = int(stats.get("likeCount", 0))
                    comments = int(stats.get("commentCount", 0))
                    duration_sec = parse_duration(details.get("duration", "PT0S"))
                    published_at = snippet.get("publishedAt", "")

                    like_ratio = (likes / views * 100) if views > 0 else 0
                    comment_ratio = (comments / views * 100) if views > 0 else 0

                    spoken_lang = snippet.get("defaultAudioLanguage", "N/A")
                    text_lang = snippet.get("defaultLanguage", "N/A")
                    tags = "|".join(snippet.get("tags", []))

                    thumbnail_url = snippet.get("thumbnails", {}).get("high", {}).get("url", "")

                    avg_daily_views = 0
                    if published_at:
                        pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                        days_live = (today - pub_dt).days
                        avg_daily_views = views / days_live if days_live > 0 else views

                    cid = snippet.get("channelId")
                    c_stats = channel_stats.get(cid, {})

                    current_cat_id = snippet.get("categoryId")
                    current_cat_name = cat_map.get(current_cat_id, str(current_cat_id))

                    processed_data.append({
                        "Rank": rank_counter,
                        "Thumbnail": thumbnail_url,
                        "Title": snippet.get("title"),
                        "Channel": snippet.get("channelTitle"),
                        "Channel Subscribers": c_stats.get("subscriberCount", "N/A"),
                        "Channel Total Videos": c_stats.get("videoCount", 0),
                        "Views": views,
                        "Likes": likes,
                        "Comments": comments,
                        "Avg Views per Day": round(avg_daily_views, 2),
                        "Published Date": published_at.split("T")[0],
                        "Like-to-View Ratio (%)": round(like_ratio, 2),
                        "Comment-to-View Ratio (%)": round(comment_ratio, 2),
                        "Duration (Seconds)": duration_sec,
                        "Spoken Language": spoken_lang,
                        "Text Language": text_lang,
                        "Video Category": current_cat_name,
                        "Tags": tags,
                        "URL": f"https://www.youtube.com/watch?v={video['id']}"
                    })
                    rank_counter += 1

                # Save to session state
                st.session_state['df_full'] = pd.DataFrame(processed_data)
                st.session_state['search_query'] = query
                st.session_state['search_country'] = selected_region_code
                st.session_state['search_year'] = display_year

        # --- DISPLAY LOGIC (Runs if data exists in memory) ---
        if 'df_full' in st.session_state:
            df_full = st.session_state['df_full']

            st.divider()
            st.success(f"✅ Search Complete! Found {len(df_full)} videos.")

            col_d1, col_d2 = st.columns([1, 4])
            with col_d1:
                filename_year = st.session_state.get('search_year', 'AllTime')
                filename_country = st.session_state.get('search_country', 'US')
                safe_query = st.session_state.get('search_query', 'results').replace(' ', '_')

                csv = df_full.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Full CSV",
                    data=csv,
                    file_name=f"youtube_{safe_query}_{filename_country}_{filename_year}.csv",
                    mime="text/csv"
                )

            st.subheader(f"Top 20 Card Preview")
            sort_col1, sort_col2 = st.columns([1, 4])
            with sort_col1:
                local_sort_option = st.selectbox(
                    "Sort Cards By",
                    ["API Rank (Default)", "Engagement Rate (Highest)", "Views (Highest)", "Daily Views (Trending)",
                     "Duration (Shortest)", "Duration (Longest)", "Newest First"],
                    index=0,
                    key="local_sort"
                )

            # Apply Local Sort
            preview_df = df_full.copy()
            if "Engagement" in local_sort_option:
                preview_df = preview_df.sort_values(by="Like-to-View Ratio (%)", ascending=False)
            elif "Views" in local_sort_option and "Daily" not in local_sort_option:
                preview_df = preview_df.sort_values(by="Views", ascending=False)
            elif "Daily" in local_sort_option:
                preview_df = preview_df.sort_values(by="Avg Views per Day", ascending=False)
            elif "Shortest" in local_sort_option:
                preview_df = preview_df.sort_values(by="Duration (Seconds)", ascending=True)
            elif "Longest" in local_sort_option:
                preview_df = preview_df.sort_values(by="Duration (Seconds)", ascending=False)
            elif "Newest" in local_sort_option:
                preview_df = preview_df.sort_values(by="Published Date", ascending=False)

            preview_list = preview_df.head(20).to_dict('records')

            # --- CSS GRID RENDER WITH INSTANT TOOLTIPS ---
            grid_html = textwrap.dedent("""
            <style>
                .video-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 24px; margin-top: 10px; }
                .video-card { background: #ffffff; border: 1px solid #e0e0e0; border-radius: 12px; transition: transform 0.2s, box-shadow 0.2s; box-shadow: 0 4px 6px rgba(0,0,0,0.05); display: flex; flex-direction: column; }
                .video-card:hover { transform: translateY(-4px); box-shadow: 0 8px 15px rgba(0,0,0,0.1); }
                .thumbnail-container { position: relative; width: 100%; padding-top: 56.25%; background: #000; border-radius: 12px 12px 0 0; overflow: hidden; }
                .thumbnail-container img { position: absolute; top: 0; left: 0; width: 100%; height: 100%; object-fit: cover; opacity: 0.9; }
                .thumbnail-container:hover img { opacity: 1.0; }
                .duration-badge { position: absolute; bottom: 8px; right: 8px; background: rgba(0, 0, 0, 0.8); color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; }
                .rank-badge { position: absolute; top: 8px; left: 8px; background: #FF0000; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.3); }
                .card-content { padding: 12px; flex-grow: 1; display: flex; flex-direction: column; }
                .video-title { font-size: 15px; font-weight: 600; color: #1a1a1a; margin-bottom: 6px; text-decoration: none; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; line-height: 1.3; }
                .video-title:hover { color: #d93025; }
                .channel-row { display: flex; justify-content: space-between; font-size: 12px; color: #606060; margin-bottom: 8px; }
                .stats-container { background: #f8f9fa; border-top: 1px solid #eee; padding: 8px 12px; font-size: 12px; color: #555; display: flex; justify-content: space-between; align-items: center; border-radius: 0 0 12px 12px; }

                /* TOOLTIP CSS */
                .tooltip { position: relative; cursor: help; }
                .tooltip:hover::after {
                    content: attr(data-tooltip);
                    position: absolute;
                    bottom: 100%;
                    left: 50%;
                    transform: translateX(-50%);
                    background-color: #333;
                    color: white;
                    padding: 5px 10px;
                    border-radius: 4px;
                    font-size: 11px;
                    white-space: nowrap;
                    z-index: 10;
                    pointer-events: none;
                    margin-bottom: 5px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.2);
                }
                .engagement-badge { background: #e6f4ea; color: #137333; padding: 2px 6px; border-radius: 4px; font-weight: 600; font-size: 11px; }
                .lang-tag { font-size: 10px; padding: 1px 4px; border: 1px solid #ddd; border-radius: 3px; color: #777; margin-left: 5px; }
            </style>
            <div class="video-grid">
            """)

            for row in preview_list:
                views_fmt = format_big_number(row['Views'])
                daily_fmt = format_big_number(row['Avg Views per Day'])
                duration_fmt = format_duration(row['Duration (Seconds)'])
                time_ago = format_time_ago(row['Published Date'])
                engagement = row['Like-to-View Ratio (%)']

                eng_style = ""
                if engagement > 5.0:
                    eng_badge = f'<span class="engagement-badge tooltip" style="background:#ceead6; color:#0d652d;" data-tooltip="High Quality: {engagement}% Likes/Views">★ {engagement}%</span>'
                elif engagement > 2.0:
                    eng_badge = f'<span class="engagement-badge tooltip" data-tooltip="Engagement: {engagement}% Likes/Views">{engagement}%</span>'
                else:
                    eng_badge = f'<span class="tooltip" style="color:#888; font-size:11px;" data-tooltip="Engagement: {engagement}% Likes/Views">{engagement}%</span>'

                lang_html = ""
                if row['Spoken Language'] != "N/A":
                    lang_html = f'<span class="lang-tag tooltip" data-tooltip="Language: {row["Spoken Language"].upper()}">{row["Spoken Language"].upper()}</span>'

                card_html = textwrap.dedent(f"""
                <div class="video-card">
                    <a href="{row['URL']}" target="_blank" class="thumbnail-container">
                        <img src="{row['Thumbnail']}" alt="{row['Title']}">
                        <div class="rank-badge tooltip" data-tooltip="Rank #{row['Rank']} in API Results">#{row['Rank']}</div>
                        <div class="duration-badge">{duration_fmt}</div>
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
                            <div class="tooltip" data-tooltip="Total Views: {row['Views']:,}">👁️ {views_fmt}</div>
                            <div class="tooltip" data-tooltip="Avg Daily Views: {row['Avg Views per Day']:.1f}" style="font-size:10px; color:#888; margin-top:2px;">🔥 {daily_fmt}/day</div>
                        </div>
                        <div style="text-align:right;">
                            {eng_badge}
                            <div>{lang_html}</div>
                        </div>
                    </div>
                </div>
                """)
                grid_html += card_html

            grid_html += "</div>"
            st.markdown(grid_html, unsafe_allow_html=True)

    except Exception as e:
        st.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
