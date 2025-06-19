Clustera is a platform around agentic memories called Snowballs. 

This project, "clustera-youtube-ingest" is one of many ingestion components, whose responsibility is to bring data from a remote source (in this case Youtube) into pgSql tables.

# Architecture & tools

All of the business logic is inside an ingestion SDK. Alongside, we have a CLI that uses the SDK to accomplish its goals, but the CLI itself does not implement any of the business logic.

- We will use Apify (via its API) to download video transcripts and metadata. We don't need the video or audio itself.
- We will use Pandas for our dataframes.
- Use sqlalchemy for db access
- Might consider using alembic from migrations later on, but for now let's just pre-create our tables in the Prep stage.

# Requirements
- We are not going to download audio or video tracks for now.
- For the purposes of this codebase, the term "video" refers to the youtube video's metadata, including transcript, etc.
- We should only ingest videos that haven't already been ingested


# Pipeline 
## Stages

0. Prep

    - Create table for all tables that we'll need in all stages. 
    - Put the all the create statements in a single SQL file inside a `migrations` folder (but we'll just have the initial 000-create-tables.sql for now)
    - Use Pandas to upsert pgSql tables in the public schema
    - Control tables should have the `ctrl_` prefix
    - Dataset tables should have the `dataset_`

1. Sync Sources

    - Input: contents of the `ctrl_youtube_lists` table, which contains all the channels and playlists that we are monitoring
    - Call stage 2 for each
    
2. List ingestion

    - Input: YT Channel or Playlist url
    - Each individual video url is requested from Apify using the actor for this stage. 
    - When apify finishes, we store video data in `dataset_youtube_video` and we trigger stage 3 for this video.

    Notes:
    - Apify actor: streamers/youtube-scraper
    - Import all relevant data about the channel and about the videos

3. Video transcript Ingestion

    - Input: Individual Youtube Video
    - When the transcript is retrieved by the apify actor, store it in `dataset_youtube_video` in the `transcript` column

    Notes:
        - Apify actor: pintostudio/youtube-transcript-scraper