import logging

logger = logging.getLogger(__name__)
tables = "yt_api"

def insert_rows(cur,conn, schema, row):
    try:
        if schema == 'staging':
            cur.execute(
                f"""
                INSERT INTO {schema}.{tables} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """, row
            )

        else:
            cur.execute(
                f"""
                INSERT INTO {schema}.{tables} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);
                """, row
            )

        conn.commit()
        logger.info(f"Inserted row for Video_ID: {row['video_id']} into {schema}.{tables}")
    except Exception as e:
        logger.error(f"Error inserting row for Video_ID: {row['video_id']} into {schema}.{tables}: {e}")
        raise e
    
def update_rows(cur,conn, schema,row):
    try:
        # Staging
        if schema == "staging":
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'
        # Core
        else:
            video_id = 'Video_ID'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'
        
        cur.execute(
            f"""
            UPDATE {schema}.{tables}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s,
                "Likes_Count" = %({likes_count})s,
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """, row
        )
        conn.commit()
        logger.info(f"Updated row for Video_ID: {row[video_id]}")
    except Exception as e:
        logger.error(f"Error updating row for Video_ID: {row[video_id]} - {e}")
        raise e 

def delete_rows(cur,conn, schema, ids_to_delete):
    try:
        ids_to_delete = f"""({', '.join(['%s'] * len(ids_to_delete))})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{tables}
            WHERE "Video_ID" IN {ids_to_delete};
            """)
        conn.commit()
        logger.info(f"Deleted rows for Video_IDs: {ids_to_delete}")
    except Exception as e:
        logger.error(f"Error deleting rows for Video_IDs: {ids_to_delete} - {e}") 
        raise e