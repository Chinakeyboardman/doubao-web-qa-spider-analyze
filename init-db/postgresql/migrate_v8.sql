-- ============================================
-- migrate_v8: 乐观锁 - 所有 status 更新加上 updated_at 条件
-- 防止并发下覆盖其他进程的更新
-- ============================================

-- 1. claim_pending_queries: 返回 updated_at（需先 DROP 因返回类型变化）
DROP FUNCTION IF EXISTS claim_pending_queries(INTEGER, VARCHAR, VARCHAR);
CREATE OR REPLACE FUNCTION claim_pending_queries(
    p_limit      INTEGER,
    p_start_id   VARCHAR DEFAULT NULL,
    p_end_id     VARCHAR DEFAULT NULL
)
RETURNS TABLE(query_id VARCHAR, query_text TEXT, updated_at TIMESTAMP) AS $$
BEGIN
    RETURN QUERY
    UPDATE qa_query q SET
        status = 'processing',
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT q2.query_id
        FROM qa_query q2
        WHERE q2.status = 'pending'
          AND (p_start_id IS NULL OR q2.query_id >= p_start_id)
          AND (p_end_id   IS NULL OR q2.query_id <= p_end_id)
        ORDER BY q2.id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    ) sub
    WHERE q.query_id = sub.query_id
    RETURNING q.query_id, q.query_text, q.updated_at;
END;
$$ LANGUAGE plpgsql;

-- 2. claim_pending_links: 返回 updated_at（需先 DROP 因返回类型变化）
DROP FUNCTION IF EXISTS claim_pending_links(INTEGER, VARCHAR[]);
CREATE OR REPLACE FUNCTION claim_pending_links(
    p_limit      INTEGER,
    p_query_ids  VARCHAR[] DEFAULT NULL
)
RETURNS TABLE(link_id VARCHAR, link_url TEXT, platform VARCHAR, content_format VARCHAR, updated_at TIMESTAMP) AS $$
BEGIN
    RETURN QUERY
    UPDATE qa_link l SET
        status = 'processing',
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT l2.link_id
        FROM qa_link l2
        WHERE l2.status = 'pending'
          AND (p_query_ids IS NULL OR l2.query_id = ANY(p_query_ids))
        ORDER BY l2.id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    ) sub
    WHERE l.link_id = sub.link_id
    RETURNING l.link_id, l.link_url, l.platform, l.content_format, l.updated_at;
END;
$$ LANGUAGE plpgsql;

-- 3. claim_pending_video_parse_v2: 返回 video_updated_at, content_updated_at
DROP FUNCTION IF EXISTS claim_pending_video_parse_v2(INTEGER, VARCHAR[]);
CREATE OR REPLACE FUNCTION claim_pending_video_parse_v2(
    p_limit     INTEGER,
    p_query_ids VARCHAR[] DEFAULT NULL
)
RETURNS TABLE(
    vid                  INTEGER,
    link_id              VARCHAR,
    query_id             VARCHAR,
    link_url             TEXT,
    raw_json             JSONB,
    model_api_input_type VARCHAR,
    video_updated_at     TIMESTAMP,
    content_updated_at   TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    UPDATE qa_link_video v
    SET status     = 'processing',
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT v2.id AS vid, v2.link_id, l2.query_id, l2.link_url,
               lc2.raw_json, v2.model_api_input_type,
               lc2.updated_at AS content_updated_at
        FROM qa_link_video v2
        JOIN qa_link l2            ON l2.link_id = v2.link_id
        LEFT JOIN qa_link_content lc2 ON lc2.link_id = v2.link_id
        WHERE v2.status IN ('pending', 'error')
          AND l2.status = 'done'
          AND COALESCE(v2.stt_text, '') = ''
          AND (p_query_ids IS NULL OR l2.query_id = ANY(p_query_ids))
          AND NOT EXISTS (
              SELECT 1 FROM qa_link_video sib
              WHERE sib.link_id = v2.link_id
                AND sib.model_api_input_type = 'input_audio'
                AND sib.status = 'done'
          )
        ORDER BY
            CASE WHEN v2.model_api_input_type = 'input_audio' THEN 0 ELSE 1 END,
            v2.id
        LIMIT p_limit
        FOR UPDATE OF v2 SKIP LOCKED
    ) sub
    WHERE v.id = sub.vid
    RETURNING sub.vid, sub.link_id, sub.query_id, sub.link_url,
              sub.raw_json, sub.model_api_input_type,
              v.updated_at AS video_updated_at,
              sub.content_updated_at;
END;
$$ LANGUAGE plpgsql;
