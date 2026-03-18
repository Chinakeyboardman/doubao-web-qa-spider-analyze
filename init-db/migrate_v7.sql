-- ============================================
-- migrate_v7: qa_link_video 复合唯一键
--   (link_id, model_api_input_type) 替代 link_id 单列唯一
--   允许同一 link 同时存在 input_audio 和 input_video 记录
-- ============================================

-- 1) 回填 model_api_input_type：已完成的行标为实际类型，待处理的标为 input_audio
UPDATE qa_link_video
SET model_api_input_type = CASE
    WHEN status IN ('done', 'skip') AND COALESCE(model_api_input_type, '') = '' THEN 'input_video'
    WHEN COALESCE(model_api_input_type, '') = '' THEN 'input_audio'
    ELSE model_api_input_type
END
WHERE COALESCE(model_api_input_type, '') = '';

-- 2) 添加 NOT NULL + DEFAULT
ALTER TABLE qa_link_video
    ALTER COLUMN model_api_input_type SET DEFAULT 'input_audio',
    ALTER COLUMN model_api_input_type SET NOT NULL;

-- 3) 删除旧的 link_id 单列唯一约束
ALTER TABLE qa_link_video
    DROP CONSTRAINT IF EXISTS qa_link_video_link_id_key;

-- 4) 添加复合唯一约束
ALTER TABLE qa_link_video
    ADD CONSTRAINT uq_link_video_link_input_type
    UNIQUE (link_id, model_api_input_type);

-- 5) 更新 claim 函数：返回 vid + model_api_input_type，UPDATE 匹配用 PK
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
    model_api_input_type VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    UPDATE qa_link_video v
    SET status     = 'processing',
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT v2.id AS vid, v2.link_id, l2.query_id, l2.link_url,
               lc2.raw_json, v2.model_api_input_type
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
              sub.raw_json, sub.model_api_input_type;
END;
$$ LANGUAGE plpgsql;
