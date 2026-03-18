-- ============================================
-- migrate_v5: 抖音视频解析状态（Step 2.6）
-- ============================================

-- 1) 新增字段（放在 qa_link_content，最终内容状态随内容行管理）
ALTER TABLE qa_link_content
    ADD COLUMN IF NOT EXISTS video_parse_status VARCHAR(16);

-- 2) 约束：合法状态
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_qa_link_content_video_parse_status'
    ) THEN
        ALTER TABLE qa_link_content ADD CONSTRAINT chk_qa_link_content_video_parse_status
            CHECK (
                video_parse_status IS NULL OR
                video_parse_status IN ('pending', 'processing', 'done', 'error', 'skip')
            );
    END IF;
END;
$$;

-- 3) 如历史上误加在 qa_link，先迁移数据到 qa_link_content（若列存在）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'qa_link'
          AND column_name = 'video_parse_status'
    ) THEN
        UPDATE qa_link_content lc
        SET video_parse_status = l.video_parse_status
        FROM qa_link l
        WHERE l.link_id = lc.link_id
          AND COALESCE(lc.video_parse_status, '') = ''
          AND COALESCE(l.video_parse_status, '') <> '';
    END IF;
END;
$$;

-- 4) 初始化历史数据（仅抖音）
-- done 且已有 stt_text -> done
UPDATE qa_link_content lc
SET video_parse_status = 'done'
FROM qa_link l
WHERE l.link_id = lc.link_id
  AND l.platform = '抖音'
  AND l.status = 'done'
  AND COALESCE(lc.video_parse_status, '') = ''
  AND COALESCE(lc.raw_json->>'stt_text', '') <> '';

-- done 且已有字幕（无需音频解析）-> skip
UPDATE qa_link_content lc
SET video_parse_status = 'skip'
FROM qa_link l
WHERE l.link_id = lc.link_id
  AND l.platform = '抖音'
  AND l.status = 'done'
  AND COALESCE(lc.video_parse_status, '') = ''
  AND COALESCE(jsonb_array_length(COALESCE(lc.raw_json->'subtitles', '[]'::jsonb)), 0) > 0;

-- 其余抖音内容行 -> pending
UPDATE qa_link_content lc
SET video_parse_status = 'pending'
FROM qa_link l
WHERE l.link_id = lc.link_id
  AND l.platform = '抖音'
  AND COALESCE(lc.video_parse_status, '') = '';

-- 5) 索引（便于 Step 2.6 扫描）
CREATE INDEX IF NOT EXISTS idx_qa_link_content_video_parse_status
    ON qa_link_content(video_parse_status);

-- 6) 回滚旧方案：删除 qa_link 上的同名字段/约束/索引
ALTER TABLE qa_link DROP CONSTRAINT IF EXISTS chk_qa_link_video_parse_status;
DROP INDEX IF EXISTS idx_qa_link_video_parse_status;
ALTER TABLE qa_link DROP COLUMN IF EXISTS video_parse_status;

-- 7) 队列抢占函数：按 video_parse_status 原子认领（FOR UPDATE SKIP LOCKED）
CREATE OR REPLACE FUNCTION claim_pending_video_parse(
    p_limit INTEGER,
    p_query_ids VARCHAR[] DEFAULT NULL
)
RETURNS TABLE(query_id VARCHAR, link_id VARCHAR, link_url TEXT, raw_json JSONB) AS $$
BEGIN
    RETURN QUERY
    UPDATE qa_link_content lc
    SET video_parse_status = 'processing',
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT lc2.link_id, l2.query_id, l2.link_url
        FROM qa_link_content lc2
        JOIN qa_link l2 ON l2.link_id = lc2.link_id
        WHERE l2.platform = '抖音'
          AND l2.status = 'done'
          AND COALESCE(lc2.video_parse_status, 'pending') IN ('pending', 'error')
          AND lc2.raw_json IS NOT NULL
          AND COALESCE(lc2.raw_json->>'stt_text', '') = ''
          AND (p_query_ids IS NULL OR l2.query_id = ANY(p_query_ids))
        ORDER BY lc2.id
        LIMIT p_limit
        FOR UPDATE OF lc2 SKIP LOCKED
    ) sub
    WHERE lc.link_id = sub.link_id
    RETURNING sub.query_id, lc.link_id, sub.link_url, lc.raw_json;
END;
$$ LANGUAGE plpgsql;
