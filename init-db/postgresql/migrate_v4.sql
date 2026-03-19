-- ============================================
-- migrate_v4: 补齐缺失字段 + status 约束 + 并发安全
-- ============================================

-- 1. qa_query / qa_link: 补 error_message, retry_count
ALTER TABLE qa_query ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE qa_query ADD COLUMN IF NOT EXISTS retry_count   INTEGER DEFAULT 0;

ALTER TABLE qa_link  ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE qa_link  ADD COLUMN IF NOT EXISTS retry_count   INTEGER DEFAULT 0;

-- 2. status CHECK 约束（防止写入非法值）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_qa_query_status'
    ) THEN
        ALTER TABLE qa_query ADD CONSTRAINT chk_qa_query_status
            CHECK (status IN ('pending', 'processing', 'done', 'error'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_qa_answer_status'
    ) THEN
        ALTER TABLE qa_answer ADD CONSTRAINT chk_qa_answer_status
            CHECK (status IN ('processing', 'done', 'error'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_qa_link_status'
    ) THEN
        ALTER TABLE qa_link ADD CONSTRAINT chk_qa_link_status
            CHECK (status IN ('pending', 'processing', 'done', 'error', 'skip'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_qa_link_content_status'
    ) THEN
        ALTER TABLE qa_link_content ADD CONSTRAINT chk_qa_link_content_status
            CHECK (status IN ('pending', 'processing', 'done', 'error'));
    END IF;
END;
$$;

-- 3. 原子抢占函数: claim_pending_queries
--    用 FOR UPDATE SKIP LOCKED 防止并发重复处理
CREATE OR REPLACE FUNCTION claim_pending_queries(
    p_limit      INTEGER,
    p_start_id   VARCHAR DEFAULT NULL,
    p_end_id     VARCHAR DEFAULT NULL
)
RETURNS TABLE(query_id VARCHAR, query_text TEXT) AS $$
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
    RETURNING q.query_id, q.query_text;
END;
$$ LANGUAGE plpgsql;

-- 4. 原子抢占函数: claim_pending_links
CREATE OR REPLACE FUNCTION claim_pending_links(
    p_limit      INTEGER,
    p_query_ids  VARCHAR[] DEFAULT NULL
)
RETURNS TABLE(link_id VARCHAR, link_url TEXT, platform VARCHAR, content_format VARCHAR) AS $$
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
    RETURNING l.link_id, l.link_url, l.platform, l.content_format;
END;
$$ LANGUAGE plpgsql;
