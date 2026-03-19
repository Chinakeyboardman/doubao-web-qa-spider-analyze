-- ============================================
-- Phase 4: 流水线状态追踪字段
-- 在 qa_query 和 qa_link 上增加 status 列
-- status 值: pending -> processing -> done / error
-- ============================================

ALTER TABLE qa_query ADD COLUMN IF NOT EXISTS status VARCHAR(16) DEFAULT 'pending';
ALTER TABLE qa_link  ADD COLUMN IF NOT EXISTS status VARCHAR(16) DEFAULT 'pending';

CREATE INDEX IF NOT EXISTS idx_qa_query_status ON qa_query(status);
CREATE INDEX IF NOT EXISTS idx_qa_link_status  ON qa_link(status);
