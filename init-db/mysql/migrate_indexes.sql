-- ============================================
-- MySQL 索引优化（基于 SQL 使用模式）
-- 减少 sort buffer、加速 WHERE/ORDER BY/JOIN
-- ============================================
-- 执行：./venv/bin/python scripts/apply_mysql_indexes.py
-- 或：mysql -u user -p db < init-db/mysql/migrate_indexes.sql
-- 若索引已存在会报 1061，可忽略

-- qa_query: WHERE status='pending' ORDER BY id LIMIT（claim、run-sync）
CREATE INDEX idx_qa_query_status_id ON qa_query(status, id);

-- qa_query: WHERE status='processing' AND updated_at < ?（超时重置）
CREATE INDEX idx_qa_query_status_updated ON qa_query(status, updated_at);

-- qa_link: WHERE status='pending' ORDER BY id LIMIT（claim_pending_links）
CREATE INDEX idx_qa_link_status_id ON qa_link(status, id);

-- qa_link: WHERE platform='抖音' AND status
CREATE INDEX idx_qa_link_platform_status ON qa_link(platform, status);

-- qa_link: WHERE status='done'（structure、audio 等）
CREATE INDEX idx_qa_link_status ON qa_link(status);

-- qa_link_video: JOIN link_id WHERE status='pending'（claim_pending_video_parse）
CREATE INDEX idx_qa_link_video_link_status ON qa_link_video(link_id, status);
