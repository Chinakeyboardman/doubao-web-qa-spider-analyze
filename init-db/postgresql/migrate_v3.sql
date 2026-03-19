-- ============================================
-- raw_json / content_json 分离
-- 爬虫只写 raw_json，content_json 由 structure 步骤生成
-- content_json 改为可空，以支持 crawler_manager 的 INSERT
-- ============================================

ALTER TABLE qa_link_content ALTER COLUMN content_json DROP NOT NULL;
