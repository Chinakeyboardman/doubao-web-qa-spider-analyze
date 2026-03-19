-- ============================================
-- 将 QA 业务表恢复到初始空状态（仅清空 qa_ 前缀表）
-- 不触碰 douyin 等其它表
-- ============================================

TRUNCATE TABLE qa_link_video, qa_link_content, qa_link, qa_answer, qa_query
  RESTART IDENTITY CASCADE;
