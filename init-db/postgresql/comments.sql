-- ============================================
-- QA 主流程四张表注释（可重复执行）
-- ============================================

COMMENT ON TABLE qa_query IS 'Query 主表：记录待处理/已处理的问题及其基础属性。';
COMMENT ON COLUMN qa_query.id IS '自增主键。';
COMMENT ON COLUMN qa_query.query_id IS '业务唯一查询ID，如 Q0001。';
COMMENT ON COLUMN qa_query.query_text IS '用户查询原文。';
COMMENT ON COLUMN qa_query.category IS '业务类目，如 3C数码、食品饮料。';
COMMENT ON COLUMN qa_query.intent_type IS '意图类型，如 信息型/交易型。';
COMMENT ON COLUMN qa_query.query_date IS '查询日期（业务维度）。';
COMMENT ON COLUMN qa_query.time_slot IS '时间段，如 上午/下午/晚上。';
COMMENT ON COLUMN qa_query.screenshot_path IS '可选截图路径。';
COMMENT ON COLUMN qa_query.remark IS '备注信息。';
COMMENT ON COLUMN qa_query.created_at IS '创建时间。';
COMMENT ON COLUMN qa_query.updated_at IS '更新时间。';

COMMENT ON TABLE qa_answer IS '回答表：记录每条 query 的豆包回答文本与原始响应。';
COMMENT ON COLUMN qa_answer.id IS '自增主键。';
COMMENT ON COLUMN qa_answer.query_id IS '关联 qa_query.query_id。';
COMMENT ON COLUMN qa_answer.answer_text IS '回答正文。';
COMMENT ON COLUMN qa_answer.answer_length IS '回答字数。';
COMMENT ON COLUMN qa_answer.has_citation IS '是否包含引用链接。';
COMMENT ON COLUMN qa_answer.citation_count IS '引用链接数量。';
COMMENT ON COLUMN qa_answer.raw_data IS '原始响应JSON（调试/追溯用）。';
COMMENT ON COLUMN qa_answer.created_at IS '创建时间。';
COMMENT ON COLUMN qa_answer.updated_at IS '更新时间。';

COMMENT ON TABLE qa_link IS '引用链接表：记录回答中提取的外部链接及抓取状态。';
COMMENT ON COLUMN qa_link.id IS '自增主键。';
COMMENT ON COLUMN qa_link.query_id IS '关联 qa_query.query_id。';
COMMENT ON COLUMN qa_link.link_id IS '业务唯一链接ID，如 Q0001_L001。';
COMMENT ON COLUMN qa_link.link_url IS '引用原始URL。';
COMMENT ON COLUMN qa_link.platform IS '平台识别结果，如 抖音/B站/小红书/通用。';
COMMENT ON COLUMN qa_link.content_format IS '内容格式，如 图文A/图文B/视频-有字幕。';
COMMENT ON COLUMN qa_link.publish_time IS '抓取到的发布时间（文本形式）。';
COMMENT ON COLUMN qa_link.popularity IS '热度指标展示字符串（如 点赞/评论/收藏/分享）。';
COMMENT ON COLUMN qa_link.fetched_at IS '内容抓取完成时间。';
COMMENT ON COLUMN qa_link.created_at IS '创建时间。';
COMMENT ON COLUMN qa_link.updated_at IS '更新时间。';

COMMENT ON TABLE qa_link_content IS '链接内容表：保存原始或结构化后的 JSONB 内容。';
COMMENT ON COLUMN qa_link_content.id IS '自增主键。';
COMMENT ON COLUMN qa_link_content.link_id IS '关联 qa_link.link_id（唯一）。';
COMMENT ON COLUMN qa_link_content.content_json IS '内容JSON（raw或结构化结果）。';
COMMENT ON COLUMN qa_link_content.created_at IS '创建时间。';
COMMENT ON COLUMN qa_link_content.updated_at IS '更新时间。';
