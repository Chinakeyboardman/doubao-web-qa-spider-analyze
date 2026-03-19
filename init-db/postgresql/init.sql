-- ============================================
-- QA 业务表初始化（qa_ 前缀）
-- 参考：docs/大规模qa数据获取.md
-- PG 首次启动时自动执行
-- ============================================

-- 表1：Query 记录主表
CREATE TABLE IF NOT EXISTS qa_query (
    id              SERIAL PRIMARY KEY,
    query_id        VARCHAR(32)  UNIQUE NOT NULL,
    query_text      TEXT         NOT NULL,
    category        VARCHAR(64),                   -- 类目，如 3C数码
    intent_type     VARCHAR(32),                   -- 意图类型：信息型/交易型
    query_date      DATE,
    time_slot       VARCHAR(16),                   -- 上午/下午/晚上
    status          VARCHAR(16)  DEFAULT 'pending'
                    CONSTRAINT chk_qa_query_status CHECK (status IN ('pending','processing','done','error')),
    error_message   TEXT,
    retry_count     INTEGER DEFAULT 0,
    screenshot_path TEXT,
    remark          TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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

-- 表2：豆包回答内容表（Level 1）
CREATE TABLE IF NOT EXISTS qa_answer (
    id              SERIAL PRIMARY KEY,
    query_id        VARCHAR(32)  NOT NULL REFERENCES qa_query(query_id) ON DELETE CASCADE,
    answer_text     TEXT,
    answer_length   INTEGER,
    status          VARCHAR(16)  DEFAULT 'done'
                    CONSTRAINT chk_qa_answer_status CHECK (status IN ('processing','done','error')),
    has_citation     BOOLEAN DEFAULT FALSE,
    citation_count  INTEGER DEFAULT 0,
    raw_data        JSONB,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE qa_answer IS '回答表：记录每条 query 的豆包回答文本与原始响应。';
COMMENT ON COLUMN qa_answer.id IS '自增主键。';
COMMENT ON COLUMN qa_answer.query_id IS '关联 qa_query.query_id。';
COMMENT ON COLUMN qa_answer.answer_text IS '回答正文。';
COMMENT ON COLUMN qa_answer.answer_length IS '回答字数。';
COMMENT ON COLUMN qa_answer.status IS '回答状态：processing/done/error。';
COMMENT ON COLUMN qa_answer.has_citation IS '是否包含引用链接。';
COMMENT ON COLUMN qa_answer.citation_count IS '引用链接数量。';
COMMENT ON COLUMN qa_answer.raw_data IS '原始响应JSON（调试/追溯用）。';
COMMENT ON COLUMN qa_answer.created_at IS '创建时间。';
COMMENT ON COLUMN qa_answer.updated_at IS '更新时间。';

-- 表3：引用链接明细表（Level 2）
CREATE TABLE IF NOT EXISTS qa_link (
    id              SERIAL PRIMARY KEY,
    query_id        VARCHAR(32)  NOT NULL REFERENCES qa_query(query_id) ON DELETE CASCADE,
    link_id         VARCHAR(32)  UNIQUE NOT NULL,
    link_url        TEXT,
    platform        VARCHAR(64),                   -- 小红书/B站/淘宝/什么值得买…
    content_format  VARCHAR(32),                   -- 图文A/图文B/视频-有字幕/视频-无字幕/商品页
    publish_time    VARCHAR(64),
    popularity      TEXT,                           -- 热度指标，原始文本
    status          VARCHAR(16)  DEFAULT 'pending'
                    CONSTRAINT chk_qa_link_status CHECK (status IN ('pending','processing','done','error','skip')),
    error_message   TEXT,
    retry_count     INTEGER DEFAULT 0,
    fetched_at      TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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

-- 表4：链接内容 JSON 表
CREATE TABLE IF NOT EXISTS qa_link_content (
    id              SERIAL PRIMARY KEY,
    link_id         VARCHAR(32)  UNIQUE NOT NULL REFERENCES qa_link(link_id) ON DELETE CASCADE,
    content_json    JSONB        NOT NULL,
    raw_json        JSONB,
    video_parse_status VARCHAR(16)
                    CONSTRAINT chk_qa_link_content_video_parse_status CHECK (
                        video_parse_status IS NULL OR
                        video_parse_status IN ('pending','processing','done','error','skip')
                    ),
    status          VARCHAR(16)  DEFAULT 'done'
                    CONSTRAINT chk_qa_link_content_status CHECK (status IN ('pending','processing','done','error')),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE qa_link_content IS '链接内容表：保存原始或结构化后的 JSONB 内容。';
COMMENT ON COLUMN qa_link_content.id IS '自增主键。';
COMMENT ON COLUMN qa_link_content.link_id IS '关联 qa_link.link_id（唯一）。';
COMMENT ON COLUMN qa_link_content.content_json IS '内容JSON（raw或结构化结果）。';
COMMENT ON COLUMN qa_link_content.raw_json IS '结构化优化前的原始JSON（用于追溯与回滚）。';
COMMENT ON COLUMN qa_link_content.video_parse_status IS '抖音专用视频解析状态：pending/processing/done/error/skip。';
COMMENT ON COLUMN qa_link_content.status IS '内容状态：processing/done/error。';
COMMENT ON COLUMN qa_link_content.created_at IS '创建时间。';
COMMENT ON COLUMN qa_link_content.updated_at IS '更新时间。';

-- 表5：抖音视频资源管理表
CREATE TABLE IF NOT EXISTS qa_link_video (
    id                   SERIAL PRIMARY KEY,
    link_id              VARCHAR(32)  NOT NULL REFERENCES qa_link(link_id) ON DELETE CASCADE,
    video_id             VARCHAR(64),
    play_url             TEXT,
    cover_url            TEXT,
    duration             INTEGER      DEFAULT 0,
    video_path           TEXT,
    audio_path           TEXT,
    stt_text             TEXT,
    subtitles            JSONB,
    transcript_model     VARCHAR(64),
    transcript_source    VARCHAR(32),
    model_api_file_id    VARCHAR(128),
    model_api_input_type VARCHAR(32)  NOT NULL DEFAULT 'input_audio',
    raw_api_response     JSONB,
    status               VARCHAR(16)  DEFAULT 'pending'
                         CONSTRAINT chk_qa_link_video_status CHECK (
                             status IN ('pending','processing','done','error','skip')
                         ),
    error_message        TEXT,
    retry_count          INTEGER      DEFAULT 0,
    fetched_at           TIMESTAMP,
    transcribed_at       TIMESTAMP,
    created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_link_video_link_input_type UNIQUE (link_id, model_api_input_type)
);
COMMENT ON TABLE  qa_link_video IS '抖音视频资源管理表：跟踪视频下载、音频提取、STT 转写全流程。';
COMMENT ON COLUMN qa_link_video.link_id              IS '关联 qa_link.link_id（复合唯一键之一）。';
COMMENT ON COLUMN qa_link_video.video_id             IS '抖音 aweme_id。';
COMMENT ON COLUMN qa_link_video.play_url             IS '视频播放地址。';
COMMENT ON COLUMN qa_link_video.cover_url            IS '视频封面图 URL。';
COMMENT ON COLUMN qa_link_video.duration             IS '视频时长（秒）。';
COMMENT ON COLUMN qa_link_video.video_path           IS '本地下载的视频文件路径。';
COMMENT ON COLUMN qa_link_video.audio_path           IS '本地提取的音频文件路径。';
COMMENT ON COLUMN qa_link_video.stt_text             IS '语音转文字结果。';
COMMENT ON COLUMN qa_link_video.subtitles            IS '字幕数据 JSON。';
COMMENT ON COLUMN qa_link_video.transcript_model     IS 'STT 使用的模型名称。';
COMMENT ON COLUMN qa_link_video.transcript_source    IS '转写来源。';
COMMENT ON COLUMN qa_link_video.model_api_file_id    IS '模型 API file_id。';
COMMENT ON COLUMN qa_link_video.model_api_input_type IS '模型 API 输入类型。';
COMMENT ON COLUMN qa_link_video.raw_api_response     IS '抖音 API 原始返回。';
COMMENT ON COLUMN qa_link_video.status               IS '处理状态：pending/processing/done/error/skip。';
COMMENT ON COLUMN qa_link_video.error_message        IS '错误详情。';
COMMENT ON COLUMN qa_link_video.retry_count          IS '重试次数。';
COMMENT ON COLUMN qa_link_video.fetched_at           IS '视频下载完成时间。';
COMMENT ON COLUMN qa_link_video.transcribed_at       IS 'STT 完成时间。';

-- ============================================
-- 索引
-- ============================================
CREATE INDEX IF NOT EXISTS idx_qa_query_category     ON qa_query(category);
CREATE INDEX IF NOT EXISTS idx_qa_query_intent       ON qa_query(intent_type);
CREATE INDEX IF NOT EXISTS idx_qa_query_date         ON qa_query(query_date);
CREATE INDEX IF NOT EXISTS idx_qa_answer_query_id    ON qa_answer(query_id);
CREATE INDEX IF NOT EXISTS idx_qa_link_query_id      ON qa_link(query_id);
CREATE INDEX IF NOT EXISTS idx_qa_link_platform      ON qa_link(platform);
CREATE INDEX IF NOT EXISTS idx_qa_link_format        ON qa_link(content_format);
CREATE INDEX IF NOT EXISTS idx_qa_link_content_link  ON qa_link_content(link_id);
CREATE INDEX IF NOT EXISTS idx_qa_link_video_status  ON qa_link_video(status);
CREATE INDEX IF NOT EXISTS idx_qa_link_video_link_id ON qa_link_video(link_id);

-- ============================================
-- updated_at 自动更新触发器
-- ============================================
CREATE OR REPLACE FUNCTION qa_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['qa_query', 'qa_answer', 'qa_link', 'qa_link_content', 'qa_link_video']
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_' || t || '_updated_at'
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER trg_%s_updated_at BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION qa_update_updated_at()',
                t, t
            );
        END IF;
    END LOOP;
END;
$$;
