-- ============================================
-- QA 业务表初始化（MySQL 8.0+ 版）
-- 对应 PostgreSQL 版: init-db/postgresql/init.sql
-- ============================================

-- 表1：Query 记录主表
CREATE TABLE IF NOT EXISTS qa_query (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    query_id        VARCHAR(32)  UNIQUE NOT NULL,
    query_text      TEXT         NOT NULL,
    category        VARCHAR(64)  DEFAULT NULL COMMENT '类目，如 3C数码',
    intent_type     VARCHAR(32)  DEFAULT NULL COMMENT '意图类型：信息型/交易型',
    query_date      DATE         DEFAULT NULL,
    time_slot       VARCHAR(16)  DEFAULT NULL COMMENT '上午/下午/晚上',
    status          VARCHAR(16)  DEFAULT 'pending',
    error_message   TEXT         DEFAULT NULL,
    retry_count     INT          DEFAULT 0,
    screenshot_path TEXT         DEFAULT NULL,
    remark          TEXT         DEFAULT NULL,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_qa_query_status CHECK (status IN ('pending','processing','done','error'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Query 主表：记录待处理/已处理的问题及其基础属性。';

-- 表2：豆包回答内容表（Level 1）
CREATE TABLE IF NOT EXISTS qa_answer (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    query_id        VARCHAR(32)  NOT NULL,
    answer_text     TEXT         DEFAULT NULL,
    answer_length   INT          DEFAULT NULL,
    status          VARCHAR(16)  DEFAULT 'done',
    has_citation    TINYINT(1)   DEFAULT 0,
    citation_count  INT          DEFAULT 0,
    raw_data        JSON         DEFAULT NULL,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_qa_answer_status CHECK (status IN ('processing','done','error')),
    CONSTRAINT fk_qa_answer_query FOREIGN KEY (query_id) REFERENCES qa_query(query_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='回答表：记录每条 query 的豆包回答文本与原始响应。';

-- 表3：引用链接明细表（Level 2）
CREATE TABLE IF NOT EXISTS qa_link (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    query_id        VARCHAR(32)  NOT NULL,
    link_id         VARCHAR(32)  UNIQUE NOT NULL,
    link_url        TEXT         DEFAULT NULL,
    platform        VARCHAR(64)  DEFAULT NULL COMMENT '小红书/B站/淘宝/什么值得买…',
    content_format  VARCHAR(32)  DEFAULT NULL COMMENT '图文A/图文B/视频-有字幕/视频-无字幕/商品页',
    publish_time    VARCHAR(64)  DEFAULT NULL,
    popularity      TEXT         DEFAULT NULL COMMENT '热度指标，原始文本',
    status          VARCHAR(16)  DEFAULT 'pending',
    error_message   TEXT         DEFAULT NULL,
    retry_count     INT          DEFAULT 0,
    fetched_at      TIMESTAMP    NULL DEFAULT NULL,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_qa_link_status CHECK (status IN ('pending','processing','done','error','skip')),
    CONSTRAINT fk_qa_link_query FOREIGN KEY (query_id) REFERENCES qa_query(query_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='引用链接表：记录回答中提取的外部链接及抓取状态。';

-- 表4：链接内容 JSON 表
CREATE TABLE IF NOT EXISTS qa_link_content (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    link_id         VARCHAR(32)  UNIQUE NOT NULL,
    content_json    JSON         DEFAULT NULL,
    raw_json        JSON         DEFAULT NULL,
    video_parse_status VARCHAR(16) DEFAULT NULL,
    status          VARCHAR(16)  DEFAULT 'done',
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_qa_link_content_video_parse_status CHECK (
        video_parse_status IS NULL OR
        video_parse_status IN ('pending','processing','done','error','skip')
    ),
    CONSTRAINT chk_qa_link_content_status CHECK (status IN ('pending','processing','done','error')),
    CONSTRAINT fk_qa_link_content_link FOREIGN KEY (link_id) REFERENCES qa_link(link_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='链接内容表：保存原始或结构化后的 JSON 内容。';

-- 表5：抖音视频资源管理表
CREATE TABLE IF NOT EXISTS qa_link_video (
    id                   INT AUTO_INCREMENT PRIMARY KEY,
    link_id              VARCHAR(32)  NOT NULL,
    video_id             VARCHAR(64)  DEFAULT NULL,
    play_url             TEXT         DEFAULT NULL,
    cover_url            TEXT         DEFAULT NULL,
    duration             INT          DEFAULT 0,
    video_path           TEXT         DEFAULT NULL,
    audio_path           TEXT         DEFAULT NULL,
    stt_text             TEXT         DEFAULT NULL,
    subtitles            JSON         DEFAULT NULL,
    transcript_model     VARCHAR(64)  DEFAULT NULL,
    transcript_source    VARCHAR(32)  DEFAULT NULL,
    model_api_file_id    VARCHAR(128) DEFAULT NULL,
    model_api_input_type VARCHAR(32)  NOT NULL DEFAULT 'input_audio',
    raw_api_response     JSON         DEFAULT NULL,
    status               VARCHAR(16)  DEFAULT 'pending',
    error_message        TEXT         DEFAULT NULL,
    retry_count          INT          DEFAULT 0,
    fetched_at           TIMESTAMP    NULL DEFAULT NULL,
    transcribed_at       TIMESTAMP    NULL DEFAULT NULL,
    created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_qa_link_video_status CHECK (
        status IN ('pending','processing','done','error','skip')
    ),
    CONSTRAINT uq_link_video_link_input_type UNIQUE (link_id, model_api_input_type),
    CONSTRAINT fk_qa_link_video_link FOREIGN KEY (link_id) REFERENCES qa_link(link_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='抖音视频资源管理表：跟踪视频下载、音频提取、STT 转写全流程。';

-- ============================================
-- 索引
-- ============================================
CREATE INDEX idx_qa_query_category     ON qa_query(category);
CREATE INDEX idx_qa_query_intent       ON qa_query(intent_type);
CREATE INDEX idx_qa_query_date         ON qa_query(query_date);
CREATE INDEX idx_qa_answer_query_id    ON qa_answer(query_id);
CREATE INDEX idx_qa_link_query_id      ON qa_link(query_id);
CREATE INDEX idx_qa_link_platform      ON qa_link(platform);
CREATE INDEX idx_qa_link_format        ON qa_link(content_format);
CREATE INDEX idx_qa_link_content_link  ON qa_link_content(link_id);
CREATE INDEX idx_qa_link_video_status  ON qa_link_video(status);
CREATE INDEX idx_qa_link_video_link_id ON qa_link_video(link_id);

-- ============================================
-- updated_at 自动更新触发器
-- MySQL 使用 ON UPDATE CURRENT_TIMESTAMP (已在列定义中), 无需额外触发器
-- ============================================
