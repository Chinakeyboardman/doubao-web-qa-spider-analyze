-- ============================================
-- migrate_v6: qa_link_video 视频资源管理表
-- 管理抖音视频 link 对应的媒体资源：
--   下载/转写状态、原始 API 返回、文件路径、STT 结果等
-- ============================================

-- 1) 建表
CREATE TABLE IF NOT EXISTS qa_link_video (
    id                   SERIAL PRIMARY KEY,
    link_id              VARCHAR(32)  NOT NULL UNIQUE
                         REFERENCES qa_link(link_id) ON DELETE CASCADE,
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
    model_api_input_type VARCHAR(32),
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
    updated_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  qa_link_video IS '抖音视频资源管理表：跟踪视频下载、音频提取、STT 转写全流程。';
COMMENT ON COLUMN qa_link_video.link_id              IS '关联 qa_link.link_id（唯一）。';
COMMENT ON COLUMN qa_link_video.video_id             IS '抖音 aweme_id。';
COMMENT ON COLUMN qa_link_video.play_url             IS '视频播放地址。';
COMMENT ON COLUMN qa_link_video.cover_url            IS '视频封面图 URL。';
COMMENT ON COLUMN qa_link_video.duration             IS '视频时长（秒）。';
COMMENT ON COLUMN qa_link_video.video_path           IS '本地下载的视频文件路径。';
COMMENT ON COLUMN qa_link_video.audio_path           IS '本地提取的音频文件路径。';
COMMENT ON COLUMN qa_link_video.stt_text             IS '语音转文字结果。';
COMMENT ON COLUMN qa_link_video.subtitles            IS '字幕数据 JSON。';
COMMENT ON COLUMN qa_link_video.transcript_model     IS 'STT 使用的模型名称。';
COMMENT ON COLUMN qa_link_video.transcript_source    IS '转写来源：audio_file_id / video_file_id_fallback / raw_text_fallback。';
COMMENT ON COLUMN qa_link_video.model_api_file_id    IS '模型 API 上传后的 file_id。';
COMMENT ON COLUMN qa_link_video.model_api_input_type IS '模型 API 输入类型：input_audio / input_video。';
COMMENT ON COLUMN qa_link_video.raw_api_response     IS '抖音 API 完整原始返回（调试/追溯用）。';
COMMENT ON COLUMN qa_link_video.status               IS '处理状态：pending/processing/done/error/skip。';
COMMENT ON COLUMN qa_link_video.error_message        IS '错误详情。';
COMMENT ON COLUMN qa_link_video.retry_count          IS '重试次数。';
COMMENT ON COLUMN qa_link_video.fetched_at           IS '视频下载完成时间。';
COMMENT ON COLUMN qa_link_video.transcribed_at       IS 'STT 转写完成时间。';

-- 2) 索引
CREATE INDEX IF NOT EXISTS idx_qa_link_video_status  ON qa_link_video(status);
CREATE INDEX IF NOT EXISTS idx_qa_link_video_link_id ON qa_link_video(link_id);

-- 3) updated_at 自动更新触发器（复用已有函数 qa_update_updated_at）
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_qa_link_video_updated_at'
    ) THEN
        CREATE TRIGGER trg_qa_link_video_updated_at
            BEFORE UPDATE ON qa_link_video
            FOR EACH ROW EXECUTE FUNCTION qa_update_updated_at();
    END IF;
END;
$$;

-- 4) 原子认领函数（FOR UPDATE SKIP LOCKED）
CREATE OR REPLACE FUNCTION claim_pending_video_parse_v2(
    p_limit     INTEGER,
    p_query_ids VARCHAR[] DEFAULT NULL
)
RETURNS TABLE(
    link_id   VARCHAR,
    query_id  VARCHAR,
    link_url  TEXT,
    raw_json  JSONB
) AS $$
BEGIN
    RETURN QUERY
    UPDATE qa_link_video v
    SET status     = 'processing',
        updated_at = CURRENT_TIMESTAMP
    FROM (
        SELECT v2.link_id, l2.query_id, l2.link_url, lc2.raw_json
        FROM qa_link_video v2
        JOIN qa_link l2          ON l2.link_id = v2.link_id
        LEFT JOIN qa_link_content lc2 ON lc2.link_id = v2.link_id
        WHERE v2.status IN ('pending', 'error')
          AND l2.status = 'done'
          AND COALESCE(v2.stt_text, '') = ''
          AND (p_query_ids IS NULL OR l2.query_id = ANY(p_query_ids))
        ORDER BY v2.id
        LIMIT p_limit
        FOR UPDATE OF v2 SKIP LOCKED
    ) sub
    WHERE v.link_id = sub.link_id
    RETURNING sub.link_id, sub.query_id, sub.link_url, sub.raw_json;
END;
$$ LANGUAGE plpgsql;

-- 5) 旧数据回填：从 qa_link_content + qa_link 中提取已有抖音视频数据
INSERT INTO qa_link_video (
    link_id, video_id, play_url, cover_url, duration,
    video_path, audio_path, stt_text, subtitles,
    transcript_model, transcript_source,
    model_api_file_id, model_api_input_type,
    status
)
SELECT
    lc.link_id,
    lc.raw_json->'video_info'->>'aweme_id',
    lc.raw_json->'video_info'->>'play_url',
    lc.raw_json->'video_info'->>'cover_url',
    COALESCE((lc.raw_json->'video_info'->>'duration')::integer, 0),
    lc.raw_json->'audio_info'->>'video_path',
    lc.raw_json->'audio_info'->>'audio_path',
    lc.raw_json->>'stt_text',
    lc.raw_json->'subtitles',
    lc.raw_json->'audio_info'->>'transcript_model',
    lc.raw_json->'audio_info'->>'transcript_source',
    lc.raw_json->'audio_info'->>'model_api_file_id',
    lc.raw_json->'audio_info'->>'model_api_input_type',
    CASE
        WHEN COALESCE(lc.video_parse_status, '') = '' THEN 'pending'
        ELSE lc.video_parse_status
    END
FROM qa_link_content lc
JOIN qa_link l ON l.link_id = lc.link_id
WHERE l.platform = '抖音'
  AND lc.raw_json IS NOT NULL
ON CONFLICT (link_id) DO NOTHING;

-- 6) 对有 stt_text 但 status 仍为 pending 的旧数据，修正为 done
UPDATE qa_link_video
SET status = 'done'
WHERE COALESCE(stt_text, '') <> ''
  AND status IN ('pending', 'processing');

-- 7) 有字幕但无 stt_text 的，标记为 skip
UPDATE qa_link_video
SET status = 'skip'
WHERE COALESCE(stt_text, '') = ''
  AND subtitles IS NOT NULL
  AND jsonb_array_length(subtitles) > 0
  AND status = 'pending';
