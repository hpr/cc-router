-- 配额感知路由 (quota-aware routing):
-- 1) subscriptions 表加 quota_feed_path 列: 指向全局配额 feed 响应里本订阅窗口对象的
--    JSON 指针 (RFC 6901, 如 /kimi/windows 或 /mimo)。NULL = 不参与配额感知排序。
--    feed URL 是全局设置 (settings.json quota_feed_url), 不进订阅行。
-- 2) 每订阅一份 feed 解析结果缓存 (重启恢复用), mirror subscription_balance_cache (migration 010)。
--    payload_json 存 QuotaFeedSnapshot 整体序列化结果, 含 windows[] + fetched_at。
CREATE TABLE subscription_quota_feed_cache (
  subscription_id TEXT PRIMARY KEY,
  fetched_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL
);

ALTER TABLE subscriptions ADD COLUMN quota_feed_path TEXT;
