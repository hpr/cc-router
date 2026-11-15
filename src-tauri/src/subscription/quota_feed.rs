//! 配额感知路由 (quota-aware routing): 从用户自备的聚合 feed 拉取各订阅的窗口余量,
//! 计算「余量进度 − 时间进度」(slack), 供调度器在 quota_aware 模式下排序候选订阅。
//!
//! 与 `balance_discovery` 的分工: 那是 provider yaml 声明、PAYG 账户余额、纯展示;
//! 本模块是用户全局配置一个 feed URL (settings.json `quota_feed_url`), 每订阅配一个
//! JSON 指针 (`subscriptions.quota_feed_path`), 结果参与调度排序。
//!
//! Feed 约定 (对齐用户聚合器 https://…/json 的自然形态):
//! - 顶层对象; 每订阅一个键, 值里含窗口对象 (窗口 = 带 `percentRemaining` 的 object)。
//! - 窗口字段: `used` / `limit` / `remaining` / `percentUsed` / `percentRemaining` /
//!   `timePercent` / `resetAt`, 均可缺省 (null 窗口整个跳过)。
//! - `timePercent` = 窗口已过时间百分比 (feed 侧算好), slack 依赖它。
//!
//! 排序指标: `slack = percentRemaining − (100 − timePercent)`, 即「比匀速消耗多剩多少」。
//! 月初刚开窗的订阅 slack ≈ 0 而不是 +100, 不会被误判为最充裕 (设计讨论确认的语义)。
//! 订阅分数 = 各窗口 slack 的最小值 (绑定窗口决定)。
//!
//! 失效策略: 只重排不跳过。feed 拉不到时沿用旧缓存; 缓存超过 `STALE_AFTER` 视为无数据,
//! 该订阅回退到配置顺序, 不改变 `is_dispatchable` (设计确认: 不做抢先跳过)。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::state::AppState;
use crate::subscription::model::SubscriptionRuntime;

/// 后台轮询间隔。首 tick 立即触发 (tokio::time::interval 语义), 启动即拉一次。
const POLL_INTERVAL_SECS: u64 = 300;

/// 缓存过期阈值 (3 个轮询周期没刷新成功 → 视为无数据, 排序回退配置顺序)。
pub const STALE_AFTER_SECS: i64 = 900;

/// 单个配额窗口 (5h / 周 / 月 / …), feed 侧字段名转 snake_case。
/// 数值统一 f64: token 数 (mimo 1.1e10) 与百分比 (kimi 63.16) 都装得下且不参与精确运算。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaFeedWindow {
    /// 窗口标签 (feed 里的键名, 如 fiveHour / week / month / mcpMonth), 仅展示用。
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent_remaining: Option<f64>,
    /// 窗口已过时间百分比; slack 计算必需, 缺省的窗口不参与排序分。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<DateTime<Utc>>,
}

/// 一条订阅的 feed 解析结果。持久化在 `subscription_quota_feed_cache`,
/// 启动时经 store::load_quota_feed_cache 装填进 runtime。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaFeedSnapshot {
    pub fetched_at: DateTime<Utc>,
    pub windows: Vec<QuotaFeedWindow>,
}

impl QuotaFeedSnapshot {
    /// 订阅排序分: 各可算窗口 slack 的最小值 (None = 无可算窗口, 排序时视为无数据)。
    pub fn slack(&self) -> Option<f64> {
        self.windows
            .iter()
            .filter_map(window_slack)
            .fold(None, |acc, s| Some(match acc {
                Some(a) if a < s => a,
                _ => s,
            }))
    }

    /// 排序分, 但超过 `STALE_AFTER_SECS` 视为无数据 (feed 断更后排序回退配置顺序)。
    pub fn slack_if_fresh(&self, now: DateTime<Utc>) -> Option<f64> {
        let age = now.signed_duration_since(self.fetched_at).num_seconds();
        if age > STALE_AFTER_SECS {
            return None;
        }
        self.slack()
    }
}

/// 单窗口 slack: `percentRemaining − (100 − timePercent)`。两个百分比都在才算。
pub fn window_slack(w: &QuotaFeedWindow) -> Option<f64> {
    Some(w.percent_remaining? - (100.0 - w.time_percent?))
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
}

fn parse_window(label: &str, obj: &serde_json::Map<String, Value>) -> QuotaFeedWindow {
    QuotaFeedWindow {
        label: label.to_string(),
        used: obj.get("used").and_then(num),
        limit: obj.get("limit").and_then(num),
        remaining: obj.get("remaining").and_then(num),
        percent_remaining: obj.get("percentRemaining").and_then(num),
        time_percent: obj.get("timePercent").and_then(num),
        reset_at: obj
            .get("resetAt")
            .and_then(|v| v.as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&Utc)),
    }
}

fn is_window_object(obj: &serde_json::Map<String, Value>) -> bool {
    obj.get("percentRemaining").and_then(num).is_some()
        || (obj.get("remaining").and_then(num).is_some()
            && obj.get("limit").and_then(num).is_some())
}

/// 解析 feed 载荷里 `path` 指向的窗口集合。
///
/// 两种形态都支持 (对齐真实 feed: kimi/glm 是 `windows` 包一层, mimo 的窗口直接挂在对象上):
/// - 指向「窗口容器」: 迭代其键值, 值为窗口 object 的收下 (字符串/数字/null 自然跳过),
///   标签用键名 (fiveHour / month / …)。
/// - 直接指向单个窗口 object: 收一条, 标签用指针最后一段。
pub fn parse_windows(path: &str, root: &Value) -> AppResult<Vec<QuotaFeedWindow>> {
    let pointer = normalize_pointer(path)?;
    let target = root
        .pointer(&pointer)
        .ok_or_else(|| AppError::BadRequest(format!("配额 feed 路径 {path} 在响应里不存在")))?;
    let obj = target
        .as_object()
        .ok_or_else(|| AppError::BadRequest(format!("配额 feed 路径 {path} 不是对象")))?;

    if is_window_object(obj) {
        let label = pointer
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("window");
        return Ok(vec![parse_window(label, obj)]);
    }

    let mut windows = Vec::new();
    for (key, value) in obj {
        // null 窗口 (如 glm 的 mcpMonth) 与非对象成员 (fetchedAt / plan 等元数据) 跳过
        let Some(inner) = value.as_object() else { continue };
        if !is_window_object(inner) { continue; }
        windows.push(parse_window(key, inner));
    }
    if windows.is_empty() {
        return Err(AppError::BadRequest(format!(
            "配额 feed 路径 {path} 下没有可识别的配额窗口"
        )));
    }
    Ok(windows)
}

/// 指针规范化: 去空白、补前导 `/` (serde_json::pointer 的要求)。
fn normalize_pointer(path: &str) -> AppResult<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest("配额 feed 路径不能为空".into()));
    }
    if trimmed == "/" {
        return Err(AppError::BadRequest(
            "配额 feed 路径不能指向整个根对象".into(),
        ));
    }
    Ok(if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    })
}

/// 拉一次 feed 并把结果写进每条配了 `quota_feed_path` 的订阅 (内存 + DB 缓存)。
/// 返回更新的订阅数。单订阅解析失败只 warn 跳过 (feed 结构问题不该拖垮整体)。
pub async fn fetch_and_apply(
    db: &SqlitePool,
    client: &reqwest::Client,
    url: &str,
    subs: &HashMap<Uuid, Arc<RwLock<SubscriptionRuntime>>>,
    now: DateTime<Utc>,
) -> AppResult<usize> {
    let root: Value = client
        .get(url)
        .send()
        .await
        .and_then(|resp| resp.error_for_status())?
        .json()
        .await?;

    // 先在读锁里收齐 (id, path), 避免 fetch/parse 期间持有任何订阅锁
    let mut targets: Vec<(Uuid, String)> = Vec::new();
    for rt in subs.values() {
        let guard = rt.read().await;
        if let Some(p) = guard
            .row
            .quota_feed_path
            .clone()
            .filter(|p| !p.trim().is_empty())
        {
            targets.push((guard.row.id, p));
        }
    }

    let mut updated = 0usize;
    for (id, path) in targets {
        let snapshot = match parse_windows(&path, &root) {
            Ok(windows) => QuotaFeedSnapshot { fetched_at: now, windows },
            Err(e) => {
                warn!(subscription_id = %id, path = %path, error = %e, "配额 feed 窗口解析失败, 该订阅沿用旧缓存");
                continue;
            }
        };
        if let Some(rt) = subs.get(&id) {
            {
                let mut guard = rt.write().await;
                guard.quota_feed_cache = Some(snapshot.clone());
            }
            if let Err(e) = crate::subscription::store::save_quota_feed_cache(db, &id, &snapshot)
                .await
            {
                warn!(subscription_id = %id, error = %e, "配额 feed 缓存落库失败 (内存已更新)");
            }
        }
        updated += 1;
    }
    Ok(updated)
}

/// 后台轮询器, 范式参考 `recheck_worker::run`。URL 未配置时空转 (不报错)。
pub async fn run(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
    loop {
        ticker.tick().await;
        let url = state.settings.read().await.quota_feed_url.trim().to_string();
        if url.is_empty() {
            continue;
        }
        let subs = state.subscriptions.read().await.clone();
        match fetch_and_apply(&state.db, &state.probe_client, &url, &subs, Utc::now()).await {
            Ok(n) => debug!(n, url = %url, "配额 feed 刷新完成"),
            Err(e) => {
                warn!(error = %e, url = %url, "配额 feed 拉取失败, 沿用旧缓存 (过期后排序回退配置顺序)")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 对齐真实聚合器形态的样例: kimi/glm 带 windows 包装, mimo 窗口直接挂对象上。
    fn sample_feed() -> Value {
        json!({
            "fetchedAt": "2026-11-15T02:37:53.272Z",
            "kimi": {
                "fetchedAt": "2026-11-15T02:37:43.132Z",
                "windows": {
                    "fiveHour": { "used": 0, "limit": 100, "remaining": 100,
                        "percentUsed": 0, "percentRemaining": 100,
                        "resetAt": "2026-11-15T06:27:09Z", "timePercent": 23.5192 },
                    "week": { "used": 0, "limit": 100, "remaining": 100,
                        "percentUsed": 0, "percentRemaining": 100,
                        "resetAt": "2026-11-21T14:27:09Z", "timePercent": 7.2476 },
                    "month": { "used": 63.16, "limit": 100, "remaining": 36.84,
                        "percentUsed": 63.16, "percentRemaining": 36.84,
                        "resetAt": "2026-11-24T14:27:19Z", "timePercent": 69.3781 }
                },
                "monthlyMessage": null
            },
            "glm": {
                "fetchedAt": "2026-11-15T02:37:42.068Z",
                "plan": "lite",
                "windows": {
                    "fiveHour": { "used": 171, "limit": 2000, "remaining": 1828,
                        "percentUsed": 8.55, "percentRemaining": 91.45,
                        "resetAt": "2026-11-15T07:00:42.019Z", "timePercent": 12.3336 },
                    "week": { "used": 1543, "limit": 10000, "remaining": 8456,
                        "percentUsed": 15.43, "percentRemaining": 84.57,
                        "resetAt": "2026-11-19T14:12:57.983Z", "timePercent": 35.9597 },
                    "mcpMonth": null
                },
                "message": null
            },
            "mimo": {
                "fetchedAt": "2026-11-15T02:37:42.552Z",
                "plan": "Standard",
                "month": { "used": 8787978068, "limit": 11000000000, "remaining": 2212021932,
                    "percentUsed": 79.89, "percentRemaining": 20.11,
                    "resetAt": "2026-11-30T16:00:00Z", "timePercent": 45.6 }
            }
        })
    }

    #[test]
    fn parses_nested_windows_container() {
        let windows = parse_windows("/kimi/windows", &sample_feed()).unwrap();
        let labels: Vec<&str> = windows.iter().map(|w| w.label.as_str()).collect();
        assert_eq!(labels, vec!["fiveHour", "week", "month"]);
        let month = &windows[2];
        assert_eq!(month.percent_remaining, Some(36.84));
        assert_eq!(month.time_percent, Some(69.3781));
        assert!(month.reset_at.is_some());
    }

    #[test]
    fn null_windows_and_metadata_keys_skipped() {
        let windows = parse_windows("/glm/windows", &sample_feed()).unwrap();
        let labels: Vec<&str> = windows.iter().map(|w| w.label.as_str()).collect();
        assert_eq!(labels, vec!["fiveHour", "week"], "mcpMonth=null 应跳过");
    }

    #[test]
    fn parses_flat_object_skipping_metadata() {
        let windows = parse_windows("/mimo", &sample_feed()).unwrap();
        assert_eq!(windows.len(), 1, "fetchedAt/plan 是字符串, 只有 month 是窗口");
        assert_eq!(windows[0].label, "month");
        assert_eq!(windows[0].limit, Some(11_000_000_000.0));
    }

    #[test]
    fn direct_window_pointer_yields_single_window() {
        let windows = parse_windows("/mimo/month", &sample_feed()).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].label, "month");
    }

    #[test]
    fn missing_path_and_empty_container_error() {
        assert!(parse_windows("/nope", &sample_feed()).is_err());
        assert!(parse_windows("", &sample_feed()).is_err());
        assert!(parse_windows("/", &sample_feed()).is_err());
        // 指到纯字符串成员: 不是对象 → 报错
        assert!(parse_windows("/glm/plan", &sample_feed()).is_err());
        // 指到没有窗口的对象 → 报错
        assert!(parse_windows("/kimi/windows/fiveHour/used", &sample_feed()).is_err());
    }

    /// slack = percentRemaining − (100 − timePercent), 订阅分取各窗口最小值。
    /// 真实 feed 数值: GLM 5h 91.45−87.67=+3.78 (周 84.57−64.04=+20.5) → +3.78;
    /// Kimi 月 36.84−30.62=+6.22 → +6.22; MiMo 月 20.11−54.4=−34.3。
    #[test]
    fn slack_takes_min_across_windows() {
        let feed = sample_feed();
        let glm = QuotaFeedSnapshot {
            fetched_at: Utc::now(),
            windows: parse_windows("/glm/windows", &feed).unwrap(),
        };
        let kimi = QuotaFeedSnapshot {
            fetched_at: Utc::now(),
            windows: parse_windows("/kimi/windows", &feed).unwrap(),
        };
        let mimo = QuotaFeedSnapshot {
            fetched_at: Utc::now(),
            windows: parse_windows("/mimo", &feed).unwrap(),
        };
        let g = glm.slack().unwrap();
        let k = kimi.slack().unwrap();
        let m = mimo.slack().unwrap();
        assert!((g - 3.783).abs() < 0.01, "glm slack {g}");
        assert!((k - 6.222).abs() < 0.01, "kimi slack {k}");
        assert!((m - (-34.29)).abs() < 0.01, "mimo slack {m}");
        // 与设计讨论一致的偏序: GLM > Kimi > MiMo
        assert!(g < k && k > m);
    }

    /// 设计确认的关键用例: 月初刚开窗 (余量≈100%, 时间过了 1%) 的纯月度订阅
    /// slack ≈ −1 而不是 +100, 不会永远排在最前。
    #[test]
    fn fresh_month_window_scores_near_zero() {
        let snap = QuotaFeedSnapshot {
            fetched_at: Utc::now(),
            windows: vec![QuotaFeedWindow {
                label: "month".into(),
                percent_remaining: Some(99.0),
                time_percent: Some(1.0),
                ..Default::default()
            }],
        };
        let slack = snap.slack().unwrap();
        assert!((slack - 0.0).abs() < 0.001, "99 − (100 − 1) = 0, got {slack}");
    }

    #[test]
    fn window_without_time_percent_excluded_from_slack() {
        let snap = QuotaFeedSnapshot {
            fetched_at: Utc::now(),
            windows: vec![
                QuotaFeedWindow {
                    label: "week".into(),
                    percent_remaining: Some(80.0),
                    time_percent: None, // 缺时间进度: 展示但不计分
                    ..Default::default()
                },
                QuotaFeedWindow {
                    label: "month".into(),
                    percent_remaining: Some(50.0),
                    time_percent: Some(40.0),
                    ..Default::default()
                },
            ],
        };
        assert_eq!(snap.slack(), Some(-10.0), "只有 month 窗口参与计算");
    }

    #[test]
    fn snapshot_serializes_for_db_roundtrip() {
        let snap = QuotaFeedSnapshot {
            fetched_at: Utc::now(),
            windows: parse_windows("/kimi/windows", &sample_feed()).unwrap(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: QuotaFeedSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.windows.len(), 3);
        assert_eq!(back.windows[0].label, "fiveHour");
    }
}
