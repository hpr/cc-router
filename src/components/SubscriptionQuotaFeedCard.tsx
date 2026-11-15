import { useState } from "react";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Spinner } from "@/components/Spinner";
import { api } from "@/api/tauri";
import { useT } from "@/i18n";
import { errorText } from "@/lib/errorText";
import type { QuotaFeedWindow, SubscriptionDto } from "@/types";

interface Props {
  subscription: SubscriptionDto;
  /** 指针草稿 (页面统一随「保存」提交 SubscriptionPatch.quota_feed_path) */
  pathDraft: string;
  onPathDraftChange: (v: string) => void;
  /** 手动刷新成功后失效订阅查询 (列表徽标 / 路由图跟着换) */
  onChanged?: () => void;
}

/** 与后端 STALE_AFTER_SECS (900s) 同口径: 过期缓存在路由里按无数据处理, 这里给个提示。 */
const STALE_MS = 15 * 60 * 1000;

/** 单窗口一行: 标签 + 余量% / 时间% (有 token 数时一并展示, 大数用紧凑格式)。 */
function WindowRow({ w, t }: { w: QuotaFeedWindow; t: ReturnType<typeof useT>["t"] }) {
  const usage =
    w.used !== undefined && w.limit !== undefined
      ? ` · ${fmtCompact(w.used)}/${fmtCompact(w.limit)}`
      : "";
  return (
    <div className="flex items-baseline justify-between gap-2 text-sm">
      <span className="text-muted-foreground">{w.label}</span>
      <span className="font-mono">
        {w.percent_remaining !== undefined ? `${w.percent_remaining.toFixed(1)}%` : "—"}
        <span className="text-xs text-muted-foreground">
          {" "}
          {t("subscriptionQuotaFeed.timeProgressPrefix")}
          {w.time_percent !== undefined ? `${w.time_percent.toFixed(1)}%` : "—"}
          {usage}
        </span>
      </span>
    </div>
  );
}

function fmtCompact(n: number): string {
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(n);
}

/**
 * 配额感知路由: 订阅编辑页的 feed 指针配置 + 最近一次解析结果展示。
 * 指针草稿由页面持有, 随主「保存」提交; 「立即刷新」拉全局 feed (refresh_quota_feed)。
 */
export function SubscriptionQuotaFeedCard({ subscription, pathDraft, onPathDraftChange, onChanged }: Props) {
  const { t } = useT();
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [updatedCount, setUpdatedCount] = useState<number | null>(null);

  const cache = subscription.quota_feed_cache;
  const stale = cache !== undefined && Date.now() - cache.fetched_at > STALE_MS;
  const invalid =
    pathDraft.trim() !== "" && (!pathDraft.trim().startsWith("/") || pathDraft.trim() === "/");

  async function doRefresh() {
    setRefreshing(true);
    setError(null);
    setUpdatedCount(null);
    try {
      const result = await api.refreshQuotaFeed();
      setUpdatedCount(result.updated);
      onChanged?.();
    } catch (e) {
      setError(errorText(e));
    } finally {
      setRefreshing(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t("subscriptionQuotaFeed.title")}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-xs text-muted-foreground">{t("subscriptionQuotaFeed.intro")}</p>
        <div className="space-y-1.5">
          <Label htmlFor="quota-feed-path">{t("subscriptionQuotaFeed.pathLabel")}</Label>
          <Input
            id="quota-feed-path"
            className="font-mono"
            placeholder="/kimi/windows"
            value={pathDraft}
            onChange={(e) => onPathDraftChange(e.target.value)}
          />
          <p className={`text-xs ${invalid ? "text-destructive" : "text-muted-foreground"}`}>
            {invalid ? t("subscriptionQuotaFeed.pathInvalid") : t("subscriptionQuotaFeed.pathHint")}
          </p>
        </div>

        {cache ? (
          <div className="space-y-1.5 rounded-md border px-3 py-2">
            {cache.windows.map((w) => (
              <WindowRow key={w.label} w={w} t={t} />
            ))}
            <div className="flex items-baseline justify-between gap-2 border-t pt-1.5 text-sm">
              <span className="text-muted-foreground">
                {t("subscriptionQuotaFeed.slackLabel")}
              </span>
              <span className="font-mono">
                {cache.slack !== undefined && cache.slack !== null
                  ? `${cache.slack >= 0 ? "+" : ""}${cache.slack.toFixed(1)}`
                  : "—"}
                <span className="ml-2 text-xs text-muted-foreground">
                  {t("subscriptionQuotaFeed.fetchedAtPrefix")}
                  {new Date(cache.fetched_at).toLocaleString()}
                  {stale ? ` · ${t("subscriptionQuotaFeed.stale")}` : ""}
                </span>
              </span>
            </div>
          </div>
        ) : (
          pathDraft.trim() !== "" && (
            <p className="text-xs text-muted-foreground">{t("subscriptionQuotaFeed.noCache")}</p>
          )
        )}

        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={doRefresh} disabled={refreshing}>
            {refreshing ? <Spinner className="h-4 w-4" /> : <RefreshCw className="h-4 w-4" />}
            {t("subscriptionQuotaFeed.refresh")}
          </Button>
          {updatedCount !== null && (
            <span className="text-xs text-muted-foreground">
              {t("subscriptionQuotaFeed.updatedCount", { n: updatedCount })}
            </span>
          )}
        </div>
        {error && (
          <p className="text-xs text-destructive">
            {t("subscriptionQuotaFeed.errPrefix")}
            {error}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
