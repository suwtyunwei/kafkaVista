<template>
  <div class="audit-page">
    <section class="audit-hero">
      <div>
        <p class="eyebrow">Audit Trail</p>
        <h1>{{ tr('操作审计', 'Audit Logs') }}</h1>
        <p>{{ auditLimited ? tr('社区版仅保留最近 1 天审计日志。', 'Community edition keeps only the last 1 day.') : tr('查看平台操作记录，支持按时间范围筛选和分页查看。', 'View platform operation records with time range filters and pagination.') }}</p>
      </div>
      <button class="primary" :disabled="loading" @click="reloadFirstPage">{{ loading ? tr('刷新中...', 'Refreshing...') : tr('查询', 'Search') }}</button>
    </section>

    <section class="filter-bar">
      <div class="field"><label>{{ tr('开始时间', 'Start Time') }}</label><input v-model="filters.start_time" type="datetime-local" /></div>
      <div class="field"><label>{{ tr('结束时间', 'End Time') }}</label><input v-model="filters.end_time" type="datetime-local" /></div>
      <div class="field"><label>{{ tr('每页条数', 'Page Size') }}</label><select v-model.number="pageSize" @change="reloadFirstPage"><option :value="10">10</option><option :value="20">20</option><option :value="50">50</option><option :value="100">100</option></select></div>
      <div class="filter-actions"><button class="ghost" @click="resetFilters">{{ tr('重置', 'Reset') }}</button><button class="primary" :disabled="loading" @click="reloadFirstPage">{{ tr('应用筛选', 'Apply') }}</button></div>
    </section>

    <section class="audit-summary">
      <div><span>{{ tr('总记录', 'Total') }}</span><strong>{{ total }}</strong></div>
      <div><span>{{ tr('当前页', 'Page') }}</span><strong>{{ page }} / {{ totalPages }}</strong></div>
      <div><span>{{ tr('保留策略', 'Retention') }}</span><strong>{{ auditLimited ? tr('1 天', '1 day') : tr('永久', 'Permanent') }}</strong></div>
    </section>

    <section class="audit-panel">
      <div class="panel-title">
        <div>
          <h2>{{ tr('审计记录', 'Audit Records') }}</h2>
          <p class="hint">{{ tr('每条只显示关键信息；需要排查时可展开原始详情。', 'Only key information is shown; expand raw detail when troubleshooting.') }}</p>
        </div>
      </div>

      <div v-if="loading" class="empty">{{ tr('正在加载审计日志...', 'Loading audit logs...') }}</div>
      <div v-else-if="auditLogs.length === 0" class="empty">{{ tr('暂无审计日志', 'No audit logs') }}</div>
      <div v-else class="audit-list">
        <article v-for="row in auditLogs" :key="row.id" class="audit-row">
          <div class="audit-severity" :class="actionTone(row.action)"></div>
          <div class="audit-main">
            <div class="audit-line">
              <strong>{{ actionLabel(row) }}</strong>
              <span>{{ tr('操作人', 'Operator') }}：{{ row.username || '-' }}</span>
              <time>{{ formatDateTime(row.created_at) }}</time>
            </div>
            <div class="audit-target"><span>{{ tr('对象', 'Target') }}</span>{{ conciseTarget(row) }}</div>
            <details v-if="row.detail" class="audit-detail">
              <summary>{{ tr('详情', 'Detail') }}</summary>
              <pre>{{ prettyDetail(row.detail) }}</pre>
            </details>
          </div>
        </article>
      </div>

      <div class="pagination-bar">
        <span>{{ tr(`共 ${total} 条`, `${total} total`) }}</span>
        <button class="ghost small" :disabled="page <= 1 || loading" @click="goPage(page - 1)">{{ tr('上一页', 'Previous') }}</button>
        <span>{{ page }} / {{ totalPages }}</span>
        <button class="ghost small" :disabled="page >= totalPages || loading" @click="goPage(page + 1)">{{ tr('下一页', 'Next') }}</button>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { listKafkaAuditLogPage } from '../api'
import { language } from '../i18n'

const tr = (zh: string, en: string) => language.value === 'en-US' ? en : zh
const auditLogs = ref<any[]>([])
const auditLimited = ref(false)
const loading = ref(false)
const page = ref(1)
const pageSize = ref(20)
const total = ref(0)
const filters = ref({ start_time: '', end_time: '' })
const totalPages = computed(() => Math.max(1, Math.ceil(total.value / pageSize.value)))

const loadAuditLogs = async () => {
  loading.value = true
  try {
    const data = await listKafkaAuditLogPage({ page: page.value, page_size: pageSize.value, start_time: toApiTime(filters.value.start_time), end_time: toApiTime(filters.value.end_time) })
    auditLogs.value = data.items || []
    auditLimited.value = !!data.limited
    total.value = data.total || 0
  } finally {
    loading.value = false
  }
}

const reloadFirstPage = () => { page.value = 1; loadAuditLogs() }
const goPage = (next: number) => { page.value = Math.min(Math.max(1, next), totalPages.value); loadAuditLogs() }
const resetFilters = () => { filters.value = { start_time: '', end_time: '' }; reloadFirstPage() }
const toApiTime = (value: string) => value ? new Date(value).toISOString() : undefined

const parseDetail = (value: string) => {
  if (!value) return {}
  try { return JSON.parse(value) } catch { return { raw: value } }
}

const detail = (row: any) => parseDetail(row.detail || '') as any
const formatDateTime = (value?: string) => value ? new Date(value).toLocaleString() : '-'
const prettyDetail = (value: string) => JSON.stringify(parseDetail(value), null, 2)

const actionMap: Record<string, [string, string]> = {
  cluster_create: ['创建集群', 'Created cluster'],
  cluster_update: ['更新集群', 'Updated cluster'],
  cluster_delete: ['删除集群配置', 'Deleted cluster config'],
  topic_create: ['创建 Topic', 'Created topic'],
  topic_delete: ['删除 Topic', 'Deleted topic'],
  topic_config_update: ['修改 Topic 配置', 'Updated topic config'],
  message_send: ['发送消息', 'Produced messages'],
  message_delete: ['删除消息', 'Deleted records'],
  group_create: ['创建 Group', 'Created group'],
  group_delete: ['删除 Group', 'Deleted group'],
  permission_update: ['更新授权', 'Updated permissions'],
  migration_start: ['启动平滑迁移', 'Started migration'],
  migration_stop: ['取消迁移同步', 'Stopped migration sync'],
  settings_update: ['更新系统设置', 'Updated settings'],
  notification_test: ['测试通知渠道', 'Tested notification channel'],
  alerting_test: ['测试监控告警', 'Tested alerting'],
  ldap_test: ['测试 LDAP 连接', 'Tested LDAP connection'],
  ldap_sync_users: ['同步 LDAP 用户', 'Synced LDAP users'],
  user_create: ['创建用户', 'Created user'],
  user_update: ['更新用户', 'Updated user'],
  user_delete: ['删除用户', 'Deleted user'],
  role_create: ['创建角色', 'Created role'],
  role_delete: ['删除角色', 'Deleted role'],
  topic_config_delete: ['删除 Topic 配置', 'Deleted topic config'],
}

const legacyOperationLabel = (row: any) => {
  const path = String(detail(row).path || row.target || '')
  if (path.includes('/migrations')) return tr('平滑迁移', 'Migration')
  if (path.includes('/topics')) return tr('Topic 操作', 'Topic operation')
  if (path.includes('/messages')) return tr('消息操作', 'Message operation')
  if (path.includes('/groups')) return tr('Consumer Group 操作', 'Consumer Group operation')
  if (path.includes('/permissions')) return tr('授权操作', 'Permission operation')
  if (path.includes('/settings')) return tr('系统设置', 'Settings')
  if (path.includes('/users')) return tr('用户操作', 'User operation')
  if (path.includes('/roles')) return tr('角色操作', 'Role operation')
  return tr('平台操作', 'Platform operation')
}
const actionLabel = (row: any) => row.action === 'operation' ? legacyOperationLabel(row) : (actionMap[row.action] ? tr(actionMap[row.action][0], actionMap[row.action][1]) : row.action)
const actionTone = (action: string) => action.includes('delete') ? 'danger' : action.includes('create') || action.includes('send') ? 'success' : action.includes('update') || action.includes('permission') ? 'warning' : 'info'
const conciseTarget = (row: any) => row.target || detail(row).path || row.cluster_id || tr('系统对象', 'System resource')

onMounted(loadAuditLogs)
</script>

<style scoped>
.audit-page { padding: 24px; display: grid; gap: 16px; }
.audit-hero { display: flex; justify-content: space-between; align-items: flex-start; gap: 18px; padding: 22px; border: 1px solid var(--border-color); border-radius: 22px; background: radial-gradient(circle at top right, rgba(34,211,238,.14), transparent 38%), var(--panel-bg); }
.eyebrow { color: var(--accent-secondary); font-size: 12px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; margin-bottom: 6px; }
.audit-hero h1 { margin: 0 0 8px; font-size: 32px; letter-spacing: -.03em; }
.audit-hero p, .hint { color: var(--text-muted); font-size: 13px; }
.primary, .ghost { border-radius: 10px; padding: 9px 13px; cursor: pointer; font: inherit; }
.primary { background: var(--accent-primary); color: #fff; border: 0; }
.ghost { background: transparent; color: var(--text-secondary); border: 1px solid var(--border-light); }
.small { padding: 6px 10px; font-size: 12px; }
button:disabled { opacity: .55; cursor: not-allowed; }
.filter-bar { display: grid; grid-template-columns: minmax(190px, 1fr) minmax(190px, 1fr) 130px auto; gap: 12px; align-items: end; padding: 14px; border: 1px solid var(--border-color); border-radius: 18px; background: var(--bg-tertiary); }
.field { display: grid; gap: 6px; }
.field label { color: var(--text-muted); font-size: 12px; }
input, select { width: 100%; padding: 9px 11px; border: 1px solid var(--border-light); border-radius: 10px; background: var(--bg-input); color: var(--text-primary); }
.filter-actions { display: flex; gap: 8px; }
.audit-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
.audit-summary div { padding: 14px; border: 1px solid var(--border-color); border-radius: 16px; background: var(--bg-tertiary); }
.audit-summary span { display: block; color: var(--text-muted); font-size: 12px; margin-bottom: 4px; }
.audit-summary strong { font-size: 20px; color: var(--text-primary); }
.audit-panel { border: 1px solid var(--border-color); border-radius: 20px; background: var(--panel-bg); padding: 16px; }
.panel-title { margin-bottom: 12px; }
.panel-title h2 { margin: 0 0 3px; }
.empty { color: var(--text-muted); text-align: center; padding: 28px; }
.audit-list { display: grid; gap: 8px; }
.audit-row { position: relative; display: grid; grid-template-columns: minmax(0, 1fr); gap: 8px; align-items: start; padding: 12px 14px 12px 18px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); }
.audit-severity { position: absolute; left: 0; top: 12px; bottom: 12px; width: 4px; border-radius: 999px; }
.audit-severity.danger { background: var(--accent-danger); }
.audit-severity.success { background: var(--accent-success); }
.audit-severity.warning { background: var(--accent-warning); }
.audit-severity.info { background: var(--accent-secondary); }
.audit-main { min-width: 0; display: grid; gap: 4px; }
.audit-line { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.audit-line strong { color: var(--text-primary); min-width: 110px; }
.audit-line span, .audit-line time { color: var(--text-muted); font-size: 12px; }
.audit-target { display: flex; gap: 8px; color: var(--text-secondary); font-size: 13px; overflow-wrap: anywhere; }
.audit-target span { flex: 0 0 auto; color: var(--text-muted); }
.audit-detail summary { cursor: pointer; color: var(--text-muted); font-size: 12px; width: fit-content; }
.audit-detail pre { margin-top: 6px; max-height: 220px; overflow: auto; color: var(--text-secondary); font-size: 12px; line-height: 1.55; white-space: pre-wrap; overflow-wrap: anywhere; }
.pagination-bar { display: flex; flex-wrap: wrap; justify-content: flex-end; align-items: center; gap: 10px; padding-top: 12px; color: var(--text-muted); font-size: 12px; }
@media (max-width: 900px) { .audit-page { padding: 16px; } .audit-hero, .filter-bar { grid-template-columns: 1fr; flex-direction: column; } .filter-bar, .audit-summary { grid-template-columns: 1fr; } .filter-actions { justify-content: flex-end; } }
</style>
