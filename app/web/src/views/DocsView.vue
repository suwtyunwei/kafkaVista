<template>
  <div class="docs-page">
    <section class="hero">
      <div>
        <p class="eyebrow">Dynamic Docs</p>
        <h1>API Docs</h1>
        <p>{{ tr('从后端 Gin 路由实时生成，部署后自动反映最新接口。', 'Generated from backend Gin routes and always reflects deployed APIs.') }}</p>
      </div>
      <div class="hero-actions">
        <button class="ghost" @click="copyOpenApi">{{ copyText }}</button>
        <button class="primary" @click="loadDocs">{{ tr('刷新', 'Refresh') }}</button>
      </div>
    </section>

    <section class="panel toolbar">
      <input v-model="keyword" :placeholder="tr('搜索 path / method / group', 'Search path / method / group')" />
      <select v-model="methodFilter"><option value="">All Methods</option><option v-for="method in methods" :key="method" :value="method">{{ method }}</option></select>
      <select v-model="groupFilter"><option value="">All Groups</option><option v-for="group in groups" :key="group" :value="group">{{ group }}</option></select>
    </section>

    <section class="panel auth-guide">
      <div>
        <strong>{{ tr('接口调试会自动携带当前登录 Token', 'API testing uses your current login token automatically') }}</strong>
        <p>{{ token ? tr(`当前登录用户：${user?.display_name || user?.username || 'unknown'}。Bearer Token 已就绪。`, `Current user: ${user?.display_name || user?.username || 'unknown'}. Bearer token is ready.`) : tr('请先登录系统，再回到 API Docs 直接请求需要认证的接口。public 接口无需 Token。', 'Please log in first, then return to API Docs to call authenticated APIs. Public APIs do not require a token.') }}</p>
      </div>
      <span :class="['token-state', token ? 'ready' : 'missing']">{{ token ? tr('Token 已验证', 'Token Ready') : tr('未登录', 'Not Logged In') }}</span>
    </section>

    <section class="panel docs-list">
      <div class="docs-head"><span>Method</span><span>Path</span><span>Group</span><span>Auth</span><span>Summary</span><span>Action</span></div>
      <div v-for="item in filteredItems" :key="`${item.method}-${item.path}`" class="docs-item">
        <div class="docs-row">
          <span :class="['method', item.method.toLowerCase()]">{{ item.method }}</span>
          <code>{{ item.path }}</code>
          <span>{{ item.group }}</span>
          <span :class="['auth', item.auth]">{{ item.auth }}</span>
          <span>{{ item.summary }}</span>
          <button class="ghost mini" @click="selectEndpoint(item)">{{ selectedKey === endpointKey(item) ? tr('收起', 'Collapse') : tr('调试', 'Try') }}</button>
        </div>
        <div v-if="selectedKey === endpointKey(item)" class="try-panel">
          <div class="try-grid">
            <label>{{ tr('Path 参数 JSON', 'Path Params JSON') }}<textarea v-model="pathParamsText" :placeholder="pathParamPlaceholder(item)"></textarea></label>
            <label>{{ tr('Query 参数 JSON', 'Query Params JSON') }}<textarea v-model="queryParamsText" placeholder='{"page":1,"page_size":20}'></textarea></label>
            <label v-if="bodyAllowed(item.method)">{{ tr('Body JSON', 'Body JSON') }}<textarea v-model="bodyText" placeholder='{"name":"demo"}'></textarea></label>
          </div>
          <div class="request-preview"><span>{{ tr('请求地址', 'Request URL') }}</span><code>{{ requestPreview(item) }}</code></div>
          <div class="try-actions">
            <button class="primary" :disabled="sendingKey === endpointKey(item) || (!token && item.auth !== 'public')" @click="sendApiRequest(item)">{{ sendingKey === endpointKey(item) ? tr('请求中...', 'Sending...') : tr('发送请求', 'Send Request') }}</button>
            <button class="ghost" @click="clearTryState">{{ tr('清空', 'Clear') }}</button>
          </div>
          <pre v-if="responseText" :class="['response-box', responseOk ? 'ok' : 'error']">{{ responseText }}</pre>
        </div>
      </div>
      <div v-if="!filteredItems.length" class="empty">{{ tr('暂无接口', 'No APIs') }}</div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { getApiDocs } from '../api'
import { getToken, getUser } from '../auth'
import { tr } from '../i18n'

const docs = ref<any>({ items: [], openapi: {} })
const keyword = ref('')
const methodFilter = ref('')
const groupFilter = ref('')
const copyText = ref(tr('复制 OpenAPI', 'Copy OpenAPI'))
const selectedKey = ref('')
const sendingKey = ref('')
const pathParamsText = ref('{}')
const queryParamsText = ref('{}')
const bodyText = ref('')
const responseText = ref('')
const responseOk = ref(false)
const token = ref(getToken())
const user = ref(getUser())

const methods = computed<string[]>(() => [...new Set<string>(docs.value.items.map((item: any) => String(item.method || '')))].filter(Boolean).sort())
const groups = computed<string[]>(() => [...new Set<string>(docs.value.items.map((item: any) => String(item.group || '')))].filter(Boolean).sort())
const filteredItems = computed(() => {
  const q = keyword.value.trim().toLowerCase()
  return docs.value.items.filter((item: any) => {
    if (methodFilter.value && item.method !== methodFilter.value) return false
    if (groupFilter.value && item.group !== groupFilter.value) return false
    if (!q) return true
    return `${item.method} ${item.path} ${item.group} ${item.auth} ${item.summary}`.toLowerCase().includes(q)
  })
})

const loadDocs = async () => { docs.value = await getApiDocs() }
const endpointKey = (item: any) => `${item.method}-${item.path}`
const bodyAllowed = (method: string) => ['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)
const parseJsonInput = (text: string, fallback: any = {}) => {
  const trimmed = text.trim()
  if (!trimmed) return fallback
  return JSON.parse(trimmed)
}
const pathParamNames = (path: string) => [...path.matchAll(/:([A-Za-z0-9_]+)/g)].map(match => match[1])
const pathParamPlaceholder = (item: any) => {
  const names = pathParamNames(item.path)
  if (!names.length) return '{}'
  return JSON.stringify(Object.fromEntries(names.map(name => [name, name === 'cluster' ? 'cluster-id' : name])), null, 2)
}
const buildRequest = (item: any) => {
  const pathParams = parseJsonInput(pathParamsText.value, {})
  const queryParams = parseJsonInput(queryParamsText.value, {})
  let path = item.path.replace(/:([A-Za-z0-9_]+)/g, (_: string, name: string) => encodeURIComponent(String(pathParams[name] ?? `:${name}`)))
  const search = new URLSearchParams()
  Object.entries(queryParams).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') search.set(key, String(value))
  })
  if (search.toString()) path += `?${search.toString()}`
  return path
}
const requestPreview = (item: any) => {
  try { return buildRequest(item) } catch { return item.path }
}
const selectEndpoint = (item: any) => {
  const key = endpointKey(item)
  selectedKey.value = selectedKey.value === key ? '' : key
  pathParamsText.value = pathParamPlaceholder(item)
  queryParamsText.value = '{}'
  bodyText.value = bodyAllowed(item.method) ? '{}' : ''
  responseText.value = ''
}
const clearTryState = () => {
  pathParamsText.value = '{}'
  queryParamsText.value = '{}'
  bodyText.value = ''
  responseText.value = ''
}
const sendApiRequest = async (item: any) => {
  const key = endpointKey(item)
  sendingKey.value = key
  responseText.value = ''
  try {
    const headers: Record<string, string> = {}
    if (token.value) headers.Authorization = `Bearer ${token.value}`
    let body: string | undefined
    if (bodyAllowed(item.method) && bodyText.value.trim()) {
      headers['Content-Type'] = 'application/json'
      body = JSON.stringify(parseJsonInput(bodyText.value, {}))
    }
    const resp = await fetch(buildRequest(item), { method: item.method, headers, body })
    const text = await resp.text()
    responseOk.value = resp.ok
    try {
      responseText.value = JSON.stringify(JSON.parse(text), null, 2)
    } catch {
      responseText.value = text || `${resp.status} ${resp.statusText}`
    }
  } catch (err: any) {
    responseOk.value = false
    responseText.value = err?.message || String(err)
  } finally {
    sendingKey.value = ''
  }
}
const copyOpenApi = async () => {
  const text = JSON.stringify(docs.value.openapi || {}, null, 2)
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text)
    } else {
      const textarea = document.createElement('textarea')
      textarea.value = text
      textarea.setAttribute('readonly', 'true')
      textarea.style.position = 'fixed'
      textarea.style.opacity = '0'
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
    }
    copyText.value = tr('已复制', 'Copied')
  } catch {
    copyText.value = tr('复制失败', 'Copy Failed')
  } finally {
    window.setTimeout(() => { copyText.value = tr('复制 OpenAPI', 'Copy OpenAPI') }, 1600)
  }
}

onMounted(loadDocs)
</script>

<style scoped>
.docs-page { padding: 24px 20px; width: 100%; }
.hero { display: flex; justify-content: space-between; gap: 20px; align-items: flex-end; padding: 30px; border: 1px solid var(--border-color); border-radius: 26px; background: radial-gradient(circle at 12% 0, rgba(34,211,238,.25), transparent 35%), var(--bg-secondary); margin-bottom: 20px; }
.eyebrow { color: var(--accent-secondary); font-size: 12px; letter-spacing: .14em; text-transform: uppercase; margin-bottom: 8px; }
h1 { font-size: 32px; margin-bottom: 8px; } .hero p { color: var(--text-muted); font-size: 13px; }
.hero-actions { display: flex; gap: 8px; flex-wrap: wrap; }
.panel { border: 1px solid var(--border-color); border-radius: 18px; background: var(--bg-secondary); padding: 18px; }
.toolbar { display: grid; grid-template-columns: minmax(220px, 1fr) 180px 220px; gap: 10px; margin-bottom: 16px; }
.auth-guide { display: flex; justify-content: space-between; align-items: center; gap: 14px; margin-bottom: 16px; }
.auth-guide p { margin: 6px 0 0; color: var(--text-muted); font-size: 13px; }
.token-state { border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: 800; white-space: nowrap; }
.token-state.ready { color: var(--accent-success); background: rgba(52,211,153,.1); }
.token-state.missing { color: var(--accent-warning); background: rgba(245,158,11,.12); }
input, select, textarea { width: 100%; background: var(--bg-tertiary); border: 1px solid var(--border-color); color: var(--text-primary); padding: 9px 11px; border-radius: 9px; }
textarea { min-height: 120px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
button { border: 0; border-radius: 9px; padding: 9px 14px; cursor: pointer; font: inherit; } .primary { background: var(--accent-primary); color: white; } .ghost { background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); } .mini { padding: 5px 9px; font-size: 12px; }
.docs-list { overflow: auto; }
.docs-head, .docs-row { display: grid; grid-template-columns: 90px minmax(280px, 1.3fr) 140px 100px minmax(220px, 1fr) 86px; gap: 10px; align-items: center; min-width: 1020px; padding: 10px 12px; border-bottom: 1px solid var(--border-color); }
.docs-head { color: var(--text-muted); font-size: 12px; background: rgba(255,255,255,.035); }
.docs-item:last-child .docs-row { border-bottom: 0; }
code { color: var(--accent-secondary); overflow-wrap: anywhere; }
.method, .auth { width: fit-content; border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 800; }
.method.get { color: #22d3ee; background: rgba(34,211,238,.1); } .method.post { color: #34d399; background: rgba(52,211,153,.1); } .method.put { color: #f59e0b; background: rgba(245,158,11,.1); } .method.delete { color: #f87171; background: rgba(248,113,113,.1); }
.auth.public { color: var(--accent-success); } .auth.admin { color: var(--accent-warning); } .auth.bearer { color: var(--accent-secondary); }
.try-panel { min-width: 1020px; padding: 14px 12px 18px; border-bottom: 1px solid var(--border-color); background: rgba(255,255,255,.025); }
.try-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.request-preview { display: grid; gap: 6px; margin-top: 10px; }
.request-preview span { color: var(--text-muted); font-size: 12px; }
.try-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 10px; }
.response-box { margin: 12px 0 0; max-height: 360px; overflow: auto; padding: 12px; border-radius: 12px; white-space: pre-wrap; background: var(--bg-tertiary); border: 1px solid var(--border-color); color: var(--text-primary); }
.response-box.ok { border-color: rgba(52,211,153,.28); }
.response-box.error { border-color: rgba(248,113,113,.35); }
.empty { color: var(--text-muted); padding: 28px; text-align: center; }
@media (max-width: 900px) { .hero, .auth-guide { align-items: flex-start; flex-direction: column; } .toolbar, .try-grid { grid-template-columns: 1fr; } }
</style>
