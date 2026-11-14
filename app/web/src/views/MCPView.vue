<template>
  <div class="mcp-page">
    <section class="mcp-hero">
      <div>
        <p class="eyebrow">Model Context Protocol</p>
        <h1>{{ tr('MCP 服务', 'MCP Server') }}</h1>
        <p>{{ tr('把 KafkaVista 的集群、Topic、消息、消费组、权限、审计与迁移能力，通过标准 MCP 协议提供给 Agent 等外部智能体。', 'Expose KafkaVista cluster, topic, message, consumer group, permission, audit and migration capabilities to Agent and other agents through the standard MCP protocol.') }}</p>
      </div>
      <div class="hero-state" :class="{ enabled: config.enabled }">
        <span class="dot"></span>
        {{ config.enabled ? tr('服务已启用', 'Service Enabled') : tr('服务未启用', 'Service Disabled') }}
      </div>
    </section>

    <div v-if="notice" class="notice" :class="{ error: noticeError }">{{ notice }}</div>

    <section class="mcp-grid">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>{{ tr('接入配置', 'Connection') }}</h2>
            <p class="hint">{{ tr('保存后开关立即生效，无需重启服务。', 'Changes take effect immediately after saving.') }}</p>
          </div>
          <span class="tag">{{ tools.length }} tools</span>
        </div>

        <div class="form-row switch-row">
          <div>
            <strong>{{ tr('启用 MCP 服务', 'Enable MCP Server') }}</strong>
            <p class="hint">{{ tr('关闭后外部连接会收到 404。', 'External connections receive 404 when disabled.') }}</p>
          </div>
          <label class="switch">
            <input v-model="config.enabled" type="checkbox" />
            <span></span>
          </label>
        </div>

        <div class="form-row">
          <label>{{ tr('审计身份', 'Audit Identity') }}</label>
          <input v-model="config.username" :placeholder="tr('例如 agent', 'e.g. agent')" />
          <p class="hint">{{ tr('所有通过 MCP 发起的变更操作都会以此身份写入审计日志。', 'All mutating MCP operations are recorded in the audit log under this identity.') }}</p>
        </div>

        <div class="secret-box">
          <div class="secret-head">
            <span>{{ tr('服务地址', 'Endpoint') }}</span>
            <button class="ghost mini" @click="copyText(endpoint)">{{ tr('复制', 'Copy') }}</button>
          </div>
          <code>{{ endpoint }}</code>
        </div>

        <div class="secret-box">
          <div class="secret-head">
            <span>Bearer Token</span>
            <div class="secret-actions">
              <button class="ghost mini" @click="showToken = !showToken">{{ showToken ? tr('隐藏', 'Hide') : tr('显示', 'Show') }}</button>
              <button class="ghost mini" @click="copyText(config.token)">{{ tr('复制', 'Copy') }}</button>
              <button class="ghost mini danger" :disabled="rotating" @click="handleRotate">{{ tr('重置', 'Rotate') }}</button>
            </div>
          </div>
          <code class="token">{{ showToken ? config.token : maskedToken }}</code>
        </div>

        <div class="actions">
          <button class="primary" :disabled="saving || loading" @click="save">{{ saving ? tr('保存中...', 'Saving...') : tr('保存配置', 'Save') }}</button>
          <button class="ghost" :disabled="loading" @click="load">{{ tr('刷新', 'Refresh') }}</button>
        </div>
      </div>

      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>{{ tr('Agent 接入', 'Agent Setup') }}</h2>
            <p class="hint">{{ tr('Agent → 连接器 → 自定义连接器 → 添加 MCP，粘贴以下 JSON。', 'Agent → Connectors → Custom connector → Add MCP, then paste the JSON below.') }}</p>
          </div>
        </div>
        <pre class="code-block">{{ agentConfig }}</pre>
        <div class="actions">
          <button class="ghost" @click="copyText(agentConfig)">{{ tr('复制配置', 'Copy Config') }}</button>
        </div>
        <ul class="steps">
          <li>{{ tr('在连接器列表中点击“信任”并“启用”。', 'Click Trust and Enable in the connector list.') }}</li>
          <li>{{ tr('工具列表会自动出现，共 ' + tools.length + ' 个 Kafka 工具。', 'The connector will discover ' + tools.length + ' Kafka tools automatically.') }}</li>
          <li>{{ tr('如需更换 Token，在本页点击“重置”后同步更新连接器 Header。', 'If the token is rotated, update the connector header accordingly.') }}</li>
        </ul>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>{{ tr('工具清单', 'Available Tools') }}</h2>
          <p class="hint">{{ tr('覆盖平台全部 Kafka 操作，工具参数与 REST API 保持一致。', 'Covers the full Kafka feature set; arguments mirror the REST API.') }}</p>
        </div>
        <input v-model="keyword" class="search" :placeholder="tr('搜索工具', 'Search tools')" />
      </div>

      <div v-if="loading" class="empty">{{ tr('正在加载工具...', 'Loading tools...') }}</div>
      <div v-else-if="groupedTools.length === 0" class="empty">{{ tr('没有匹配的工具', 'No matching tools') }}</div>
      <div v-else class="tool-groups">
        <div v-for="group in groupedTools" :key="group.name" class="tool-group">
          <h3>{{ group.name }}<span>{{ group.items.length }}</span></h3>
          <div class="tool-grid">
            <article v-for="tool in group.items" :key="tool.name" class="tool-card">
              <code>{{ tool.name }}</code>
              <p>{{ tool.description }}</p>
              <details>
                <summary>{{ tr('查看参数', 'Arguments') }}</summary>
                <pre>{{ prettySchema(tool.input_schema) }}</pre>
              </details>
            </article>
          </div>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>{{ tr('调用示例', 'Quick Test') }}</h2>
          <p class="hint">{{ tr('在终端验证 MCP 是否可用。', 'Verify the MCP endpoint from a terminal.') }}</p>
        </div>
        <button class="ghost mini" @click="copyText(curlExample)">{{ tr('复制命令', 'Copy') }}</button>
      </div>
      <pre class="code-block">{{ curlExample }}</pre>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { getMcpConfig, rotateMcpToken, saveMcpConfig } from '../api'
import { tr } from '../i18n'

const config = ref({ enabled: false, token: '', username: 'agent', endpoint: '/api/mcp' })
const tools = ref<any[]>([])
const loading = ref(false)
const saving = ref(false)
const rotating = ref(false)
const showToken = ref(false)
const keyword = ref('')
const notice = ref('')
const noticeError = ref(false)

const maskedToken = computed(() => config.value.token ? `${config.value.token.slice(0, 8)}${'•'.repeat(20)}${config.value.token.slice(-6)}` : '—')
const endpoint = computed(() => {
  const path = config.value.endpoint || '/api/mcp'
  if (/^https?:\/\//i.test(path)) return path
  return `${window.location.origin}${path}`
})

const agentConfig = computed(() => JSON.stringify({
  mcpServers: {
    kafkavista: {
      type: 'streamableHttp',
      url: endpoint.value,
      headers: { Authorization: `Bearer ${config.value.token}` },
    },
  },
}, null, 2))

const curlExample = computed(() => {
  const payload = JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' })
  return `curl -s '${endpoint.value}' \\\n  -H 'Content-Type: application/json' \\\n  -H 'Accept: application/json, text/event-stream' \\\n  -H 'Authorization: Bearer ${config.value.token || '<token>'}' \\\n  --data-raw '${payload}'`
})

const groupedTools = computed(() => {
  const query = keyword.value.trim().toLowerCase()
  const groups = new Map<string, any[]>()
  tools.value.forEach((tool) => {
    if (query && !`${tool.name} ${tool.description}`.toLowerCase().includes(query)) return
    const name = tool.group || 'Other'
    if (!groups.has(name)) groups.set(name, [])
    groups.get(name)!.push(tool)
  })
  return [...groups.entries()].map(([name, items]) => ({ name, items }))
})

const prettySchema = (schema: any) => JSON.stringify(schema || {}, null, 2)

const applyConfig = (data: any) => {
  if (!data) return
  config.value = {
    enabled: !!data.enabled,
    token: data.token || '',
    username: data.username || 'agent',
    endpoint: data.endpoint || '/api/mcp',
  }
  if (Array.isArray(data.tools)) tools.value = data.tools
}

const flash = (message: string, error = false) => {
  notice.value = message
  noticeError.value = error
  window.setTimeout(() => { notice.value = '' }, 3200)
}

const load = async () => {
  loading.value = true
  try {
    applyConfig(await getMcpConfig())
  } catch (err: any) {
    flash(err?.response?.data?.detail || err?.message || tr('加载失败', 'Failed to load'), true)
  } finally {
    loading.value = false
  }
}

const save = async () => {
  saving.value = true
  try {
    applyConfig(await saveMcpConfig({ enabled: config.value.enabled, username: config.value.username }))
    flash(tr('MCP 配置已保存', 'MCP configuration saved'))
  } catch (err: any) {
    flash(err?.response?.data?.detail || err?.message || tr('保存失败', 'Failed to save'), true)
  } finally {
    saving.value = false
  }
}

const handleRotate = async () => {
  if (!window.confirm(tr('重置后旧 Token 立即失效，已接入的 Agent 需要更新配置。继续？', 'Rotating invalidates the old token immediately. Connected Agent integrations must be updated. Continue?'))) return
  rotating.value = true
  try {
    applyConfig(await rotateMcpToken())
    flash(tr('Token 已重置', 'Token rotated'))
  } catch (err: any) {
    flash(err?.response?.data?.detail || err?.message || tr('重置失败', 'Failed to rotate'), true)
  } finally {
    rotating.value = false
  }
}

const copyText = async (text: string) => {
  if (!text) return
  try {
    await navigator.clipboard.writeText(text)
  } catch {
    const area = document.createElement('textarea')
    area.value = text
    area.style.position = 'fixed'
    area.style.opacity = '0'
    document.body.appendChild(area)
    area.select()
    document.execCommand('copy')
    document.body.removeChild(area)
  }
  flash(tr('已复制到剪贴板', 'Copied to clipboard'))
}

onMounted(load)
</script>

<style scoped>
.mcp-page { padding: 24px; display: grid; gap: 16px; }
.mcp-hero { display: flex; justify-content: space-between; align-items: flex-start; gap: 18px; padding: 22px; border: 1px solid var(--border-color); border-radius: 22px; background: radial-gradient(circle at top right, rgba(99, 102, 241, .18), transparent 40%), var(--panel-bg); }
.eyebrow { color: var(--accent-secondary); font-size: 12px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; margin-bottom: 6px; }
.mcp-hero h1 { margin: 0 0 8px; font-size: 32px; letter-spacing: -.03em; }
.mcp-hero p, .hint { color: var(--text-muted); font-size: 13px; }
.hero-state { display: inline-flex; align-items: center; gap: 8px; padding: 9px 14px; border-radius: 999px; border: 1px solid var(--border-light); background: var(--bg-tertiary); color: var(--text-muted); font-size: 12px; font-weight: 700; white-space: nowrap; }
.hero-state .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--text-muted); }
.hero-state.enabled { color: var(--accent-success); border-color: rgba(52, 211, 153, .35); background: rgba(52, 211, 153, .1); }
.hero-state.enabled .dot { background: var(--accent-success); box-shadow: 0 0 0 4px rgba(52, 211, 153, .15); }
.notice { padding: 11px 14px; border-radius: 12px; border: 1px solid rgba(52, 211, 153, .35); background: rgba(52, 211, 153, .1); color: var(--accent-success); font-size: 13px; }
.notice.error { border-color: rgba(248, 113, 113, .35); background: rgba(248, 113, 113, .1); color: var(--accent-danger); }
.mcp-grid { display: grid; grid-template-columns: minmax(0, 1.05fr) minmax(0, .95fr); gap: 16px; }
.panel { border: 1px solid var(--border-color); border-radius: 20px; background: var(--panel-bg); padding: 18px; display: grid; gap: 14px; align-content: start; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.panel-head h2 { margin: 0 0 4px; font-size: 18px; }
.tag { padding: 4px 9px; border-radius: 999px; background: rgba(99, 102, 241, .12); color: var(--accent-primary); font-size: 12px; font-weight: 700; }
.form-row { display: grid; gap: 7px; }
.form-row > label { color: var(--text-secondary); font-size: 13px; font-weight: 600; }
.form-row strong { color: var(--text-primary); }
.switch-row { grid-template-columns: minmax(0, 1fr) auto; align-items: center; padding: 10px 12px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); }
.switch { position: relative; display: inline-flex; width: 46px; height: 26px; }
.switch input { opacity: 0; width: 0; height: 0; }
.switch span { position: absolute; inset: 0; border-radius: 999px; background: var(--border-light); cursor: pointer; transition: .2s; }
.switch span::before { content: ''; position: absolute; width: 20px; height: 20px; left: 3px; top: 3px; border-radius: 50%; background: #fff; transition: .2s; }
.switch input:checked + span { background: var(--accent-primary); }
.switch input:checked + span::before { transform: translateX(20px); }
input, .search { width: 100%; padding: 10px 12px; border: 1px solid var(--border-light); border-radius: 10px; background: var(--bg-input); color: var(--text-primary); font: inherit; }
.search { max-width: 250px; }
.secret-box { display: grid; gap: 7px; padding: 12px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); }
.secret-head { display: flex; justify-content: space-between; align-items: center; gap: 10px; color: var(--text-muted); font-size: 12px; font-weight: 700; }
.secret-actions { display: flex; gap: 6px; }
.secret-box code { overflow-wrap: anywhere; color: var(--text-primary); font-size: 13px; }
.secret-box code.token { color: var(--accent-secondary); }
.actions { display: flex; flex-wrap: wrap; gap: 8px; }
.primary, .ghost { border-radius: 10px; padding: 9px 13px; cursor: pointer; font: inherit; }
.primary { background: var(--accent-primary); color: #fff; border: 0; }
.primary:hover:not(:disabled) { background: var(--accent-primary-hover); }
.ghost { background: transparent; color: var(--text-secondary); border: 1px solid var(--border-light); }
.ghost:hover:not(:disabled) { background: var(--bg-hover); color: var(--text-primary); }
.ghost.danger { color: var(--accent-danger); border-color: rgba(248, 113, 113, .35); }
.mini { padding: 4px 8px; font-size: 11px; }
button:disabled { opacity: .55; cursor: not-allowed; }
.code-block { margin: 0; padding: 14px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--code-bg); color: var(--text-secondary); font-size: 12px; line-height: 1.65; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; }
.steps { margin: 0; padding-left: 18px; display: grid; gap: 6px; color: var(--text-muted); font-size: 12px; }
.tool-groups { display: grid; gap: 18px; }
.tool-group h3 { display: flex; align-items: center; gap: 8px; margin: 0 0 10px; font-size: 14px; color: var(--text-secondary); }
.tool-group h3 span { padding: 2px 8px; border-radius: 999px; background: var(--bg-tertiary); color: var(--text-muted); font-size: 11px; }
.tool-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; }
.tool-card { display: grid; gap: 8px; align-content: start; padding: 13px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); }
.tool-card code { color: var(--accent-secondary); font-size: 12px; font-weight: 700; }
.tool-card p { color: var(--text-muted); font-size: 12px; line-height: 1.55; }
.tool-card details summary { cursor: pointer; color: var(--text-muted); font-size: 11px; }
.tool-card pre { margin-top: 7px; max-height: 240px; overflow: auto; padding: 10px; border-radius: 10px; background: var(--bg-input); color: var(--text-secondary); font-size: 11px; line-height: 1.5; }
.empty { color: var(--text-muted); text-align: center; padding: 26px; }
@media (max-width: 1080px) { .mcp-grid { grid-template-columns: 1fr; } }
@media (max-width: 760px) { .mcp-page { padding: 16px; } .mcp-hero { flex-direction: column; } .panel-head { flex-direction: column; } .search { max-width: none; } }
</style>
