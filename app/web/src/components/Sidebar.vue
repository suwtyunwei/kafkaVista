<template>
  <div class="sidebar" :class="{ collapsed: sidebarCollapsed }">
    <div class="sidebar-header">
      <div class="sidebar-logo" @click="openKafkaHome">
        <img :src="logoUrl" :alt="platformName" />
        <span v-show="!sidebarCollapsed">{{ platformName }}</span>
      </div>
      <button class="sidebar-toggle" @click="toggleCollapse" :title="sidebarCollapsed ? '展开' : '收起'">
        <svg v-if="sidebarCollapsed" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polyline points="9 18 15 12 9 6" />
        </svg>
        <svg v-else width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polyline points="15 18 9 12 15 6" />
        </svg>
      </button>
    </div>

    <div class="sidebar-nav">
      <button class="nav-item" :class="{ active: isKafkaPage }" @click="openKafkaHome">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <ellipse cx="12" cy="5" rx="8" ry="3" />
          <path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
          <path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" />
        </svg>
        <span>{{ t('kafkaManage') }}</span>
        <span v-if="showKafkaSubnav" class="nav-caret">{{ kafkaExpanded ? '▾' : '▸' }}</span>
      </button>

      <div v-if="isKafkaSubnavExpanded" class="kafka-subnav">
        <button v-for="item in kafkaTabs" :key="item.id" class="kafka-subitem" :class="{ active: activeKafkaTab === item.id }" @click="openKafkaTab(item.id)">
          <span>{{ item.label }}</span>
        </button>
      </div>

      <button v-if="isAdminUser" class="nav-item" :class="{ active: route.name === 'audit' }" @click="emit('navigate', '/audit')">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
          <polyline points="14 2 14 8 20 8" />
          <line x1="8" y1="13" x2="16" y2="13" />
          <line x1="8" y1="17" x2="14" y2="17" />
          <polyline points="8 9 10 11 13 7" />
        </svg>
        <span>{{ tr('操作审计', 'Audit Logs') }}</span>
      </button>

      <button v-if="isAdminUser && appStatus.enterprise" class="nav-item" :class="{ active: route.name === 'migration' }" @click="emit('navigate', '/migration')">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M4 7h12" />
          <path d="M12 3l4 4-4 4" />
          <path d="M20 17H8" />
          <path d="M12 13l-4 4 4 4" />
        </svg>
        <span>{{ tr('平滑迁移', 'Migration') }}</span>
      </button>

    </div>

    <div class="sidebar-footer">
      <button v-if="isAdminUser" class="nav-item" :class="{ active: route.name === 'docs' }" @click="emit('navigate', '/docs')">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
          <path d="M4 4.5A2.5 2.5 0 0 1 6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5z" />
          <line x1="8" y1="7" x2="16" y2="7" />
          <line x1="8" y1="11" x2="14" y2="11" />
        </svg>
        <span>API Docs</span>
      </button>
      <button v-if="isAdminUser" class="nav-item" :class="{ active: route.name === 'settings' }" @click="emit('navigate', '/settings')">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06A1.65 1.65 0 0 0 15 19.4a1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06A2 2 0 1 1 7.04 4.3l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09A1.65 1.65 0 0 0 15 4.6a1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9c.14.31.48.99 1.51 1H21a2 2 0 1 1 0 4h-.09c-1.03.01-1.37.69-1.51 1z" />
        </svg>
        <span>{{ t('settings') }}</span>
      </button>
      <button class="nav-item theme-toggle" @click="handleThemeToggle">
        <svg v-if="themeMode === 'dark'" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="5" />
          <line x1="12" y1="1" x2="12" y2="3" />
          <line x1="12" y1="21" x2="12" y2="23" />
          <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
          <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
          <line x1="1" y1="12" x2="3" y2="12" />
          <line x1="21" y1="12" x2="23" y2="12" />
          <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
          <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
        </svg>
        <svg v-else width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
        </svg>
        <span>{{ themeMode === 'dark' ? t('lightMode') : t('darkMode') }}</span>
      </button>

      <div class="user-section">
        <div class="user-info">
          <div class="user-avatar">{{ userInitial }}</div>
          <div class="user-details">
            <span class="user-name">{{ displayName }}</span>
            <span class="user-login">{{ username }}</span>
          </div>
        </div>
        <button class="logout-btn" @click="$emit('logout')" title="退出登录">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
            <polyline points="16 17 21 12 16 7" />
            <line x1="21" y1="12" x2="9" y2="12" />
          </svg>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { getUser, isAdmin } from '../auth'
import { getAppStatus } from '../api'
import { getTheme, toggleTheme, type ThemeMode } from '../theme'
import { language, setLanguage, t } from '../i18n'

const tr = (zh: string, en: string) => language.value === 'en-US' ? en : zh

const route = useRoute()
const KAFKA_WORKSPACE_KEY = 'kafka_active_workspace'
const emit = defineEmits<{
  logout: []
  navigate: [path: string]
  'sidebar-toggle': [collapsed: boolean]
}>()

const user = computed(() => getUser())
const username = computed(() => user.value?.username || '')
const displayName = computed(() => user.value?.display_name || 'User')
const userInitial = computed(() => displayName.value.charAt(0).toUpperCase())
const isAdminUser = computed(() => isAdmin())
const kafkaTabs = computed(() => [
  { id: 'topics', label: t('topics') },
  { id: 'brokers', label: t('brokers') },
  { id: 'groups', label: t('groups') },
])
const isKafkaPage = computed(() => route.name === 'kafka')
const kafkaWorkspace = ref<{ clusterId: string; tab: string; canManagePermissions?: boolean; monitoringEnabled?: boolean } | null>(null)
const kafkaExpanded = ref(localStorage.getItem('kafka_menu_expanded') !== 'false')
const currentKafkaClusterId = computed(() => typeof route.query.cluster === 'string' ? route.query.cluster : '')
const showKafkaSubnav = computed(() => isKafkaPage.value && !!currentKafkaClusterId.value && kafkaWorkspace.value?.clusterId === currentKafkaClusterId.value)
const isKafkaSubnavExpanded = computed(() => showKafkaSubnav.value && kafkaExpanded.value && !sidebarCollapsed.value)
const activeKafkaTab = computed(() => typeof route.query.tab === 'string' ? route.query.tab : kafkaWorkspace.value?.tab || 'topics')
const sidebarCollapsed = ref(localStorage.getItem('sidebar_collapsed') === 'true')
const themeMode = ref<ThemeMode>(getTheme())
const appStatus = ref<any>({ edition: 'enterprise', enterprise: true })
const platformName = computed(() => appStatus.value.platform_name || 'kafkaVista')
const logoUrl = computed(() => appStatus.value.logo_url || '/favicon.png?v=2026052102')

const loadKafkaWorkspace = () => {
  const raw = localStorage.getItem(KAFKA_WORKSPACE_KEY)
  if (!raw) {
    kafkaWorkspace.value = null
    return
  }
  try {
    const parsed = JSON.parse(raw)
    kafkaWorkspace.value = parsed?.clusterId ? parsed : null
  } catch {
    kafkaWorkspace.value = null
  }
}

const handleKafkaWorkspaceChange = (event: Event) => {
  const detail = (event as CustomEvent).detail
  kafkaWorkspace.value = detail?.clusterId ? detail : null
}

const openKafkaHome = () => {
  kafkaWorkspace.value = null
  localStorage.removeItem(KAFKA_WORKSPACE_KEY)
  window.dispatchEvent(new CustomEvent('kafka-home-select'))
  emit('navigate', '/kafka')
}

const openKafkaTab = (tab: string) => {
  const clusterId = kafkaWorkspace.value?.clusterId
  if (!clusterId) return
  kafkaExpanded.value = true
  localStorage.setItem('kafka_menu_expanded', 'true')
  kafkaWorkspace.value = { clusterId, tab }
  localStorage.setItem(KAFKA_WORKSPACE_KEY, JSON.stringify(kafkaWorkspace.value))
  window.dispatchEvent(new CustomEvent('kafka-tab-select', { detail: kafkaWorkspace.value }))
  emit('navigate', `/kafka?cluster=${encodeURIComponent(clusterId)}&tab=${encodeURIComponent(tab)}`)
}

const toggleCollapse = () => {
  sidebarCollapsed.value = !sidebarCollapsed.value
  localStorage.setItem('sidebar_collapsed', String(sidebarCollapsed.value))
  emit('sidebar-toggle', sidebarCollapsed.value)
}

const handleThemeToggle = () => {
  themeMode.value = toggleTheme()
}

onMounted(() => {
  loadKafkaWorkspace()
  getAppStatus().then((status) => {
    appStatus.value = status
    setLanguage(status.language || language.value)
  }).catch(() => {})
  window.addEventListener('kafka-workspace-change', handleKafkaWorkspaceChange as EventListener)
})

onUnmounted(() => {
  window.removeEventListener('kafka-workspace-change', handleKafkaWorkspaceChange as EventListener)
})
</script>

<style scoped>
.sidebar { display: flex; flex-direction: column; height: 100%; transition: width 0.2s ease; overflow: hidden; }
.sidebar.collapsed { width: 60px !important; }
.sidebar.collapsed .sidebar-logo span, .sidebar.collapsed .nav-item span, .sidebar.collapsed .nav-caret, .sidebar.collapsed .user-details, .sidebar.collapsed .logout-btn { display: none !important; }
.sidebar.collapsed .sidebar-logo, .sidebar.collapsed .nav-item { justify-content: center; }
.sidebar-header { padding: 16px; border-bottom: 1px solid var(--border-color, #1e293b); }
.sidebar-logo { display: flex; align-items: center; gap: 10px; cursor: pointer; color: var(--text-primary, #e2e8f0); font-size: 18px; font-weight: 800; white-space: nowrap; }
.sidebar-logo img { width: 42px; height: 42px; object-fit: contain; border-radius: 12px; }
.sidebar-toggle { width: 24px; height: 24px; min-width: 24px; border: none; background: transparent; color: var(--text-muted, #64748b); cursor: pointer; border-radius: 4px; display: flex; align-items: center; justify-content: center; transition: all 0.15s; margin-left: auto; }
.sidebar-toggle:hover { background: var(--bg-hover, #1e293b); color: var(--text-primary, #e2e8f0); }
.sidebar-nav { flex: 1; overflow-y: auto; padding: 8px; }
.nav-item { display: flex; align-items: center; gap: 10px; width: 100%; padding: 10px 12px; border: none; background: transparent; color: var(--text-secondary, #94a3b8); font-size: 13px; cursor: pointer; border-radius: var(--radius-sm, 6px); transition: all 0.15s; font-family: inherit; text-align: left; }
.nav-item:hover { background: var(--bg-hover, #1e293b); color: var(--text-primary, #e2e8f0); }
.nav-item.active { background: rgba(99, 102, 241, 0.1); color: var(--accent-primary, #6366f1); }
.nav-caret { margin-left: auto; color: var(--text-muted, #64748b); font-size: 12px; }
.kafka-subnav { display: grid; gap: 2px; margin: 2px 0 6px 28px; }
.kafka-subitem { width: 100%; border: 0; background: transparent; color: var(--text-muted, #64748b); text-align: left; font: inherit; font-size: 12px; padding: 7px 10px; border-radius: var(--radius-sm, 6px); cursor: pointer; }
.kafka-subitem:hover { background: var(--bg-hover, #1e293b); color: var(--text-primary, #e2e8f0); }
.kafka-subitem.active { background: rgba(99, 102, 241, 0.12); color: var(--accent-primary, #6366f1); font-weight: 700; }
.sidebar-footer { padding: 12px 8px; border-top: 1px solid var(--border-color, #1e293b); }
.theme-toggle svg { color: var(--accent-warning, #fbbf24); }
.edition-badge { display: grid; gap: 2px; margin: 0 12px 8px; padding: 8px 10px; border: 1px solid var(--border-color, #1e293b); border-radius: 10px; color: var(--text-muted, #64748b); background: rgba(255,255,255,.03); font-size: 11px; }
.edition-badge strong { color: var(--text-primary, #e2e8f0); font-size: 12px; }
.edition-badge.enterprise strong { color: var(--accent-success, #34d399); }
.edition-badge.expired strong { color: var(--accent-warning, #fbbf24); }
.user-section { display: flex; align-items: center; justify-content: space-between; padding: 8px 12px 0; margin-top: 4px; }
.user-info { display: flex; align-items: center; gap: 10px; min-width: 0; }
.user-avatar { width: 32px; height: 32px; border-radius: 50%; background: linear-gradient(135deg, var(--accent-primary, #6366f1), #7c3aed); color: white; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 700; flex-shrink: 0; }
.user-details { display: flex; flex-direction: column; min-width: 0; }
.user-name { font-size: 12px; font-weight: 600; color: var(--text-primary, #e2e8f0); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.user-login { font-size: 10px; color: var(--text-muted, #64748b); }
.logout-btn { display: flex; align-items: center; justify-content: center; width: 28px; height: 28px; border: none; background: transparent; color: var(--text-muted, #64748b); cursor: pointer; border-radius: 6px; flex-shrink: 0; transition: all 0.2s; }
.logout-btn:hover { background: rgba(248, 113, 113, 0.15); color: var(--accent-danger, #f87171); }
</style>
