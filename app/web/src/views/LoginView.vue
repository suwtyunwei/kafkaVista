<template>
  <div class="login-page">
    <!-- 背景装饰 -->
    <div class="bg-grid"></div>
    <div class="bg-glow glow-1"></div>
    <div class="bg-glow glow-2"></div>

    <div class="login-container">
      <!-- 左侧品牌区 -->
      <div class="brand-panel">
        <div class="brand-content">
          <div class="brand-icon">
            <img :src="logoUrl" :alt="platformName" />
          </div>
          <h1 class="brand-title">{{ platformName }}</h1>
          <p class="brand-subtitle">企业级 Kafka 管理平台<br>聚焦集群、Topic、消息、消费组与权限审计</p>
          <div class="brand-features">
            <div class="brand-feature">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="20 6 9 17 4 12" />
              </svg>
              <span>多 Kafka 集群统一管理</span>
            </div>
            <div class="brand-feature">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="20 6 9 17 4 12" />
              </svg>
              <span>Topic、消息、Consumer Group 运维</span>
            </div>
            <div class="brand-feature">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="20 6 9 17 4 12" />
              </svg>
              <span>权限控制与操作审计</span>
            </div>
          </div>
        </div>
      </div>

      <!-- 右侧登录区 -->
      <div class="login-panel">
        <div class="login-form-wrapper">
          <div class="login-header">
            <h2>欢迎回来</h2>
            <p>登录后进入 Kafka 管理平台</p>
          </div>

          <form class="login-form" @submit.prevent="handleLogin">
            <div class="form-group">
              <label for="username">用户名</label>
              <div class="input-wrapper" :class="{ focused: focusedField === 'username', error: errors.username }">
                <svg class="input-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                  <circle cx="12" cy="7" r="4" />
                </svg>
                <input
                  id="username"
                  v-model="username"
                  type="text"
                  placeholder="请输入用户名"
                  autocomplete="username"
                  @focus="focusedField = 'username'"
                  @blur="focusedField = ''"
                  @keydown.enter.prevent="handleLogin"
                />
              </div>
              <span v-if="errors.username" class="field-error">{{ errors.username }}</span>
            </div>

            <div class="form-group">
              <label for="password">密码</label>
              <div class="input-wrapper" :class="{ focused: focusedField === 'password', error: errors.password }">
                <svg class="input-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
                  <path d="M7 11V7a5 5 0 0 1 10 0v4" />
                </svg>
                <input
                  id="password"
                  v-model="password"
                  :type="showPassword ? 'text' : 'password'"
                  placeholder="请输入密码"
                  autocomplete="current-password"
                  @focus="focusedField = 'password'"
                  @blur="focusedField = ''"
                  @keydown.enter.prevent="handleLogin"
                />
                <button type="button" class="toggle-password" @click="showPassword = !showPassword" tabindex="-1">
                  <svg v-if="showPassword" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24" />
                    <line x1="1" y1="1" x2="23" y2="23" />
                  </svg>
                  <svg v-else width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                    <circle cx="12" cy="12" r="3" />
                  </svg>
                </button>
              </div>
              <span v-if="errors.password" class="field-error">{{ errors.password }}</span>
            </div>

            <div v-if="!ssoStatus.oidc_enabled" class="login-options-row">
              <div class="ldap-option">
                <label class="ldap-checkbox">
                  <input type="checkbox" v-model="useLdap" :disabled="ssoStatus.oidc_enabled" />
                  <span class="checkmark"></span>
                  <span class="ldap-label">LDAP 登录</span>
                </label>
              </div>
              <div class="ldap-option">
                <label class="ldap-checkbox">
                  <input type="checkbox" v-model="rememberMe" />
                  <span class="checkmark"></span>
                  <span class="ldap-label">记住密码</span>
                </label>
              </div>
            </div>

            <p v-if="ssoStatus.oidc_enabled" class="login-hint">OIDC 已启用时，LDAP 登录和记住登录不可用</p>

            <div v-if="errorMessage" class="error-banner">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10" />
                <line x1="12" y1="8" x2="12" y2="12" />
                <line x1="12" y1="16" x2="12.01" y2="16" />
              </svg>
              <span>{{ errorMessage }}</span>
            </div>

            <button type="submit" class="login-btn" :disabled="loading">
              <span v-if="loading" class="spinner"></span>
              <span v-else>登 录</span>
            </button>
            <button v-if="ssoStatus.oidc_enabled" type="button" class="sso-btn" @click="handleOidcLogin">{{ ssoStatus.oidc_button_text || '使用 OIDC 登录' }}</button>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { setToken, setUser } from '../auth'
import { getSsoStatus } from '../api'

const router = useRouter()
const route = useRoute()

const username = ref('')
const password = ref('')
const useLdap = ref(false)
const rememberMe = ref(false)
const showPassword = ref(false)
const loading = ref(false)
const focusedField = ref('')
const errorMessage = ref('')
const errors = ref<Record<string, string>>({})
const ssoStatus = ref({ oidc_enabled: false, ldap_enabled: false, oidc_button_text: '', platform_name: 'kafkaVista', logo_url: '/favicon.png?v=2026052102' })
const platformName = ref('kafkaVista')
const logoUrl = ref('/favicon.png?v=2026052102')
const REMEMBER_CREDENTIAL_KEY = 'kafkavista_remembered_login'

const validate = (): boolean => {
  errors.value = {}
  if (!username.value.trim()) {
    errors.value.username = '请输入用户名'
  }
  if (!password.value) {
    errors.value.password = '请输入密码'
  }
  return Object.keys(errors.value).length === 0
}

const handleLogin = async () => {
  if (!validate()) return

  loading.value = true
  errorMessage.value = ''

  try {
    const resp = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: username.value.trim(),
        password: password.value,
        use_ldap: useLdap.value,
      }),
    })

    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}))
      throw new Error(data.detail || '登录失败')
    }

    const data = await resp.json()
    const rememberLocalPassword = rememberMe.value && !useLdap.value && !ssoStatus.value.oidc_enabled
    setToken(data.data.access_token, rememberMe.value)
    setUser(data.data.user, rememberMe.value)
    if (rememberLocalPassword) {
      localStorage.setItem(REMEMBER_CREDENTIAL_KEY, btoa(unescape(encodeURIComponent(JSON.stringify({ username: username.value.trim(), password: password.value })))))
    } else {
      localStorage.removeItem(REMEMBER_CREDENTIAL_KEY)
    }

    const redirect = (route.query.redirect as string) || '/'
    router.push(redirect)
  } catch (err: unknown) {
    errorMessage.value = err instanceof Error ? err.message : '登录失败，请检查网络连接'
  } finally {
    loading.value = false
  }
}

const handleOidcLogin = () => {
  window.location.href = '/api/auth/oidc/login'
}

const loadRememberedLogin = () => {
  const raw = localStorage.getItem(REMEMBER_CREDENTIAL_KEY)
  if (!raw) return
  try {
    const parsed = JSON.parse(decodeURIComponent(escape(atob(raw))))
    username.value = parsed.username || ''
    password.value = parsed.password || ''
    rememberMe.value = !!username.value && !!password.value
  } catch {
    localStorage.removeItem(REMEMBER_CREDENTIAL_KEY)
  }
}

onMounted(async () => {
  if (typeof route.query.sso_token === 'string' && typeof route.query.sso_user === 'string') {
    try {
      const normalized = route.query.sso_user.replace(/-/g, '+').replace(/_/g, '/')
      const padded = normalized + '='.repeat((4 - normalized.length % 4) % 4)
      const userText = decodeURIComponent(escape(atob(padded)))
      setToken(route.query.sso_token, true)
      setUser(JSON.parse(userText), true)
      router.replace('/kafka')
      return
    } catch {
      errorMessage.value = 'SSO 登录回调解析失败'
    }
  }
  ssoStatus.value = await getSsoStatus()
  platformName.value = ssoStatus.value.platform_name || 'kafkaVista'
  logoUrl.value = ssoStatus.value.logo_url || '/favicon.png?v=2026052102'
  document.title = platformName.value
  useLdap.value = ssoStatus.value.ldap_enabled === true && !ssoStatus.value.oidc_enabled
  if (ssoStatus.value.oidc_enabled || useLdap.value) {
    rememberMe.value = false
    localStorage.removeItem(REMEMBER_CREDENTIAL_KEY)
  } else {
    loadRememberedLogin()
  }
})
</script>

<style scoped>
/* --- 页面布局 --- */
.login-page {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-primary, #0a0e17);
  position: relative;
  overflow: hidden;
  padding: 24px;
}

/* --- 背景装饰 --- */
.bg-grid {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(99, 102, 241, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(99, 102, 241, 0.03) 1px, transparent 1px);
  background-size: 60px 60px;
}

.bg-glow {
  position: absolute;
  border-radius: 50%;
  filter: blur(120px);
  opacity: 0.15;
  pointer-events: none;
}

.glow-1 {
  width: 500px;
  height: 500px;
  background: var(--accent-primary, #6366f1);
  top: -200px;
  right: -100px;
}

.glow-2 {
  width: 400px;
  height: 400px;
  background: var(--accent-secondary, #22d3ee);
  bottom: -150px;
  left: -100px;
}

/* --- 容器 --- */
.login-container {
  display: flex;
  width: 100%;
  max-width: 900px;
  min-height: 560px;
  background: var(--bg-secondary, #111827);
  border: 1px solid var(--border-color, #1e293b);
  border-radius: var(--radius-xl, 20px);
  overflow: hidden;
  position: relative;
  z-index: 1;
  box-shadow:
    0 0 80px rgba(99, 102, 241, 0.06),
    0 20px 60px rgba(0, 0, 0, 0.3);
}

/* --- 左侧品牌区 --- */
.brand-panel {
  flex: 1;
  background: linear-gradient(135deg, rgba(99, 102, 241, 0.08) 0%, rgba(34, 211, 238, 0.04) 100%);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px;
  position: relative;
  overflow: hidden;
}

.brand-panel::before {
  content: '';
  position: absolute;
  inset: 0;
  background:
    radial-gradient(circle at 30% 50%, rgba(99, 102, 241, 0.08) 0%, transparent 60%),
    radial-gradient(circle at 70% 50%, rgba(34, 211, 238, 0.06) 0%, transparent 60%);
}

.brand-content {
  text-align: center;
  position: relative;
  z-index: 1;
}

.brand-icon {
  width: 112px;
  height: 112px;
  margin: 0 auto 24px;
  background: transparent;
  border-radius: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  box-shadow: none;
}

.brand-icon img {
  width: 104px;
  height: 104px;
  object-fit: contain;
}

.brand-title {
  font-size: 28px;
  font-weight: 700;
  color: var(--text-primary, #e2e8f0);
  margin-bottom: 8px;
  letter-spacing: -0.5px;
  white-space: nowrap;
}

.brand-subtitle {
  font-size: 15px;
  color: var(--text-muted, #64748b);
  margin-bottom: 40px;
}

.brand-features {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.brand-feature {
  display: flex;
  align-items: center;
  gap: 12px;
  color: var(--text-secondary, #94a3b8);
  font-size: 14px;
}

.brand-feature svg {
  color: var(--accent-success, #34d399);
  flex-shrink: 0;
}

/* --- 右侧登录区 --- */
.login-panel {
  width: 420px;
  min-width: 420px;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px;
  background: var(--bg-secondary, #111827);
}

.login-form-wrapper {
  width: 100%;
  max-width: 340px;
}

.login-header {
  margin-bottom: 32px;
}

.login-header h2 {
  font-size: 24px;
  font-weight: 700;
  color: var(--text-primary, #e2e8f0);
  margin-bottom: 8px;
}

.login-header p {
  font-size: 14px;
  color: var(--text-muted, #64748b);
}

/* --- 表单 --- */
.login-form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.login-options-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-top: -8px;
}

/* --- 登录选项复选框 --- */
.ldap-option {
  display: flex;
  align-items: center;
}

.ldap-checkbox {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  user-select: none;
  position: relative;
  padding-left: 28px;
}

.ldap-checkbox input {
  position: absolute;
  opacity: 0;
  cursor: pointer;
  height: 0;
  width: 0;
}

.checkmark {
  position: absolute;
  left: 0;
  top: 50%;
  transform: translateY(-50%);
  width: 18px;
  height: 18px;
  border: 2px solid var(--border-light, #2d3a4f);
  border-radius: 4px;
  background: var(--bg-input, #0d1320);
  transition: all 0.2s ease;
}

.ldap-checkbox:hover input ~ .checkmark {
  border-color: var(--accent-primary-hover, #818cf8);
}

.ldap-checkbox input:checked ~ .checkmark {
  background: var(--accent-primary, #6366f1);
  border-color: var(--accent-primary, #6366f1);
}

.checkmark::after {
  content: '';
  position: absolute;
  display: none;
  left: 4px;
  top: 1px;
  width: 6px;
  height: 10px;
  border: solid white;
  border-width: 0 2px 2px 0;
  transform: rotate(45deg);
}

.ldap-checkbox input:checked ~ .checkmark::after {
  display: block;
}

.ldap-label {
  font-size: 13px;
  color: var(--text-secondary, #94a3b8);
}

.ldap-checkbox input:checked ~ .ldap-label {
  color: var(--accent-primary, #6366f1);
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.form-group label {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-secondary, #94a3b8);
}

.input-wrapper {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 0 14px;
  height: 44px;
  background: var(--bg-input, #0d1320);
  border: 1px solid var(--border-light, #2d3a4f);
  border-radius: var(--radius-md, 10px);
  transition: all 0.2s ease;
}

.input-wrapper:hover {
  border-color: var(--accent-primary-hover, #818cf8);
}

.input-wrapper.focused {
  border-color: var(--accent-primary, #6366f1);
  box-shadow: 0 0 0 3px var(--accent-primary-glow, rgba(99, 102, 241, 0.15));
}

.input-wrapper.error {
  border-color: var(--accent-danger, #f87171);
  box-shadow: 0 0 0 3px rgba(248, 113, 113, 0.15);
}

.input-icon {
  color: var(--text-muted, #64748b);
  flex-shrink: 0;
}

.input-wrapper.focused .input-icon {
  color: var(--accent-primary, #6366f1);
}

.input-wrapper input {
  flex: 1;
  border: none;
  background: transparent;
  color: var(--text-primary, #e2e8f0);
  font-size: 14px;
  outline: none;
  height: 100%;
  font-family: inherit;
}

.input-wrapper input::placeholder {
  color: var(--text-muted, #64748b);
}

.toggle-password {
  display: flex;
  align-items: center;
  justify-content: center;
  background: none;
  border: none;
  color: var(--text-muted, #64748b);
  cursor: pointer;
  padding: 4px;
  transition: color 0.2s;
}

.toggle-password:hover {
  color: var(--text-secondary, #94a3b8);
}

.field-error {
  font-size: 12px;
  color: var(--accent-danger, #f87171);
  padding-left: 4px;
}

/* --- 错误横幅 --- */
.error-banner {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  background: rgba(248, 113, 113, 0.1);
  border: 1px solid rgba(248, 113, 113, 0.25);
  border-radius: var(--radius-md, 10px);
  color: var(--accent-danger, #f87171);
  font-size: 13px;
}

.error-banner svg {
  flex-shrink: 0;
}

.login-hint {
  margin: -4px 0 0;
  color: var(--text-muted, #64748b);
  font-size: 12px;
}

/* --- 登录按钮 --- */
.login-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 44px;
  border: none;
  border-radius: var(--radius-md, 10px);
  background: linear-gradient(135deg, var(--accent-primary, #6366f1), #7c3aed);
  color: white;
  font-size: 15px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s ease;
  letter-spacing: 2px;
  font-family: inherit;
}

.login-btn:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 4px 20px rgba(99, 102, 241, 0.35);
}

.login-btn:active:not(:disabled) {
  transform: translateY(0);
}

.login-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.sso-btn {
  width: 100%;
  height: 42px;
  border: 1px solid var(--border-light, #2d3a4f);
  border-radius: var(--radius-md, 10px);
  background: var(--bg-tertiary, #1a2235);
  color: var(--text-primary, #e2e8f0);
  font-size: 14px;
  cursor: pointer;
  font-family: inherit;
}

.sso-btn:hover {
  border-color: var(--accent-primary, #6366f1);
  color: var(--accent-primary-hover, #818cf8);
}

/* --- Loading Spinner --- */
.spinner {
  width: 20px;
  height: 20px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: white;
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

/* --- Responsive --- */
@media (max-width: 768px) {
  .brand-panel {
    display: none;
  }

  .login-panel {
    width: 100%;
    min-width: unset;
    padding: 40px 28px;
  }

  .login-container {
    min-height: unset;
    max-width: 420px;
  }
}

@media (max-width: 480px) {
  .login-page {
    padding: 12px;
  }

  .login-panel {
    padding: 28px 20px;
  }
}
</style>
