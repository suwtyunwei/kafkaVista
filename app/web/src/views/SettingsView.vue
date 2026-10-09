<template>
  <div class="settings-page">
    <section class="hero">
      <div>
        <p class="eyebrow">System Settings</p>
        <h1>{{ tr('系统设置', 'Settings') }}</h1>
        <p>{{ tr('配置登录角色、LDAP 登录、OIDC / Keycloak 单点登录和 Kafka 集群细粒度授权。', 'Configure login roles, LDAP, OIDC / Keycloak SSO, and Kafka cluster fine-grained permissions.') }}</p>
      </div>
      <button v-if="activeTab === 'basic' || activeTab === 'monitoring' || activeTab === 'monitoringAlerts' || activeTab === 'alertConfig'" class="primary" :disabled="saving" @click="saveAll">{{ saving ? tr('保存中...', 'Saving...') : tr('保存设置', 'Save Settings') }}</button>
    </section>

    <div class="settings-tabs">
      <button :class="{ active: activeTab === 'basic' }" @click="activeTab = 'basic'">{{ tr('基础设置', 'Basic') }}</button>
      <button :class="{ active: activeTab === 'monitoring' }" @click="activeTab = 'monitoring'">{{ tr('监控指标', 'Monitoring') }}</button>
      <button :class="{ active: activeTab === 'monitoringAlerts' }" @click="activeTab = 'monitoringAlerts'">{{ tr('监控告警', 'Monitoring Alerts') }}</button>
      <button :class="{ active: activeTab === 'users' }" @click="activeTab = 'users'">{{ tr('用户与角色', 'Users & Roles') }}</button>
      <button :class="{ active: activeTab === 'kafkaAuth' }" @click="activeTab = 'kafkaAuth'">{{ tr('Kafka 授权', 'Kafka Auth') }}</button>
    </div>

    <section v-if="activeTab === 'basic'" class="grid">
      <div class="panel compact-panel">
        <h2>{{ tr('界面设置', 'UI Settings') }}</h2>
        <div class="setting-row">
          <div><label>{{ tr('界面语言', 'Language') }}</label><p class="hint">{{ tr('切换侧边栏和通用入口显示语言。', 'Switch sidebar and common entry display language.') }}</p></div>
          <select v-model="settings.ui.language" class="compact-select" @change="applyLanguage"><option value="zh-CN">{{ tr('中文', 'Chinese') }}</option><option value="en-US">English</option></select>
        </div>
        <div class="setting-row">
          <div><label>{{ tr('默认主题', 'Default Theme') }}</label><p class="hint">{{ tr('设置界面默认使用亮色或暗色模式。', 'Set interface default light or dark mode.') }}</p></div>
          <select v-model="settings.ui.theme" class="compact-select" @change="applyThemeSetting"><option value="dark">{{ tr('暗色模式', 'Dark Mode') }}</option><option value="light">{{ tr('亮色模式', 'Light Mode') }}</option></select>
        </div>
        <div class="setting-row">
          <div><label>{{ tr('平台名称', 'Platform Name') }}</label><p class="hint">{{ tr('自定义侧边栏、登录页和浏览器标题。', 'Customize sidebar, login page, and browser title.') }}</p></div>
          <input v-model="settings.ui.platform_name" class="compact-input" placeholder="kafkaVista" />
        </div>
        <div class="setting-row">
          <div><label>Logo URL</label><p class="hint">{{ tr('填写图片 URL，例如 /logo.png 或 https://example.com/logo.png。', 'Image URL, e.g. /logo.png or https://example.com/logo.png.') }}</p></div>
          <input v-model="settings.ui.logo_url" class="compact-input" placeholder="/favicon.png?v=2026052102" />
        </div>
        <p class="hint">{{ tr('用户角色请在“用户与角色”选项卡维护；Kafka 访问范围请在“Kafka 授权”选项卡维护。', 'User roles are maintained in the "Users & Roles" tab; Kafka access scope in the "Kafka Auth" tab.') }}</p>
      </div>

      <div class="panel">
        <div class="panel-title"><h2>{{ tr('LDAP 设置', 'LDAP Settings') }}</h2><div class="actions"><button class="ghost small" @click="testLdap">{{ tr('测试连接', 'Test Connection') }}</button><button class="primary small" @click="syncLdap">{{ tr('一键拉取用户', 'Sync Users') }}</button></div></div>
        <label class="check"><input v-model="settings.ldap.enabled" type="checkbox" :disabled="settings.oidc.enabled" @change="onLdapToggle" /> {{ tr('启用 LDAP 登录', 'Enable LDAP Login') }}</label>
        <p v-if="settings.oidc.enabled" class="hint warn">{{ tr('OIDC 已启用时，LDAP 登录会被关闭。', 'LDAP login is disabled when OIDC is enabled.') }}</p>
        <input v-model="settings.ldap.url" placeholder="ldap://ldap.example.com:389" />
        <input v-model="settings.ldap.bind_dn" :placeholder="tr('Bind DN，如 cn=admin,dc=example,dc=com', 'Bind DN, e.g. cn=admin,dc=example,dc=com')" />
        <input v-model="settings.ldap.bind_password" type="password" :placeholder="tr('Bind 密码，留空不修改', 'Bind password, leave empty to keep')" />
        <input v-model="settings.ldap.base_dn" :placeholder="tr('Base DN，如 ou=users,dc=example,dc=com', 'Base DN, e.g. ou=users,dc=example,dc=com')" />
        <input v-model="settings.ldap.user_filter" :placeholder="tr('用户过滤器，如 (uid=%s)', 'User filter, e.g. (uid=%s)')" />
        <div class="two"><input v-model="settings.ldap.display_name_attr" :placeholder="tr('显示名属性 cn', 'Display name attribute cn')" /><input v-model="settings.ldap.email_attr" :placeholder="tr('邮箱属性 mail', 'Email attribute mail')" /></div>
        <label class="check"><input v-model="settings.ldap.start_tls" type="checkbox" /> StartTLS</label>
      </div>

      <div class="panel wide">
        <h2>{{ tr('OIDC / Keycloak 设置', 'OIDC / Keycloak Settings') }}</h2>
        <label class="check"><input v-model="settings.oidc.enabled" type="checkbox" :disabled="settings.ldap.enabled" @change="onOidcToggle" /> {{ tr('启用 OIDC 单点登录', 'Enable OIDC SSO') }}</label>
        <p v-if="settings.ldap.enabled" class="hint warn">{{ tr('LDAP 已启用时，OIDC 单点登录会被关闭。', 'OIDC SSO is disabled when LDAP login is enabled.') }}</p>
        <div class="two"><input v-model="settings.oidc.issuer_url" :placeholder="tr('Issuer URL，如 http://keycloak/realms/demo', 'Issuer URL, e.g. http://keycloak/realms/demo')" /><input v-model="settings.oidc.redirect_url" :placeholder="tr('Callback URL，如 http://host:3004/api/auth/oidc/callback', 'Callback URL, e.g. http://host:3004/api/auth/oidc/callback')" /></div>
        <div class="two"><input v-model="settings.oidc.client_id" placeholder="Client ID" /><input v-model="settings.oidc.client_secret" type="password" :placeholder="tr('Client Secret，留空不修改', 'Client Secret, leave empty to keep')" /></div>
        <div class="two"><input v-model="settings.oidc.scopes" placeholder="openid profile email" /><input v-model="settings.oidc.username_claim" :placeholder="tr('用户名 Claim，如 preferred_username', 'Username Claim, e.g. preferred_username')" /></div>
        <div class="two"><input v-model="settings.oidc.role_claim" :placeholder="tr('角色 Claim，如 roles', 'Role Claim, e.g. roles')" /><input v-model="adminRolesText" :placeholder="tr('管理员角色，如 admin,kafkavista-admin', 'Admin roles, e.g. admin,kafkavista-admin')" /></div>
        <input v-model="settings.oidc.button_text" :placeholder="tr('登录按钮名称，如 使用公司 SSO 登录', 'Login button text, e.g. Sign in with Company SSO')" />
        <p class="hint">{{ tr('Keycloak 推荐 Redirect URI：`http://你的域名或IP:3004/api/auth/oidc/callback`。', 'Keycloak recommended Redirect URI: `http://your-domain-or-ip:3004/api/auth/oidc/callback`.') }}</p>
      </div>

    </section>

    <section v-if="activeTab === 'monitoring'" class="panel monitoring-panel">
      <div class="panel-title"><h2>{{ tr('监控指标', 'Monitoring Metrics') }}</h2><span class="edition-pill">Prometheus Exporter</span></div>
      <label class="check"><input v-model="settings.monitoring.enabled" type="checkbox" /> {{ tr('开启 Prometheus Exporter', 'Enable Prometheus Exporter') }}</label>
      <p class="hint">{{ tr('开启后对外暴露 Kafka Exporter 指标，Prometheus 可抓取总地址或每个集群的独立地址。', 'When enabled, exposes Kafka Exporter metrics. Prometheus can scrape the global URL or each cluster\'s independent URL.') }}</p>
      <div class="metrics-list">
        <div class="metrics-row"><span>{{ tr('全部集群', 'All Clusters') }}</span><code>{{ metricsUrl }}</code></div>
        <div v-for="cluster in clusters" :key="cluster.id" class="metrics-row"><span>{{ cluster.name }}</span><code>{{ metricsClusterUrl(cluster) }}</code></div>
      </div>
    </section>

    <section v-if="activeTab === 'monitoringAlerts'" class="panel monitoring-alerts-panel">
      <div class="panel-title"><h2>{{ tr('监控告警', 'Monitoring Alerts') }}</h2><div class="actions"><button class="ghost small" @click="openAlertConfigPage">{{ tr('规则配置文件', 'Rule Config') }}</button><span class="edition-pill">Alerting</span></div></div>
      <p class="hint">{{ tr('配置积压、Broker 资源与离线告警通知。', 'Configure lag, broker resource, and broker offline alert notifications.') }}</p>
      <label class="check"><input v-model="settings.monitoring.alerting.enabled" type="checkbox" /> {{ tr('开启监控告警', 'Enable Monitoring Alerts') }}</label>
      <div class="alert-rule-list compact-rules">
        <div class="alert-rule-head">
          <span>{{ tr('状态', 'Status') }}</span>
          <span>{{ tr('规则', 'Rule') }}</span>
          <span>{{ tr('适用集群', 'Clusters') }}</span>
          <span>{{ tr('方向', 'Direction') }}</span>
          <span>{{ tr('阈值', 'Threshold') }}</span>
          <span>{{ tr('单位', 'Unit') }}</span>
          <span>{{ tr('当前值', 'Current') }}</span>
        </div>
        <div v-for="rule in settings.monitoring.alerting.rules" :key="rule.key" :class="['alert-rule-row', { enabled: rule.enabled }]">
          <label class="switch rule-switch"><input v-model="rule.enabled" type="checkbox" /><span></span></label>
          <strong class="rule-title">{{ alertRuleTitle(rule.key, rule.name) }}<span class="rule-help">?<span class="rule-tooltip">{{ alertRuleDesc(rule.key) }}</span></span></strong>
          <details v-if="alertRuleUsesMigrationJobs(rule.key)" class="cluster-dropdown migration-job-dropdown" :open="openClusterDropdown === rule.key" @toggle="onClusterDropdownToggle($event, rule.key)">
            <summary @click.prevent="toggleClusterDropdown(rule.key)">{{ selectedMigrationJobLabel(rule) }}</summary>
            <div class="cluster-dropdown-menu">
              <button type="button" :class="['cluster-select-all', { active: !rule.migration_job_ids?.length }]" @click="rule.migration_job_ids = []">{{ tr('所有迁移任务', 'All Migration Jobs') }}</button>
              <label v-for="job in migrationJobs" :key="job.id" class="cluster-check"><input v-model="rule.migration_job_ids" type="checkbox" :value="job.id" /> {{ migrationJobOptionLabel(job) }}</label>
              <em v-if="!migrationJobs.length" class="dropdown-empty">{{ tr('暂无迁移任务', 'No migration jobs') }}</em>
            </div>
          </details>
          <span v-else-if="!alertRuleUsesClusters(rule.key)" class="rule-global-scope">{{ tr('全局规则', 'Global Rule') }}</span>
          <details v-else class="cluster-dropdown" :open="openClusterDropdown === rule.key" @toggle="onClusterDropdownToggle($event, rule.key)">
            <summary @click.prevent="toggleClusterDropdown(rule.key)">{{ selectedClusterLabel(rule) }}</summary>
            <div class="cluster-dropdown-menu">
              <button type="button" :class="['cluster-select-all', { active: !rule.cluster_ids?.length }]" @click="rule.cluster_ids = []">{{ tr('全部集群', 'All Clusters') }}</button>
              <label v-for="cluster in clusters" :key="cluster.id" class="cluster-check"><input v-model="rule.cluster_ids" type="checkbox" :value="cluster.id" /> {{ cluster.name }}</label>
            </div>
          </details>
          <select v-model="rule.direction" class="rule-direction">
            <option value=">=">&gt;=</option>
            <option value=">">&gt;</option>
            <option value="<=">&lt;=</option>
            <option value="<">&lt;</option>
          </select>
          <input v-model.number="rule.threshold" type="number" min="0" class="rule-threshold-input" />
          <em>{{ alertRuleUnit(rule.unit) }}</em>
          <div class="rule-current-value">
            <button class="ghost small" :disabled="checkingRuleKey === rule.key" @click="checkAlertRuleValue(rule)">{{ checkingRuleKey === rule.key ? tr('查看中...', 'Checking...') : tr('查看当前值', 'View Value') }}</button>
            <small v-if="alertRuleValueText(rule.key)">{{ alertRuleValueText(rule.key) }}</small>
          </div>
        </div>
      </div>
      <div class="panel-subtitle"><h3>{{ tr('通知渠道', 'Notification Channels') }}</h3><span>{{ tr('填写需要启用的通知地址，可同时配置多个。', 'Fill in notification endpoints; multiple channels can be configured.') }}</span></div>
      <div class="notify-channel-list">
        <div class="notify-channel-row">
          <div><strong>{{ tr('钉钉机器人', 'DingTalk Bot') }}</strong><span>{{ tr('填写钉钉自定义机器人 Webhook，告警会发送到对应群。', 'Enter a DingTalk custom bot webhook; alerts are sent to that group.') }}</span></div>
          <div class="notify-input"><input v-model="settings.monitoring.alerting.dingtalk_webhook" placeholder="https://oapi.dingtalk.com/robot/send?access_token=..." /><button class="ghost small" :disabled="!settings.monitoring.alerting.enabled || testingChannel === 'dingtalk' || !settings.monitoring.alerting.dingtalk_webhook" @click="sendTestNotification('dingtalk')">{{ testingChannel === 'dingtalk' ? tr('发送中...', 'Sending...') : tr('测试发送', 'Test') }}</button></div>
          <label class="check notify-enabled"><input v-model="settings.monitoring.alerting.dingtalk_enabled" type="checkbox" /> {{ tr('启用', 'Enabled') }}</label>
        </div>
        <div class="notify-channel-row">
          <div><strong>{{ tr('飞书机器人', 'Feishu Bot') }}</strong><span>{{ tr('填写飞书群机器人 Webhook，适合运维告警群。', 'Enter a Feishu bot webhook for operations alert groups.') }}</span></div>
          <div class="notify-input"><input v-model="settings.monitoring.alerting.feishu_webhook" placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/..." /><button class="ghost small" :disabled="!settings.monitoring.alerting.enabled || testingChannel === 'feishu' || !settings.monitoring.alerting.feishu_webhook" @click="sendTestNotification('feishu')">{{ testingChannel === 'feishu' ? tr('发送中...', 'Sending...') : tr('测试发送', 'Test') }}</button></div>
          <label class="check notify-enabled"><input v-model="settings.monitoring.alerting.feishu_enabled" type="checkbox" /> {{ tr('启用', 'Enabled') }}</label>
        </div>
        <div class="notify-channel-row smtp-row">
          <div><strong>{{ tr('邮件 SMTP', 'Email SMTP') }}</strong><span>{{ tr('配置 SMTP 服务器、账号密码和收件人，告警会直接发送邮件。', 'Configure SMTP server, credentials, and recipients; alerts are sent by email directly.') }}</span></div>
          <div class="smtp-config-grid">
            <input v-model="settings.monitoring.alerting.email_smtp_host" :placeholder="tr('SMTP 服务器，例如 smtp.example.com', 'SMTP host, e.g. smtp.example.com')" />
            <input v-model.number="settings.monitoring.alerting.email_smtp_port" type="number" min="1" placeholder="587" />
            <input v-model="settings.monitoring.alerting.email_smtp_username" :placeholder="tr('SMTP 账号', 'SMTP username')" />
            <input v-model="settings.monitoring.alerting.email_smtp_password" type="password" :placeholder="tr('SMTP 密码，留空不修改', 'SMTP password, leave empty to keep')" />
            <input v-model="settings.monitoring.alerting.email_from" :placeholder="tr('发件人，例如 alert@example.com', 'From, e.g. alert@example.com')" />
            <input v-model="settings.monitoring.alerting.email_to" :placeholder="tr('收件人，多个用逗号分隔', 'Recipients, comma-separated')" />
            <label class="check smtp-tls"><input v-model="settings.monitoring.alerting.email_use_tls" type="checkbox" /> {{ tr('使用 SSL/TLS（465 端口常用）', 'Use SSL/TLS, usually port 465') }}</label>
            <label class="check smtp-tls"><input v-model="settings.monitoring.alerting.email_smtp_enabled" type="checkbox" /> {{ tr('启用邮件通知', 'Enable Email') }}</label>
            <button class="ghost small" :disabled="!settings.monitoring.alerting.enabled || testingChannel === 'email' || !emailSmtpReady" @click="sendTestNotification('email')">{{ testingChannel === 'email' ? tr('发送中...', 'Sending...') : tr('测试发送', 'Test') }}</button>
          </div>
        </div>
      </div>
    </section>

    <section v-if="activeTab === 'alertConfig'" class="panel alert-config-page">
      <div class="panel-title">
        <div><h2>{{ tr('规则配置文件', 'Rule Config File') }}</h2><p class="hint">{{ tr('独立页面在线编辑监控告警规则 JSON。应用配置后返回告警页确认，点击保存设置后持久化。', 'Edit monitoring alert rule JSON on a dedicated page. Apply it, return to alerts, then save settings to persist.') }}</p></div>
        <div class="actions"><button class="ghost" @click="backToAlertSettings">{{ tr('返回监控告警', 'Back to Alerts') }}</button></div>
      </div>
      <div class="alert-config-editor page-mode">
        <div class="config-editor-head">
          <div><h3>{{ tr('规则配置文件', 'Rule Config File') }}</h3><span>{{ tr('支持在线编辑 JSON 配置，应用后会同步到上方规则卡片。', 'Edit JSON config online; applying it syncs to the rule cards above.') }}</span></div>
          <div class="config-editor-actions"><button class="ghost small" @click="refreshAlertRulesConfig">{{ tr('从当前规则生成', 'Generate From Current') }}</button><button class="primary small" @click="applyAlertRulesConfig">{{ tr('应用配置', 'Apply Config') }}</button></div>
        </div>
        <textarea v-model="alertRulesConfigText" spellcheck="false" class="config-editor-textarea"></textarea>
        <p class="hint">{{ tr('字段：key/name/enabled/threshold/unit/direction/cluster_ids/migration_job_ids。cluster_ids 为空表示全部集群；migration_job_ids 为空表示所有迁移任务。', 'Fields: key/name/enabled/threshold/unit/direction/cluster_ids/migration_job_ids. Empty cluster_ids means all clusters; empty migration_job_ids means all migration jobs.') }}</p>
      </div>
    </section>

    <section v-if="activeTab === 'users'" class="panel users-panel">
      <div class="panel-title">
        <h2>{{ tr('用户与角色', 'Users & Roles') }}</h2>
        <div class="actions">
          <button class="primary" :disabled="isExternalAuth" :title="isExternalAuth ? tr('LDAP/OIDC 启用时不能创建本地用户', 'Local user creation is disabled when LDAP/OIDC is enabled') : ''" @click="openUserDialog">{{ tr('创建用户', 'Create User') }}</button>
          <button class="primary" @click="openRoleDialog">{{ tr('创建角色', 'Create Role') }}</button>
          <button class="ghost" @click="loadUsersAndRoles">{{ tr('刷新', 'Refresh') }}</button>
        </div>
      </div>
      <p v-if="isExternalAuth" class="hint warn">{{ tr('已启用 LDAP 或 OIDC，本地用户创建已禁用。请通过创建角色并为 LDAP/OIDC 用户分配角色来授权。', 'LDAP or OIDC is enabled. Local user creation is disabled. Create roles and assign them to LDAP/OIDC users for authorization.') }}</p>
      <div class="role-list">
        <span v-for="role in roles" :key="role.name" class="role-chip">{{ role.name }}<button v-if="!['admin','user'].includes(role.name)" @click="removeRole(role.name)">×</button></span>
      </div>
      <div class="user-table">
        <div class="user-head"><span>{{ tr('用户名', 'Username') }}</span><span>{{ tr('显示名', 'Display Name') }}</span><span>{{ tr('来源', 'Source') }}</span><span>{{ tr('角色', 'Role') }}</span><span>{{ tr('状态', 'Status') }}</span><span>{{ tr('操作', 'Actions') }}</span></div>
        <div v-for="u in users" :key="u.username" class="user-row">
          <span>{{ u.username }}</span><span>{{ u.display_name || '-' }}</span><span :class="{ 'source-external': u.source === 'ldap' || u.source === 'oidc' }">{{ u.source || 'local' }}</span>
          <select v-model="u.role" @change="saveUser(u)"><option v-for="role in roles" :key="role.name" :value="role.name">{{ role.name }}</option></select>
          <label class="check"><input v-model="u.is_active" type="checkbox" @change="saveUser(u)" /> {{ tr('启用', 'Active') }}</label>
          <div class="user-actions">
            <button class="ghost small" :disabled="isExternalAuth && u.source !== 'local'" @click="resetPassword(u)">{{ tr('重置密码', 'Reset Password') }}</button>
            <button v-if="u.source === 'local' && u.username !== 'admin'" class="ghost small danger" :disabled="isExternalAuth" @click="removeUser(u)">{{ tr('删除', 'Delete') }}</button>
          </div>
        </div>
      </div>
    </section>

    <div v-if="showUserDialog" class="modal-backdrop" @click.self="showUserDialog = false">
      <form class="modal-card" @submit.prevent="createUser">
        <div class="modal-title"><h3>{{ tr('创建用户', 'Create User') }}</h3><button type="button" class="modal-close" @click="showUserDialog = false">×</button></div>
        <input v-model="userForm.username" :placeholder="tr('用户名，如 demo', 'Username, e.g. demo')" autofocus />
        <input v-model="userForm.display_name" :placeholder="tr('显示名，可选', 'Display name, optional')" />
        <input v-model="userForm.email" :placeholder="tr('邮箱，可选', 'Email, optional')" />
        <input v-model="userForm.password" type="password" :placeholder="tr('初始密码', 'Initial password')" />
        <select v-model="userForm.role"><option v-for="role in roles" :key="role.name" :value="role.name">{{ role.name }}</option></select>
        <div class="modal-actions"><button type="button" class="ghost" @click="showUserDialog = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" :disabled="!userForm.username || !userForm.password">{{ tr('创建用户', 'Create User') }}</button></div>
      </form>
    </div>

    <div v-if="showRoleDialog" class="modal-backdrop" @click.self="showRoleDialog = false">
      <form class="modal-card" @submit.prevent="createRole">
        <div class="modal-title"><h3>{{ tr('创建角色', 'Create Role') }}</h3><button type="button" class="modal-close" @click="showRoleDialog = false">×</button></div>
        <input v-model="roleForm.name" :placeholder="tr('角色名称，如 ops', 'Role name, e.g. ops')" autofocus />
        <input v-model="roleForm.description" :placeholder="tr('角色描述，可选', 'Role description, optional')" />
        <div class="modal-actions"><button type="button" class="ghost" @click="showRoleDialog = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" :disabled="!roleForm.name">{{ tr('创建角色', 'Create Role') }}</button></div>
      </form>
    </div>

    <section v-if="activeTab === 'kafkaAuth'" class="panel kafka-auth-panel">
      <div class="panel-title">
        <div>
          <h2>{{ tr('Kafka 授权', 'Kafka Auth') }}</h2>
          <p class="hint">{{ tr('按用户或角色配置 Kafka 集群细粒度权限。用户会同时继承所属角色的 Kafka 授权。', 'Configure fine-grained Kafka cluster permissions by user or role. Users also inherit permissions from their roles.') }}</p>
        </div>
        <button class="ghost" @click="loadKafkaAuth">{{ tr('刷新授权', 'Refresh Auth') }}</button>
      </div>
      <div class="auth-toolbar">
        <select v-model="authSubjectType" @change="loadClusterPermission"><option value="user">{{ tr('授权用户', 'Authorize User') }}</option><option value="role">{{ tr('授权角色', 'Authorize Role') }}</option></select>
        <select v-if="authSubjectType === 'user'" v-model="authUsername" @change="loadClusterPermission"><option value="">{{ tr('选择用户', 'Select User') }}</option><option v-for="u in users" :key="u.username" :value="u.username">{{ u.username }} / {{ u.display_name || '-' }}</option></select>
        <select v-else v-model="authRole" @change="loadClusterPermission"><option value="">{{ tr('选择角色', 'Select Role') }}</option><option v-for="r in roles" :key="r.name" :value="r.name">{{ r.name }} / {{ r.description || '-' }}</option></select>
        <select v-model="authClusterId" @change="loadClusterPermission"><option value="">{{ tr('选择 Kafka 集群', 'Select Kafka Cluster') }}</option><option v-for="c in clusters" :key="c.id" :value="c.id">{{ c.name }} / {{ c.bootstrap_servers }}</option></select>
      </div>
      <div class="permission-grid">
        <label v-for="action in actionOptions" :key="action.value" class="permission-card">
          <input v-model="authActions" type="checkbox" :value="action.value" />
          <strong>{{ action.label }}</strong>
          <span>{{ action.desc }}</span>
        </label>
      </div>
      <div class="auth-actions">
        <button class="ghost" :disabled="!currentAuthSubject || !authClusterId" @click="selectReadOnly">{{ tr('只读权限', 'Read Only') }}</button>
        <button class="ghost" :disabled="!currentAuthSubject || !authClusterId" @click="selectOps">{{ tr('运维权限', 'Ops') }}</button>
        <button class="ghost" :disabled="!currentAuthSubject || !authClusterId" @click="authActions = []">{{ tr('清空', 'Clear') }}</button>
        <button class="primary" :disabled="!currentAuthSubject || !authClusterId || authSaving" @click="saveClusterPermission">{{ authSaving ? tr('保存中...', 'Saving...') : tr('保存 Kafka 授权', 'Save Kafka Auth') }}</button>
      </div>
    </section>

    <div v-if="noticeDialog.visible" class="modal-backdrop" @click.self="noticeDialog.visible = false">
      <div :class="['notice-card', noticeDialog.type]">
        <div class="notice-icon">{{ noticeDialog.type === 'error' ? '!' : noticeDialog.type === 'warn' ? 'i' : '✓' }}</div>
        <div class="notice-body">
          <p class="notice-eyebrow">{{ noticeDialog.eyebrow }}</p>
          <h3>{{ noticeDialog.title }}</h3>
          <div class="notice-message">
            <p v-for="(line, index) in noticeMessageLines" :key="index">{{ line }}</p>
          </div>
        </div>
        <div class="modal-actions notice-actions"><button class="primary" @click="noticeDialog.visible = false">{{ tr('知道了', 'OK') }}</button></div>
      </div>
    </div>

  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { createSystemRole, createSystemUser, deleteSystemRole, deleteSystemUser, getAlertRuleValue, getAppStatus, getSystemSettings, getSsoStatus, listKafkaClusters, listKafkaMigrations, listKafkaPermissions, listSystemRoles, listSystemUsers, saveKafkaPermissions, saveSystemSettings, syncLdapUsers, testLdapSettings, testNotificationChannel, updateSystemUser } from '../api'
import { setLanguage, tr, language } from '../i18n'
import { applyTheme, getTheme, type ThemeMode } from '../theme'

const route = useRoute()
const router = useRouter()
const settings = ref<any>({ rbac: {}, ldap: {}, oidc: {} })
const license = ref<any>({ enterprise: true, edition: 'enterprise', language: 'zh-CN' })
const ssoStatus = ref({ ldap_enabled: false, oidc_enabled: false })
const users = ref<any[]>([])
const roles = ref<any[]>([])
const clusters = ref<any[]>([])
const migrationJobs = ref<any[]>([])
const activeTab = ref<'basic' | 'monitoring' | 'monitoringAlerts' | 'alertConfig' | 'users' | 'kafkaAuth'>('basic')
const saving = ref(false)
const authSaving = ref(false)
const testingChannel = ref('')
const checkingRuleKey = ref('')
const alertRuleValues = ref<Record<string, any[]>>({})
const alertRulesConfigText = ref('')
const openClusterDropdown = ref('')
const adminRolesText = ref('')
const authUsername = ref('')
const authSubjectType = ref<'user' | 'role'>('user')
const authRole = ref('')
const authClusterId = ref('')
const authActions = ref<string[]>([])
const clusterPermissions = ref<any[]>([])
const roleForm = ref({ name: '', description: '' })
const userForm = ref({ username: '', display_name: '', email: '', role: 'user', password: '' })
const showUserDialog = ref(false)
const showRoleDialog = ref(false)
const noticeDialog = ref({ visible: false, type: 'success', eyebrow: 'Success', title: '', message: '' })
const isExternalAuth = computed(() => ssoStatus.value.ldap_enabled === true || ssoStatus.value.oidc_enabled === true)
const currentAuthSubject = computed(() => authSubjectType.value === 'role' ? authRole.value : authUsername.value)
const emailSmtpReady = computed(() => !!settings.value.monitoring?.alerting?.email_smtp_host && !!settings.value.monitoring?.alerting?.email_from && !!settings.value.monitoring?.alerting?.email_to)
const noticeMessageLines = computed(() => String(noticeDialog.value.message || '').split('\n').filter(Boolean))

const showNotice = (title: string, message = '', type: 'success' | 'error' | 'warn' = 'success') => {
  noticeDialog.value = { visible: true, type, eyebrow: type === 'error' ? 'Error' : type === 'warn' ? 'Notice' : 'Success', title, message }
}

const errorMessage = (err: any, fallback: string) => err?.response?.data?.detail || err?.message || fallback

const defaultAlertRules = () => [
  { key: 'consumer_group_lag', name: 'Consumer Group Lag', enabled: true, threshold: 10000, unit: 'messages', direction: '>=', cluster_ids: [] },
  { key: 'broker_offline', name: 'Broker Offline', enabled: true, threshold: 1, unit: 'broker', direction: '>=', cluster_ids: [] },
  { key: 'broker_unavailable', name: 'Broker Unavailable', enabled: true, threshold: 1, unit: 'broker', direction: '>=', cluster_ids: [] },
  { key: 'under_replicated_partition', name: 'Under Replicated Partition', enabled: true, threshold: 1, unit: 'partition', direction: '>=', cluster_ids: [] },
  { key: 'offline_partition', name: 'Offline Partition', enabled: true, threshold: 1, unit: 'partition', direction: '>=', cluster_ids: [] },
  { key: 'topic_partition_count', name: 'Topic Partition Count', enabled: false, threshold: 2000, unit: 'partition', direction: '>=', cluster_ids: [] },
  { key: 'topic_log_size', name: 'Topic Log Size', enabled: false, threshold: 107374182400, unit: 'bytes', direction: '>=', cluster_ids: [] },
  { key: 'consumer_member_zero', name: 'Consumer Group No Members', enabled: true, threshold: 1, unit: 'group', direction: '>=', cluster_ids: [] },
  { key: 'migration_incremental_sync_abnormal', name: 'Migration Incremental Sync Abnormal', enabled: true, threshold: 1, unit: 'job', direction: '>=', cluster_ids: [], migration_job_ids: [] },
]

const alertRuleTitles: Record<string, [string, string]> = {
  consumer_group_lag: ['消费组积压', 'Consumer Group Lag'],
  broker_offline: ['Broker 离线', 'Broker Offline'],
  broker_unavailable: ['Broker 不可用', 'Broker Unavailable'],
  under_replicated_partition: ['副本不同步分区', 'Under Replicated Partitions'],
  offline_partition: ['离线分区', 'Offline Partitions'],
  topic_partition_count: ['Topic 分区数过高', 'Topic Partition Count'],
  topic_log_size: ['Topic 日志容量过高', 'Topic Log Size'],
  consumer_member_zero: ['消费组无成员', 'Consumer Group No Members'],
  migration_incremental_sync_abnormal: ['迁移增量同步异常', 'Migration Incremental Sync Abnormal'],
}

const alertRuleDescriptions: Record<string, [string, string]> = {
  consumer_group_lag: ['消费组总积压达到阈值时告警，用于发现消费堆积。', 'Alerts when total consumer group lag reaches the threshold.'],
  broker_offline: ['可用 Broker 数低于阈值时告警，用于发现 Broker 离线。', 'Alerts when available broker count is below the threshold.'],
  broker_unavailable: ['不可用 Broker 数达到阈值时告警。', 'Alerts when unavailable broker count reaches the threshold.'],
  under_replicated_partition: ['副本不同步分区数达到阈值时告警。', 'Alerts when under-replicated partitions reach the threshold.'],
  offline_partition: ['离线分区数达到阈值时告警。', 'Alerts when offline partitions reach the threshold.'],
  topic_partition_count: ['Topic 数达到阈值时告警，用于发现集群规模过大。', 'Alerts when topic count reaches the threshold.'],
  topic_log_size: ['单个 Topic 日志大小达到阈值时告警。', 'Alerts when a topic log size reaches the threshold.'],
  consumer_member_zero: ['无在线成员的消费组数量达到阈值时告警。', 'Alerts when groups without active members reach the threshold.'],
  migration_incremental_sync_abnormal: ['平滑迁移任务开启增量同步后失败时告警，用于发现增量同步异常中断。', 'Alerts when a smooth migration with incremental sync enabled fails.'],
}

const alertRuleTitle = (key: string, fallback: string) => {
  const item = alertRuleTitles[key]
  return item ? tr(item[0], item[1]) : fallback
}
const alertRuleDesc = (key: string) => {
  const item = alertRuleDescriptions[key]
  return item ? tr(item[0], item[1]) : tr('该规则达到配置阈值时触发告警。', 'This rule alerts when the configured threshold is reached.')
}
const alertRuleUnit = (unit: string) => unit === 'messages' ? tr('条消息', 'messages') : unit === 'broker' ? 'broker' : unit === 'partition' ? tr('个分区', 'partitions') : unit === 'group' ? 'group' : unit === 'job' ? tr('个任务', 'jobs') : unit
const alertRuleValueText = (key: string) => {
  const items = alertRuleValues.value[key] || []
  if (!items.length) return ''
  const first = items[0]
  const value = first.available === false || first.value === null || first.value === undefined ? tr('暂无数据', 'No data') : `${first.value} ${alertRuleUnit(first.unit || '')}`.trim()
  const suffix = items.length > 1 ? tr(`等 ${items.length} 个集群`, `${items.length} clusters`) : (first.cluster || '')
  return suffix ? `${suffix}: ${value}` : value
}
const alertRuleUsesClusters = (key: string) => key !== 'migration_incremental_sync_abnormal'
const alertRuleUsesMigrationJobs = (key: string) => key === 'migration_incremental_sync_abnormal'
const supportedAlertRuleKeys = () => new Set(defaultAlertRules().map((rule: any) => rule.key))
const selectedClusterLabel = (rule: any) => {
  if (!alertRuleUsesClusters(rule.key)) return tr('全局规则', 'Global Rule')
  const ids = Array.isArray(rule.cluster_ids) ? rule.cluster_ids : []
  if (ids.length === 0) return tr('全部集群', 'All Clusters')
  if (ids.length === 1) return clusters.value.find(cluster => cluster.id === ids[0])?.name || tr('已选 1 个', '1 selected')
  return tr(`已选 ${ids.length} 个集群`, `${ids.length} selected`)
}
const selectedMigrationJobLabel = (rule: any) => {
  const ids = Array.isArray(rule.migration_job_ids) ? rule.migration_job_ids : []
  if (ids.length === 0) return tr('所有迁移任务', 'All Migration Jobs')
  if (ids.length === 1) return migrationJobs.value.find(job => job.id === ids[0])?.id || tr('已选 1 个任务', '1 job selected')
  return tr(`已选 ${ids.length} 个任务`, `${ids.length} jobs selected`)
}
const migrationJobOptionLabel = (job: any) => `${job.id} / ${job.source_cluster || '-'} -> ${job.target_cluster || '-'} / ${statusLabel(job.status)}`
const statusLabel = (status: string) => status === 'completed' ? tr('已完成', 'Completed') : status === 'failed' ? tr('失败', 'Failed') : status === 'running' ? tr('运行中', 'Running') : status === 'incremental' ? tr('增量同步中', 'Incremental Syncing') : status === 'stopping' ? tr('停止中', 'Stopping') : status === 'stopped' ? tr('已停止', 'Stopped') : tr('排队中', 'Queued')

const toggleClusterDropdown = (key: string) => {
  openClusterDropdown.value = openClusterDropdown.value === key ? '' : key
}

const onClusterDropdownToggle = (event: Event, key: string) => {
  const details = event.currentTarget as HTMLDetailsElement
  if (details.open && openClusterDropdown.value !== key) {
    openClusterDropdown.value = key
  }
}

const closeClusterDropdownOnOutsideClick = (event: MouseEvent) => {
  const target = event.target as HTMLElement | null
  if (!target?.closest('.cluster-dropdown')) {
    openClusterDropdown.value = ''
  }
}

const normalizeAlertRule = (rule: any) => ({
  ...rule,
  cluster_ids: alertRuleUsesClusters(rule.key) && Array.isArray(rule.cluster_ids) ? rule.cluster_ids : [],
  migration_job_ids: alertRuleUsesMigrationJobs(rule.key) && Array.isArray(rule.migration_job_ids) ? rule.migration_job_ids : [],
})

const ensureAlerting = () => {
  settings.value.monitoring ||= { enabled: false, dashboard_enabled: false, topic_limit: 200, group_limit: 200 }
  settings.value.monitoring.alerting ||= { enabled: false, lag_threshold: 10000, broker_resource_threshold: 85, offline_broker_alert: true, cooldown_minutes: 10, dingtalk_webhook: '', feishu_webhook: '', email_webhook: '', email_smtp_host: '', email_smtp_port: 587, email_smtp_username: '', email_smtp_password: '', email_from: '', email_to: '', email_use_tls: false, rules: defaultAlertRules() }
  if (settings.value.monitoring.alerting.dingtalk_enabled === undefined) settings.value.monitoring.alerting.dingtalk_enabled = true
  if (settings.value.monitoring.alerting.feishu_enabled === undefined) settings.value.monitoring.alerting.feishu_enabled = true
  if (settings.value.monitoring.alerting.email_smtp_enabled === undefined) settings.value.monitoring.alerting.email_smtp_enabled = true
  settings.value.monitoring.alerting.email_smtp_port ||= 587
  if (!Array.isArray(settings.value.monitoring.alerting.rules) || settings.value.monitoring.alerting.rules.length === 0) settings.value.monitoring.alerting.rules = defaultAlertRules()
  const supported = supportedAlertRuleKeys()
  settings.value.monitoring.alerting.rules = settings.value.monitoring.alerting.rules.filter((rule: any) => supported.has(rule.key))
  const existingRuleKeys = new Set(settings.value.monitoring.alerting.rules.map((rule: any) => rule.key))
  settings.value.monitoring.alerting.rules = [...settings.value.monitoring.alerting.rules, ...defaultAlertRules().filter((rule: any) => !existingRuleKeys.has(rule.key))]
  settings.value.monitoring.alerting.rules = settings.value.monitoring.alerting.rules.map(normalizeAlertRule)
}

const refreshAlertRulesConfig = () => {
  ensureAlerting()
  alertRulesConfigText.value = JSON.stringify(settings.value.monitoring.alerting.rules, null, 2)
}

const openAlertConfigPage = () => {
  refreshAlertRulesConfig()
  activeTab.value = 'alertConfig'
  router.push({ name: 'alertRules' })
}

const backToAlertSettings = () => {
  activeTab.value = 'monitoringAlerts'
  router.push({ name: 'settings' })
}

const applyAlertRulesConfig = () => {
  try {
    const parsed = JSON.parse(alertRulesConfigText.value)
    if (!Array.isArray(parsed)) throw new Error(tr('配置根节点必须是数组', 'Config root must be an array'))
    settings.value.monitoring.alerting.rules = parsed.map((item: any) => ({
      key: String(item.key || '').trim(),
      name: String(item.name || item.key || '').trim(),
      enabled: item.enabled === true,
      threshold: Number(item.threshold || 0),
      unit: String(item.unit || ''),
      direction: String(item.direction || '>='),
      cluster_ids: alertRuleUsesClusters(String(item.key || '').trim()) && Array.isArray(item.cluster_ids) ? item.cluster_ids.map((id: any) => String(id)).filter(Boolean) : [],
      migration_job_ids: alertRuleUsesMigrationJobs(String(item.key || '').trim()) && Array.isArray(item.migration_job_ids) ? item.migration_job_ids.map((id: any) => String(id)).filter(Boolean) : [],
    })).filter((item: any) => item.key)
    ensureAlerting()
    refreshAlertRulesConfig()
    showNotice(tr('配置已应用', 'Config Applied'), tr('告警规则配置已应用，点击保存设置后生效。', 'Alert rule config applied. Save settings to persist it.'))
  } catch (err: any) {
    showNotice(tr('配置格式错误', 'Invalid Config'), err.message || tr('请检查 JSON 格式。', 'Check the JSON format.'), 'error')
  }
}

const actionOptions = computed(() => [
  { value: 'cluster_view', label: tr('查看集群', 'View Cluster'), desc: tr('允许看到并进入该 Kafka 集群', 'Allows viewing and entering this Kafka cluster') },
  { value: 'topic_create', label: tr('创建 Topic', 'Create Topic'), desc: tr('允许新增 Topic', 'Allows creating new topics') },
  { value: 'topic_delete', label: tr('删除 Topic', 'Delete Topic'), desc: tr('允许删除 Topic', 'Allows deleting topics') },
  { value: 'topic_config_manage', label: tr('管理 Topic 配置', 'Manage Topic Config'), desc: tr('允许修改 retention、cleanup.policy 等配置', 'Allows modifying retention, cleanup.policy, etc.') },
  { value: 'message_read', label: tr('读取消息', 'Read Messages'), desc: tr('允许消费和查询 Topic 消息', 'Allows consuming and querying topic messages') },
  { value: 'message_send', label: tr('写入消息', 'Write Messages'), desc: tr('允许单条或批量 Produce Message', 'Allows single or batch produce messages') },
  { value: 'message_delete', label: tr('删除消息', 'Delete Messages'), desc: tr('允许按 offset 删除记录', 'Allows deleting records by offset') },
  { value: 'group_create', label: tr('创建 Group', 'Create Group'), desc: tr('登记创建 Consumer Group 操作', 'Registers create consumer group operation') },
  { value: 'group_delete', label: tr('删除 Group', 'Delete Group'), desc: tr('允许删除 Consumer Group', 'Allows deleting consumer groups') },
  { value: 'permission_manage', label: tr('Kafka 授权', 'Kafka Auth'), desc: tr('允许管理该集群的 Kafka 授权', 'Allows managing Kafka permissions for this cluster') },
])

const loadSettings = async () => {
  settings.value = await getSystemSettings()
  ssoStatus.value = await getSsoStatus()
  migrationJobs.value = await listKafkaMigrations().catch(() => [])
  settings.value.ui ||= { language: 'zh-CN' }
  settings.value.ui.theme ||= getTheme()
  settings.value.ui.platform_name ||= 'kafkaVista'
  settings.value.ui.logo_url ||= '/favicon.png?v=2026052102'
  ensureAlerting()
  settings.value.monitoring.topic_limit ||= 200
  settings.value.monitoring.group_limit ||= 200
  settings.value.license ||= { active_key: '', keys: [] }
  settings.value.license.keys ||= []
  settings.value.license.active_key ||= ''
  if (settings.value.oidc?.enabled) {
    settings.value.ldap.enabled = false
  } else if (settings.value.ldap?.enabled) {
    settings.value.oidc.enabled = false
  }
  license.value = await getAppStatus()
  settings.value.ui.language = settings.value.ui.language || license.value.language || 'zh-CN'
  setLanguage(settings.value.ui.language)
  adminRolesText.value = (settings.value.oidc?.admin_roles || []).join(',')
  refreshAlertRulesConfig()
  if (route.name === 'alertRules') activeTab.value = 'alertConfig'
}

watch(() => settings.value.oidc?.enabled, (enabled) => {
  if (enabled) {
    settings.value.ldap.enabled = false
  }
})

watch(() => route.name, (name) => {
  if (name === 'alertRules') {
    refreshAlertRulesConfig()
    activeTab.value = 'alertConfig'
  }
})

const applyLanguage = () => setLanguage(settings.value.ui?.language || 'zh-CN')
const applyThemeSetting = () => applyTheme((settings.value.ui?.theme === 'light' ? 'light' : 'dark') as ThemeMode)
const metricsUrl = computed(() => `${window.location.origin}/metrics`)
const metricsClusterUrl = (cluster: any) => `${window.location.origin}/metrics/${encodeURIComponent(cluster.id)}`

const persistSettings = async () => {
  settings.value.rbac ||= { default_role: 'user', admin_users: ['admin'] }
  settings.value.license ||= { active_key: '', keys: [] }
  ensureAlerting()
  settings.value.monitoring.alerting.rules = settings.value.monitoring.alerting.rules.map(normalizeAlertRule)
  settings.value.oidc.admin_roles = splitList(adminRolesText.value)
  await saveSystemSettings(settings.value)
  await loadSettings()
}

const sendTestNotification = async (channel: string) => {
  testingChannel.value = channel
  try {
    await persistSettings()
    await testNotificationChannel(channel)
    showNotice(tr('测试消息已发送', 'Test Message Sent'), tr('请检查对应通知渠道。', 'Check the notification channel.'))
  } catch (err: any) {
    showNotice(tr('测试发送失败', 'Failed to Send Test'), errorMessage(err, tr('测试发送失败', 'Failed to send test message')), 'error')
  } finally {
    testingChannel.value = ''
  }
}

const checkAlertRuleValue = async (rule: any) => {
  checkingRuleKey.value = rule.key
  try {
    const data = await getAlertRuleValue(normalizeAlertRule(rule))
    alertRuleValues.value = { ...alertRuleValues.value, [rule.key]: data.items || [] }
    const lines = (data.items || []).map((item: any) => {
      const value = item.available === false || item.value === null || item.value === undefined ? tr('暂无数据', 'No data') : `${item.value} ${alertRuleUnit(item.unit || '')}`.trim()
      const state = item.error ? item.error : (item.triggered ? tr('已达到告警条件', 'Triggered') : tr('未达到告警条件', 'Not triggered'))
      const detail = item.detail && item.detail !== 'no_current_value_source' ? ` (${item.detail})` : ''
      return `${item.cluster || '-'}: ${value}${detail} - ${state}`
    })
    showNotice(tr('当前值', 'Current Value'), lines.length ? lines.join('\n') : tr('没有可用集群。', 'No available cluster.'))
  } catch (err: any) {
    showNotice(tr('当前值查询失败', 'Failed to Query Value'), errorMessage(err, tr('当前值查询失败', 'Failed to query current value')), 'error')
  } finally {
    checkingRuleKey.value = ''
  }
}

const loadUsers = async () => { users.value = await listSystemUsers() }
const loadRoles = async () => { roles.value = await listSystemRoles() }
const loadUsersAndRoles = async () => { await Promise.all([loadUsers(), loadRoles()]) }
const loadClusters = async () => { clusters.value = await listKafkaClusters() }
const splitList = (value: string) => value.split(',').map(item => item.trim()).filter(Boolean)

const onLdapToggle = () => {
  if (settings.value.ldap.enabled) {
    settings.value.oidc.enabled = false
  }
}

const onOidcToggle = () => {
  if (settings.value.oidc.enabled) {
    settings.value.ldap.enabled = false
  }
}

const saveAll = async () => {
  saving.value = true
  try {
    await persistSettings()
    showNotice(tr('保存成功', 'Saved Successfully'), tr('系统设置已保存并生效。', 'System settings have been saved and applied.'))
  } finally {
    saving.value = false
  }
}

const saveUser = async (u: any) => {
  await updateSystemUser(u.username, { role: u.role, is_active: u.is_active, display_name: u.display_name, email: u.email })
  await loadUsers()
}

const openUserDialog = () => {
  userForm.value = { username: '', display_name: '', email: '', role: roles.value[0]?.name || 'user', password: '' }
  showUserDialog.value = true
}

const openRoleDialog = () => {
  roleForm.value = { name: '', description: '' }
  showRoleDialog.value = true
}

const createUser = async () => {
  try {
    await createSystemUser(userForm.value)
    userForm.value = { username: '', display_name: '', email: '', role: 'user', password: '' }
    showUserDialog.value = false
    await loadUsers()
  } catch (err: any) {
    showNotice(tr('用户创建失败', 'Failed to Create User'), errorMessage(err, tr('用户创建失败', 'Failed to create user')), 'error')
  }
}

const resetPassword = async (u: any) => {
  const password = prompt(`为用户 ${u.username} 设置新密码`)
  if (!password) return
  await updateSystemUser(u.username, { password })
  showNotice(tr('密码已更新', 'Password Updated'))
}

const removeUser = async (u: any) => {
  if (!confirm(`删除用户 ${u.username}？`)) return
  await deleteSystemUser(u.username)
  await loadUsers()
}

const createRole = async () => {
  try {
    await createSystemRole(roleForm.value)
    roleForm.value = { name: '', description: '' }
    showRoleDialog.value = false
    await loadUsersAndRoles()
  } catch (err: any) {
    showNotice(tr('角色创建失败', 'Failed to Create Role'), errorMessage(err, tr('角色创建失败', 'Failed to create role')), 'error')
  }
}

const removeRole = async (role: string) => {
  if (!confirm(`删除角色 ${role}？使用该角色的用户会回退为 user。`)) return
  await deleteSystemRole(role)
  await loadUsersAndRoles()
}

const loadKafkaAuth = async () => {
  await Promise.all([loadUsersAndRoles(), loadClusters()])
  if (!authUsername.value && users.value.length) authUsername.value = users.value[0].username
  if (!authRole.value && roles.value.length) authRole.value = roles.value[0].name
  if (!authClusterId.value && clusters.value.length) authClusterId.value = clusters.value[0].id
  await loadClusterPermission()
}

const loadClusterPermission = async () => {
  authActions.value = []
  clusterPermissions.value = []
  if (!authClusterId.value || !currentAuthSubject.value) return
  clusterPermissions.value = await listKafkaPermissions(authClusterId.value)
  const row = clusterPermissions.value.find((item: any) => (item.subject_type || 'user') === authSubjectType.value && (item.subject || item.username || item.role) === currentAuthSubject.value)
  authActions.value = row?.actions ? [...row.actions] : []
}

const selectReadOnly = () => {
  authActions.value = ['cluster_view', 'message_read']
}

const selectOps = () => {
  authActions.value = ['cluster_view', 'topic_create', 'topic_config_manage', 'message_read', 'message_send', 'group_create']
}

const saveClusterPermission = async () => {
  if (!currentAuthSubject.value || !authClusterId.value || authSaving.value) return
  authSaving.value = true
  try {
    await saveKafkaPermissions(authClusterId.value, { subject_type: authSubjectType.value, username: authSubjectType.value === 'user' ? authUsername.value : undefined, role: authSubjectType.value === 'role' ? authRole.value : undefined, actions: authActions.value })
    await loadClusterPermission()
    showNotice(tr('Kafka 授权已保存', 'Kafka Permissions Saved'), tr('用户重新进入 Kafka 管理后即可看到最新权限。', 'Users can re-enter Kafka Management to see the latest permissions.'))
  } finally {
    authSaving.value = false
  }
}

const testLdap = async () => {
  try {
    const resp = await testLdapSettings(settings.value)
    showNotice(tr('LDAP 连接成功', 'LDAP Connected'), tr(`测试匹配 ${resp.data?.matched ?? 0} 个用户。`, `Matched ${resp.data?.matched ?? 0} users.`))
  } catch (err: any) {
    showNotice(tr('LDAP 测试失败', 'LDAP Test Failed'), errorMessage(err, tr('LDAP 测试失败', 'LDAP test failed')), 'error')
  }
}

const syncLdap = async () => {
  if (!confirm('确认从 LDAP 拉取用户并写入本地用户表？')) return
  try {
    const resp = await syncLdapUsers()
    showNotice(tr('LDAP 同步完成', 'LDAP Sync Complete'), tr(`写入 ${resp.data?.synced ?? 0} 个用户，匹配 ${resp.data?.matched ?? 0} 个 LDAP 条目。`, `Wrote ${resp.data?.synced ?? 0} users, matched ${resp.data?.matched ?? 0} LDAP entries.`))
    await loadUsers()
  } catch (err: any) {
    showNotice(tr('LDAP 同步失败', 'LDAP Sync Failed'), errorMessage(err, tr('LDAP 同步失败', 'LDAP sync failed')), 'error')
  }
}

onMounted(async () => {
  document.addEventListener('click', closeClusterDropdownOnOutsideClick)
  await loadSettings()
  await loadKafkaAuth()
})

onUnmounted(() => {
  document.removeEventListener('click', closeClusterDropdownOnOutsideClick)
})
</script>

<style scoped>
.settings-page { padding: 24px 20px; width: 100%; }
.hero { display: flex; justify-content: space-between; gap: 20px; align-items: flex-end; padding: 30px; border: 1px solid var(--border-color); border-radius: 26px; background: radial-gradient(circle at 12% 0, rgba(34,211,238,.25), transparent 35%), var(--bg-secondary); margin-bottom: 20px; }
.eyebrow { color: var(--accent-secondary); font-size: 12px; letter-spacing: .14em; text-transform: uppercase; margin-bottom: 8px; }
h1 { font-size: 32px; margin-bottom: 8px; } h2 { font-size: 18px; margin-bottom: 14px; } .hero p, .hint { color: var(--text-muted); font-size: 13px; }
.settings-tabs { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; padding: 8px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-secondary); }
.settings-tabs button { background: transparent; color: var(--text-secondary); border: 1px solid transparent; }
.settings-tabs button.active { color: #fff; background: var(--accent-primary); border-color: var(--accent-primary); }
.grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; margin-bottom: 16px; }
.panel { border: 1px solid var(--border-color); border-radius: 18px; background: var(--bg-secondary); padding: 18px; display: grid; gap: 10px; }
.compact-panel { align-content: start; min-height: 100%; }
.wide { grid-column: 1 / -1; }
label { color: var(--text-secondary); font-size: 13px; }
input, select { width: 100%; background: var(--bg-tertiary); border: 1px solid var(--border-color); color: var(--text-primary); padding: 9px 11px; border-radius: 9px; }
.setting-row { display: grid; grid-template-columns: minmax(0, 1fr) 140px; align-items: center; gap: 16px; padding: 10px 12px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-tertiary); }
.setting-row label { display: block; margin-bottom: 2px; }
.setting-row .hint { margin: 0; font-size: 12px; }
.compact-select { width: 140px; min-width: 140px; justify-self: end; padding: 7px 10px; }
.two { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
.check { display: flex; align-items: center; gap: 8px; } .check input { width: auto; }
.warn { color: var(--accent-warning); }
.source-external { color: var(--accent-secondary); font-weight: 600; }
button { border: 0; border-radius: 9px; padding: 9px 14px; cursor: pointer; font: inherit; }
.primary { background: var(--accent-primary); color: white; } .primary:disabled { opacity: .6; cursor: not-allowed; }
.ghost { background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); }
.small { padding: 6px 10px; font-size: 12px; }
.panel-title { display: flex; align-items: center; justify-content: space-between; }
.panel-subtitle { display: grid; gap: 3px; margin-top: 8px; padding-top: 12px; border-top: 1px solid var(--border-color); }
.panel-subtitle h3 { margin: 0; font-size: 15px; }
.panel-subtitle span { color: var(--text-muted); font-size: 12px; }
.alert-rule-list { margin: 12px 0; border: 1px solid var(--border-color); border-radius: 14px; overflow: auto; background: var(--bg-tertiary); }
.alert-rule-head, .alert-rule-row { display: grid; grid-template-columns: 70px minmax(180px, 1fr) 170px 100px 130px 100px 170px; gap: 10px; align-items: center; min-width: 1000px; padding: 8px 12px; border-bottom: 1px solid var(--border-color); }
.alert-rule-head { color: var(--text-muted); font-size: 12px; background: rgba(255,255,255,.035); }
.alert-rule-head span:last-child { width: 170px; text-align: center; }
.alert-rule-row:last-child { border-bottom: 0; }
.alert-rule-row.enabled { background: rgba(52,211,153,.06); }
.alert-rule-row strong { color: var(--text-primary); font-size: 13px; line-height: 1.3; }
.rule-title { display: inline-flex; align-items: center; gap: 6px; min-width: 0; }
.rule-help { position: relative; display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; flex: 0 0 auto; border-radius: 999px; border: 1px solid rgba(34,211,238,.32); color: var(--accent-secondary); background: rgba(34,211,238,.08); font-size: 11px; font-weight: 800; cursor: help; }
.rule-tooltip { position: absolute; left: 50%; bottom: calc(100% + 8px); z-index: 50; display: none; width: 260px; padding: 9px 10px; border: 1px solid rgba(34,211,238,.28); border-radius: 10px; background: rgba(15,23,42,.98); color: #dbeafe; box-shadow: 0 16px 42px rgba(0,0,0,.38); transform: translateX(-50%); font-size: 12px; font-weight: 500; line-height: 1.55; white-space: normal; text-align: left; }
.rule-tooltip::after { content: ''; position: absolute; left: 50%; top: 100%; width: 8px; height: 8px; background: rgba(15,23,42,.98); border-right: 1px solid rgba(34,211,238,.28); border-bottom: 1px solid rgba(34,211,238,.28); transform: translate(-50%, -50%) rotate(45deg); }
.rule-help:hover .rule-tooltip { display: block; }
.alert-rule-row em { color: var(--text-muted); font-style: normal; font-size: 12px; white-space: nowrap; }
.rule-current-value { display: grid; gap: 4px; align-self: start; justify-self: start; width: 170px; }
.rule-current-value button { width: 170px; box-sizing: border-box; }
.rule-current-value small { display: block; width: 170px; box-sizing: border-box; color: var(--text-muted); font-size: 11px; line-height: 1.35; overflow-wrap: anywhere; }
.rule-switch { justify-self: start; }
.rule-direction, .rule-threshold-input { padding: 6px 8px; border-radius: 8px; }
.rule-threshold-input { text-align: right; }
.cluster-dropdown { position: relative; width: 170px; }
.rule-global-scope { width: 170px; box-sizing: border-box; padding: 7px 10px; border: 1px solid rgba(34,211,238,.28); border-radius: 9px; background: rgba(34,211,238,.08); color: var(--accent-secondary); font-size: 12px; text-align: center; }
.cluster-dropdown summary { list-style: none; cursor: pointer; padding: 7px 28px 7px 10px; border: 1px solid var(--border-light); border-radius: 9px; background: var(--bg-input); color: var(--text-secondary); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cluster-dropdown summary::-webkit-details-marker { display: none; }
.cluster-dropdown summary::after { content: '▾'; position: absolute; right: 10px; top: 7px; color: var(--text-muted); }
.cluster-dropdown[open] summary { border-color: var(--accent-primary); color: var(--text-primary); }
.cluster-dropdown-menu { position: absolute; z-index: 20; top: calc(100% + 6px); left: 0; width: min(280px, 80vw); max-height: 280px; overflow: auto; display: grid; gap: 6px; padding: 10px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-secondary); box-shadow: 0 18px 46px rgba(0,0,0,.35); }
.migration-job-dropdown .cluster-dropdown-menu { width: min(520px, 90vw); }
.dropdown-empty { padding: 8px; color: var(--text-muted); font-style: normal; font-size: 12px; }
.alert-rule-row:nth-last-child(-n+4) .cluster-dropdown-menu { top: auto; bottom: calc(100% + 6px); }
.cluster-select-all, .cluster-check { display: inline-flex; align-items: center; gap: 5px; width: 100%; margin: 0; padding: 6px 8px; border: 1px solid var(--border-color); border-radius: 9px; background: var(--panel-soft-bg); color: var(--text-muted); font-size: 12px; white-space: nowrap; }
.cluster-select-all.active { color: var(--accent-success); border-color: rgba(52,211,153,.35); background: rgba(52,211,153,.1); }
.cluster-check input { width: auto; }
.switch { position: relative; display: inline-flex; width: 40px; height: 22px; margin: 0; }
.switch input { opacity: 0; width: 0; height: 0; }
.switch span { position: absolute; inset: 0; border-radius: 999px; background: rgba(148,163,184,.35); transition: .2s; }
.switch span::before { content: ''; position: absolute; width: 18px; height: 18px; left: 2px; top: 2px; border-radius: 50%; background: #fff; transition: .2s; }
.switch input:checked + span { background: var(--accent-success); }
.switch input:checked + span::before { transform: translateX(18px); }
.alert-config-editor { display: grid; gap: 10px; padding: 12px; border: 1px solid var(--border-color); border-radius: 16px; background: var(--bg-tertiary); }
.config-editor-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.config-editor-head h3 { margin: 0 0 4px; font-size: 15px; }
.config-editor-head span { color: var(--text-muted); font-size: 12px; }
.config-editor-actions { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.config-editor-textarea { width: 100%; min-height: 260px; resize: vertical; border: 1px solid var(--border-color); border-radius: 12px; padding: 12px; background: #0b1220; color: #dbeafe; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; line-height: 1.55; }
.notify-channel-list { display: grid; gap: 10px; }
.notify-channel-row { display: grid; grid-template-columns: minmax(230px, 320px) minmax(0, 1fr) 92px; gap: 12px; align-items: center; padding: 12px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); }
.notify-channel-row div { display: grid; gap: 4px; min-width: 0; }
.notify-channel-row strong { color: var(--text-primary); font-size: 14px; }
.notify-channel-row span { color: var(--text-muted); font-size: 12px; line-height: 1.45; }
.notify-channel-row input { margin: 0; }
.notify-input { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; align-items: center; }
.notify-input button { white-space: nowrap; }
.notify-enabled { justify-content: center; white-space: nowrap; }
.edition-pill { border: 1px solid var(--border-color); border-radius: 999px; padding: 4px 9px; color: var(--text-muted); font-size: 12px; }
.edition-pill.enterprise { color: var(--accent-success); border-color: rgba(52,211,153,.35); background: rgba(52,211,153,.08); }
.license-summary { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
.license-summary div { border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); padding: 14px; display: grid; gap: 5px; }
.license-summary span { color: var(--text-muted); font-size: 12px; }
.license-summary strong { color: var(--text-primary); font-size: 16px; }
.license-activate { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; }
.metrics-list { border: 1px solid var(--border-color); border-radius: 14px; overflow: auto; }
.metrics-row { display: grid; grid-template-columns: minmax(140px, 220px) 1fr; gap: 10px; align-items: center; padding: 10px 12px; border-bottom: 1px solid var(--border-color); }
.metrics-row:last-child { border-bottom: 0; }
.metrics-row span { color: var(--text-secondary); font-weight: 600; }
.metrics-row code { color: var(--accent-secondary); word-break: break-all; }
.danger { color: var(--accent-danger); }
.field { display: grid; gap: 6px; }
.modal-backdrop { position: fixed; inset: 0; z-index: 50; display: flex; align-items: center; justify-content: center; padding: 18px; background: rgba(15, 23, 42, .68); backdrop-filter: blur(4px); }
.modal-card { width: min(520px, 100%); display: grid; gap: 12px; padding: 20px; border: 1px solid var(--border-color); border-radius: 18px; background: var(--bg-secondary); box-shadow: 0 24px 80px rgba(0,0,0,.35); }
.notice-card { width: min(520px, 100%); display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 14px; padding: 22px; border: 1px solid rgba(52,211,153,.28); border-radius: 20px; background: linear-gradient(145deg, rgba(15,23,42,.98), rgba(30,41,59,.96)); box-shadow: 0 28px 90px rgba(0,0,0,.42); }
.notice-card.error { border-color: rgba(248,113,113,.35); }
.notice-card.warn { border-color: rgba(251,191,36,.35); }
.notice-icon { width: 48px; height: 48px; border-radius: 16px; display: grid; place-items: center; color: #fff; font-weight: 900; background: linear-gradient(135deg, #10b981, #22d3ee); }
.notice-card.error .notice-icon { background: linear-gradient(135deg, #ef4444, #f97316); }
.notice-card.warn .notice-icon { background: linear-gradient(135deg, #f59e0b, #6366f1); }
.notice-body { min-width: 0; display: grid; gap: 5px; }
.notice-eyebrow { margin: 0; color: var(--accent-secondary); font-size: 11px; font-weight: 800; letter-spacing: .14em; text-transform: uppercase; }
.notice-body h3 { margin: 0; font-size: 20px; }
.notice-message { display: grid; gap: 8px; margin-top: 4px; }
.notice-message p { margin: 0; padding: 8px 10px; border: 1px solid rgba(148,163,184,.18); border-radius: 10px; background: rgba(15,23,42,.34); color: var(--text-secondary); font-size: 13px; line-height: 1.55; overflow-wrap: anywhere; }
.notice-actions { grid-column: 1 / -1; }
.modal-title { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.modal-title h3 { margin: 0; font-size: 18px; }
.modal-close { padding: 2px 8px; border-radius: 999px; background: transparent; color: var(--text-muted); font-size: 20px; line-height: 1; }
.modal-actions { display: flex; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
.role-list { display: flex; flex-wrap: wrap; gap: 8px; }
.role-chip { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--border-color); border-radius: 999px; padding: 4px 9px; background: var(--bg-tertiary); color: var(--text-secondary); font-size: 12px; }
.role-chip button { padding: 0 4px; border-radius: 999px; background: transparent; color: var(--accent-danger); }
.user-table { border: 1px solid var(--border-color); border-radius: 14px; overflow: auto; }
.user-head, .user-row { display: grid; grid-template-columns: 1.1fr 1.1fr 100px 120px 100px 80px; gap: 10px; align-items: center; padding: 10px 12px; border-bottom: 1px solid var(--border-color); }
.user-head { color: var(--text-muted); font-size: 12px; background: var(--bg-tertiary); }
.user-row:last-child { border-bottom: 0; }
.user-actions { display: flex; gap: 6px; flex-wrap: wrap; }
.kafka-auth-panel { margin-top: 16px; }
.auth-toolbar { display: grid; grid-template-columns: minmax(130px, 180px) minmax(180px, 280px) minmax(260px, 1fr); gap: 10px; }
.permission-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
.permission-card { display: grid; grid-template-columns: auto 1fr; gap: 4px 10px; align-items: start; border: 1px solid var(--border-color); border-radius: 12px; padding: 12px; background: var(--bg-tertiary); }
.permission-card input { width: auto; margin-top: 3px; }
.permission-card strong { color: var(--text-primary); font-size: 13px; }
.permission-card span { grid-column: 2; color: var(--text-muted); font-size: 12px; }
.auth-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
button:disabled { opacity: .5; cursor: not-allowed; }
@media (max-width: 900px) { .grid, .two, .auth-toolbar, .permission-grid, .license-summary, .license-activate, .alert-rule-list, .notify-channel-row, .notify-input { grid-template-columns: 1fr; } .wide { grid-column: auto; } .panel-title, .config-editor-head { align-items: flex-start; gap: 10px; flex-direction: column; } .user-head, .user-row, .metrics-row { grid-template-columns: 1fr; } .setting-row { grid-template-columns: 1fr; align-items: start; } }
</style>
