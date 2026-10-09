import axios from 'axios'
import { getToken, removeToken } from './auth'

export const kafkaApi = axios.create({
  baseURL: '/kafka-api',
  timeout: 60000,
})

export const api = axios.create({
  baseURL: '/api',
  timeout: 60000,
})

const attachToken = (config: any) => {
  const token = getToken()
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
}

kafkaApi.interceptors.request.use(attachToken)
api.interceptors.request.use(attachToken)

kafkaApi.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      removeToken()
      window.location.href = '/login'
    }
    return Promise.reject(error)
  },
)

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      removeToken()
      window.location.href = '/login'
    }
    return Promise.reject(error)
  },
)

export async function login(data: { username: string; password: string }) {
  const resp = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}))
    throw new Error(err.detail || '登录失败')
  }
  return resp.json()
}

export async function checkHealth() {
  const resp = await fetch('/api/health')
  if (!resp.ok) throw new Error('Backend not available')
  return resp.json()
}

export async function getAppStatus() {
  const resp = await fetch('/api/app/status')
  if (!resp.ok) return { edition: 'enterprise', enterprise: true, language: 'zh-CN' }
  const data = await resp.json()
  return data.data || { edition: 'enterprise', enterprise: true, language: 'zh-CN' }
}

export async function getSsoStatus() {
  const resp = await fetch('/api/auth/sso')
  if (!resp.ok) return { oidc_enabled: false, ldap_enabled: false }
  const data = await resp.json()
  return data.data || { oidc_enabled: false, ldap_enabled: false }
}

export async function getApiDocs() {
  const resp = await fetch('/api/docs')
  if (!resp.ok) throw new Error('API docs unavailable')
  const data = await resp.json()
  return data.data || { items: [], openapi: {} }
}

export async function getSystemSettings() {
  const resp = await api.get('/admin/settings')
  return resp.data.data?.settings || resp.data.data
}

export async function saveSystemSettings(data: any) {
  const resp = await api.put('/admin/settings', data)
  return resp.data
}

export async function testNotificationChannel(channel: string) {
  const resp = await api.post('/admin/settings/notification/test', { channel })
  return resp.data
}

export async function getAlertRuleValue(rule: any) {
  const resp = await api.post('/admin/settings/alerting/value', { rule })
  return resp.data.data || { items: [] }
}

export async function testLdapSettings(data: any) {
  const resp = await api.post('/admin/settings/ldap/test', data)
  return resp.data
}

export async function syncLdapUsers() {
  const resp = await api.post('/admin/settings/ldap/sync-users')
  return resp.data
}

export async function listSystemUsers() {
  const resp = await api.get('/admin/users')
  return resp.data.data?.items || []
}

export async function createSystemUser(data: any) {
  const resp = await api.post('/admin/users', data)
  return resp.data
}

export async function listSystemRoles() {
  const resp = await api.get('/admin/roles')
  return resp.data.data?.items || []
}

export async function createSystemRole(data: any) {
  const resp = await api.post('/admin/roles', data)
  return resp.data
}

export async function deleteSystemRole(role: string) {
  const resp = await api.delete(`/admin/roles/${encodeURIComponent(role)}`)
  return resp.data
}

export async function updateSystemUser(username: string, data: any) {
  const resp = await api.put(`/admin/users/${encodeURIComponent(username)}`, data)
  return resp.data
}

export async function deleteSystemUser(username: string) {
  const resp = await api.delete(`/admin/users/${encodeURIComponent(username)}`)
  return resp.data
}

export const KAFKA_ACTIONS = [
  'cluster_view',
  'topic_create',
  'topic_delete',
  'topic_config_manage',
  'message_read',
  'message_send',
  'message_delete',
  'group_create',
  'group_delete',
  'permission_manage',
]

export async function listKafkaClusters() {
  const resp = await kafkaApi.get('/clusters')
  return resp.data.data?.items || []
}

export async function getKafkaOverview(clusterId: string) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/overview`)
  return resp.data.data
}

export async function getKafkaClusterDetail(clusterId: string, params: { include_topics?: boolean } = {}) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/detail`, { params })
  return resp.data.data
}

export async function getKafkaClusterStats(clusterId: string) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/stats`)
  return resp.data.data
}

export async function listKafkaTopics(clusterId: string, params: any = {}) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/topics`, { params })
  return resp.data.data
}

export async function getKafkaMetrics(clusterId: string, params: any = {}) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/metrics`, { params })
  return resp.data.data
}

export async function getKafkaTopic(clusterId: string, topic: string) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}`)
  return resp.data.data
}

export async function readKafkaMessages(clusterId: string, params: any) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/messages`, { params })
  return resp.data.data
}

export async function readKafkaTopicData(clusterId: string, topic: string, params: any) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/data`, { params })
  return resp.data.data
}

export async function getKafkaTopicConfigs(clusterId: string, topic: string) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/configs`)
  return resp.data.data
}

export async function updateKafkaTopicConfigs(clusterId: string, topic: string, configs: Record<string, string | null>) {
  const resp = await kafkaApi.put(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/configs`, { configs })
  return resp.data
}

export async function deleteKafkaTopicConfig(clusterId: string, topic: string, configName: string) {
  const resp = await kafkaApi.delete(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/configs/${encodeURIComponent(configName)}`)
  return resp.data
}

export async function sendKafkaMessage(clusterId: string, data: any) {
  const resp = await kafkaApi.post(`/clusters/${clusterId}/messages`, data)
  return resp.data
}

export async function deleteKafkaRecords(clusterId: string, data: any) {
  const resp = await kafkaApi.delete(`/clusters/${clusterId}/messages`, { data })
  return resp.data
}

export async function createKafkaTopic(clusterId: string, data: any) {
  const resp = await kafkaApi.post(`/clusters/${clusterId}/topics`, data)
  return resp.data
}

export async function updateKafkaTopicPartitions(clusterId: string, topic: string, partitions: number) {
  const resp = await kafkaApi.put(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/partitions`, { partitions })
  return resp.data
}

export async function deleteKafkaTopic(clusterId: string, topic: string) {
  const resp = await kafkaApi.delete(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}`)
  return resp.data
}

export async function deleteKafkaGroup(clusterId: string, groupId: string) {
  const resp = await kafkaApi.delete(`/clusters/${clusterId}/groups/${encodeURIComponent(groupId)}`)
  return resp.data
}

export async function deleteKafkaGroupTopic(clusterId: string, groupId: string, topic: string) {
  const resp = await kafkaApi.delete(`/clusters/${clusterId}/groups/${encodeURIComponent(groupId)}/topics/${encodeURIComponent(topic)}`)
  return resp.data
}

export async function getKafkaGroupDetail(clusterId: string, groupId: string, params: any = {}) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/groups/${encodeURIComponent(groupId)}`, { params })
  return resp.data.data
}

export async function getKafkaGroupSummaries(clusterId: string, groupIds: string[]) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/groups/summary`, { params: { group_ids: groupIds.join(',') } })
  return resp.data.data
}

export async function getKafkaGroupHistory(clusterId: string, groupId: string, params: any = {}) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/groups/${encodeURIComponent(groupId)}/history`, { params })
  return resp.data.data
}

export async function createKafkaGroup(clusterId: string, groupId: string) {
  const resp = await kafkaApi.post(`/clusters/${clusterId}/groups/${encodeURIComponent(groupId)}`)
  return resp.data
}

export async function createKafkaCluster(data: any) {
  const resp = await kafkaApi.post('/clusters', data)
  return resp.data
}

export async function updateKafkaCluster(clusterId: string, data: any) {
  const resp = await kafkaApi.put(`/clusters/${clusterId}`, data)
  return resp.data
}

export async function deleteKafkaCluster(clusterId: string) {
  const resp = await kafkaApi.delete(`/clusters/${clusterId}`)
  return resp.data
}

export async function listKafkaPermissions(clusterId: string) {
  const resp = await kafkaApi.get(`/clusters/${clusterId}/permissions`)
  return resp.data.data?.items || []
}

export async function saveKafkaPermissions(clusterId: string, data: any) {
  const resp = await kafkaApi.put(`/clusters/${clusterId}/permissions`, data)
  return resp.data
}

export async function listKafkaAuditLogs(clusterId?: string) {
  const resp = await kafkaApi.get('/audit-logs', { params: clusterId ? { cluster_id: clusterId } : {} })
  return resp.data.data?.items || []
}

export async function listKafkaAuditLogPage(params: any = {}) {
  const resp = await kafkaApi.get('/audit-logs', { params })
  return resp.data.data || { items: [], limited: false }
}

export async function listKafkaMigrations() {
  const resp = await kafkaApi.get('/migrations')
  return resp.data.data?.items || []
}

export async function checkKafkaMigration(data: any, signal?: AbortSignal) {
  const resp = await kafkaApi.post('/migrations/check', data, { signal })
  return resp.data.data
}

export async function createKafkaMigration(data: any) {
  const resp = await kafkaApi.post('/migrations', data)
  return resp.data.data
}

export async function getKafkaMigration(jobId: string) {
  const resp = await kafkaApi.get(`/migrations/${encodeURIComponent(jobId)}`)
  return resp.data.data
}

export async function stopKafkaMigration(jobId: string) {
  const resp = await kafkaApi.post(`/migrations/${encodeURIComponent(jobId)}/stop`)
  return resp.data.data
}

export default kafkaApi
