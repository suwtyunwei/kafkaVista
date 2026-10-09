import { ref } from 'vue'

export type Language = 'zh-CN' | 'en-US'

const LANGUAGE_KEY = 'kafkavista_language'

export const language = ref<Language>((localStorage.getItem(LANGUAGE_KEY) as Language) || 'zh-CN')

const commonZh = {
  kafkaManage: 'Kafka 管理', settings: '系统设置', lightMode: '亮色模式', darkMode: '暗色模式', topics: 'Topic', brokers: 'Broker', groups: 'Consumer Group', monitoringDashboard: 'Monitor', kafkaAuth: 'Kafka 授权', community: '社区版', enterprise: '完整版', trial: '试用版', expired: '已过期',
  kafkaClusterList: 'Kafka 集群列表', manageClusterDesc: '管理当前 Kafka 实例的 Broker、Topic、消息、Consumer Group 和权限。', selectClusterDesc: '先选择一个 Kafka 实例，进入后再管理 Topic、消息和消费组。', backToClusterList: '返回集群列表', refreshing: '刷新中...', refresh: '刷新', selectKafkaInstance: '选择 Kafka 实例', selectKafkaInstanceHint: '点击集群卡片进入工作台。只有具备查看权限的集群会显示在这里。', searchInstance: '搜索实例名称 / IP...', addKafkaInstance: '新增 Kafka 实例', instanceName: '实例名称', type: '类型', description: '描述', createdAt: '创建时间', actions: '操作', standalone: '单机', cluster: '集群', edit: '编辑', delete: '删除', noKafkaCluster: '暂无可查看的 Kafka 集群', totalClusters: '共 {count} 个集群', prevPage: '上一页', nextPage: '下一页', pageSize: '{count} 条/页',
  brokerNodes: 'Broker 节点', alive: '存活', abnormal: '异常', status: '状态', noBrokerInfo: '暂无 Broker 信息', topicList: 'Topic 列表', topicListHint: '点击 Topic 进入独立管理页，查看分区、消息和配置。', addTopic: '新增 Topic', searchTopic: '搜索 Topic', topicName: 'Topic 名称', partitions: '分区', messageCount: '消息量', enterManage: '进入管理', totalPage: '共 {total} 条，第 {page} / {pages} 页', noTopic: '暂无 Topic 或无权限查看', partitionCount: '{count} 个分区', messagesCount: '{count} 条消息',
  queryMode: '查询模式', timeCondition: '时间条件', noTimeFilter: '不按时间', searchByTime: '按时间搜索', searchByTimeRange: '按时间范围', searchByOffsetRange: '按 offset 范围', defaultNewestHint: '留空默认从最新消息拉取', startTime: '开始时间', endTime: '结束时间', selectStartTime: '选择开始时间', selectEndTime: '选择结束时间', date: '日期', time: '时间', now: '现在', clear: '清空', confirm: '确定', optionalContains: '可选，包含匹配', liveStop: '停止实时', livePull: '实时拉取', quickTime: '快捷时间', lastMinutes: '近 {count} 分钟', lastHours: '近 {count} 小时', lastDay: '近 24 小时', selectCurrentResults: '全选当前结果', selectedItems: '已选择 {selected} / {total} 条', clearSelection: '清空选择', exportSelected: '导出选中', exportAll: '导出全部', select: '选择', kafkaTime: 'Kafka 时间', collapse: '收缩', expand: '展开', copied: '已复制', copy: '复制', export: '导出', noMessageResult: '暂无消息结果', refreshConfigs: '刷新配置',
}

const commonEn = {
  kafkaManage: 'Kafka Management', settings: 'Settings', lightMode: 'Light Mode', darkMode: 'Dark Mode', topics: 'Topics', brokers: 'Brokers', groups: 'Consumer Groups', monitoringDashboard: 'Monitor', kafkaAuth: 'Kafka Auth', community: 'Community', enterprise: 'Full Edition', trial: 'Trial', expired: 'Expired',
  kafkaClusterList: 'Kafka Clusters', manageClusterDesc: 'Manage brokers, topics, messages, consumer groups, and permissions for this Kafka instance.', selectClusterDesc: 'Select a Kafka instance first, then manage topics, messages, and consumer groups.', backToClusterList: 'Back to Clusters', refreshing: 'Refreshing...', refresh: 'Refresh', selectKafkaInstance: 'Select Kafka Instance', selectKafkaInstanceHint: 'Click a cluster card to enter the workspace. Only clusters with view permission are shown.', searchInstance: 'Search instance name / IP...', addKafkaInstance: 'Add Kafka Instance', instanceName: 'Instance Name', type: 'Type', description: 'Description', createdAt: 'Created At', actions: 'Actions', standalone: 'Standalone', cluster: 'Cluster', edit: 'Edit', delete: 'Delete', noKafkaCluster: 'No visible Kafka clusters', totalClusters: '{count} clusters', prevPage: 'Previous', nextPage: 'Next', pageSize: '{count} / page',
  brokerNodes: 'Broker Nodes', alive: 'Alive', abnormal: 'Abnormal', status: 'Status', noBrokerInfo: 'No broker information', topicList: 'Topics', topicListHint: 'Open a topic to manage partitions, messages, and configurations.', addTopic: 'Add Topic', searchTopic: 'Search Topic', topicName: 'Topic Name', partitions: 'Partitions', messageCount: 'Messages', enterManage: 'Manage', totalPage: '{total} items, page {page} / {pages}', noTopic: 'No topics or no permission', partitionCount: '{count} partitions', messagesCount: '{count} messages',
  queryMode: 'Query Mode', timeCondition: 'Time Filter', noTimeFilter: 'No time filter', searchByTime: 'From time', searchByTimeRange: 'Time range', searchByOffsetRange: 'Offset range', defaultNewestHint: 'Leave empty to pull latest messages', startTime: 'Start Time', endTime: 'End Time', selectStartTime: 'Select start time', selectEndTime: 'Select end time', date: 'Date', time: 'Time', now: 'Now', clear: 'Clear', confirm: 'OK', optionalContains: 'Optional, contains match', liveStop: 'Stop Live', livePull: 'Live Pull', quickTime: 'Quick Time', lastMinutes: 'Last {count} minutes', lastHours: 'Last {count} hours', lastDay: 'Last 24 hours', selectCurrentResults: 'Select current results', selectedItems: 'Selected {selected} / {total}', clearSelection: 'Clear Selection', exportSelected: 'Export Selected', exportAll: 'Export All', select: 'Select', kafkaTime: 'Kafka Time', collapse: 'Collapse', expand: 'Expand', copied: 'Copied', copy: 'Copy', export: 'Export', noMessageResult: 'No message results', refreshConfigs: 'Refresh Configs',
}

const messages: Record<Language, Record<string, string>> = { 'zh-CN': commonZh, 'en-US': commonEn }

export function setLanguage(value: string) {
  const next = value === 'en-US' ? 'en-US' : 'zh-CN'
  language.value = next
  localStorage.setItem(LANGUAGE_KEY, next)
  window.dispatchEvent(new CustomEvent('language-change', { detail: next }))
}

export function t(key: string, params: Record<string, string | number> = {}) {
  let text = messages[language.value][key] || key
  Object.entries(params).forEach(([name, value]) => {
    text = text.split(`{${name}}`).join(String(value))
  })
  return text
}

export const tr = (zh: string, en: string) => language.value === 'en-US' ? en : zh
