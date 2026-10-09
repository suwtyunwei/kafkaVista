<template>
  <div class="kafka-page">
    <div v-if="!currentCluster" class="hero">
      <div>
        <p class="eyebrow">Kafka Operations</p>
        <h1>{{ currentCluster ? currentCluster.name : tr('Kafka 集群列表', 'Kafka Clusters') }}</h1>
        <p>{{ currentCluster ? tr('管理当前 Kafka 实例的 Broker、Topic、消息、Consumer Group 和权限。', 'Manage brokers, topics, messages, consumer groups, and permissions for this Kafka instance.') : tr('先选择一个 Kafka 实例，进入后再管理 Topic、消息和消费组。', 'Select a Kafka instance first, then manage topics, messages, and consumer groups.') }}</p>
      </div>
      <div class="hero-actions">
        <button v-if="currentCluster" class="ghost" @click="backToClusters">{{ tr('返回集群列表', 'Back to Clusters') }}</button>
        <button class="primary" :disabled="loading" @click="refreshAll()">{{ loading ? tr('刷新中...', 'Refreshing...') : tr('刷新', 'Refresh') }}</button>
      </div>
    </div>

    <section v-if="!currentCluster" class="cluster-home panel">
      <div class="panel-title">
        <div>
          <h2>{{ tr('选择 Kafka 实例', 'Select Kafka Instance') }}</h2>
          <p class="muted">{{ tr('点击集群卡片进入工作台。只有具备查看权限的集群会显示在这里。', 'Click a cluster card to enter the workspace. Only clusters with view permission are shown.') }}</p>
        </div>
        <div class="panel-actions">
          <input v-model="clusterSearch" :placeholder="tr('搜索实例名称 / IP...', 'Search instance name / IP...')" class="search-input" />
          <button v-if="isAdminUser" class="primary small" @click="openCluster(null)">{{ tr('新增 Kafka 实例', 'Add Kafka Instance') }}</button>
        </div>
      </div>
      <div class="cluster-table">
        <div class="cluster-head"><span>{{ tr('实例名称', 'Instance Name') }}</span><span>{{ tr('类型', 'Type') }}</span><span>Topic</span><span>Broker</span><span>Consumer Group</span><span>{{ tr('描述', 'Description') }}</span><span>{{ tr('创建时间', 'Created At') }}</span><span>{{ tr('操作', 'Actions') }}</span></div>
        <button v-for="cluster in pagedClusters" :key="cluster.id" class="cluster-row" @click="enterCluster(cluster.id)">
          <span><strong>{{ cluster.name }}</strong><small>{{ cluster.bootstrap_servers }}</small></span>
          <span>{{ cluster.cluster_type === 'single' ? tr('单机', 'Standalone') : tr('集群', 'Cluster') }}</span>
          <span>{{ cluster.topic_count ?? '-' }}</span>
          <span>{{ cluster.broker_count ?? '-' }}</span>
          <span>{{ cluster.group_count ?? '-' }}</span>
          <span class="muted" style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{{ cluster.description || '-' }}</span>
          <span>{{ formatTimeText(cluster.created_at) }}</span>
          <span class="cluster-actions"><button v-if="isAdminUser" class="ghost mini" @click.stop="openCluster(cluster)">{{ tr('编辑', 'Edit') }}</button><button v-if="isAdminUser" class="danger mini" @click.stop="deleteClusterFromList(cluster)">{{ tr('删除', 'Delete') }}</button></span>
        </button>
      </div>
      <div v-if="filteredClusters.length === 0" class="empty">{{ tr('暂无可查看的 Kafka 集群', 'No visible Kafka clusters') }}</div>
      <div v-if="filteredClusters.length > 0" class="pagination-bar cluster-pagination">
        <span>{{ tr(`共 ${filteredClusters.length} 个集群`, `${filteredClusters.length} clusters`) }}</span>
        <span class="page-btns">
          <button :disabled="clusterPage <= 1" @click="clusterPage--">{{ tr('上一页', 'Previous') }}</button>
          <span>{{ clusterPage }} / {{ clusterTotalPages }}</span>
          <button :disabled="clusterPage >= clusterTotalPages" @click="clusterPage++">{{ tr('下一页', 'Next') }}</button>
        </span>
        <select v-model.number="clusterPageSize">
          <option :value="10">{{ tr('10 条/页', '10 / page') }}</option><option :value="20">{{ tr('20 条/页', '20 / page') }}</option><option :value="50">{{ tr('50 条/页', '50 / page') }}</option>
        </select>
      </div>
    </section>

    <div v-else>
      <main class="content-stack">
        <section v-if="currentCluster && activeTab === 'brokers'" class="panel">
            <div class="panel-title compact"><h2>{{ tr('Broker 节点', 'Broker Nodes') }}</h2><div class="title-side"><span class="cluster-inline-pill">{{ currentClusterLabel }}</span><span>{{ clusterDetail.alive_broker_count ?? 0 }} / {{ clusterDetail.broker_count || 0 }} {{ tr('存活', 'alive') }}</span></div></div>
            <div class="broker-table">
              <div class="broker-head"><span>Node ID</span><span>Host</span><span>Role</span><span>Rack</span><span>{{ tr('状态', 'Status') }}</span></div>
              <div v-for="broker in clusterDetail.brokers || []" :key="broker.node_id" class="broker-row">
                <strong>#{{ broker.node_id }}</strong>
                <span>{{ broker.host }}</span>
                <span>{{ broker.is_controller || broker.node_id === clusterDetail.controller_id ? 'Controller' : 'Broker' }}</span>
                <span>{{ broker.rack || '-' }}</span>
                <span :class="['broker-status', broker.alive ? 'alive' : 'down']">{{ broker.alive ? tr('存活', 'Alive') : tr('异常', 'Abnormal') }}</span>
              </div>
              <div v-if="!(clusterDetail.brokers || []).length" class="empty compact">{{ tr('暂无 Broker 信息', 'No broker information') }}</div>
            </div>
        </section>

        <section v-if="activeTab === 'topics' && !selectedTopic" class="panel">
          <div class="panel-title">
            <div>
              <h2>{{ tr('Topic 列表', 'Topics') }}</h2>
              <p class="muted">{{ tr('点击 Topic 进入独立管理页，查看分区、消息和 Configurations。', 'Open a topic to manage partitions, messages, and configurations.') }}</p>
            </div>
            <div class="actions">
              <span class="cluster-inline-pill">{{ currentClusterLabel }}</span>
              <span class="count-pill">{{ tr(`共 ${topicTotal} 个 Topic`, `${topicTotal} Topics`) }}</span>
              <button class="ghost small" @click="backToClusters">{{ tr('返回集群列表', 'Back to Clusters') }}</button>
              <button class="primary small" :disabled="loading" @click="refreshAll()">{{ loading ? tr('刷新中...', 'Refreshing...') : tr('刷新', 'Refresh') }}</button>
              <button v-if="can('topic_create') || can('topic_manage')" class="ghost small" @click="showTopicModal = true">{{ tr('新增 Topic', 'Add Topic') }}</button>
            </div>
          </div>
          <input v-model="topicKeyword" class="search-input" :placeholder="tr('搜索 Topic', 'Search Topic')" />
          <div class="topic-table">
            <div class="topic-head"><span>{{ tr('Topic 名称', 'Topic Name') }}</span><span>{{ tr('分区', 'Partitions') }}</span><button class="sort-head" @click="toggleLogSizeSort">Log Size {{ logSizeSortLabel }}</button><span>{{ tr('消息量', 'Messages') }}</span><span>{{ tr('操作', 'Actions') }}</span></div>
            <button v-for="topic in topicPageItems" :key="topic.topic" class="topic-row" @click="selectTopic(topic.topic)">
              <span>{{ topic.topic }}</span>
              <span>{{ topic.partition_count ?? '-' }}</span>
              <span>{{ formatLogSize(topic.log_size || 0) }}</span>
              <span>{{ formatNumber(topic.message_count || 0) }}</span>
              <strong>{{ tr('进入管理', 'Manage') }}</strong>
            </button>
          </div>
          <div class="pagination-bar">
            <span>{{ tr(`共 ${topicTotal} 条，第 ${topicPage} / ${topicTotalPages} 页`, `${topicTotal} items, page ${topicPage} / ${topicTotalPages}`) }}</span>
            <select v-model.number="topicPageSize"><option :value="10">{{ tr('10 条/页', '10 / page') }}</option><option :value="20">{{ tr('20 条/页', '20 / page') }}</option><option :value="50">{{ tr('50 条/页', '50 / page') }}</option><option :value="100">{{ tr('100 条/页', '100 / page') }}</option></select>
            <button class="ghost small" :disabled="topicPage <= 1" @click="topicPage--">{{ tr('上一页', 'Previous') }}</button>
            <button class="ghost small" :disabled="topicPage >= topicTotalPages" @click="topicPage++">{{ tr('下一页', 'Next') }}</button>
          </div>
          <div v-if="topicTotal === 0" class="empty compact">{{ tr('暂无 Topic 或无权限查看', 'No topics or no permission') }}</div>
        </section>

        <section v-if="activeTab === 'topics' && selectedTopic" class="panel topic-detail-page">
            <div v-if="selectedTopic" class="topic-summary">
              <strong>{{ selectedTopic }}</strong>
              <span class="cluster-inline-pill">{{ currentClusterLabel }}</span>
              <span>{{ tr(`${topicPartitions.length} 个分区`, `${topicPartitions.length} partitions`) }}</span>
              <span>{{ tr(`${formatNumber(topicSummaryMap[selectedTopic]?.message_count || 0)} 条消息`, `${formatNumber(topicSummaryMap[selectedTopic]?.message_count || 0)} messages`) }}</span>
              <button :class="['ghost small', { selected: topicSubTab === 'overview' }]" @click="topicSubTab = 'overview'">Overview</button>
              <button :class="['ghost small', { selected: topicSubTab === 'messages' }]" @click="topicSubTab = 'messages'">Consume Message</button>
              <button v-if="can('message_send')" class="primary small" @click="openProduceModal">Produce Message</button>
              <button :class="['ghost small', { selected: topicSubTab === 'configs' }]" @click="topicSubTab = 'configs'">Configurations</button>
              <button v-if="can('topic_manage')" class="ghost small" @click="openPartitionModal">{{ tr('修改分区', 'Edit Partitions') }}</button>
              <button v-if="can('topic_delete') || can('topic_manage')" class="danger small" @click="removeTopic">{{ tr('删除', 'Delete') }}</button>
            </div>
            <div v-if="selectedTopic && topicSubTab === 'overview'" class="partition-grid compact-grid">
              <button v-for="p in topicPartitions" :key="p.partition" :class="['partition-card', { active: Number(messageForm.partition) === p.partition }]" @click="messageForm.partition = p.partition">
                <label>Partition {{ p.partition }}</label>
                <strong>{{ p.beginning_offset ?? '-' }} - {{ p.end_offset ?? '-' }}</strong>
                <span>{{ tr(`${formatNumber(p.message_count ?? p.lag_window ?? 0)} 条消息`, `${formatNumber(p.message_count ?? p.lag_window ?? 0)} messages`) }}</span>
                <small v-if="p.leader !== undefined && p.leader !== null">Leader {{ p.leader }}</small>
              </button>
            </div>
            <div v-if="selectedTopic && topicSubTab === 'messages'" class="topic-message-box">
              <div class="panel-title compact"><h2>Consume Message</h2></div>
              <div class="offset-summary">
                <div><label>Beginning Offset</label><strong>{{ selectedPartitionInfo?.beginning_offset ?? '-' }}</strong></div>
                <div><label>End Offset</label><strong>{{ selectedPartitionInfo?.end_offset ?? '-' }}</strong></div>
              </div>
              <div class="message-tools message-control-grid" @keydown.enter.prevent="handleMessageSearchEnter">
                <div class="field"><label>partition</label><select v-model="messageForm.partition"><option value="all">All Partitions</option><option v-for="p in topicPartitions" :key="p.partition" :value="p.partition">Partition {{ p.partition }}</option></select></div>
                <div class="field"><label>{{ t('queryMode') }}</label><select v-model="messageForm.timeMode"><option value="none">{{ t('noTimeFilter') }}</option><option value="from">{{ t('searchByTime') }}</option><option value="range">{{ t('searchByTimeRange') }}</option><option value="offsetRange">{{ t('searchByOffsetRange') }}</option></select></div>
                <div class="field"><label>autoOffsetReset</label><select v-model="messageForm.autoOffsetReset"><option value="newest">newest</option><option value="earliest">earliest</option></select></div>
                <div v-if="messageForm.timeMode === 'none'" class="field"><label>start offset</label><input v-model="messageForm.offset" :placeholder="t('defaultNewestHint')" /></div>
                <div v-if="messageForm.timeMode === 'offsetRange'" class="field"><label>start offset</label><input v-model="messageForm.offset" placeholder="例如 100" /></div>
                <div v-if="messageForm.timeMode === 'offsetRange'" class="field"><label>end offset</label><input v-model="messageForm.endOffset" :placeholder="tr('例如 200，包含该 offset', 'e.g. 200, inclusive')" /></div>
                <div v-if="messageForm.timeMode === 'from' || messageForm.timeMode === 'range'" class="field datetime-field"><label>{{ t('startTime') }}</label><button type="button" class="datetime-trigger" @click="activeDateTimePicker = activeDateTimePicker === 'startTime' ? '' : 'startTime'">{{ formatDateTimeInput(messageForm.startTime) || t('selectStartTime') }}</button><div v-if="activeDateTimePicker === 'startTime'" class="datetime-popover"><div class="dt-picker-row"><span class="dt-picker-group"><label>{{ t('date') }}</label><input :value="datePart(messageForm.startTime)" type="date" class="dt-native" @input="updateDateTime('startTime', 'date', $event)" /></span><span class="dt-picker-group"><label>{{ t('time') }}</label><input :value="timePart(messageForm.startTime)" type="time" step="1" class="dt-native" @input="updateDateTime('startTime', 'time', $event)" /></span></div><div class="datetime-actions"><button class="ghost" @click="setDateTimeNow('startTime')">{{ t('now') }}</button><button class="ghost" @click="messageForm.startTime = ''">{{ t('clear') }}</button><button class="primary" @click="activeDateTimePicker = ''">{{ t('confirm') }}</button></div></div></div>
                <div v-if="messageForm.timeMode === 'range'" class="field datetime-field"><label>{{ t('endTime') }}</label><button type="button" class="datetime-trigger" @click="activeDateTimePicker = activeDateTimePicker === 'endTime' ? '' : 'endTime'">{{ formatDateTimeInput(messageForm.endTime) || t('selectEndTime') }}</button><div v-if="activeDateTimePicker === 'endTime'" class="datetime-popover"><div class="dt-picker-row"><span class="dt-picker-group"><label>{{ t('date') }}</label><input :value="datePart(messageForm.endTime)" type="date" class="dt-native" @input="updateDateTime('endTime', 'date', $event)" /></span><span class="dt-picker-group"><label>{{ t('time') }}</label><input :value="timePart(messageForm.endTime)" type="time" step="1" class="dt-native" @input="updateDateTime('endTime', 'time', $event)" /></span></div><div class="datetime-actions"><button class="ghost" @click="setDateTimeNow('endTime')">{{ t('now') }}</button><button class="ghost" @click="messageForm.endTime = ''">{{ t('clear') }}</button><button class="primary" @click="activeDateTimePicker = ''">{{ t('confirm') }}</button></div></div></div>
                <div class="field"><label>Key</label><input v-model="messageForm.keySearch" :placeholder="t('optionalContains')" /></div>
                <div class="field"><label>Value</label><input v-model="messageForm.valueSearch" :placeholder="t('optionalContains')" /></div>
                <div class="field count-field"><label>count</label><input v-model.number="messageForm.count" type="number" min="1" max="200" /></div>
                <div class="message-action-buttons">
                  <button class="primary" :disabled="!can('message_read') || messagesLoading" @click="loadMessages">{{ messagesLoading ? 'Pulling...' : 'Pull' }}</button>
                  <button :class="['ghost', 'live-toggle', { active: liveStreaming }]" :disabled="!can('message_read') || messageForm.partition === 'all'" @click="toggleLiveStream">{{ liveStreaming ? t('liveStop') : t('livePull') }}</button>
                </div>
              </div>
              <div v-if="messageForm.timeMode === 'from' || messageForm.timeMode === 'range'" class="time-shortcuts">
                <span>{{ t('quickTime') }}</span>
                <button class="ghost mini" @click="setMessageTimeRange(15)">{{ t('lastMinutes', { count: 15 }) }}</button>
                <button class="ghost mini" @click="setMessageTimeRange(60)">{{ t('lastHours', { count: 1 }) }}</button>
                <button class="ghost mini" @click="setMessageTimeRange(360)">{{ t('lastHours', { count: 6 }) }}</button>
                <button class="ghost mini" @click="setMessageTimeRange(1440)">{{ t('lastDay') }}</button>
              </div>
              <p v-if="liveStreamStatus" :class="['muted', 'message-hint', { live: liveStreaming }]">{{ liveStreamStatus }}</p>
              <p v-if="messageSearchHint" class="muted message-hint">{{ messageSearchHint }}</p>
              <div class="message-export-bar">
                <label class="select-all"><input type="checkbox" :checked="messages.length > 0 && selectedMessageKeys.length === messages.length" :disabled="messages.length === 0" @change="toggleSelectAllMessages" /> {{ t('selectCurrentResults') }}</label>
                <span class="muted">{{ t('selectedItems', { selected: selectedMessageKeys.length, total: messages.length }) }}</span>
                <button class="ghost small" :disabled="selectedMessageKeys.length === 0" @click="clearSelectedMessages">{{ t('clearSelection') }}</button>
                <button class="primary small" :disabled="messages.length === 0" @click="exportMessages">{{ selectedMessageKeys.length ? t('exportSelected') : t('exportAll') }}</button>
              </div>
              <div class="message-list">
                <article v-for="msg in messages" :key="`${msg.partition}-${msg.offset}`" class="message-card">
                  <div class="message-meta">
                    <label class="message-select"><input v-model="selectedMessageKeys" type="checkbox" :value="messageKey(msg)" /> {{ t('select') }}</label>
                    <span>partition {{ msg.partition }}</span>
                    <span>offset {{ msg.offset }}</span>
                    <span>key <span v-html="highlightMessageKey(msg.key || '-')"></span></span>
                    <span class="message-time">{{ t('kafkaTime') }} {{ formatMessageTime(msg.timestamp) }}</span>
                    <button class="ghost mini" @click="toggleMessage(msg)">{{ isMessageExpanded(msg) ? t('collapse') : t('expand') }}</button>
                    <button class="ghost mini" @click="copyMessage(msg)">{{ copiedMessageKey === messageKey(msg) ? t('copied') : t('copy') }}</button>
                    <button class="ghost mini" @click="exportMessage(msg)">{{ t('export') }}</button>
                  </div>
                  <pre :class="['message-value', 'json-viewer', { collapsed: !isMessageExpanded(msg), plain: !isJsonMessage(msg.value) }]" v-html="highlightMessageValue(msg.value)"></pre>
                </article>
                <div v-if="messages.length === 0" class="empty compact">{{ t('noMessageResult') }}</div>
              </div>
            </div>
            <div v-if="selectedTopic && topicSubTab === 'configs'" class="config-panel">
              <div class="config-toolbar"><span>Topic Configurations</span><button class="ghost small" @click="loadTopicConfigs">{{ t('refreshConfigs') }}</button></div>
              <div class="config-layout">
                <div class="config-table">
                  <div class="config-head"><span>Name</span><span>Value</span><span>Edit</span></div>
                  <div v-for="cfg in topicConfigs" :key="cfg.name" class="config-row">
                    <span class="config-name">{{ cfg.name }}</span>
                    <span v-if="editingConfigName !== cfg.name" class="config-value">{{ cfg.sensitive ? '******' : cfg.value }}<small v-if="configValueHint(cfg)">{{ configValueHint(cfg) }}</small></span>
                    <span v-else class="config-value"><input v-model="editingConfigValue" class="inline-config-input" /></span>
                    <span class="config-edit">
                      <template v-if="isAdminUser && cfg.allowed">
                        <button v-if="editingConfigName !== cfg.name" class="ghost mini" @click="editTopicConfig(cfg)">Edit</button>
                        <button v-if="editingConfigName === cfg.name" class="primary mini" @click="saveTopicConfig(cfg.name)">Save</button>
                        <button v-if="editingConfigName === cfg.name" class="ghost mini" @click="cancelTopicConfigEdit">Cancel</button>
                        <button class="danger mini" @click="deleteTopicConfig(cfg.name)">{{ tr('默认', 'Default') }}</button>
                      </template>
                      <em v-else>-</em>
                    </span>
                  </div>
                </div>
              </div>
            </div>
        </section>

        <section v-if="activeTab === 'groups' && !selectedGroup" class="panel">
          <div>
            <div class="panel-title compact"><h2>{{ tr('Consumer Group 列表', 'Consumer Groups') }}</h2><div class="title-side"><span class="cluster-inline-pill">{{ currentClusterLabel }}</span><span>{{ groupSummariesLoading ? tr('加载详情中...', 'Loading details...') : tr(`${filteredGroups.length} 个`, `${filteredGroups.length}`) }}</span><button v-if="can('group_create')" class="primary small" @click="openGroupModal">{{ tr('创建 Group', 'Create Group') }}</button></div></div>
            <div class="group-search-bar">
              <select v-model="groupSearchMode" class="group-search-mode">
                <option value="group">{{ tr('按 Group 搜索', 'Search by Group') }}</option>
                <option value="topic">{{ tr('按 Topic 搜索', 'Search by Topic') }}</option>
              </select>
              <input v-model="groupKeyword" class="search-input" :placeholder="groupSearchMode === 'topic' ? tr('搜索 Topic，列出绑定该 Topic 的 Group', 'Search topic and list groups bound to it') : tr('搜索 Group', 'Search Group')" />
            </div>
            <div class="group-list-box">
              <div v-for="group in filteredGroups" :key="group" :class="['group-card', { active: selectedGroup === group }]" @click="selectGroup(group)">
                <div class="group-card-main">
                  <div class="group-title-row">
                    <strong>{{ group }}</strong>
                    <span class="group-pill">{{ groupSummary(group).topics.length }} Topic</span>
                    <span class="group-pill">{{ groupSummary(group).memberCount }} Member</span>
                    <span class="group-pill warn">Lag {{ formatNumber(groupSummary(group).totalLag) }}</span>
                  </div>
                  <div class="group-topic-line">
                    <span v-if="groupSummary(group).topics.length === 0" class="muted">{{ tr('暂无已提交 offset 的 Topic', 'No topics with committed offsets') }}</span>
                    <span v-for="topic in groupSummary(group).topics.slice(0, 6)" :key="topic" :class="['topic-chip', { matched: groupSearchMode === 'topic' && topicMatchesGroupSearch(topic) }]">{{ topic }}</span>
                    <span v-if="groupSummary(group).topics.length > 6" class="muted">+{{ groupSummary(group).topics.length - 6 }}</span>
                  </div>
                  <div class="group-member-line">
                    <span v-if="groupSummary(group).members.length === 0" class="muted">{{ tr('当前无在线 member', 'No online members') }}</span>
                    <span v-for="member in groupSummary(group).members.slice(0, 3)" :key="member.member_id || member.client_id" class="member-chip">{{ member.client_id || member.member_id || '-' }} {{ member.client_host || '' }}</span>
                    <span v-if="groupSummary(group).members.length > 3" class="muted">+{{ groupSummary(group).members.length - 3 }}</span>
                  </div>
                </div>
                <div class="group-card-side">
                  <span>{{ tr(`${groupSummary(group).partitionCount} 分区`, `${groupSummary(group).partitionCount} partitions`) }}</span>
                  <button v-if="can('group_delete')" class="danger mini" @click.stop="removeGroup(group)">{{ tr('删除', 'Delete') }}</button>
                </div>
              </div>
              <div v-if="filteredGroups.length === 0" class="empty compact">{{ tr('暂无 Group', 'No groups') }}</div>
            </div>
          </div>
        </section>

        <section v-if="activeTab === 'groups' && selectedGroup" class="panel">
          <div>
            <div class="panel-title compact"><h2>{{ selectedGroup }}</h2><div class="actions"><span class="cluster-inline-pill">{{ currentClusterLabel }}</span><span v-if="groupDetail.group">Lag {{ formatNumber(groupDetail.total_lag || 0) }}</span><button class="ghost small" @click="backToGroupList">{{ tr('返回 Group 列表', 'Back to Groups') }}</button></div></div>
            <div class="group-stat-grid">
              <div><label>Total Lag</label><strong>{{ formatNumber(groupDetail.total_lag || 0) }}</strong></div>
              <div><label>Topic</label><strong>{{ (groupDetail.active_topics?.length || groupDetail.topics?.length || 0) }}</strong></div>
              <div><label>Member</label><strong>{{ groupDetail.member_count || 0 }}</strong></div>
              <div><label>Partition</label><strong>{{ (groupDetail.partitions || []).length }}</strong></div>
            </div>
            <div class="group-detail-block">
              <div class="panel-title compact"><h2>{{ tr('正在消费的 Topic', 'Consuming Topics') }}</h2><span>{{ tr(`${groupTopicRows.length} 个`, `${groupTopicRows.length}`) }}</span></div>
              <div class="group-topic-detail-table">
                <div class="group-topic-detail-head"><span>Topic</span><span>Host</span><span>{{ tr('操作', 'Actions') }}</span></div>
                <div v-for="row in groupTopicRows" :key="row.topic" class="group-topic-detail-row">
                  <button class="offset-topic-link" @click="openTopicFromGroup(row.topic)">{{ row.topic }}</button>
                  <span class="offset-host" :title="row.host || '-'">{{ row.host || '-' }}</span>
                  <button v-if="can('group_delete')" class="unlink-topic-btn" @click="unlinkGroupTopic(row.topic)">{{ tr('解除订阅', 'Unsubscribe') }}</button>
                  <span v-else>-</span>
                </div>
                <div v-if="!groupTopicRows.length" class="empty compact">{{ tr('暂无正在消费的 Topic', 'No consuming topics') }}</div>
              </div>
            </div>
            <div class="group-detail-block">
              <div class="panel-title compact"><h2>Member</h2><span>{{ tr(`${(groupDetail.members || []).length} 个`, `${(groupDetail.members || []).length}`) }}</span></div>
              <div class="member-table">
                <div class="member-head"><span>Client ID</span><span>Host</span><span>Assignments</span></div>
                <div v-for="member in groupDetail.members || []" :key="member.member_id || member.client_id" class="member-row">
                  <span>{{ member.client_id || '-' }}</span>
                  <span>{{ member.client_host || '-' }}</span>
                  <span class="assignment-list">
                    <em v-if="!(member.assignments || []).length">-</em>
                    <button v-for="item in member.assignments || []" :key="`${item.topic}-${item.partition}`" class="assignment-chip" @click="openTopicFromGroup(item.topic)">{{ item.topic }}-{{ item.partition }}</button>
                  </span>
                </div>
                <div v-if="!(groupDetail.members || []).length" class="empty compact">{{ tr('当前无在线 member', 'No online members') }}</div>
              </div>
            </div>
            <div class="group-offset-table">
              <div class="offset-head"><span>Topic</span><span>Host</span><span>{{ tr('分区', 'Partition') }}</span><span>{{ tr('提交', 'Committed') }}</span><span>{{ tr('末尾', 'End') }}</span><span>{{ tr('积压', 'Lag') }}</span></div>
              <div v-for="row in groupDetail.partitions || []" :key="`${row.topic}-${row.partition}`" class="offset-row">
                <button class="offset-topic-link" @click="openTopicFromGroup(row.topic)">{{ row.topic }}</button><span class="offset-host" :title="row.host || '-'">{{ row.host || '-' }}</span><span>{{ row.partition }}</span><span>{{ row.committed_offset ?? '-' }}</span><span>{{ row.end_offset }}</span><strong>{{ row.lag }}</strong>
              </div>
              <div v-if="!(groupDetail.partitions || []).length" class="empty compact">{{ tr('暂无 offset 和积压信息', 'No offset or lag information') }}</div>
            </div>
          </div>
        </section>

      </main>
    </div>

    <div v-if="showClusterModal" class="modal-mask" @click.self="showClusterModal = false">
      <div class="modal">
        <h3>{{ editingCluster?.id ? tr('编辑集群', 'Edit Cluster') : tr('新增集群', 'Add Cluster') }}</h3>
        <div class="form">
          <input v-model="clusterForm.name" :placeholder="tr('集群名称', 'Cluster name')" />
          <select v-model="clusterForm.cluster_type"><option value="single">{{ tr('单机', 'Standalone') }}</option><option value="cluster">{{ tr('集群', 'Cluster') }}</option></select>
          <input v-model="clusterForm.bootstrap_servers" :placeholder="clusterForm.cluster_type === 'single' ? tr('单机地址，如 10.0.0.1:9092', 'Standalone address, e.g. 10.0.0.1:9092') : tr('集群地址，多个用英文逗号分隔，如 host1:9092,host2:9092', 'Cluster addresses, comma-separated, e.g. host1:9092,host2:9092')" />
          <select v-model="clusterForm.security_protocol"><option>PLAINTEXT</option><option>SASL_PLAINTEXT</option><option>SASL_SSL</option><option>SSL</option></select>
          <input v-model="clusterForm.sasl_mechanism" :placeholder="tr('SASL 机制，可选', 'SASL mechanism, optional')" />
          <input v-model="clusterForm.sasl_username" :placeholder="tr('SASL 用户名，可选', 'SASL username, optional')" />
          <input v-model="clusterForm.sasl_password" type="password" :placeholder="tr('SASL 密码，可选', 'SASL password, optional')" />
          <textarea v-model="clusterForm.description" :placeholder="tr('描述', 'Description')"></textarea>
        </div>
        <div class="modal-actions"><button class="ghost" @click="showClusterModal = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" @click="saveCluster">{{ tr('保存', 'Save') }}</button></div>
      </div>
    </div>

    <div v-if="showTopicModal" class="modal-mask" @click.self="showTopicModal = false">
      <div class="modal topic-create-modal">
        <div class="modal-heading">
          <div class="modal-icon topic-icon">T</div>
          <div>
            <p class="modal-eyebrow">Topic Provisioning</p>
            <h3>{{ tr('新增 Topic', 'Add Topic') }}</h3>
          </div>
        </div>
        <p class="muted modal-desc">{{ tr('创建后会立即写入当前 Kafka 实例，请确认分区数和副本数符合生产规划。', 'The topic will be created immediately in the current Kafka instance. Confirm partitions and replication factor before submitting.') }}</p>
        <div class="form">
          <div class="field"><label>Name</label><input v-model="topicForm.topic" :placeholder="tr('Topic 名称，如 order-events', 'Topic name, e.g. order-events')" /></div>
          <div class="field"><label>Partitions Num</label><input v-model.number="topicForm.partitions" type="number" min="1" :placeholder="tr('分区数，如 3', 'Partitions, e.g. 3')" /></div>
          <div class="field"><label>Replication Factor</label><input v-model.number="topicForm.replication_factor" type="number" min="1" :placeholder="tr('副本数，如 1', 'Replication factor, e.g. 1')" /></div>
        </div>
        <div class="modal-actions"><button class="ghost" @click="showTopicModal = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" @click="createTopic">{{ tr('创建', 'Create') }}</button></div>
      </div>
    </div>

    <div v-if="showPartitionModal" class="modal-mask" @click.self="showPartitionModal = false">
      <div class="modal topic-create-modal">
        <div class="modal-heading">
          <div class="modal-icon topic-icon">P</div>
          <div>
            <p class="modal-eyebrow">Topic Partition</p>
            <h3>{{ tr('修改分区数', 'Edit Partitions') }}</h3>
          </div>
        </div>
        <p class="muted modal-desc">{{ tr('Kafka 只支持增加 Topic 分区数，不支持减少。请确认业务分区策略后再提交。', 'Kafka only supports increasing topic partitions, not decreasing them. Confirm the partitioning strategy before submitting.') }}</p>
        <div class="form">
          <div class="field"><label>{{ tr('当前分区数', 'Current Partitions') }}</label><input :value="topicPartitions.length" disabled /></div>
          <div class="field"><label>{{ tr('目标分区数', 'Target Partitions') }}</label><input v-model.number="partitionForm.partitions" type="number" :min="topicPartitions.length + 1" :placeholder="tr('例如 3', 'e.g. 3')" /></div>
        </div>
        <div class="modal-actions"><button class="ghost" @click="showPartitionModal = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" :disabled="partitionForm.partitions <= topicPartitions.length" @click="updateTopicPartitions">{{ tr('确认修改', 'Update') }}</button></div>
      </div>
    </div>

    <div v-if="showGroupModal" class="modal-mask" @click.self="showGroupModal = false">
      <div class="modal group-create-modal">
        <div class="modal-heading">
          <div class="modal-icon group-icon">G</div>
          <div>
            <p class="modal-eyebrow">Consumer Group</p>
            <h3>{{ tr('创建 Consumer Group', 'Create Consumer Group') }}</h3>
          </div>
        </div>
        <p class="muted modal-desc">{{ tr('创建后会先显示在 KafkaVista 的 Group 列表中；消费者使用该 Group ID 并提交 offset 后，会补齐真实 Topic、分区和 Lag 信息。', 'The group appears in KafkaVista first. After consumers use this Group ID and commit offsets, real topic, partition, and lag details will be populated.') }}</p>
        <div class="form group-create-form" @keydown.enter.prevent="createGroup">
          <div class="field"><label>Group ID</label><input v-model="groupForm.group_id" autofocus :placeholder="tr('例如 order-service-consumer', 'e.g. order-service-consumer')" /></div>
          <div class="group-create-hint">
            <strong>{{ tr('创建说明', 'Creation Note') }}</strong>
            <span>{{ tr('不会修改 Topic 或 offset；消费者提交 offset 后会自动建立真实关系。', 'No topics or offsets are changed; the real relation is created after consumers commit offsets.') }}</span>
          </div>
        </div>
        <div class="modal-actions"><button class="ghost" @click="showGroupModal = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" :disabled="!groupForm.group_id.trim()" @click="createGroup">{{ tr('创建', 'Create') }}</button></div>
      </div>
    </div>

    <div v-if="confirmDialog.visible" class="modal-mask confirm-mask" @click.self="resolveConfirm(false)">
      <div :class="['modal', 'confirm-modal', confirmDialog.tone]">
        <div class="modal-heading">
          <div class="modal-icon confirm-icon">{{ confirmDialog.tone === 'danger' ? '!' : '?' }}</div>
          <div>
            <p class="modal-eyebrow">{{ confirmDialog.eyebrow }}</p>
            <h3>{{ confirmDialog.title }}</h3>
          </div>
        </div>
        <p class="confirm-message">{{ confirmDialog.message }}</p>
        <div v-if="confirmDialog.target" class="confirm-target"><span>{{ tr('操作对象', 'Target') }}</span><strong>{{ confirmDialog.target }}</strong></div>
        <div class="modal-actions"><button class="ghost" @click="resolveConfirm(false)">{{ confirmDialog.cancelText }}</button><button :class="confirmDialog.tone === 'danger' ? 'danger' : 'primary'" @click="resolveConfirm(true)">{{ confirmDialog.confirmText }}</button></div>
      </div>
    </div>

    <div v-if="noticeDialog.visible" class="modal-mask confirm-mask" @click.self="noticeDialog.visible = false">
      <div class="modal notice-modal">
        <div class="success-hero notice-hero">
          <div class="success-icon">
            <svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4">
              <path d="M20 6L9 17l-5-5" />
            </svg>
          </div>
          <div>
            <p class="success-eyebrow">{{ noticeDialog.eyebrow }}</p>
            <h3>{{ noticeDialog.title }}</h3>
          </div>
        </div>
        <p class="success-desc">{{ noticeDialog.message }}</p>
        <div v-if="noticeDialog.target" class="success-summary notice-summary">
          <div><span>{{ tr('操作对象', 'Target') }}</span><strong>{{ noticeDialog.target }}</strong></div>
          <div><span>{{ tr('结果', 'Result') }}</span><strong>{{ tr('已完成', 'Completed') }}</strong></div>
        </div>
        <div class="modal-actions success-actions"><button class="primary success-confirm" @click="noticeDialog.visible = false">{{ tr('完成', 'Done') }}</button></div>
      </div>
    </div>

    <div v-if="errorDialog.visible" class="modal-mask confirm-mask" @click.self="errorDialog.visible = false">
      <div class="modal notice-modal error-notice-modal">
        <div class="success-hero notice-hero">
          <div class="success-icon error-icon">!</div>
          <div>
            <p class="success-eyebrow error-eyebrow">{{ errorDialog.eyebrow }}</p>
            <h3>{{ errorDialog.title }}</h3>
          </div>
        </div>
        <p class="success-desc">{{ errorDialog.message }}</p>
        <div v-if="errorDialog.target" class="success-summary notice-summary error-summary">
          <div><span>{{ tr('操作对象', 'Target') }}</span><strong>{{ errorDialog.target }}</strong></div>
          <div><span>{{ tr('结果', 'Result') }}</span><strong>{{ tr('未完成', 'Not Completed') }}</strong></div>
        </div>
        <div class="modal-actions success-actions"><button class="primary error-confirm" @click="errorDialog.visible = false">{{ tr('知道了', 'OK') }}</button></div>
      </div>
    </div>

    <div v-if="showProduceModal" class="modal-mask" @click.self="showProduceModal = false">
      <div class="modal produce-modal">
        <h3>Produce Message</h3>
        <p class="muted modal-desc">{{ tr(`Topic: ${selectedTopic}。支持上传 JSON 文件、JSON 数组、JSON Lines、分号分隔 JSON，或普通多行文本。JSON 对象可包含 key、value、partition。`, `Topic: ${selectedTopic}. Supports JSON files, JSON arrays, JSON Lines, semicolon-separated JSON, or plain multiline text. JSON objects may include key, value, and partition.`) }}</p>
        <div class="produce-tabs">
          <button :class="['ghost', 'small', { selected: produceMode === 'single' }]" @click="produceMode = 'single'">{{ tr('单条写入', 'Single') }}</button>
          <button :class="['ghost', 'small', { selected: produceMode === 'batch' }]" :disabled="!enterpriseEnabled" @click="produceMode = 'batch'">{{ tr('批量导入', 'Batch Import') }}</button>
        </div>
        <p v-if="!enterpriseEnabled" class="muted modal-desc">{{ tr('批量导入消息属于完整版功能，完整版密钥过期后不可用。', 'Batch import is a Full Edition feature and is unavailable when the Full Edition license expires.') }}</p>
        <div class="form">
          <template v-if="produceMode === 'single'">
            <div class="field"><label>Partition</label><select v-model="sendForm.partition"><option v-for="p in topicPartitions" :key="p.partition" :value="String(p.partition)">Partition {{ p.partition }}</option></select></div>
            <div class="field"><label>Key</label><input v-model="sendForm.key" :placeholder="tr('可选', 'Optional')" /></div>
            <div class="field"><label>Value</label><textarea v-model="sendForm.value" placeholder="Message Value"></textarea></div>
          </template>
          <template v-else>
            <div class="batch-toolbar">
              <div class="field"><label>{{ tr('默认 Partition', 'Default Partition') }}</label><select v-model="batchDefaultPartition"><option value="">{{ tr('由 Kafka 分配', 'Assigned by Kafka') }}</option><option v-for="p in topicPartitions" :key="p.partition" :value="String(p.partition)">Partition {{ p.partition }}</option></select></div>
              <div class="field"><label>{{ tr('导入文件', 'Import File') }}</label><input type="file" accept=".json,.jsonl,.txt,application/json,text/plain" @change="importBatchFile" /></div>
            </div>
            <div class="field"><label>{{ tr('批量内容', 'Batch Content') }}</label><textarea v-model="batchImportText" class="batch-textarea" :placeholder="tr('支持：JSON 文件、JSON 数组、JSON Lines、分号分隔 JSON、普通多行文本。可带 Topic: xxx 头部。', 'Supports JSON files, JSON arrays, JSON Lines, semicolon-separated JSON, plain multiline text. Topic: xxx header is allowed.')"></textarea></div>
            <div :class="['batch-status', batchParseResult.error ? 'error' : '']">
              <span>{{ batchParseResult.error || tr(`已解析 ${batchParseResult.items.length} 条消息`, `Parsed ${batchParseResult.items.length} messages`) }}</span>
              <button class="ghost mini" type="button" @click="loadBatchExample">{{ tr('填入示例', 'Load Example') }}</button>
            </div>
            <div v-if="batchParseResult.items.length" class="batch-preview">
              <div v-for="(item, index) in batchParseResult.items.slice(0, 5)" :key="index" class="batch-preview-row">
                <span>#{{ index + 1 }}</span><span>partition {{ item.partition ?? 'auto' }}</span><span>key {{ item.key || '-' }}</span><strong>{{ item.value.slice(0, 120) }}</strong>
              </div>
              <p v-if="batchParseResult.items.length > 5" class="muted">{{ tr(`仅预览前 5 条，其余 ${batchParseResult.items.length - 5} 条会一起写入。`, `Only the first 5 are previewed; the remaining ${batchParseResult.items.length - 5} will be written together.`) }}</p>
            </div>
          </template>
        </div>
        <div class="modal-actions"><button class="ghost" @click="showProduceModal = false">{{ tr('取消', 'Cancel') }}</button><button class="primary" :disabled="produceSubmitDisabled" @click="sendMessage">{{ sending ? 'Producing...' : (produceMode === 'batch' ? tr(`批量写入 ${batchParseResult.items.length} 条`, `Batch Write ${batchParseResult.items.length}`) : 'Produce Message') }}</button></div>
      </div>
    </div>

    <div v-if="showPermissionSavedModal" class="modal-mask" @click.self="showPermissionSavedModal = false">
      <div class="modal success-modal">
        <div class="success-hero">
          <div class="success-icon">
            <svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4">
              <path d="M20 6L9 17l-5-5" />
            </svg>
          </div>
          <div>
            <p class="success-eyebrow">Permission Updated</p>
            <h3>{{ tr('已成功保存', 'Saved Successfully') }}</h3>
          </div>
        </div>
        <p class="success-desc">{{ tr('Kafka 授权配置已保存，用户刷新页面或重新进入 Kafka 管理后即可看到最新权限。', 'Kafka permissions have been saved. Users can refresh or re-enter Kafka Management to see the latest permissions.') }}</p>
        <div class="success-summary">
          <div><span>{{ tr('授权用户', 'Authorized User') }}</span><strong>{{ lastSavedPermission.username || '-' }}</strong></div>
          <div><span>{{ tr('Kafka 实例', 'Kafka Instance') }}</span><strong>{{ currentCluster?.name || '-' }}</strong></div>
        </div>
        <div class="success-permissions">
          <span v-for="action in savedPermissionActions" :key="action" class="permission-chip">{{ action }}</span>
        </div>
        <div class="modal-actions success-actions"><button class="primary success-confirm" @click="showPermissionSavedModal = false">{{ tr('完成', 'Done') }}</button></div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { getToken, isAdmin } from '../auth'
import { t, language } from '../i18n'
const tr = (zh: string, en: string) => language.value === 'en-US' ? en : zh
import {
  createKafkaCluster,
  createKafkaGroup,
  createKafkaTopic,
  deleteKafkaCluster,
  deleteKafkaGroup,
  deleteKafkaGroupTopic,
  deleteKafkaRecords,
  deleteKafkaTopic,
  deleteKafkaTopicConfig,
  getKafkaClusterStats,
  getKafkaClusterDetail,
  getKafkaGroupDetail,
  getKafkaGroupSummaries,
  getKafkaOverview,
  getKafkaTopic,
  getKafkaTopicConfigs,
  getAppStatus,
  listKafkaTopics,
  listKafkaAuditLogs,
  listKafkaClusters,
  listKafkaPermissions,
  readKafkaTopicData,
  saveKafkaPermissions,
  listSystemUsers,
  sendKafkaMessage,
  updateKafkaTopicConfigs,
  updateKafkaTopicPartitions,
  updateKafkaCluster,
} from '../api'

const route = useRoute()
const router = useRouter()
const KAFKA_WORKSPACE_KEY = 'kafka_active_workspace'
const kafkaTabs = ['topics', 'brokers', 'groups']

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
const isAdminUser = computed(() => isAdmin())
const loading = ref(false)
const clusters = ref<any[]>([])
const clusterSearch = ref('')
const clusterPage = ref(1)
const clusterPageSize = ref(10)
const filteredClusters = computed(() => {
  const q = clusterSearch.value.trim().toLowerCase()
  if (!q) return clusters.value
  return clusters.value.filter(c =>
    c.name.toLowerCase().includes(q) ||
    c.bootstrap_servers.toLowerCase().includes(q) ||
    (c.description || '').toLowerCase().includes(q)
  )
})
const pagedClusters = computed(() => {
  const start = (clusterPage.value - 1) * clusterPageSize.value
  return filteredClusters.value.slice(start, start + clusterPageSize.value)
})
const clusterTotalPages = computed(() => Math.ceil(filteredClusters.value.length / clusterPageSize.value))
const selectedClusterId = ref('')
const activeTab = ref('topics')
const overview = ref<any>({})
const clusterDetail = ref<any>({ brokers: [] })
const topicDetail = ref<any>({})
const selectedTopic = ref('')
const topicSubTab = ref('overview')
const topicConfigs = ref<any[]>([])
const topicConfigAllowlist = ref<string[]>([])
const editingConfigName = ref('')
const editingConfigValue = ref('')
const selectedGroup = ref('')
const groupDetail = ref<any>({ partitions: [] })
const groupSummaries = ref<Record<string, any>>({})
const groupSummariesLoading = ref(false)
const topicKeyword = ref('')
const topicPage = ref(1)
const topicPageSize = ref(50)
const topicPageItems = ref<any[]>([])
const topicTotal = ref(0)
const topicSortBy = ref('')
const topicSortOrder = ref<'asc' | 'desc'>('desc')
const groupKeyword = ref('')
const groupSearchMode = ref<'group' | 'topic'>('group')
const messages = ref<any[]>([])
const selectedMessageKeys = ref<string[]>([])
const expandedMessages = ref<Record<string, boolean>>({})
const copiedMessageKey = ref('')
const messageSearchHint = ref('')
const messagesLoading = ref(false)
const activeDateTimePicker = ref('')
const liveStreaming = ref(false)
const liveStreamStatus = ref('')
const sending = ref(false)
const enterpriseEnabled = ref(true)
const permissions = ref<any[]>([])
const systemUsers = ref<any[]>([])
const showClusterModal = ref(false)
const showTopicModal = ref(false)
const showPartitionModal = ref(false)
const showGroupModal = ref(false)
const showProduceModal = ref(false)
const showPermissionSavedModal = ref(false)
const confirmDialog = ref({ visible: false, title: '', message: '', target: '', eyebrow: 'Confirm Action', confirmText: '', cancelText: '', tone: 'danger' })
const noticeDialog = ref({ visible: false, title: '', message: '', target: '', eyebrow: 'Success' })
const errorDialog = ref({ visible: false, title: '', message: '', target: '', eyebrow: 'Failed' })
const permissionSaving = ref(false)
const lastSavedPermission = ref<{ username: string; actions: string[] }>({ username: '', actions: [] })
const editingCluster = ref<any>(null)
const clusterForm = ref<any>({})
const topicForm = ref({ topic: '', partitions: 1, replication_factor: 1 })
const partitionForm = ref({ partitions: 1 })
const messageForm = ref<any>({ partition: 0, autoOffsetReset: 'newest', offset: '', endOffset: '', count: 20, timeMode: 'none', startTime: '', endTime: '', keySearch: '', valueSearch: '' })
const produceMode = ref<'single' | 'batch'>('single')
const sendForm = ref({ partition: '', key: '', value: '' })
const batchDefaultPartition = ref('')
const batchImportText = ref('')
const deleteForm = ref({ partition: 0, before_offset: 0 })
const groupForm = ref({ group_id: '' })
const permissionForm = ref<any>({ username: '', actions: ['cluster_view'] })
let liveStreamAbort: AbortController | undefined
let liveStreamReader: ReadableStreamDefaultReader<Uint8Array> | undefined

const currentCluster = computed(() => clusters.value.find(c => c.id === selectedClusterId.value))
const currentClusterLabel = computed(() => currentCluster.value ? `${currentCluster.value.name} / ${currentCluster.value.bootstrap_servers}` : '-')
const activeTabLabel = computed(() => {
  if (activeTab.value === 'topics' && selectedTopic.value && topicSubTab.value === 'messages') return tr('Topic 消息', 'Topic Messages')
  if (activeTab.value === 'topics' && selectedTopic.value) return tr('Topic 详情', 'Topic Detail')
  if (activeTab.value === 'topics') return tr('Topic 列表', 'Topic List')
  if (activeTab.value === 'groups' && selectedGroup.value) return tr('Group 详情', 'Group Detail')
  if (activeTab.value === 'groups') return tr('Group 列表', 'Group List')
  if (activeTab.value === 'brokers') return tr('Broker 节点', 'Broker Nodes')
  return tr('Kafka 工作台', 'Kafka Workspace')
})
const groups = computed(() => overview.value.groups || [])
const topicTotalPages = computed(() => Math.max(1, Math.ceil(topicTotal.value / topicPageSize.value)))
const topicSummaryMap = computed(() => Object.fromEntries(topicPageItems.value.map((item: any) => [item.topic, item])))
const topicPartitions = computed(() => {
  const partitions = topicDetail.value.partitions || []
  if (partitions.length) return partitions
  const count = topicSummaryMap.value[selectedTopic.value]?.partition_count || 0
  return Array.from({ length: count }, (_, partition) => ({ partition, beginning_offset: null, end_offset: null, message_count: 0, lag_window: 0 }))
})
const selectedPartitionInfo = computed(() => messageForm.value.partition === 'all' ? null : topicPartitions.value.find((p: any) => p.partition === Number(messageForm.value.partition)))
const batchParseResult = computed(() => parseBatchMessages(batchImportText.value))
const produceSubmitDisabled = computed(() => sending.value || (produceMode.value === 'single' ? !sendForm.value.value : !!batchParseResult.value.error || batchParseResult.value.items.length === 0))
const topicMatchesGroupSearch = (topic: string) => {
  const keyword = groupKeyword.value.trim().toLowerCase()
  return !!keyword && topic.toLowerCase().includes(keyword)
}
const filteredGroups = computed(() => {
  const keyword = groupKeyword.value.trim().toLowerCase()
  if (!keyword) return groups.value
  if (groupSearchMode.value === 'topic') {
    return groups.value.filter((group: string) => groupSummary(group).topics.some((topic: string) => topic.toLowerCase().includes(keyword)))
  }
  return groups.value.filter((group: string) => group.toLowerCase().includes(keyword))
})
const visibleGroups = computed(() => filteredGroups.value.slice(0, 60))
const can = (action: string) => isAdminUser.value || !!currentCluster.value?.permissions?.includes(action)
const actionLabelMap = computed(() => Object.fromEntries(actionOptions.value.map(item => [item.value, item.label])))
const savedPermissionActions = computed(() => lastSavedPermission.value.actions.map(action => actionLabelMap.value[action] || action))
let confirmResolver: ((value: boolean) => void) | undefined

const openConfirm = (options: { title: string; message: string; target?: string; confirmText?: string; cancelText?: string; tone?: 'danger' | 'primary'; eyebrow?: string }) => new Promise<boolean>((resolve) => {
  confirmResolver = resolve
  confirmDialog.value = {
    visible: true,
    title: options.title,
    message: options.message,
    target: options.target || '',
    eyebrow: options.eyebrow || tr('确认操作', 'Confirm Action'),
    confirmText: options.confirmText || tr('确认', 'Confirm'),
    cancelText: options.cancelText || tr('取消', 'Cancel'),
    tone: options.tone || 'danger',
  }
})

const resolveConfirm = (value: boolean) => {
  confirmDialog.value.visible = false
  confirmResolver?.(value)
  confirmResolver = undefined
}

const showSuccessNotice = (options: { title: string; message: string; target?: string; eyebrow?: string }) => {
  noticeDialog.value = { visible: true, title: options.title, message: options.message, target: options.target || '', eyebrow: options.eyebrow || 'Success' }
}

const showErrorNotice = (options: { title: string; message: string; target?: string; eyebrow?: string }) => {
  errorDialog.value = { visible: true, title: options.title, message: options.message, target: options.target || '', eyebrow: options.eyebrow || 'Failed' }
}

const refreshAll = async (options: { loadStats?: boolean } = {}) => {
  loading.value = true
  try {
    clusters.value = await listKafkaClusters().catch(() => [])
    if (options.loadStats !== false) loadClusterStats()
    if (selectedClusterId.value) {
      await loadOverview()
    }
  } finally {
    loading.value = false
  }
}

const loadClusterStats = async () => {
  const visibleClusters = [...clusters.value]
  await Promise.all(visibleClusters.map(async (cluster) => {
    const stats = await getKafkaClusterStats(cluster.id).catch(() => null)
    if (!stats) return
    clusters.value = clusters.value.map(item => item.id === cluster.id ? { ...item, ...stats } : item)
  }))
}

const enterCluster = async (clusterId: string, options: { loadOverview?: boolean } = {}) => {
  selectedClusterId.value = clusterId
  if (!kafkaTabs.includes(String(activeTab.value))) activeTab.value = 'topics'
  if (!route.query.topic) selectedTopic.value = ''
  topicSubTab.value = 'overview'
  if (!route.query.group) selectedGroup.value = ''
  topicDetail.value = {}
  groupDetail.value = { partitions: [] }
  messages.value = []
  selectedMessageKeys.value = []
  syncKafkaWorkspace()
  const topic = typeof route.query.topic === 'string' ? route.query.topic : undefined
  const group = typeof route.query.group === 'string' ? route.query.group : undefined
  router.replace({
    path: '/kafka',
    query: {
      ...route.query,
      cluster: clusterId,
      tab: activeTab.value,
      topic: activeTab.value === 'topics' ? topic : undefined,
      group: activeTab.value === 'groups' ? group : undefined,
    },
  })
  if (options.loadOverview !== false) await loadOverview()
}

const backToClusters = () => {
  stopLiveStream()
  selectedClusterId.value = ''
  selectedTopic.value = ''
  selectedGroup.value = ''
  topicDetail.value = {}
  groupDetail.value = { partitions: [] }
  messages.value = []
  selectedMessageKeys.value = []
  localStorage.removeItem(KAFKA_WORKSPACE_KEY)
  window.dispatchEvent(new CustomEvent('kafka-workspace-change', { detail: null }))
  router.replace({ path: '/kafka' })
}

const syncKafkaWorkspace = () => {
  if (!selectedClusterId.value) return
  const payload = { clusterId: selectedClusterId.value, tab: activeTab.value, canManagePermissions: can('permission_manage') }
  localStorage.setItem(KAFKA_WORKSPACE_KEY, JSON.stringify(payload))
  window.dispatchEvent(new CustomEvent('kafka-workspace-change', { detail: payload }))
}

const handleKafkaTabSelect = (event: Event) => {
  const detail = (event as CustomEvent).detail
  if (!detail?.clusterId || detail.clusterId !== selectedClusterId.value) return
  if (!kafkaTabs.includes(detail.tab)) return
  activeTab.value = detail.tab
  if (detail.tab === 'topics') backToTopicList()
}

const handleKafkaHomeSelect = () => {
  backToClusters()
}

const loadOverview = async () => {
  if (!selectedClusterId.value) return
  overview.value = await getKafkaOverview(selectedClusterId.value).catch((err) => {
    alert(err.response?.data?.detail || err.message || tr('Kafka 集群连接失败', 'Kafka cluster connection failed'))
    return {}
  })
  if (activeTab.value === 'brokers') {
    await loadBrokerDetail()
  }
  if (activeTab.value === 'topics') {
    await loadTopicPage()
  }
  if (activeTab.value === 'groups') loadGroupSummaries()
  if (can('permission_manage')) await loadPermissions()
}

const loadBrokerDetail = async () => {
  if (!selectedClusterId.value) return
  clusterDetail.value = await getKafkaClusterDetail(selectedClusterId.value, { include_topics: false }).catch(() => ({ brokers: [] }))
}

const groupSummary = (group: string) => {
  const detail = groupSummaries.value[group] || {}
  const topics = detail.active_topics?.length ? detail.active_topics : detail.topics || [...new Set((detail.partitions || []).map((item: any) => item.topic).filter(Boolean))]
  return {
    topics,
    members: detail.members || [],
    memberCount: detail.member_count ?? (detail.members || []).length,
    totalLag: detail.total_lag || 0,
    partitionCount: (detail.partitions || []).length,
  }
}

const groupTopicRows = computed(() => {
  const topics = groupDetail.value.active_topics?.length ? groupDetail.value.active_topics : groupDetail.value.topics || []
  return topics.map((topic: string) => {
    const hosts = new Set<string>()
    ;(groupDetail.value.partitions || []).forEach((row: any) => {
      if (row.topic !== topic) return
      ;(row.hosts || []).forEach((host: string) => host && hosts.add(host))
      if (row.host) String(row.host).split(',').map(item => item.trim()).filter(Boolean).forEach(host => hosts.add(host))
    })
    ;(groupDetail.value.members || []).forEach((member: any) => {
      if (!(member.assignments || []).some((item: any) => item.topic === topic)) return
      if (member.client_host) hosts.add(member.client_host)
    })
    return { topic, host: [...hosts].join(', ') }
  })
})

const loadGroupSummaries = async () => {
  if (!selectedClusterId.value || activeTab.value !== 'groups') return
  const needAllGroups = groupSearchMode.value === 'topic' && groupKeyword.value.trim()
  const sourceGroups = needAllGroups ? groups.value : visibleGroups.value
  const missing = sourceGroups.filter((group: string) => !groupSummaries.value[group])
  if (!missing.length) return
  groupSummariesLoading.value = true
  try {
    for (let i = 0; i < missing.length; i += 80) {
      const batch = missing.slice(i, i + 80)
      const data = await getKafkaGroupSummaries(selectedClusterId.value, batch).catch(() => ({ items: [] }))
      const summaries = Object.fromEntries((data.items || []).map((item: any) => [item.group, item]))
      groupSummaries.value = { ...groupSummaries.value, ...summaries }
    }
  } finally {
    groupSummariesLoading.value = false
  }
}

const loadTopicPage = async () => {
  if (!selectedClusterId.value) return
  const data = await listKafkaTopics(selectedClusterId.value, {
    page: topicPage.value,
    page_size: topicPageSize.value,
    name: topicKeyword.value || undefined,
    sort_by: topicSortBy.value || undefined,
    sort_order: topicSortOrder.value,
  }).catch(() => ({ items: [], total: 0 }))
  topicPageItems.value = data.items || []
  topicTotal.value = data.total || 0
}

const logSizeSortLabel = computed(() => {
  if (topicSortBy.value !== 'log_size') return '↕'
  return topicSortOrder.value === 'desc' ? '↓' : '↑'
})

const toggleLogSizeSort = () => {
  if (topicSortBy.value !== 'log_size') {
    topicSortBy.value = 'log_size'
    topicSortOrder.value = 'desc'
  } else if (topicSortOrder.value === 'desc') {
    topicSortOrder.value = 'asc'
  } else {
    topicSortBy.value = ''
    topicSortOrder.value = 'desc'
  }
  topicPage.value = 1
  loadTopicPage()
}

const formatNumber = (value: number) => value >= 100000 ? value.toLocaleString() : String(Math.round(value || 0))

const selectTopic = async (topic: string, options: { loadConfigs?: boolean } = {}) => {
  stopLiveStream()
  selectedTopic.value = topic
  topicSubTab.value = 'messages'
  messages.value = []
  selectedMessageKeys.value = []
  router.replace({ path: '/kafka', query: { ...route.query, cluster: selectedClusterId.value, tab: activeTab.value, topic } })
  topicDetail.value = await getKafkaTopic(selectedClusterId.value, topic).catch((err) => {
    alert(err.response?.data?.detail || err.message || tr('Topic 分区 offset 加载失败', 'Failed to load topic partition offsets'))
    return {}
  })
  const first = topicDetail.value.partitions?.[0]
  messageForm.value.partition = first?.partition ?? 0
  messageForm.value.offset = ''
  messageForm.value.endOffset = ''
  deleteForm.value.partition = first?.partition ?? 0
  if (options.loadConfigs !== false) await loadTopicConfigs()
}

const setPartitionOffsets = (partition: number, beginningOffset: number, endOffset: number) => {
  const partitions = topicPartitions.value.map((item: any) => item.partition === partition
    ? { ...item, beginning_offset: beginningOffset, end_offset: endOffset, message_count: Math.max(endOffset - beginningOffset, 0), lag_window: Math.max(endOffset - beginningOffset, 0) }
    : item)
  topicDetail.value = { ...topicDetail.value, partitions }
}

watch(() => messageForm.value.partition, () => {
  stopLiveStream()
  messageForm.value.offset = ''
  messageForm.value.endOffset = ''
})

watch([topicKeyword, topicPageSize], () => {
  topicPage.value = 1
  loadTopicPage()
})

watch(topicPage, loadTopicPage)

watch(activeTab, (tab) => {
  if (selectedClusterId.value) {
    if (tab !== 'topics') {
      stopLiveStream()
      selectedTopic.value = ''
    }
    if (tab !== 'groups') selectedGroup.value = ''
    syncKafkaWorkspace()
    router.replace({ path: '/kafka', query: { ...route.query, cluster: selectedClusterId.value, tab, topic: tab === 'topics' && selectedTopic.value ? selectedTopic.value : undefined, group: tab === 'groups' && selectedGroup.value ? selectedGroup.value : undefined } })
  }
  if (tab === 'groups') loadGroupSummaries()
  if (tab === 'brokers') loadBrokerDetail()
  if (tab === 'topics') loadTopicPage()
  if (tab === 'permissions' && can('permission_manage')) loadPermissions()
})

watch([groupKeyword, groupSearchMode, visibleGroups], () => {
  if (activeTab.value === 'groups') loadGroupSummaries()
})

watch(() => route.query.tab, (tab) => {
  if (!selectedClusterId.value || typeof tab !== 'string') return
  if (tab === 'monitoring') {
    activeTab.value = 'topics'
    router.replace({ path: '/kafka', query: { ...route.query, tab: 'topics' } })
    return
  }
  if (kafkaTabs.includes(tab) && tab !== activeTab.value) activeTab.value = tab
})

watch(() => route.query.topic, async (topic) => {
  if (!selectedClusterId.value || activeTab.value !== 'topics') return
  const nextTopic = typeof topic === 'string' ? topic : ''
  if (!nextTopic) {
    if (!selectedTopic.value) return
    stopLiveStream()
    selectedTopic.value = ''
    topicDetail.value = {}
    topicConfigs.value = []
    messages.value = []
    selectedMessageKeys.value = []
    topicSubTab.value = 'overview'
    return
  }
  if (nextTopic !== selectedTopic.value) {
    restoreMessageQuery()
    await selectTopic(nextTopic, { loadConfigs: route.query.subtab !== 'messages' })
    restoreMessageQuery()
    if (shouldAutoLoadMessagesFromQuery()) await loadMessages()
  }
})

watch(topicSubTab, (tab) => {
  if (tab === 'configs' && selectedTopic.value && topicConfigs.value.length === 0) loadTopicConfigs()
})

watch(() => route.query.group, async (group) => {
  if (!selectedClusterId.value || activeTab.value !== 'groups') return
  const nextGroup = typeof group === 'string' ? group : ''
  if (!nextGroup) {
    if (!selectedGroup.value) return
    selectedGroup.value = ''
    groupDetail.value = { partitions: [] }
    return
  }
  if (nextGroup !== selectedGroup.value) await selectGroup(nextGroup)
})

watch(topicTotalPages, () => {
  if (topicPage.value > topicTotalPages.value) topicPage.value = topicTotalPages.value
})

const backToTopicList = () => {
  stopLiveStream()
  selectedTopic.value = ''
  topicDetail.value = {}
  topicConfigs.value = []
  messages.value = []
  selectedMessageKeys.value = []
  topicSubTab.value = 'overview'
  router.replace({ path: '/kafka', query: { cluster: selectedClusterId.value, tab: activeTab.value } })
}

const selectGroup = async (group: string) => {
  selectedGroup.value = group
  router.replace({ path: '/kafka', query: { cluster: selectedClusterId.value, tab: activeTab.value, group } })
  groupDetail.value = await getKafkaGroupDetail(selectedClusterId.value, group).catch((err) => {
    alert(err.response?.data?.detail || err.message || tr('消费组详情加载失败', 'Failed to load consumer group details'))
    return { partitions: [] }
  })
  groupSummaries.value = { ...groupSummaries.value, [group]: groupDetail.value }
}

const openTopicFromGroup = async (topic: string) => {
  if (!topic) return
  selectedGroup.value = ''
  groupDetail.value = { partitions: [] }
  activeTab.value = 'topics'
  await selectTopic(topic)
}

const backToGroupList = () => {
  selectedGroup.value = ''
  groupDetail.value = { partitions: [] }
  router.replace({ path: '/kafka', query: { cluster: selectedClusterId.value, tab: activeTab.value } })
}

const loadMessages = async () => {
  const timeMode = messageForm.value.timeMode === 'from' || messageForm.value.timeMode === 'range'
  const offsetRangeMode = messageForm.value.timeMode === 'offsetRange'
  if (timeMode && !messageForm.value.startTime) {
    alert(tr('请选择开始时间', 'Please select start time'))
    return
  }
  if (messageForm.value.timeMode === 'range' && !messageForm.value.endTime) {
    alert(tr('请选择结束时间', 'Please select end time'))
    return
  }
  if (offsetRangeMode && messageForm.value.offset === '' && messageForm.value.endOffset === '') {
    alert(tr('请填写开始 offset 或结束 offset', 'Please enter start offset or end offset'))
    return
  }
  const startTimeMs = timeMode ? dateTimeLocalToMs(messageForm.value.startTime) : undefined
  const endTimeMs = messageForm.value.timeMode === 'range' ? dateTimeLocalToMs(messageForm.value.endTime) : undefined
  if (timeMode && startTimeMs === undefined) {
    alert(tr('开始时间格式不正确', 'Invalid start time format'))
    return
  }
  if (messageForm.value.timeMode === 'range' && endTimeMs === undefined) {
    alert(tr('结束时间格式不正确', 'Invalid end time format'))
    return
  }
  if (startTimeMs !== undefined && endTimeMs !== undefined && endTimeMs < startTimeMs) {
    alert(tr('结束时间不能早于开始时间', 'End time cannot be earlier than start time'))
    return
  }
  const startOffset = (messageForm.value.timeMode === 'none' || offsetRangeMode) && messageForm.value.offset !== '' ? Number(messageForm.value.offset) : undefined
  const endOffset = offsetRangeMode && messageForm.value.endOffset !== '' ? Number(messageForm.value.endOffset) : undefined
  if (startOffset !== undefined && (!Number.isInteger(startOffset) || startOffset < 0)) {
    alert(tr('开始 offset 必须是非负整数', 'Start offset must be a non-negative integer'))
    return
  }
  if (endOffset !== undefined && (!Number.isInteger(endOffset) || endOffset < 0)) {
    alert(tr('结束 offset 必须是非负整数', 'End offset must be a non-negative integer'))
    return
  }
  if (startOffset !== undefined && endOffset !== undefined && endOffset < startOffset) {
    alert(tr('结束 offset 不能小于开始 offset', 'End offset cannot be less than start offset'))
    return
  }
  messagesLoading.value = true
  try {
    const data = await readKafkaTopicData(selectedClusterId.value, selectedTopic.value, {
      partition: messageForm.value.partition,
      autoOffsetReset: messageForm.value.autoOffsetReset,
      offset: startTimeMs !== undefined ? undefined : startOffset,
      end_offset: startTimeMs !== undefined ? undefined : endOffset,
      start_time_ms: startTimeMs,
      end_time_ms: endTimeMs,
      key_search: messageForm.value.keySearch?.trim() || undefined,
      value_search: messageForm.value.valueSearch?.trim() || undefined,
      count: messageForm.value.count,
      timeout_ms: messageForm.value.timeMode !== 'none' || messageForm.value.keySearch?.trim() || messageForm.value.valueSearch?.trim() ? 60000 : Math.max(1500, Math.min(300000, messageForm.value.count * 10)),
    })
    messages.value = data.messages || []
    selectedMessageKeys.value = []
    syncMessageQuery()
    messageSearchHint.value = formatMessageSearchHint(data)
    if (messageForm.value.partition !== 'all' && data.beginning_offset !== undefined && data.beginning_offset !== null && data.end_offset !== undefined && data.end_offset !== null) {
      setPartitionOffsets(Number(messageForm.value.partition), data.beginning_offset, data.end_offset)
    }
    expandedMessages.value = {}
  } finally {
    messagesLoading.value = false
  }
}

const handleMessageSearchEnter = () => {
  if (!can('message_read') || messagesLoading.value) return
  loadMessages()
}

const syncMessageQuery = () => {
  if (!selectedClusterId.value || !selectedTopic.value) return
  router.replace({
    path: '/kafka',
    query: {
      cluster: selectedClusterId.value,
      tab: 'topics',
      topic: selectedTopic.value,
      subtab: 'messages',
      partition: String(messageForm.value.partition ?? 0),
      autoOffsetReset: messageForm.value.autoOffsetReset || 'newest',
      offset: (messageForm.value.timeMode === 'none' || messageForm.value.timeMode === 'offsetRange') ? messageForm.value.offset || undefined : undefined,
      endOffset: messageForm.value.timeMode === 'offsetRange' ? messageForm.value.endOffset || undefined : undefined,
      count: String(messageForm.value.count || 20),
      timeMode: messageForm.value.timeMode || 'none',
      startTime: messageForm.value.startTime || undefined,
      endTime: messageForm.value.endTime || undefined,
      keySearch: messageForm.value.keySearch?.trim() || undefined,
      valueSearch: messageForm.value.valueSearch?.trim() || undefined,
    },
  })
}

const restoreMessageQuery = () => {
  if (route.query.subtab === 'messages') topicSubTab.value = 'messages'
  if (route.query.partition !== undefined) messageForm.value.partition = route.query.partition === 'all' ? 'all' : Number(route.query.partition) || 0
  if (typeof route.query.autoOffsetReset === 'string') messageForm.value.autoOffsetReset = route.query.autoOffsetReset
  if (typeof route.query.offset === 'string') messageForm.value.offset = route.query.offset
  if (typeof route.query.endOffset === 'string') messageForm.value.endOffset = route.query.endOffset
  if (typeof route.query.count === 'string') messageForm.value.count = Number(route.query.count) || 20
  if (typeof route.query.timeMode === 'string') messageForm.value.timeMode = route.query.timeMode
  if (typeof route.query.startTime === 'string') messageForm.value.startTime = route.query.startTime
  if (typeof route.query.endTime === 'string') messageForm.value.endTime = route.query.endTime
  if (typeof route.query.keySearch === 'string') messageForm.value.keySearch = route.query.keySearch
  if (typeof route.query.valueSearch === 'string') messageForm.value.valueSearch = route.query.valueSearch
}

const shouldAutoLoadMessagesFromQuery = () => route.query.subtab === 'messages' && !!route.query.topic

const toggleLiveStream = () => {
  if (liveStreaming.value) stopLiveStream()
  else startLiveStream()
}

const stopLiveStream = () => {
  liveStreamAbort?.abort()
  liveStreamReader?.cancel().catch(() => {})
  liveStreamAbort = undefined
  liveStreamReader = undefined
  liveStreaming.value = false
  if (liveStreamStatus.value) liveStreamStatus.value = '实时拉取已停止'
}

const startLiveStream = async () => {
  if (!selectedClusterId.value || !selectedTopic.value) return
  if (messageForm.value.partition === 'all') {
    liveStreamStatus.value = 'All Partitions 暂不支持实时拉取，请选择具体 Partition。'
    return
  }
  stopLiveStream()
  const token = getToken()
  const params = new URLSearchParams({ partition: String(messageForm.value.partition) })
  if (messageForm.value.offset !== '') params.set('offset', String(messageForm.value.offset))
  if (messageForm.value.keySearch?.trim()) params.set('key_search', messageForm.value.keySearch.trim())
  if (messageForm.value.valueSearch?.trim()) params.set('value_search', messageForm.value.valueSearch.trim())
  const url = `/kafka-api/clusters/${encodeURIComponent(selectedClusterId.value)}/topics/${encodeURIComponent(selectedTopic.value)}/stream?${params.toString()}`
  liveStreamAbort = new AbortController()
  liveStreaming.value = true
  liveStreamStatus.value = '实时拉取已开启，等待新消息...'
  try {
    const resp = await fetch(url, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      signal: liveStreamAbort.signal,
    })
    if (!resp.ok || !resp.body) throw new Error(`HTTP ${resp.status}`)
    const reader = resp.body.getReader()
    liveStreamReader = reader
    const decoder = new TextDecoder()
    let buffer = ''
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const events = buffer.split('\n\n')
      buffer = events.pop() || ''
      for (const event of events) handleStreamEvent(event)
    }
  } catch (err: any) {
    if (err?.name !== 'AbortError') liveStreamStatus.value = `实时拉取异常：${err?.message || err}`
  } finally {
    liveStreamReader = undefined
    if (liveStreamAbort && !liveStreamAbort.signal.aborted) {
      liveStreaming.value = false
      liveStreamAbort = undefined
    }
  }
}

const handleStreamEvent = (event: string) => {
  const eventType = event.split('\n').find(line => line.startsWith('event:'))?.slice(6).trim()
  const data = event.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trim()).join('\n')
  if (!data) return
  let payload: any
  try {
    payload = JSON.parse(data)
  } catch {
    return
  }
  if (eventType === 'ready') {
    liveStreamStatus.value = `实时拉取已连接，从 offset ${payload.offset ?? '-'} 开始`
    return
  }
  const key = messageKey(payload)
  if (messages.value.some(item => messageKey(item) === key)) return
  messages.value = [payload, ...messages.value].slice(0, 500)
  selectedMessageKeys.value = selectedMessageKeys.value.filter(key => messages.value.some(item => messageKey(item) === key))
  liveStreamStatus.value = `实时拉取中，最新 offset ${payload.offset}`
  if (selectedPartitionInfo.value?.end_offset !== undefined && payload.offset >= selectedPartitionInfo.value.end_offset) {
    setPartitionOffsets(payload.partition, selectedPartitionInfo.value.beginning_offset ?? 0, payload.offset + 1)
  }
}

const formatMessageSearchHint = (data: any) => {
  if (!data) return ''
  const parts = [`seek offset ${data.seek_offset ?? '-'}`]
  if (data.end_offset_filter !== undefined && data.end_offset_filter !== null) parts.push(`end offset ${data.end_offset_filter}`)
  if (data.start_time_ms) parts.push(`start ${formatTime(data.start_time_ms)}`)
  if (data.end_time_ms) parts.push(`end ${formatTime(data.end_time_ms)}`)
  if (data.key_search) parts.push(`key 包含 "${data.key_search}"`)
  if (data.value_search) parts.push(`value 包含 "${data.value_search}"`)
  const orderText = data.order_by === 'timestamp_desc' ? '按 Kafka 时间最新在上' : data.order_by === 'event_time' ? '按业务时间最新在上' : '按 Kafka offset 最新在上'
  if (Array.isArray(data.searched_partitions) && data.searched_partitions.length > 1) parts.push(`Partition ${data.searched_partitions.join(',')}`)
  parts.push(`扫描 ${data.scanned ?? messages.value.length} 条，返回 ${messages.value.length} 条，${orderText}`)
  if (!data.start_time_ms && !data.end_time_ms && (data.end_offset_filter === undefined || data.end_offset_filter === null)) parts.push(`默认基于 count=${data.count ?? messageForm.value.count} 条最新消息搜索`)
  return parts.join(' / ')
}

const dateTimeLocalToMs = (value: string) => {
  const timestamp = value ? new Date(value).getTime() : NaN
  return Number.isFinite(timestamp) ? timestamp : undefined
}

const msToDateTimeLocal = (timestamp: number) => {
  const date = new Date(timestamp)
  const offset = date.getTimezoneOffset() * 60000
  return new Date(timestamp - offset).toISOString().slice(0, 19)
}

const datePart = (value: string) => (value || '').slice(0, 10)
const timePart = (value: string) => (value || '').slice(11, 19) || '00:00:00'
const formatDateTimeInput = (value: string) => value ? value.replace('T', ' ') : ''

const updateDateTime = (field: 'startTime' | 'endTime', part: 'date' | 'time', event: Event) => {
  const val = (event.target as HTMLInputElement).value
  const curDate = datePart(messageForm.value[field]) || datePart(new Date().toISOString())
  const curTime = timePart(messageForm.value[field])
  messageForm.value[field] = part === 'date' ? `${val}T${curTime}` : `${curDate}T${val}`
}

const setDateTimeNow = (field: 'startTime' | 'endTime') => {
  messageForm.value[field] = msToDateTimeLocal(Date.now())
}

const setMessageTimeRange = (minutes: number) => {
  const end = Date.now()
  const start = end - minutes * 60 * 1000
  messageForm.value.startTime = msToDateTimeLocal(start)
  if (messageForm.value.timeMode !== 'range') messageForm.value.timeMode = 'range'
  messageForm.value.endTime = msToDateTimeLocal(end)
}

const formatMessageTime = (timestamp: number | string | undefined | null) => {
  const value = Number(timestamp)
  return Number.isFinite(value) && value > 0 ? formatTime(value) : '-'
}

const messageKey = (msg: any) => `${msg.partition}-${msg.offset}`
const isMessageExpanded = (msg: any) => !!expandedMessages.value[messageKey(msg)]
const parseJsonValue = (value: any) => {
  if (typeof value !== 'string') return value
  const text = value.trim()
  if (!text || !['{', '['].includes(text[0])) return null
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}
const isJsonMessage = (value: any) => parseJsonValue(value) !== null
const formatMessageValue = (value: any) => {
  const parsed = parseJsonValue(value)
  if (parsed !== null) return JSON.stringify(parsed, null, 2)
  return String(value ?? '')
}
const escapeHtml = (value: string) => value
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;')
const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
const highlightText = (text: string, keyword: string) => {
  const escaped = escapeHtml(text)
  const normalizedKeyword = keyword.trim()
  if (!normalizedKeyword) return escaped
  const pattern = new RegExp(escapeRegExp(escapeHtml(normalizedKeyword)), 'gi')
  return escaped.replace(pattern, match => `<mark class="message-hit">${match}</mark>`)
}
const highlightMessageKey = (value: any) => highlightText(String(value ?? ''), messageForm.value.keySearch || '')
const highlightMessageValue = (value: any) => highlightText(formatMessageValue(value), messageForm.value.valueSearch || '')
const toggleMessage = (msg: any) => {
  const key = messageKey(msg)
  expandedMessages.value = { ...expandedMessages.value, [key]: !expandedMessages.value[key] }
}
const copyMessage = async (msg: any) => {
  const text = msg.value || ''
  try {
    if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text)
    else fallbackCopy(text)
    copiedMessageKey.value = messageKey(msg)
    window.setTimeout(() => {
      if (copiedMessageKey.value === messageKey(msg)) copiedMessageKey.value = ''
    }, 1200)
  } catch {
    fallbackCopy(text)
    copiedMessageKey.value = messageKey(msg)
  }
}

const exportMessage = (msg: any) => {
  downloadJson(formatExportPayload([msg]), `${selectedTopic.value}-${msg.partition}-${msg.offset}.json`)
}

const exportMessages = () => {
  if (!messages.value.length) return
  const selected = selectedMessageKeys.value.length
    ? messages.value.filter(item => selectedMessageKeys.value.includes(messageKey(item)))
    : messages.value
  const partition = messageForm.value.partition ?? 'all'
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  downloadText(selected.map(item => item.raw_value ?? item.value ?? '').join(';\n'), `${selectedTopic.value}-partition-${partition}-${timestamp}.txt`)
}

const toggleSelectAllMessages = (event: Event) => {
  const checked = (event.target as HTMLInputElement).checked
  selectedMessageKeys.value = checked ? messages.value.map(messageKey) : []
}

const clearSelectedMessages = () => {
  selectedMessageKeys.value = []
}

const formatExportPayload = (items: any[]) => ({
  cluster_id: selectedClusterId.value,
  topic: selectedTopic.value,
  exported_at: new Date().toISOString(),
  count: items.length,
  messages: items.map((item) => ({
    topic: item.topic,
    partition: item.partition,
    offset: item.offset,
    timestamp: item.timestamp,
    timestamp_text: formatMessageTime(item.timestamp),
    event_time_ms: item.event_time_ms,
    key: item.key || '',
    value: parseJsonValue(item.value) ?? item.value ?? '',
    raw_value: item.value ?? '',
    headers: item.headers || [],
  })),
})

const downloadJson = (payload: any, filename: string) => {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json;charset=utf-8' })
  downloadBlob(blob, filename)
}

const downloadText = (text: string, filename: string) => {
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' })
  downloadBlob(blob, filename)
}

const downloadBlob = (blob: Blob, filename: string) => {
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = sanitizeFilename(filename)
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

const sanitizeFilename = (name: string) => name.replace(/[\\/:*?"<>|\s]+/g, '_').slice(0, 180)

const fallbackCopy = (text: string) => {
  const textarea = document.createElement('textarea')
  textarea.value = text
  textarea.setAttribute('readonly', 'readonly')
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  document.body.appendChild(textarea)
  textarea.select()
  document.execCommand('copy')
  document.body.removeChild(textarea)
}

const openProduceModal = () => {
  const partitions = topicPartitions.value || []
  const selected = partitions.some((p: any) => p.partition === messageForm.value.partition) ? messageForm.value.partition : partitions[0]?.partition
  sendForm.value = { partition: String(selected ?? 0), key: '', value: '' }
  batchDefaultPartition.value = String(selected ?? '')
  produceMode.value = 'single'
  showProduceModal.value = true
  getAppStatus().then(status => { enterpriseEnabled.value = !!status.enterprise }).catch(() => {})
}

const normalizeProduceValue = (value: any) => {
  if (value === undefined || value === null) return ''
  return typeof value === 'string' ? value : JSON.stringify(value)
}

const normalizeProduceItem = (item: any) => {
  const fallbackPartition = batchDefaultPartition.value === '' ? undefined : Number(batchDefaultPartition.value)
  if (item && typeof item === 'object' && !Array.isArray(item)) {
    const rawValue = item.raw_value ?? item.value ?? item.message ?? item.msg ?? item.payload ?? item.body ?? item
    const partitionValue = item.partition === undefined || item.partition === null || item.partition === '' ? fallbackPartition : Number(item.partition)
    return {
      partition: typeof partitionValue === 'number' && Number.isFinite(partitionValue) ? partitionValue : undefined,
      key: item.key === undefined || item.key === null ? '' : String(item.key),
      value: normalizeProduceValue(rawValue),
    }
  }
  return { partition: typeof fallbackPartition === 'number' && Number.isFinite(fallbackPartition) ? fallbackPartition : undefined, key: '', value: normalizeProduceValue(item) }
}

const normalizeBatchExportLine = (line: string) => {
  let value = line.trim().replace(/^P\s*\[?\s*(?=\{)/, '').replace(/[;；\s]+$/, '').trim()
  if (value.endsWith(']') && value.startsWith('{')) value = value.slice(0, -1).trim()
  if (value === ']' || value === '[') return ''
  return value
}

const normalizeBatchText = (text: string) => text
  .trim()
  .replace(/^导出\s*/g, '')
  .replace(/^export\s*/i, '')
  .split(/\r?\n/)
  .map(line => line.trim())
  .filter(line => line && !/^Topic\s*[:：]/i.test(line))
  .join('\n')
  .trim()

const normalizeBatchItems = (source: any[]) => {
  const items = source.map(normalizeProduceItem).filter((item) => item.value !== '')
  if (items.length > 1000) return { items: [], error: '单次最多写入 1000 条消息' }
  return { items, error: items.length ? '' : '未解析到可写入的消息内容' }
}

const parseBatchMessages = (text: string): { items: Array<{ partition?: number; key: string; value: string }>; error: string } => {
  const trimmed = normalizeBatchText(text)
  if (!trimmed) return { items: [], error: '' }
  try {
    const parsed = JSON.parse(normalizeBatchExportLine(trimmed))
    const source: any[] = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.messages) ? parsed.messages : [parsed]
    return normalizeBatchItems(source)
  } catch {
    const chunks = trimmed.includes(';') || trimmed.includes('；')
      ? trimmed.split(/[;；]+/).map(normalizeBatchExportLine).filter(Boolean)
      : trimmed.split(/\r?\n/).map(line => line.trim()).filter(Boolean)
    const parsedItems = chunks.map((line) => {
      try {
        return JSON.parse(line)
      } catch {
        return line
      }
    })
    return normalizeBatchItems(parsedItems)
  }
}

const importBatchFile = async (event: Event) => {
  const file = (event.target as HTMLInputElement).files?.[0]
  if (!file) return
  batchImportText.value = await file.text()
}

const loadBatchExample = () => {
  produceMode.value = 'batch'
  batchImportText.value = '第一条测试消息；\n第二条测试消息；\n第三条测试消息'
}

const loadTopicConfigs = async () => {
  if (!selectedTopic.value) return
  const data = await getKafkaTopicConfigs(selectedClusterId.value, selectedTopic.value).catch(() => ({ configs: [], allowlist: [] }))
  topicConfigs.value = data.configs || []
  topicConfigAllowlist.value = data.allowlist || []
}

const editTopicConfig = (cfg: any) => {
  editingConfigName.value = cfg.name
  editingConfigValue.value = cfg.sensitive ? '' : String(cfg.value ?? '')
}

const cancelTopicConfigEdit = () => {
  editingConfigName.value = ''
  editingConfigValue.value = ''
}

const saveTopicConfig = async (name: string) => {
  if (!selectedTopic.value || !name) return
  const ok = await openConfirm({ title: tr('修改 Topic 配置', 'Update Topic Config'), message: tr('该配置会立即写入 Kafka，请确认新值已检查无误。', 'This config will be applied to Kafka immediately. Confirm the new value is correct.'), target: `${selectedTopic.value} / ${name}`, confirmText: tr('确认修改', 'Update'), tone: 'primary' })
  if (!ok) return
  await updateKafkaTopicConfigs(selectedClusterId.value, selectedTopic.value, { [name]: editingConfigValue.value })
  cancelTopicConfigEdit()
  await loadTopicConfigs()
}

const deleteTopicConfig = async (name: string) => {
  if (!selectedTopic.value) return
  const ok = await openConfirm({ title: tr('恢复默认配置', 'Restore Default Config'), message: tr('将删除该 Topic 覆盖配置并恢复 Kafka 默认值。', 'This removes the topic-level override and restores the Kafka default value.'), target: `${selectedTopic.value} / ${name}`, confirmText: tr('恢复默认', 'Restore'), tone: 'danger' })
  if (!ok) return
  await deleteKafkaTopicConfig(selectedClusterId.value, selectedTopic.value, name)
  await loadTopicConfigs()
}

const sendMessage = async () => {
  sending.value = true
  try {
    if (produceMode.value === 'batch') {
      if (!enterpriseEnabled.value) {
        alert(tr('批量导入消息属于完整版功能，完整版密钥过期后不可用', 'Batch import is a Full Edition feature and unavailable when the Full Edition license expires'))
        return
      }
      if (batchParseResult.value.error || batchParseResult.value.items.length === 0) return
      await sendKafkaMessage(selectedClusterId.value, { topic: selectedTopic.value, messages: batchParseResult.value.items })
      batchImportText.value = ''
    } else {
      await sendKafkaMessage(selectedClusterId.value, { topic: selectedTopic.value, partition: Number(sendForm.value.partition), key: sendForm.value.key || undefined, value: sendForm.value.value })
      sendForm.value.value = ''
    }
    showProduceModal.value = false
    await loadMessages()
  } finally {
    sending.value = false
  }
}

const deleteRecords = async () => {
  const ok = await openConfirm({ title: tr('删除分区记录', 'Delete Partition Records'), message: tr('该操作会删除指定 offset 之前的记录，执行后无法从 KafkaVista 恢复。', 'This deletes records before the specified offset and cannot be restored from KafkaVista.'), target: `${selectedTopic.value} / partition ${deleteForm.value.partition} / offset < ${deleteForm.value.before_offset}`, confirmText: tr('确认删除', 'Delete'), tone: 'danger' })
  if (!ok) return
  await deleteKafkaRecords(selectedClusterId.value, { topic: selectedTopic.value, ...deleteForm.value })
  await selectTopic(selectedTopic.value)
}

const createTopic = async () => {
  const topicName = topicForm.value.topic
  try {
    await createKafkaTopic(selectedClusterId.value, topicForm.value)
    showTopicModal.value = false
    topicForm.value = { topic: '', partitions: 1, replication_factor: 1 }
    await loadOverview()
    await loadTopicPage()
    showSuccessNotice({ title: tr('Topic 创建成功', 'Topic Created'), message: tr('Topic 已创建完成，列表和统计信息已刷新。', 'The topic has been created. Lists and statistics have been refreshed.'), target: topicName, eyebrow: 'Topic Ready' })
  } catch (err: any) {
    alert(err.response?.data?.detail || err.message || tr('Topic 创建失败', 'Failed to create topic'))
  }
}

const openPartitionModal = () => {
  partitionForm.value.partitions = topicPartitions.value.length + 1
  showPartitionModal.value = true
}

const updateTopicPartitions = async () => {
  const currentPartitions = topicPartitions.value.length
  const targetPartitions = Number(partitionForm.value.partitions)
  if (!selectedTopic.value || targetPartitions <= currentPartitions) return
  const ok = await openConfirm({ title: tr('修改 Topic 分区数', 'Update Topic Partitions'), message: tr('分区数只能增加，修改后 Kafka 不支持回退到更少分区。', 'Partitions can only be increased. Kafka does not support reverting to fewer partitions.'), target: `${selectedTopic.value}: ${currentPartitions} -> ${targetPartitions}`, confirmText: tr('确认修改', 'Update'), tone: 'primary', eyebrow: 'Topic Partition' })
  if (!ok) return
  try {
    await updateKafkaTopicPartitions(selectedClusterId.value, selectedTopic.value, targetPartitions)
    showPartitionModal.value = false
    await selectTopic(selectedTopic.value)
    await loadOverview()
    await loadTopicPage()
    showSuccessNotice({ title: tr('分区数修改成功', 'Partitions Updated'), message: tr('Topic 分区数已增加，详情和列表已刷新。', 'Topic partitions have been increased. Details and lists have been refreshed.'), target: `${selectedTopic.value}: ${targetPartitions}`, eyebrow: 'Topic Partition' })
  } catch (err: any) {
    showErrorNotice({ title: tr('分区数修改失败', 'Failed to Update Partitions'), message: err.response?.data?.detail || err.message || tr('分区数修改失败，请稍后重试。', 'Failed to update partitions. Please try again later.'), target: selectedTopic.value, eyebrow: 'Topic Partition' })
  }
}

const removeTopic = async () => {
  const topicName = selectedTopic.value
  const ok = await openConfirm({ title: tr('删除 Topic', 'Delete Topic'), message: tr('删除 Topic 会移除其中的分区和消息数据，请确认没有生产或消费依赖。', 'Deleting a topic removes its partitions and message data. Confirm there are no producer or consumer dependencies.'), target: selectedTopic.value, confirmText: tr('确认删除 Topic', 'Delete Topic'), tone: 'danger', eyebrow: 'Danger Zone' })
  if (!ok) return
  try {
    await deleteKafkaTopic(selectedClusterId.value, topicName)
    backToTopicList()
    await loadOverview()
    await loadTopicPage()
    showSuccessNotice({ title: tr('Topic 删除成功', 'Topic Deleted'), message: tr('该 Topic 已从 Kafka 集群删除，列表和统计信息已刷新。', 'The topic has been deleted from the Kafka cluster. Lists and statistics have been refreshed.'), target: topicName, eyebrow: 'Topic Removed' })
  } catch (err: any) {
    alert(err.response?.data?.detail || err.message || tr('Topic 删除失败', 'Failed to delete topic'))
  }
}

const removeGroup = async (groupId: string) => {
  const ok = await openConfirm({ title: tr('删除 Consumer Group', 'Delete Consumer Group'), message: tr('将删除该 Consumer Group 的 Kafka 元数据，请确认业务已停止使用。', 'This deletes Kafka metadata for the consumer group. Confirm the workload no longer uses it.'), target: groupId, confirmText: tr('确认删除', 'Delete'), tone: 'danger' })
  if (!ok) return
  await deleteKafkaGroup(selectedClusterId.value, groupId)
  await loadOverview()
}

const openGroupModal = () => {
  groupForm.value.group_id = ''
  showGroupModal.value = true
}

const unlinkGroupTopic = async (topic: string) => {
  if (!selectedGroup.value || !topic) return
  const ok = await openConfirm({ title: tr('解除 Group 与 Topic 订阅关系', 'Unsubscribe Group from Topic'), message: tr('该操作会删除该 Group 在该 Topic 下所有分区的已提交 offset。若仍有消费者正在订阅该 Topic，Kafka 可能会拒绝操作。', 'This deletes all committed offsets for this group on the topic. Kafka may reject it if active consumers are still subscribed.'), target: `${selectedGroup.value} / ${topic}`, confirmText: tr('确认解除', 'Unsubscribe'), tone: 'danger', eyebrow: 'Consumer Offset' })
  if (!ok) return
  try {
    await deleteKafkaGroupTopic(selectedClusterId.value, selectedGroup.value, topic)
    groupSummaries.value = { ...groupSummaries.value, [selectedGroup.value]: undefined }
    await selectGroup(selectedGroup.value)
    await loadOverview()
    showSuccessNotice({ title: tr('订阅关系已解除', 'Subscription Removed'), message: tr('已删除该 Group 在该 Topic 下的已提交 offset，Group 详情已刷新。', 'Committed offsets for this group and topic were deleted. Group details have been refreshed.'), target: `${selectedGroup.value} / ${topic}`, eyebrow: 'Offset Removed' })
  } catch (err: any) {
    showErrorNotice({ title: tr('解除订阅关系失败', 'Failed to Remove Subscription'), message: err.response?.data?.detail || err.message || tr('解除订阅关系失败，请稍后重试。', 'Failed to remove subscription. Please try again later.'), target: `${selectedGroup.value} / ${topic}`, eyebrow: 'Kafka Offset' })
  }
}

const createGroup = async () => {
  const groupId = groupForm.value.group_id.trim()
  if (!groupId) return
  try {
    await createKafkaGroup(selectedClusterId.value, groupId)
    showGroupModal.value = false
    groupForm.value.group_id = ''
    await loadOverview()
    showSuccessNotice({ title: tr('Group 创建成功', 'Group Created'), message: tr('已加入 Consumer Group 列表。消费者使用该 Group ID 并提交 offset 后，会显示真实消费关系。', 'The group has been added to the Consumer Group list. Real consumer relations appear after consumers use this Group ID and commit offsets.'), target: groupId, eyebrow: 'Group Ready' })
  } catch (err: any) {
    showErrorNotice({ title: tr('Group 创建失败', 'Failed to Create Group'), message: err.response?.data?.detail || err.message || tr('Group 创建失败，请稍后重试。', 'Failed to create group. Please try again later.'), target: groupId, eyebrow: 'Consumer Group' })
  }
}

const openCluster = (cluster: any) => {
  editingCluster.value = cluster
  clusterForm.value = cluster ? { ...cluster, cluster_type: cluster.cluster_type || 'cluster', sasl_password: '' } : { name: '', cluster_type: 'single', bootstrap_servers: '', security_protocol: 'PLAINTEXT', sasl_mechanism: '', sasl_username: '', sasl_password: '', description: '', is_active: true }
  showClusterModal.value = true
}

const saveCluster = async () => {
  if (editingCluster.value?.id) await updateKafkaCluster(editingCluster.value.id, clusterForm.value)
  else await createKafkaCluster(clusterForm.value)
  showClusterModal.value = false
  await refreshAll()
}

const removeCluster = async () => {
  if (!currentCluster.value) return
  const ok = await openConfirm({ title: tr('删除集群配置', 'Delete Cluster Config'), message: tr('只删除 KafkaVista 中的集群配置，不会删除真实 Kafka 集群。', 'Only the cluster config in KafkaVista is removed. The real Kafka cluster is not deleted.'), target: currentCluster.value.name, confirmText: tr('确认删除', 'Delete'), tone: 'danger' })
  if (!ok) return
  await deleteKafkaCluster(currentCluster.value.id)
  selectedClusterId.value = ''
  await refreshAll()
}

const deleteClusterFromList = async (cluster: any) => {
  if (!cluster?.id) return
  const ok = await openConfirm({ title: tr('删除集群配置', 'Delete Cluster Config'), message: tr('只删除 KafkaVista 中的集群配置，不会删除真实 Kafka 集群。', 'Only the cluster config in KafkaVista is removed. The real Kafka cluster is not deleted.'), target: cluster.name, confirmText: tr('确认删除', 'Delete'), tone: 'danger' })
  if (!ok) return
  try {
    await deleteKafkaCluster(cluster.id)
    if (selectedClusterId.value === cluster.id) selectedClusterId.value = ''
    await refreshAll()
    alert(tr('集群已删除', 'Cluster deleted'))
  } catch (err: any) {
    alert(err.response?.data?.detail || err.message || tr('集群删除失败', 'Failed to delete cluster'))
  }
}

const loadPermissions = async () => {
  if (!selectedClusterId.value) return
  const [permissionItems, userItems] = await Promise.all([
    listKafkaPermissions(selectedClusterId.value).catch(() => []),
    listSystemUsers().catch(() => []),
    listKafkaAuditLogs(selectedClusterId.value).catch(() => []),
  ])
  permissions.value = permissionItems
  systemUsers.value = userItems
}

const editPermission = (item: any) => {
  permissionForm.value = { username: item.username, actions: [...item.actions] }
}

const selectPermissionUser = () => {
  const row = permissions.value.find((item: any) => item.username === permissionForm.value.username)
  permissionForm.value.actions = row?.actions ? [...row.actions] : ['cluster_view']
}

const selectReadOnlyPermission = () => {
  permissionForm.value.actions = ['cluster_view', 'message_read']
}

const selectOpsPermission = () => {
  permissionForm.value.actions = ['cluster_view', 'topic_create', 'topic_config_manage', 'message_read', 'message_send', 'group_create']
}

const formatPermissionActions = (actions: string[]) => actions.map(action => actionLabelMap.value[action] || action).join(', ')

const savePermission = async () => {
  if (!selectedClusterId.value || permissionSaving.value) return
  permissionSaving.value = true
  try {
    lastSavedPermission.value = { username: permissionForm.value.username, actions: [...permissionForm.value.actions] }
    await saveKafkaPermissions(selectedClusterId.value, permissionForm.value)
    permissionForm.value = { username: '', actions: ['cluster_view'] }
    await loadPermissions()
    showPermissionSavedModal.value = true
  } finally {
    permissionSaving.value = false
  }
}

const formatTime = (timestamp: number) => timestamp ? new Date(timestamp).toLocaleString() : '-'
const formatTimeText = (value: string) => value ? new Date(value).toLocaleString() : '-'
const formatDate = (value: string) => value ? new Date(value).toLocaleDateString() : '-'
const formatLogSize = (bytes: number) => {
  const value = Number(bytes || 0)
  if (value <= 0) return '0 B'
  const gb = value / 1024 / 1024 / 1024
  if (gb >= 1) return `${gb.toFixed(2)} GB`
  const mb = value / 1024 / 1024
  if (mb >= 1) return `${mb.toFixed(2)} MB`
  const kb = value / 1024
  if (kb >= 1) return `${kb.toFixed(2)} KB`
  return `${Math.round(value)} B`
}

const configValueHint = (cfg: any) => {
  if (cfg?.name !== 'retention.ms' || cfg?.sensitive) return ''
  const ms = Number(cfg.value)
  if (!Number.isFinite(ms) || ms < 0) return ''
  const hours = ms / 1000 / 60 / 60
  if (hours >= 24) return `约 ${hours.toFixed(1)} 小时 / ${(hours / 24).toFixed(2)} 天`
  return `约 ${hours.toFixed(2)} 小时`
}

onMounted(async () => {
  getAppStatus().then(status => { enterpriseEnabled.value = !!status.enterprise }).catch(() => {})
  window.addEventListener('kafka-tab-select', handleKafkaTabSelect as EventListener)
  window.addEventListener('kafka-home-select', handleKafkaHomeSelect as EventListener)
  const clusterId = typeof route.query.cluster === 'string' ? route.query.cluster : ''
  const tab = typeof route.query.tab === 'string' && kafkaTabs.includes(route.query.tab) ? route.query.tab : 'topics'
  const topic = typeof route.query.topic === 'string' ? route.query.topic : ''
  const group = typeof route.query.group === 'string' ? route.query.group : ''
  const restoringMessages = !!clusterId && tab === 'topics' && !!topic && route.query.subtab === 'messages'
  await refreshAll({ loadStats: !restoringMessages })
  if (clusterId && clusters.value.some(cluster => cluster.id === clusterId)) {
    activeTab.value = tab
    await enterCluster(clusterId, { loadOverview: !restoringMessages })
    if (topic && activeTab.value === 'topics') {
      restoreMessageQuery()
      await selectTopic(topic, { loadConfigs: !restoringMessages })
      restoreMessageQuery()
      if (shouldAutoLoadMessagesFromQuery()) await loadMessages()
    }
    if (group && activeTab.value === 'groups') await selectGroup(group)
  }
})
onUnmounted(() => {
  window.removeEventListener('kafka-tab-select', handleKafkaTabSelect as EventListener)
  window.removeEventListener('kafka-home-select', handleKafkaHomeSelect as EventListener)
  stopLiveStream()
})
</script>

<style scoped>
.kafka-page { width: 100%; margin: 0; padding: 24px 20px; min-width: 0; overflow-x: hidden; }
.hero { display: flex; justify-content: space-between; gap: 20px; align-items: flex-end; padding: 30px; border: 1px solid var(--border-color); border-radius: 26px; background: radial-gradient(circle at 12% 0, rgba(34,211,238,.25), transparent 35%), radial-gradient(circle at 88% 10%, rgba(99,102,241,.22), transparent 30%), var(--bg-secondary); margin-bottom: 20px; }
.hero-actions { display: flex; gap: 10px; flex-wrap: wrap; }
.eyebrow { color: var(--accent-secondary); font-size: 12px; letter-spacing: .14em; text-transform: uppercase; margin-bottom: 8px; }
h1 { font-size: 32px; margin-bottom: 8px; } h2 { font-size: 18px; } .hero p, .muted { color: var(--text-muted); font-size: 13px; }
.panel-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.search-input { background: var(--bg-tertiary); border: 1px solid var(--border-color); color: var(--text-primary); padding: 7px 12px; border-radius: 8px; font-size: 13px; width: 260px; }
.search-input:focus { outline: none; border-color: var(--accent-primary); }
.cluster-pagination { margin-top: 12px; }
.cluster-pagination .page-btns { display: flex; align-items: center; gap: 10px; }
.cluster-pagination .page-btns button { background: none; border: 1px solid var(--border-color); color: var(--text-primary); padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; }
.cluster-pagination .page-btns button:disabled { opacity: 0.4; cursor: not-allowed; }
.cluster-home { margin-top: 18px; }
.cluster-table { border: 1px solid var(--border-color); border-radius: 18px; overflow: auto; background: var(--bg-tertiary); }
.cluster-head, .cluster-row { display: grid; grid-template-columns: minmax(240px, 1fr) 80px 90px 90px 140px 160px 170px 130px; gap: 12px; align-items: center; padding: 13px 14px; min-width: 1170px; }
.cluster-head { color: var(--text-muted); font-size: 12px; background: rgba(255,255,255,.04); }
.cluster-row { width: 100%; border: 0; border-top: 1px solid var(--border-color); background: transparent; color: var(--text-primary); text-align: left; cursor: pointer; font-family: inherit; }
.cluster-row:hover { background: rgba(99,102,241,.12); }
.cluster-row strong, .cluster-row small { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cluster-row small { color: var(--text-muted); font-size: 11px; margin-top: 4px; }
.cluster-actions { display: flex; gap: 8px; align-items: center; justify-content: flex-end; }
.cluster-actions em { color: var(--accent-primary); font-weight: 700; font-size: 12px; font-style: normal; }
.title-side { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.cluster-inline-pill { display: inline-flex; align-items: center; max-width: min(460px, 42vw); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; border: 1px solid rgba(34,211,238,.28); border-radius: 999px; padding: 4px 9px; color: var(--accent-secondary); background: rgba(34,211,238,.08); font-size: 12px; font-weight: 700; }
.content-stack { display: grid; gap: 18px; min-width: 0; }
.panel { border: 1px solid var(--border-color); background: var(--panel-bg); border-radius: 20px; padding: 18px; min-width: 0; }
.panel-title { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 14px; }
.panel-title.compact { margin-bottom: 10px; }
.actions { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
.cluster-item, .list-item { width: 100%; text-align: left; border: 1px solid var(--border-color); background: var(--bg-tertiary); color: var(--text-primary); border-radius: 14px; padding: 12px; cursor: pointer; }
.cluster-item span { display: block; color: var(--text-muted); font-size: 12px; margin-top: 5px; overflow-wrap: anywhere; }
.cluster-item.active, .list-item.active, .partition-card.active, .list-row.active { border-color: var(--accent-primary); background: rgba(99,102,241,.16); }
.workspace-tabs { display: flex; gap: 8px; overflow-x: auto; padding-top: 14px; margin-top: 14px; border-top: 1px solid var(--border-color); }
.workspace-tab { border: 1px solid var(--border-color); background: var(--bg-tertiary); color: var(--text-secondary); border-radius: 999px; padding: 8px 14px; cursor: pointer; white-space: nowrap; }
.workspace-tab.active { color: #fff; background: var(--accent-primary); border-color: var(--accent-primary); }
.summary-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
.summary-card { text-align: left; padding: 16px; border-radius: 16px; border: 1px solid var(--border-color); background: var(--bg-tertiary); color: var(--text-primary); cursor: pointer; transition: border-color .2s, transform .2s, background .2s; }
.summary-card:hover { border-color: var(--accent-primary); transform: translateY(-1px); background: rgba(99,102,241,.12); }
label { display: block; color: var(--text-muted); font-size: 12px; margin-bottom: 7px; }
.summary-card strong { display: block; font-size: 28px; margin-bottom: 6px; }
.summary-card span { color: var(--text-muted); font-size: 12px; }
.metric-card { border: 1px solid var(--border-color); border-radius: 16px; background: var(--bg-tertiary); padding: 14px; min-width: 0; }
.metric-title { display: flex; justify-content: space-between; gap: 10px; align-items: baseline; margin-bottom: 10px; }
.metric-title span { color: var(--text-muted); font-size: 12px; }
.metric-title strong { font-size: 22px; }
.sparkline { width: 100%; height: 120px; display: block; border-radius: 12px; background: var(--panel-soft-bg); margin-bottom: 10px; }
.sparkline path { fill: none; stroke: #22d3ee; stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; }
.sparkline.consume path { stroke: #34d399; }
.sparkline.lag path { stroke: #f59e0b; }
.metric-list { display: grid; gap: 6px; }
.metric-list div { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 10px; color: var(--text-muted); font-size: 12px; }
.metric-list span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.metric-list strong { color: var(--text-primary); }
.dashboard-list { margin: 12px 0; grid-template-columns: repeat(2, minmax(0, 1fr)); display: grid; }
.monitor-time-menu { position: relative; }
.monitor-time-menu > button { max-width: 360px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.monitor-time-popover { position: absolute; right: 0; top: calc(100% + 8px); z-index: 20; width: min(360px, 88vw); display: grid; gap: 8px; padding: 12px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-secondary); box-shadow: 0 18px 50px rgba(0,0,0,.35); }
.monitor-time-popover label { margin: 0; color: var(--text-muted); font-size: 12px; }
.monitor-time-actions { display: flex; gap: 8px; justify-content: flex-end; flex-wrap: wrap; }
.grafana-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin: 12px 0; }
.grafana-grid.maximizing .grafana-card:not(.maximized) { display: none; }
.grafana-grid.maximizing { grid-template-columns: 1fr; }
.grafana-card { position: relative; overflow: visible; border: 1px solid var(--border-color); border-radius: 16px; padding: 14px; min-height: 190px; background: linear-gradient(180deg, rgba(255,255,255,.055), rgba(255,255,255,.018)); }
.grafana-card.maximized { min-height: 520px; }
.grafana-card::before { content: ''; position: absolute; inset: 0; background-image: linear-gradient(rgba(255,255,255,.04) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,.04) 1px, transparent 1px); background-size: 40px 40px; opacity: .55; pointer-events: none; }
.grafana-head { position: relative; display: flex; justify-content: space-between; align-items: baseline; gap: 12px; margin-bottom: 10px; }
.grafana-head span { color: var(--text-muted); font-size: 12px; letter-spacing: .08em; text-transform: uppercase; }
.grafana-head strong { font-size: 28px; color: var(--text-primary); }
.grafana-card small { position: relative; color: var(--text-muted); font-size: 12px; }
.grafana-plot { position: relative; display: grid; grid-template-columns: 58px minmax(0, 1fr); gap: 8px; align-items: stretch; margin-bottom: 8px; }
.grafana-yaxis { position: relative; z-index: 1; display: grid; grid-template-rows: auto 1fr auto auto; gap: 4px; min-height: 120px; color: var(--text-muted); font-size: 11px; text-align: right; }
.grafana-yaxis em { font-style: normal; color: var(--accent-secondary); font-size: 10px; text-transform: uppercase; }
.grafana-chart-wrap { position: relative; min-width: 0; overflow: visible; }
.grafana-chart { position: relative; width: 100%; height: 120px; display: block; }
.grafana-card.maximized .grafana-chart, .grafana-card.maximized .grafana-yaxis { height: 420px; min-height: 420px; }
.grafana-chart path { fill: none; stroke: #22d3ee; stroke-width: 3; stroke-linecap: round; stroke-linejoin: round; filter: drop-shadow(0 0 8px rgba(34,211,238,.35)); }
.grafana-card.green .grafana-chart path { stroke: #34d399; filter: drop-shadow(0 0 8px rgba(52,211,153,.35)); }
.grafana-card.purple .grafana-chart path { stroke: #a78bfa; filter: drop-shadow(0 0 8px rgba(167,139,250,.35)); }
.grafana-card.amber .grafana-chart path { stroke: #f59e0b; filter: drop-shadow(0 0 8px rgba(245,158,11,.35)); }
.grafana-crosshair { position: absolute; top: 0; bottom: 0; width: 1px; z-index: 4; background: rgba(226,232,240,.55); transform: translateX(-50%); pointer-events: none; }
.grafana-crosshair i { position: absolute; top: 50%; left: 50%; width: 10px; height: 10px; border-radius: 999px; background: #fff; border: 2px solid var(--accent-secondary); transform: translate(-50%, -50%); box-shadow: 0 0 0 4px rgba(34,211,238,.18); }
.grafana-tooltip { position: absolute; top: 8px; z-index: 5; transform: translateX(-50%); display: grid; gap: 9px; min-width: 340px; max-width: 520px; padding: 14px 16px; border: 1px solid rgba(148,163,184,.45); border-radius: 14px; background: rgba(15,23,42,.96); box-shadow: 0 16px 46px rgba(0,0,0,.42); pointer-events: none; }
.grafana-tooltip b { color: var(--text-primary); font-size: 18px; line-height: 1.2; }
.grafana-tooltip span { color: var(--text-muted); font-size: 13px; }
.tooltip-kv { display: grid; gap: 7px; padding-top: 8px; border-top: 1px solid rgba(148,163,184,.25); }
.tooltip-kv div { display: grid; grid-template-columns: 82px minmax(0, 1fr); gap: 10px; align-items: center; }
.tooltip-kv em { color: var(--text-muted); font-style: normal; font-size: 12px; }
.tooltip-kv strong { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--accent-secondary); font-size: 13px; font-weight: 700; }
.split-panel { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
.module-board { display: grid; gap: 10px; }
.module-card { position: relative; overflow: hidden; text-align: left; border: 1px solid var(--border-color); border-radius: 16px; padding: 18px; background: var(--bg-tertiary); color: var(--text-primary); cursor: pointer; min-height: 108px; transition: border-color .2s, transform .2s; }
.module-card:hover { transform: translateY(-1px); border-color: var(--accent-primary); }
.module-card::after { content: ''; position: absolute; width: 120px; height: 120px; right: -42px; top: -42px; border-radius: 50%; opacity: .18; background: currentColor; }
.module-card span { display: inline-flex; color: var(--text-muted); font-size: 12px; margin-bottom: 8px; }
.module-card strong { display: block; font-size: 18px; margin-bottom: 6px; }
.module-card small { color: var(--text-muted); font-size: 12px; line-height: 1.5; }
.module-card.topic { color: #22d3ee; }
.module-card.group { color: #34d399; }
.module-card.message { color: #a78bfa; }
.list-box { max-height: 320px; overflow: auto; display: grid; gap: 8px; }
.broker-table { border: 1px solid var(--border-color); border-radius: 14px; overflow: auto; background: var(--bg-tertiary); }
.broker-head, .broker-row { display: grid; grid-template-columns: 90px minmax(180px, 1fr) 120px 120px 100px; gap: 10px; align-items: center; padding: 11px 12px; font-size: 12px; min-width: 660px; }
.broker-head { color: var(--text-muted); background: rgba(255,255,255,.04); }
.broker-row { border-top: 1px solid var(--border-color); }
.broker-row strong { color: var(--accent-secondary); }
.broker-row span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.broker-status { width: fit-content; border-radius: 999px; padding: 3px 9px; border: 1px solid var(--border-color); }
.broker-status.alive { color: var(--accent-success); background: rgba(52,211,153,.1); border-color: rgba(52,211,153,.25); }
.broker-status.down { color: var(--accent-danger); background: rgba(248,113,113,.1); border-color: rgba(248,113,113,.25); }
.group-list-box { max-height: 720px; overflow: auto; display: grid; gap: 10px; padding-right: 4px; }
.group-search-bar { display: grid; grid-template-columns: 150px minmax(0, 1fr); gap: 10px; align-items: start; margin-bottom: 10px; }
.group-search-mode { min-height: 40px; }
.group-card { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 14px; border: 1px solid var(--border-color); border-radius: 16px; background: var(--bg-tertiary); cursor: pointer; transition: border-color .2s, background .2s; }
.group-card:hover { border-color: var(--accent-primary); background: rgba(99,102,241,.12); }
.group-card.active { border-color: var(--accent-primary); background: rgba(99,102,241,.16); }
.group-card-main { min-width: 0; display: grid; gap: 8px; }
.group-title-row, .group-topic-line, .group-member-line { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; min-width: 0; }
.group-title-row strong { overflow-wrap: anywhere; font-size: 14px; }
.count-pill, .group-pill, .topic-chip, .member-chip { display: inline-flex; align-items: center; max-width: 360px; border-radius: 999px; padding: 3px 9px; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.count-pill { color: var(--text-secondary); background: rgba(59,130,246,.1); border: 1px solid rgba(59,130,246,.25); }
.group-pill { color: var(--text-secondary); background: rgba(255,255,255,.05); border: 1px solid var(--border-color); }
.group-pill.warn { color: var(--accent-warning); border-color: rgba(251,191,36,.24); background: rgba(251,191,36,.1); }
.topic-chip { color: var(--accent-secondary); background: rgba(34,211,238,.1); border: 1px solid rgba(34,211,238,.2); }
.topic-chip.matched { color: var(--accent-warning); background: rgba(251,191,36,.14); border-color: rgba(251,191,36,.34); }
.topic-relation-chip { display: inline-flex; align-items: center; gap: 6px; padding: 4px; border: 1px solid rgba(148,163,184,.16); border-radius: 999px; background: rgba(255,255,255,.035); }
.topic-relation-chip .topic-chip { border-radius: 999px; }
.group-topic-detail-table { border: 1px solid var(--border-color); border-radius: 12px; overflow: auto; }
.group-topic-detail-head, .group-topic-detail-row { display: grid; grid-template-columns: minmax(180px, 1fr) minmax(150px, 220px) 110px; gap: 8px; align-items: center; padding: 10px 12px; font-size: 12px; }
.group-topic-detail-head { color: var(--text-muted); background: rgba(255,255,255,.04); }
.group-topic-detail-row { border-top: 1px solid var(--border-color); }
.unlink-topic-btn { border: 1px solid rgba(248,113,113,.28); background: rgba(248,113,113,.1); color: #fca5a5; border-radius: 999px; padding: 3px 8px; font-size: 12px; }
.unlink-topic-btn:hover { background: rgba(248,113,113,.18); border-color: rgba(248,113,113,.45); }
.clickable-chip { cursor: pointer; font-family: inherit; }
.clickable-chip:hover, .assignment-chip:hover, .offset-topic-link:hover { border-color: var(--accent-secondary); background: rgba(34,211,238,.18); }
.member-chip { color: var(--accent-success); background: rgba(52,211,153,.1); border: 1px solid rgba(52,211,153,.2); }
.group-card-side { display: grid; gap: 8px; justify-items: end; color: var(--text-muted); font-size: 12px; }
.group-create { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; margin-bottom: 10px; }
.list-row { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 11px 12px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-tertiary); overflow-wrap: anywhere; }
.list-row.clickable { cursor: pointer; }
.list-row strong { color: var(--accent-secondary); font-size: 12px; }
.search-input { margin-bottom: 10px; }
.group-search-bar .search-input { margin-bottom: 0; }
.topic-table { border: 1px solid var(--border-color); border-radius: 16px; overflow: auto; background: var(--bg-tertiary); max-height: 680px; }
.topic-head, .topic-row { display: grid; grid-template-columns: minmax(0, 1fr) 90px 120px 120px 120px; gap: 12px; align-items: center; padding: 12px 14px; }
.topic-head { color: var(--text-muted); font-size: 12px; background: rgba(255,255,255,.04); }
.sort-head { border: 0; background: transparent; color: var(--accent-primary); padding: 0; text-align: left; font: inherit; font-weight: 700; cursor: pointer; }
.topic-row { width: 100%; text-align: left; border: 0; border-top: 1px solid var(--border-color); background: transparent; color: var(--text-primary); cursor: pointer; font-family: inherit; }
.topic-row:hover { background: rgba(99,102,241,.12); }
.topic-row span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.topic-row strong { color: var(--accent-primary); font-size: 12px; text-align: right; }
.pagination-bar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; padding-top: 12px; color: var(--text-muted); font-size: 12px; }
.pagination-bar select { width: auto; min-width: 110px; padding: 7px 10px; }
.topic-detail-page { min-height: 560px; }
.back-inline { margin-bottom: 10px; }
.quick-actions { display: grid; gap: 10px; }
.topic-summary { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; padding: 12px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); margin-bottom: 10px; }
.topic-summary span { color: var(--text-muted); font-size: 12px; }
.compact-grid { max-height: 420px; overflow: auto; }
.group-stat-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin-bottom: 12px; }
.group-stat-grid div { border: 1px solid var(--border-color); border-radius: 12px; padding: 10px; background: var(--panel-soft-bg); }
.group-stat-grid strong { font-size: 22px; color: var(--accent-secondary); }
.group-detail-block { border: 1px solid var(--border-color); border-radius: 14px; padding: 12px; background: var(--panel-soft-bg); margin-bottom: 12px; }
.detail-line { min-height: 30px; }
.member-table { border: 1px solid var(--border-color); border-radius: 12px; overflow: auto; }
.member-head, .member-row { display: grid; grid-template-columns: minmax(180px, .8fr) 140px minmax(260px, 1fr); gap: 10px; align-items: center; padding: 10px 12px; font-size: 12px; }
.member-head { color: var(--text-muted); background: rgba(255,255,255,.04); }
.member-row { border-top: 1px solid var(--border-color); }
.member-row span { min-width: 0; overflow-wrap: anywhere; }
.assignment-list { display: flex; flex-wrap: wrap; gap: 6px; }
.assignment-chip { max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--accent-secondary); border: 1px solid rgba(34,211,238,.2); background: rgba(34,211,238,.08); border-radius: 999px; padding: 2px 7px; font: inherit; font-weight: 500; cursor: pointer; }
.group-offset-table { border: 1px solid var(--border-color); border-radius: 14px; overflow: auto; max-height: 520px; }
.offset-head, .offset-row { display: grid; grid-template-columns: minmax(180px, 1fr) minmax(130px, 180px) 70px 110px 110px 90px; gap: 8px; align-items: center; padding: 10px 12px; font-size: 12px; }
.offset-head { color: var(--text-muted); background: rgba(255,255,255,.04); }
.offset-row { border-top: 1px solid var(--border-color); }
.offset-row span:first-child, .offset-topic-link, .offset-host { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.offset-topic-link { text-align: left; color: var(--accent-secondary); background: rgba(34,211,238,.08); border: 1px solid rgba(34,211,238,.18); border-radius: 999px; padding: 3px 8px; font: inherit; cursor: pointer; }
.offset-row strong { color: var(--accent-warning); }
.partition-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-bottom: 14px; }
.partition-card { text-align: left; border: 1px solid var(--border-color); border-radius: 14px; padding: 12px; background: var(--bg-tertiary); color: var(--text-primary); cursor: pointer; }
.partition-card span { color: var(--text-muted); font-size: 12px; display: block; margin-top: 4px; }
.offset-summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin: 10px 0 12px; }
.offset-summary div { border: 1px solid var(--border-color); border-radius: 12px; background: var(--panel-soft-bg); padding: 10px 12px; }
.offset-summary strong { font-size: 18px; color: var(--accent-secondary); }
.message-tools { display: grid; grid-template-columns: repeat(auto-fit, minmax(145px, 1fr)); gap: 10px; align-items: end; margin: 12px 0; }
.message-control-grid { position: sticky; top: 0; z-index: 3; padding: 8px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--panel-sticky-bg); }
.message-control-grid .count-field { min-width: 88px; }
.message-action-buttons { grid-column: 1 / -1; display: flex; justify-content: flex-start; gap: 8px; align-items: center; padding-top: 2px; }
.message-action-buttons button { min-width: 110px; height: 38px; white-space: nowrap; }
.datetime-field { position: relative; }
.datetime-trigger { width: 100%; min-height: 36px; padding: 8px 10px; border: 1px solid var(--border-light); border-radius: 10px; background: var(--bg-input); color: var(--text-primary); text-align: left; font: inherit; font-size: 12px; cursor: pointer; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.datetime-trigger:hover { border-color: var(--accent-primary); }
.datetime-popover { position: absolute; z-index: 30; top: calc(100% + 6px); left: 0; width: min(400px, 94vw); padding: 16px; border: 1px solid var(--border-light); border-radius: 16px; background: var(--bg-secondary); box-shadow: 0 18px 40px rgba(0,0,0,.32); }
.dt-picker-row { display: flex; gap: 12px; align-items: flex-start; }
.dt-picker-group { flex: 1; display: grid; gap: 6px; }
.dt-picker-group label { font-size: 12px; font-weight: 600; color: var(--text-muted); }
.dt-native { width: 100%; min-height: 44px; padding: 0 12px; border: 1px solid var(--border-light); border-radius: 10px; background: var(--bg-tertiary); color: var(--text-primary); font-size: 16px; font-family: inherit; box-sizing: border-box; cursor: pointer; }
.dt-native:hover { border-color: var(--accent-primary); background: var(--bg-input); }
.dt-native:focus { outline: none; border-color: var(--accent-primary); box-shadow: 0 0 0 3px rgba(99,102,241,.2); }
.datetime-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--border-light); }
.datetime-actions button { padding: 8px 18px; font-size: 13px; border-radius: 8px; }
.time-shortcuts { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin: -4px 0 12px; padding: 8px 10px; border: 1px dashed var(--border-light); border-radius: 12px; background: var(--panel-soft-bg); }
.time-shortcuts span { color: var(--text-muted); font-size: 12px; margin-right: 2px; }
.time-shortcuts button { white-space: nowrap; }
.message-hint { margin: -4px 0 10px; }
.message-hint.live { color: var(--accent-secondary); }
.message-export-bar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; padding: 8px 10px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--panel-soft-bg); margin-bottom: 10px; }
.select-all, .message-select { display: inline-flex; align-items: center; gap: 5px; margin: 0; color: var(--text-muted); font-size: 12px; }
.select-all input, .message-select input { width: auto; }
.live-toggle.active { color: var(--accent-success); border-color: rgba(52,211,153,.45); background: rgba(52,211,153,.12); }
.top-tools { grid-template-columns: minmax(240px, 420px); }
.field { display: grid; }
input, select, textarea { width: 100%; background: var(--bg-input); color: var(--text-primary); border: 1px solid var(--border-light); border-radius: 10px; padding: 10px 12px; font-family: inherit; }
textarea { min-height: 120px; resize: vertical; }
.message-list { display: grid; gap: 6px; max-height: 760px; overflow: auto; min-width: 0; max-width: 100%; }
.message-card { border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-tertiary); padding: 8px 10px; min-width: 0; max-width: 100%; overflow-x: hidden; }
.message-meta { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; color: var(--text-muted); font-size: 12px; margin-bottom: 6px; overflow-wrap: anywhere; }
.message-time { color: var(--accent-secondary); }
.json-viewer { margin-top: 8px; padding: 14px; border: 1px solid rgba(99,102,241,.24); border-radius: 12px; background: var(--code-bg); color: var(--text-primary); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; line-height: 1.7; tab-size: 2; overflow: auto; min-width: 0; max-width: 100%; }
.json-viewer.plain { color: var(--text-primary); border-color: var(--border-color); background: var(--panel-soft-bg); }
.message-value.collapsed { max-height: 34px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; padding: 7px 10px; }
.message-value.collapsed::after { display: none; }
.message-hit { color: #111827; background: #fde047; border-radius: 4px; padding: 0 2px; }
.config-panel { display: grid; gap: 12px; }
.config-toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; color: var(--text-muted); font-size: 12px; }
.config-layout { display: block; }
.config-table { border: 1px solid var(--border-color); border-radius: 16px; overflow: auto; background: var(--bg-tertiary); max-height: 620px; }
.config-head, .config-row { display: grid; grid-template-columns: minmax(220px, .9fr) minmax(180px, 1fr) 130px; gap: 10px; align-items: center; padding: 11px 12px; font-size: 12px; }
.config-head { color: var(--text-muted); background: rgba(255,255,255,.04); position: sticky; top: 0; z-index: 1; }
.config-row { border-top: 1px solid var(--border-color); }
.config-name { color: var(--text-primary); font-weight: 600; overflow-wrap: anywhere; }
.config-value { color: var(--text-secondary); overflow-wrap: anywhere; }
.config-value small { margin-left: 8px; color: var(--accent-warning); font-size: 11px; white-space: nowrap; }
.config-edit { display: flex; gap: 6px; align-items: center; }
.inline-config-input { padding: 7px 9px; font-size: 12px; }
pre { white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; color: var(--text-primary); font-size: 12px; line-height: 1.6; min-width: 0; max-width: 100%; }
.send-form, .form { display: grid; gap: 10px; }
.danger-zone { border-color: rgba(248,113,113,.32); }
.permission-toolbar { display: grid; grid-template-columns: minmax(220px, 360px) minmax(180px, 1fr); gap: 10px; margin-bottom: 12px; }
.permission-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-bottom: 12px; }
.permission-card { display: grid; grid-template-columns: auto 1fr; gap: 4px 10px; align-items: start; border: 1px solid var(--border-color); border-radius: 12px; padding: 12px; background: var(--bg-tertiary); }
.permission-card input { width: auto; margin-top: 3px; }
.permission-card strong { color: var(--text-primary); font-size: 13px; }
.permission-card span { grid-column: 2; color: var(--text-muted); font-size: 12px; }
.permission-actions { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; margin-bottom: 12px; }
.check { display: flex; align-items: center; gap: 6px; padding: 9px 10px; border: 1px solid var(--border-light); border-radius: 10px; margin: 0; }
.check input { width: auto; }
.permission-list { display: grid; gap: 8px; }
.permission-row { display: grid; grid-template-columns: 160px 1fr; gap: 12px; padding: 10px 12px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-tertiary); cursor: pointer; }
.permission-row span { color: var(--text-muted); font-size: 12px; overflow-wrap: anywhere; }
.primary { background: var(--accent-primary); color: #fff; border: 0; border-radius: 10px; padding: 10px 14px; cursor: pointer; }
.ghost { background: transparent; color: var(--text-secondary); border: 1px solid var(--border-light); border-radius: 10px; padding: 9px 12px; cursor: pointer; }
.ghost.selected { background: rgba(99,102,241,.18); color: var(--accent-primary); border-color: rgba(99,102,241,.42); }
.danger { background: rgba(248,113,113,.12); color: var(--accent-danger); border: 1px solid rgba(248,113,113,.28); border-radius: 10px; padding: 9px 12px; cursor: pointer; }
.small { padding: 6px 10px; font-size: 12px; } .mini { padding: 4px 8px; font-size: 12px; }
button:disabled { opacity: .55; cursor: not-allowed; }
.empty { color: var(--text-muted); text-align: center; padding: 28px; } .empty.compact { padding: 12px; }
.modal-mask { position: fixed; inset: 0; z-index: 100; background: rgba(0,0,0,.65); display: grid; place-items: center; padding: 20px; overflow: auto; }
.modal { width: min(620px, 100%); background: var(--bg-secondary); border: 1px solid var(--border-color); border-radius: 20px; padding: 22px; }
.modal h3 { margin-bottom: 16px; }
.modal-heading { display: flex; gap: 14px; align-items: center; margin-bottom: 14px; }
.modal-heading h3 { margin: 0; font-size: 22px; letter-spacing: -.02em; }
.modal-eyebrow { margin: 0 0 4px; color: var(--accent-secondary); font-size: 11px; font-weight: 800; letter-spacing: .14em; text-transform: uppercase; }
.modal-icon { flex: 0 0 auto; width: 48px; height: 48px; border-radius: 16px; display: grid; place-items: center; color: #fff; font-weight: 900; box-shadow: 0 16px 36px rgba(0,0,0,.22); }
.topic-create-modal { overflow: hidden; border-color: rgba(34,211,238,.24); background: linear-gradient(145deg, rgba(15,23,42,.98), rgba(30,41,59,.96)); box-shadow: 0 28px 90px rgba(0,0,0,.42); }
.topic-icon { background: linear-gradient(135deg, #22d3ee, #6366f1); }
.group-create-modal { overflow: hidden; border-color: rgba(167,139,250,.26); background: radial-gradient(circle at top right, rgba(167,139,250,.16), transparent 34%), linear-gradient(145deg, rgba(15,23,42,.98), rgba(30,41,59,.96)); box-shadow: 0 28px 90px rgba(0,0,0,.44); }
.group-icon { background: linear-gradient(135deg, #8b5cf6, #22d3ee); }
.group-create-form { gap: 14px; }
.group-create-hint { display: grid; gap: 5px; padding: 12px 14px; border: 1px solid rgba(167,139,250,.22); border-radius: 14px; background: rgba(167,139,250,.08); color: var(--text-secondary); font-size: 13px; line-height: 1.6; }
.group-create-hint strong { color: #c4b5fd; }
.confirm-mask { backdrop-filter: blur(5px); }
.confirm-modal { width: min(560px, 100%); overflow: hidden; background: linear-gradient(145deg, rgba(15,23,42,.98), rgba(30,41,59,.96)); box-shadow: 0 30px 90px rgba(0,0,0,.48); }
.confirm-modal.danger { border-color: rgba(248,113,113,.34); }
.confirm-modal.primary { border-color: rgba(99,102,241,.34); }
.confirm-modal.danger .confirm-icon { background: linear-gradient(135deg, #ef4444, #f97316); }
.confirm-modal.primary .confirm-icon { background: linear-gradient(135deg, #6366f1, #22d3ee); }
.confirm-message { margin: 0 0 14px; color: var(--text-secondary); line-height: 1.7; }
.confirm-target { display: grid; gap: 5px; padding: 12px 14px; border: 1px solid rgba(148,163,184,.2); border-radius: 14px; background: rgba(255,255,255,.045); }
.confirm-target span { color: var(--text-muted); font-size: 12px; }
.confirm-target strong { color: var(--text-primary); overflow-wrap: anywhere; }
.produce-modal { width: min(760px, 100%); max-height: calc(100vh - 40px); overflow: auto; display: flex; flex-direction: column; }
.produce-tabs { display: flex; gap: 8px; margin-bottom: 12px; }
.batch-toolbar { display: grid; grid-template-columns: minmax(160px, 220px) minmax(0, 1fr); gap: 10px; align-items: end; }
.batch-textarea { min-height: 260px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
.batch-status { display: flex; justify-content: space-between; gap: 10px; align-items: center; padding: 9px 11px; border: 1px solid rgba(52,211,153,.25); border-radius: 12px; background: rgba(52,211,153,.08); color: var(--accent-success); font-size: 12px; }
.batch-status.error { border-color: rgba(248,113,113,.3); background: rgba(248,113,113,.1); color: var(--accent-danger); }
.batch-preview { display: grid; gap: 6px; max-height: 190px; overflow: auto; border: 1px solid var(--border-color); border-radius: 12px; padding: 8px; background: var(--panel-soft-bg); }
.batch-preview-row { display: grid; grid-template-columns: 44px 100px 130px minmax(0, 1fr); gap: 8px; align-items: center; padding: 7px 8px; border-radius: 8px; background: var(--bg-tertiary); color: var(--text-muted); font-size: 12px; }
.batch-preview-row strong { color: var(--text-primary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.success-modal { width: min(520px, 100%); padding: 0; overflow: hidden; border-color: rgba(52,211,153,.28); background: linear-gradient(145deg, rgba(15,23,42,.96), rgba(30,41,59,.96)); box-shadow: 0 30px 90px rgba(0,0,0,.46); }
.notice-modal { width: min(520px, 100%); padding: 0; overflow: hidden; border-color: rgba(52,211,153,.28); background: linear-gradient(145deg, rgba(15,23,42,.98), rgba(30,41,59,.96)); box-shadow: 0 30px 90px rgba(0,0,0,.46); }
.notice-modal::before { content: ''; display: block; height: 8px; background: linear-gradient(90deg, #10b981, #22d3ee); }
.error-notice-modal { border-color: rgba(248,113,113,.35); }
.error-notice-modal::before { background: linear-gradient(90deg, #ef4444, #f97316); }
.error-icon { background: linear-gradient(135deg, #ef4444, #f97316); font-size: 24px; font-weight: 900; }
.error-eyebrow { color: #fca5a5; }
.error-summary { border-color: rgba(248,113,113,.18); }
.error-confirm { min-width: 110px; background: linear-gradient(135deg, #ef4444, #f97316); box-shadow: 0 12px 24px rgba(239,68,68,.18); }
.notice-hero { padding-bottom: 10px; }
.notice-summary { grid-template-columns: 1fr 1fr; }
.success-modal::before { content: ''; display: block; height: 8px; background: linear-gradient(90deg, #22d3ee, #34d399, #a78bfa); }
.success-hero { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 16px; align-items: center; padding: 26px 28px 12px; text-align: left; }
.success-icon { width: 64px; height: 64px; border-radius: 20px; display: grid; place-items: center; color: #ecfdf5; background: linear-gradient(135deg, #10b981, #22d3ee); box-shadow: 0 16px 36px rgba(16,185,129,.28); }
.success-eyebrow { margin: 0 0 5px; color: #34d399; font-size: 11px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; }
.success-modal h3 { margin: 0; font-size: 24px; letter-spacing: -.02em; }
.success-desc { margin: 0; padding: 0 28px 18px; color: var(--text-secondary); font-size: 13px; line-height: 1.7; text-align: left; }
.success-summary { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin: 0 28px 14px; }
.success-summary div { min-width: 0; padding: 12px 14px; border: 1px solid rgba(148,163,184,.18); border-radius: 14px; background: rgba(255,255,255,.045); }
.success-summary span { display: block; margin-bottom: 6px; color: var(--text-muted); font-size: 12px; }
.success-summary strong { display: block; color: var(--text-primary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.success-permissions { display: flex; flex-wrap: wrap; gap: 8px; margin: 0 28px; padding: 12px; border: 1px dashed rgba(52,211,153,.28); border-radius: 14px; background: rgba(52,211,153,.06); }
.permission-chip { color: #bbf7d0; border: 1px solid rgba(52,211,153,.26); background: rgba(52,211,153,.12); border-radius: 999px; padding: 4px 9px; font-size: 12px; }
.success-actions { justify-content: flex-end; margin: 0; padding: 18px 28px 26px; }
.success-confirm { min-width: 110px; background: linear-gradient(135deg, #10b981, #22d3ee); box-shadow: 0 12px 24px rgba(16,185,129,.18); }
.modal-desc { margin: -6px 0 14px; }
.produce-modal textarea { min-height: 220px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
.modal-actions { display: flex; gap: 10px; justify-content: flex-end; margin-top: 14px; }
.produce-modal .modal-actions { position: sticky; bottom: -22px; z-index: 2; margin: 14px -22px -22px; padding: 14px 22px 18px; background: linear-gradient(180deg, rgba(15,23,42,.84), var(--bg-secondary) 34%); border-top: 1px solid var(--border-color); }
@media (max-width: 900px) { .kafka-page { padding: 16px; } .hero { flex-direction: column; align-items: flex-start; } .title-side { justify-content: flex-start; } .cluster-inline-pill { max-width: 100%; } .split-panel, .group-card, .group-search-bar, .batch-toolbar, .batch-preview-row, .permission-toolbar, .permission-grid, .grafana-grid { grid-template-columns: 1fr; } .message-tools, .offset-head, .offset-row, .group-topic-detail-head, .group-topic-detail-row, .config-head, .config-row { grid-template-columns: 1fr; } .message-action-buttons { justify-content: stretch; } .message-action-buttons button { flex: 1; min-width: 0; } .summary-grid { grid-template-columns: 1fr; } .permission-row { grid-template-columns: 1fr; } .group-card-side { justify-items: start; } }
@media (max-width: 520px) { .message-action-buttons { flex-direction: column; } .message-action-buttons button { width: 100%; } }
</style>
