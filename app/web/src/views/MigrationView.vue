<template>
  <div class="migration-page">
    <section class="hero">
      <div>
        <p class="eyebrow">{{ tr('数据迁移', 'Migration') }}</p>
        <h1>{{ tr('Kafka 平滑迁移', 'Kafka Smooth Migration') }}</h1>
        <p>{{ tr('限速、分批迁移 Topic 元数据和消息，降低对源端与目标端 Kafka 的性能影响。', 'Throttle and batch topic metadata and messages to reduce impact on source and target Kafka.') }}</p>
      </div>
      <div class="hero-actions">
        <button :class="['ghost', { selected: pageMode === 'config' }]" @click="openConfigPage">{{ tr('迁移配置', 'Config') }}</button>
        <button :class="['ghost', { selected: pageMode === 'progress' }]" @click="openProgressPage">{{ tr('迁移进度', 'Progress') }}</button>
        <button v-if="pageMode === 'progress'" class="ghost" :disabled="refreshing" @click="refreshJobs">{{ refreshing ? tr('刷新中...', 'Refreshing...') : tr('刷新进度', 'Refresh') }}</button>
      </div>
    </section>

    <section v-if="pageMode === 'config'" class="config-page">
      <form class="panel setup" @submit.prevent="startMigration">
        <div class="panel-title"><h2>{{ tr('迁移配置', 'Migration Config') }}</h2><span>{{ tr('简洁安全默认值', 'Safe defaults') }}</span></div>
        <div class="two">
          <label>{{ tr('源 Kafka', 'Source Kafka') }}<select v-model="form.source_cluster_id"><option value="">{{ tr('请选择', 'Select') }}</option><option v-for="c in clusters" :key="c.id" :value="c.id">{{ c.name }}</option></select></label>
          <label>{{ tr('目标 Kafka', 'Target Kafka') }}<select v-model="form.target_cluster_id"><option value="">{{ tr('请选择', 'Select') }}</option><option v-for="c in clusters" :key="c.id" :value="c.id">{{ c.name }}</option></select></label>
        </div>
        <label>{{ tr('指定 Topic（可选）', 'Topics Optional') }}<textarea v-model="topicsText" :placeholder="tr('每行一个 Topic；留空表示迁移全部非内部 Topic', 'One topic per line; leave empty to migrate all non-internal topics')"></textarea></label>
        <div class="option-row"><label><input v-model="form.create_topics" type="checkbox" /> {{ tr('自动创建 Topic', 'Create topics') }}</label><label><input v-model="form.copy_topic_configs" type="checkbox" /> {{ tr('复制常用 Topic 配置', 'Copy common configs') }}</label><label><input v-model="form.copy_data" type="checkbox" /> {{ tr('复制消息数据', 'Copy messages') }}</label><label><input v-model="form.incremental_sync" :disabled="!form.copy_data" type="checkbox" /> {{ tr('开启实时增量同步', 'Enable realtime incremental sync') }}</label><label><input v-model="form.overwrite_existing_topics" :disabled="!form.create_topics" type="checkbox" /> {{ tr('覆盖目标端已有 Topic', 'Overwrite existing target topics') }}</label></div>
        <p class="hint">{{ tr('点击启动迁移后会自动弹出检测窗口，检查源/目标 Kafka 连接和目标端同名 Topic。', 'A safety check dialog opens automatically when starting migration to verify source/target Kafka and duplicate target topics.') }}</p>
        <div class="three">
          <label>{{ tr('批大小', 'Batch Size') }}<input v-model.number="form.batch_size" type="number" min="1" max="500" /></label>
          <label>{{ tr('批间隔 ms', 'Throttle ms') }}<input v-model.number="form.throttle_ms" type="number" min="500" /></label>
          <label>{{ tr('每 Topic 最多消息', 'Max messages/topic') }}<input v-model.number="form.max_messages_per_topic" type="number" min="0" /></label>
        </div>
        <div class="three">
          <label>{{ tr('目标分区数', 'Target Partitions') }}<input v-model.number="form.target_partitions" type="number" min="0" /></label>
          <label>{{ tr('目标副本数', 'Replication Factor') }}<input v-model.number="form.replication_factor" type="number" min="1" /></label>
          <label>{{ tr('增量轮询间隔 ms', 'Incremental poll ms') }}<input v-model.number="form.incremental_poll_ms" :disabled="!form.incremental_sync" type="number" min="30000" /></label>
        </div>
        <p class="hint">{{ tr('目标分区数填 0 表示按策略默认。集群到单机会强制覆盖为 1 分区 1 副本；其他场景会优先使用这里的配置。', 'Target partitions 0 means strategy default. Cluster to standalone is forced to 1 partition and 1 replica; other scenarios prefer these values.') }}</p>
        <p class="hint">{{ tr('建议先小批量验证，再迁移全部 Topic。较高批间隔可以进一步降低 Kafka 压力。开启实时增量同步后，后端会自动限制批大小、提高批间隔，并以不低于 30 秒的低频轮询读取源 Kafka，避免影响源端性能和稳定性。', 'Validate with a small batch first. Higher throttle reduces Kafka pressure further. When realtime incremental sync is enabled, the backend caps batch size, raises throttling, and polls source Kafka no more frequently than every 30 seconds to protect source performance and stability.') }}</p>
        <button class="primary" :disabled="starting || !canStart">{{ starting ? tr('启动中...', 'Starting...') : tr('启动迁移', 'Start Migration') }}</button>
      </form>

      <div class="panel safety-panel">
        <h2>{{ tr('迁移策略', 'Migration Strategy') }}</h2>
        <div class="strategy-list">
          <span>{{ tr('批量限速，避免压垮 Kafka。', 'Batch throttling avoids Kafka pressure.') }}</span>
          <span>{{ tr('先建 Topic，再复制配置和消息。', 'Create topics first, then configs and messages.') }}</span>
          <span>{{ tr('集群到集群：默认原分区、原副本迁移。', 'Cluster to cluster: preserves source partitions and replicas by default.') }}</span>
          <span>{{ tr('集群到单机：目标固定为 1 分区、1 副本。', 'Cluster to standalone: target is fixed to 1 partition and 1 replica.') }}</span>
          <span>{{ tr('单机到集群：默认 3 副本，分区数和副本数可配置。', 'Standalone to cluster: defaults to 3 replicas; partitions and replicas are configurable.') }}</span>
        </div>
      </div>
    </section>

    <section v-else class="progress-page">
      <div class="panel jobs">
        <div class="panel-title"><h2>{{ tr('迁移进度', 'Progress') }}</h2><span>{{ jobs.length }} jobs</span></div>
        <div v-if="!jobs.length" class="empty">{{ tr('暂无迁移任务', 'No migration jobs yet') }}</div>
        <article v-for="job in jobs" :key="job.id" class="job-card" :class="job.status" @click="toggleJobDetail(job.id)">
          <div class="job-head"><strong>{{ job.source_cluster }} → {{ job.target_cluster }}</strong><span>{{ statusLabel(job.status) }}</span></div>
          <div class="progress"><i :style="{ width: `${job.progress || 0}%` }"></i></div>
          <div class="job-meta"><span>{{ job.progress || 0 }}%</span><span>{{ tr('Topic', 'Topics') }} {{ job.done_topics || 0 }}/{{ job.total_topics || 0 }}</span><span>{{ tr('消息', 'Messages') }} {{ job.copied_messages || 0 }}</span><span>{{ tr('耗时', 'Elapsed') }} {{ elapsedTime(job) }}</span></div>
          <div class="live-topic">
            <span>{{ tr('当前 Topic', 'Current Topic') }}</span>
            <strong>{{ job.current_topic || tr('等待开始', 'Waiting') }}</strong>
          </div>
          <div class="job-stage">
            <span>{{ migrationStageMessage(job) }}</span>
            <small>{{ tr('最后更新', 'Updated') }} {{ formatDateTime(job.updated_at) }}</small>
          </div>
          <div v-if="canStopJob(job)" class="job-actions" @click.stop>
            <button class="danger small" :disabled="stoppingJobId === job.id" @click="stopMigrationJob(job)">{{ stoppingJobId === job.id ? tr('取消中...', 'Stopping...') : tr('取消同步', 'Stop Sync') }}</button>
          </div>
          <div v-if="job.current_total || job.current_copied" class="current-message-progress">
            <span>{{ tr('当前 Topic 消息', 'Current topic messages') }} {{ job.current_copied || 0 }}/{{ job.current_total || '-' }}</span>
            <div class="mini-progress"><i :style="{ width: `${currentTopicProgress(job)}%` }"></i></div>
          </div>
          <div v-if="expandedJobId === job.id" class="job-detail" @click.stop>
            <div><span>{{ tr('任务 ID', 'Job ID') }}</span><strong>{{ job.id }}</strong></div>
            <div><span>{{ tr('源 Kafka', 'Source Kafka') }}</span><strong>{{ job.source_cluster }}</strong></div>
            <div><span>{{ tr('目标 Kafka', 'Target Kafka') }}</span><strong>{{ job.target_cluster }}</strong></div>
            <div><span>{{ tr('当前 Topic', 'Current Topic') }}</span><strong>{{ job.current_topic || '-' }}</strong></div>
            <div><span>{{ tr('当前 Topic 消息', 'Current topic messages') }}</span><strong>{{ currentTopicMessageLabel(job) }}</strong></div>
            <div><span>{{ tr('已完成 Topic', 'Finished Topics') }}</span><strong>{{ job.done_topics || 0 }}/{{ job.total_topics || 0 }}</strong></div>
            <div><span>{{ tr('累计复制消息', 'Copied messages') }}</span><strong>{{ job.copied_messages || 0 }}</strong></div>
            <div class="job-detail-topics"><span>{{ tr('迁移 Topic', 'Topics') }}</span><div class="topic-detail-list"><em v-if="!(job.topics || []).length">-</em><strong v-for="(topic, index) in job.topics || []" :key="topic" :class="migrationTopicStatus(job, topic, index).className"><small>#{{ index + 1 }}</small>{{ topic }}<b>{{ migrationTopicStatus(job, topic, index).label }}</b></strong></div></div>
            <div><span>{{ tr('自动创建 Topic', 'Create topics') }}</span><strong>{{ enabledLabel(job.options?.create_topics) }}</strong></div>
            <div><span>{{ tr('复制 Topic 配置', 'Copy configs') }}</span><strong>{{ enabledLabel(job.options?.copy_topic_configs) }}</strong></div>
            <div><span>{{ tr('复制消息数据', 'Copy data') }}</span><strong>{{ enabledLabel(job.options?.copy_data) }}</strong></div>
            <div><span>{{ tr('实时增量同步', 'Realtime incremental sync') }}</span><strong>{{ enabledLabel(job.options?.incremental_sync) }}</strong></div>
            <div><span>{{ tr('覆盖目标 Topic', 'Overwrite target topics') }}</span><strong>{{ enabledLabel(job.options?.overwrite_existing_topics) }}</strong></div>
            <div><span>{{ tr('批大小', 'Batch size') }}</span><strong>{{ job.options?.batch_size || '-' }}</strong></div>
            <div><span>{{ tr('批间隔', 'Throttle') }}</span><strong>{{ job.options?.throttle_ms || 0 }} ms</strong></div>
            <div><span>{{ tr('增量轮询间隔', 'Incremental poll') }}</span><strong>{{ job.options?.incremental_poll_ms || '-' }} ms</strong></div>
            <div><span>{{ tr('目标分区数', 'Target partitions') }}</span><strong>{{ job.options?.target_partitions || '-' }}</strong></div>
            <div><span>{{ tr('目标副本数', 'Replication factor') }}</span><strong>{{ job.options?.replication_factor || '-' }}</strong></div>
            <div><span>{{ tr('每 Topic 最大消息', 'Max messages/topic') }}</span><strong>{{ job.options?.max_messages_per_topic || tr('不限', 'Unlimited') }}</strong></div>
            <div><span>{{ tr('开始时间', 'Started at') }}</span><strong>{{ formatDateTime(job.created_at) }}</strong></div>
            <div><span>{{ tr('完成时间', 'Completed at') }}</span><strong>{{ formatDateTime(job.completed_at) }}</strong></div>
          </div>
        </article>
      </div>
    </section>

    <div v-if="preflightDialogOpen" class="modal-mask" @click.self="closePreflightDialog">
      <div class="preflight-dialog">
        <div class="dialog-head">
          <div>
            <p class="eyebrow">Preflight Check</p>
            <h2>{{ tr('迁移前安全检测', 'Migration Safety Check') }}</h2>
          </div>
          <button class="ghost small" type="button" @click="closePreflightDialog">{{ checking ? tr('取消检测', 'Cancel Check') : tr('关闭', 'Close') }}</button>
        </div>
        <p class="dialog-desc">{{ preflightDialogMessage }}</p>
        <div class="check-progress"><i :style="{ width: `${preflightProgress}%` }"></i></div>
        <div class="check-percent">{{ preflightProgress }}%</div>
        <div class="check-steps">
          <div v-for="step in preflightSteps" :key="step.key" :class="['check-step', step.status]">
            <span>{{ stepIcon(step.status) }}</span>
            <div><strong>{{ step.title }}</strong><small>{{ step.desc }}</small></div>
          </div>
        </div>
        <button class="ghost small detail-toggle" type="button" @click="preflightDetailOpen = !preflightDetailOpen">{{ preflightDetailOpen ? tr('收起详情', 'Hide Details') : tr('展开详情', 'Show Details') }}</button>
        <div v-if="preflightDetailOpen" class="check-detail">
          <div><span>{{ tr('当前阶段', 'Current phase') }}</span><strong>{{ preflightPhaseLabel }}</strong></div>
          <div><span>{{ tr('检测 Topic 数', 'Checked topics') }}</span><strong>{{ preflightTopicCount }}</strong></div>
          <div><span>{{ tr('比对方式', 'Compare mode') }}</span><strong>{{ tr('源/目标 Topic 集合批量比对', 'Batch compare source/target topic sets') }}</strong></div>
          <div class="check-detail-topics">
            <span>{{ tr('正在比对 / 已比对 Topic', 'Comparing / checked topics') }}</span>
            <div class="topic-chips neutral"><span v-for="topic in preflightDetailTopics" :key="topic">{{ topic }}</span><em v-if="!preflightDetailTopics.length">{{ tr('留空表示检测全部非内部 Topic，检测完成后展示结果', 'Empty means all non-internal topics. Results appear after the check completes.') }}</em></div>
          </div>
        </div>
        <div v-if="preflight?.existing_topics?.length" class="dialog-conflicts">
          <div class="conflict-head"><strong>{{ tr('检测到目标端已有同名 Topic', 'Existing target topics found') }}：{{ preflight.existing_topics.length }}</strong><button class="ghost small" type="button" @click="conflictListOpen = !conflictListOpen">{{ conflictListOpen ? tr('收起', 'Collapse') : tr('展开', 'Expand') }}</button></div>
          <div v-if="conflictListOpen" class="topic-conflict-list"><span v-for="(topic, index) in preflight.existing_topics" :key="topic"><small>#{{ index + 1 }}</small>{{ topic }}</span></div>
          <p>{{ tr('如需继续迁移，请先关闭弹窗，勾选“覆盖目标端已有 Topic”选项，然后再次点击启动迁移。启动后会先删除目标端同名 Topic，再重新创建并迁移。', 'To continue, close this dialog, enable "Overwrite existing target topics", then start again. Existing target topics will be deleted first, then recreated and migrated.') }}</p>
        </div>
        <div v-if="overwriteConfirmOpen" class="overwrite-confirm">
          <strong>{{ tr('确认覆盖目标端已有 Topic？', 'Confirm overwriting target topics?') }}</strong>
          <p>{{ tr('这些 Topic 会先从目标 Kafka 删除，再按迁移计划重新创建并写入数据。该操作不可自动回滚。', 'These topics will be deleted from the target Kafka first, then recreated and filled according to the migration plan. This cannot be rolled back automatically.') }}</p>
          <div class="dialog-actions"><button class="ghost" type="button" :disabled="startingJob" @click="overwriteConfirmOpen = false">{{ tr('返回检查结果', 'Back') }}</button><button class="danger" type="button" :disabled="startingJob" @click="startMigrationJob">{{ startingJob ? tr('创建任务中...', 'Creating job...') : tr('确认覆盖并开始迁移', 'Overwrite and Start') }}</button></div>
        </div>
        <div v-if="preflightPhase === 'error'" class="dialog-conflicts error-box">
          <strong>{{ tr('检测失败', 'Check failed') }}</strong>
          <p>{{ preflightError }}</p>
        </div>
        <div class="dialog-actions">
          <button class="ghost" type="button" @click="closePreflightDialog">{{ checking ? tr('取消检测', 'Cancel Check') : preflightCanProceed ? tr('稍后迁移', 'Later') : tr('关闭', 'Close') }}</button>
          <button v-if="preflightCanProceed" class="primary" type="button" :disabled="startingJob" @click="confirmStartMigration">{{ startingJob ? tr('创建任务中...', 'Creating job...') : tr('下一步，开始迁移', 'Next, Start Migration') }}</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { checkKafkaMigration, createKafkaMigration, getKafkaMigration, listKafkaClusters, listKafkaMigrations, stopKafkaMigration } from '../api'
import { tr } from '../i18n'

const route = useRoute()
const router = useRouter()
const clusters = ref<any[]>([])
const jobs = ref<any[]>([])
const topicsText = ref('')
const starting = ref(false)
const startingJob = ref(false)
const checking = ref(false)
const refreshing = ref(false)
const preflight = ref<any>(null)
const preflightDialogOpen = ref(false)
const preflightDetailOpen = ref(false)
const conflictListOpen = ref(false)
const overwriteConfirmOpen = ref(false)
const preflightPhase = ref<'idle' | 'source' | 'target' | 'topics' | 'done' | 'error'>('idle')
const preflightError = ref('')
const preflightCancelled = ref(false)
const nowTick = ref(Date.now())
const expandedJobId = ref('')
const stoppingJobId = ref('')
const form = ref({ source_cluster_id: '', target_cluster_id: '', create_topics: true, copy_topic_configs: true, copy_data: true, overwrite_existing_topics: false, incremental_sync: false, batch_size: 50, throttle_ms: 1000, incremental_poll_ms: 30000, max_messages_per_topic: 0, replication_factor: 3, target_partitions: 1 })
let timer = 0
let preflightAbortController: AbortController | null = null

const canStart = computed(() => form.value.source_cluster_id && form.value.target_cluster_id && form.value.source_cluster_id !== form.value.target_cluster_id && (form.value.create_topics || form.value.copy_topic_configs || form.value.copy_data))
const pageMode = computed(() => route.name === 'migrationProgress' ? 'progress' : 'config')
const statusLabel = (status: string) => status === 'completed' ? tr('已完成', 'Completed') : status === 'failed' ? tr('失败', 'Failed') : status === 'running' ? tr('运行中', 'Running') : status === 'incremental' ? tr('增量同步中', 'Incremental Syncing') : status === 'stopping' ? tr('停止中', 'Stopping') : status === 'stopped' ? tr('已停止', 'Stopped') : tr('排队中', 'Queued')
const migrationStageMessage = (job: any) => {
  const text = String(job.error || job.message || '')
  if (!text) return '-'
  const copyMatch = text.match(/^正在限速复制消息（当前 Topic (\d+)\/(\d+) 条）$/)
  if (copyMatch) return tr(text, `Copying messages with throttling (current topic ${copyMatch[1]}/${copyMatch[2]} messages)`)
  const incrementalMatch = text.match(/^正在安全低频实时增量同步（当前 Topic 分区 (\d+)，offset (\d+)\/(\d+)）$/)
  if (incrementalMatch) return tr(text, `Running safe low-frequency incremental sync (partition ${incrementalMatch[1]}, offset ${incrementalMatch[2]}/${incrementalMatch[3]})`)
  const messages: Record<string, string> = {
    '正在生成迁移计划': 'Generating migration plan',
    '迁移计划已生成': 'Migration plan generated',
    '正在迁移 Topic 元数据': 'Migrating topic metadata',
    '正在覆盖目标端已有 Topic': 'Overwriting existing target topic',
    '正在限速复制消息': 'Copying messages with throttling',
    'Topic 迁移完成': 'Topic migration completed',
    '全量迁移完成，正在安全低频实时增量同步': 'Full migration completed, running safe low-frequency incremental sync',
    '正在安全低频实时增量同步': 'Running safe low-frequency incremental sync',
    '正在停止迁移任务': 'Stopping migration job',
    '迁移任务已停止': 'Migration job stopped',
    '迁移失败': 'Migration failed',
    '迁移完成': 'Migration completed',
  }
  return messages[text] ? tr(text, messages[text]) : text
}
const preflightProgress = computed(() => preflightPhase.value === 'source' ? 28 : preflightPhase.value === 'target' ? 58 : preflightPhase.value === 'topics' ? 82 : preflightPhase.value === 'done' ? 100 : preflightPhase.value === 'error' ? 100 : 8)
const preflightCanProceed = computed(() => preflightPhase.value === 'done' && preflight.value && (!preflight.value.existing_topics?.length || form.value.overwrite_existing_topics))
const preflightDialogMessage = computed(() => {
  if (preflightPhase.value === 'source') return tr('正在连接源 Kafka，确认迁移来源可访问。', 'Connecting to source Kafka to verify the migration source.')
  if (preflightPhase.value === 'target') return tr('正在连接目标 Kafka，确认目标实例状态正常。', 'Connecting to target Kafka to verify the destination instance.')
  if (preflightPhase.value === 'topics') return tr('正在检查目标端是否已有同名 Topic。', 'Checking whether target topics already exist.')
  if (preflightPhase.value === 'error') return preflightError.value || tr('检测失败，请检查 Kafka 实例配置。', 'Check failed. Please verify Kafka instance settings.')
  if (preflight.value?.existing_topics?.length) return tr('检测完成，但目标端已有同名 Topic，需要确认覆盖策略。', 'Check completed, but target topics already exist. Confirm overwrite strategy.')
  if (preflightPhase.value === 'done') return tr('检测通过，可以安全启动迁移。', 'Check passed. You can start the migration safely.')
  return tr('准备执行迁移前安全检测。', 'Preparing migration safety checks.')
})
const preflightPhaseLabel = computed(() => preflightPhase.value === 'source' ? tr('连接源 Kafka', 'Connecting source Kafka') : preflightPhase.value === 'target' ? tr('连接目标 Kafka', 'Connecting target Kafka') : preflightPhase.value === 'topics' ? tr('比对目标端 Topic', 'Comparing target topics') : preflightPhase.value === 'done' ? tr('检测完成', 'Check completed') : preflightPhase.value === 'error' ? tr('检测失败', 'Check failed') : tr('准备检测', 'Preparing'))
const requestedTopics = computed(() => topicsText.value.split('\n').map(item => item.trim()).filter(Boolean))
const preflightDetailTopics = computed(() => {
  const topics = preflight.value?.topics?.length ? preflight.value.topics : requestedTopics.value
  return topics.slice(0, 80)
})
const preflightTopicCount = computed(() => preflight.value?.topics?.length || requestedTopics.value.length || tr('全部 Topic', 'All topics'))
const preflightSteps = computed(() => {
  const statusFor = (step: 'source' | 'target' | 'topics') => {
    if (preflightPhase.value === 'error') return 'error'
    const order = ['source', 'target', 'topics']
    const current = order.indexOf(preflightPhase.value)
    const target = order.indexOf(step)
    if (preflightPhase.value === 'done') return step === 'topics' && preflight.value?.existing_topics?.length ? 'warning' : 'done'
    if (current === target) return 'running'
    if (current > target) return 'done'
    return 'pending'
  }
  return [
    { key: 'source', status: statusFor('source'), title: tr('源 Kafka 连接检测', 'Source Kafka connection'), desc: preflight.value?.source_ok ? tr('连接正常', 'Connected') : tr('确认源实例可访问', 'Verify source is reachable') },
    { key: 'target', status: statusFor('target'), title: tr('目标 Kafka 连接检测', 'Target Kafka connection'), desc: preflight.value?.target_ok ? tr('连接正常', 'Connected') : tr('确认目标实例可访问', 'Verify target is reachable') },
    { key: 'topics', status: statusFor('topics'), title: tr('同名 Topic 检查', 'Duplicate topic check'), desc: preflight.value?.existing_topics?.length ? tr(`发现 ${preflight.value.existing_topics.length} 个冲突 Topic`, `${preflight.value.existing_topics.length} conflicting topics found`) : tr('检查目标端 Topic 列表', 'Check target topic list') },
  ]
})

const openConfigPage = () => router.push({ name: 'migration' })
const openProgressPage = () => router.push({ name: 'migrationProgress' })
const toggleJobDetail = (jobId: string) => { expandedJobId.value = expandedJobId.value === jobId ? '' : jobId }
const canStopJob = (job: any) => ['queued', 'running', 'incremental'].includes(job.status)
const enabledLabel = (value: any) => value ? tr('启用', 'Enabled') : tr('关闭', 'Disabled')
const formatDateTime = (value?: string) => value ? new Date(value).toLocaleString() : '-'
const currentTopicProgress = (job: any) => {
  const total = Number(job.current_total || 0)
  if (!total) return 0
  return Math.min(100, Math.round(Number(job.current_copied || 0) / total * 100))
}
const currentTopicMessageLabel = (job: any) => {
  if (job.current_total || job.current_copied) return `${job.current_copied || 0}/${job.current_total || '-'}`
  return tr('当前版本迁移任务未返回单 Topic 消息进度', 'Current job does not expose per-topic message progress')
}
const migrationTopicStatus = (job: any, topic: string, index: number) => {
  const doneTopics = Number(job.done_topics || 0)
  if (job.current_topic === topic && ['queued', 'running', 'incremental', 'stopping'].includes(job.status)) return { className: 'syncing', label: tr('同步中', 'Syncing') }
  if (job.status === 'failed' && job.current_topic === topic) return { className: 'failed', label: tr('失败', 'Failed') }
  if (index < doneTopics) return { className: 'synced', label: tr('已同步', 'Synced') }
  return { className: 'pending', label: tr('待同步', 'Pending') }
}
const elapsedTime = (job: any) => {
  const start = new Date(job.created_at || '').getTime()
  if (!Number.isFinite(start)) return '-'
  const end = job.completed_at ? new Date(job.completed_at).getTime() : nowTick.value
  const seconds = Math.max(0, Math.round((end - start) / 1000))
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  const restSeconds = seconds % 60
  if (minutes < 60) return `${minutes}m ${restSeconds}s`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ${minutes % 60}m`
}

const loadJobs = async () => {
  jobs.value = await listKafkaMigrations()
  jobs.value.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())
}

const refreshJobs = async () => {
  refreshing.value = true
  nowTick.value = Date.now()
  try {
    await loadJobs()
    await pollRunningJobs()
  } finally {
    refreshing.value = false
  }
}

const pollRunningJobs = async () => {
  nowTick.value = Date.now()
  const running = jobs.value.filter(job => ['queued', 'running', 'incremental', 'stopping'].includes(job.status))
  if (!running.length) return
  const updated = await Promise.all(running.map(job => getKafkaMigration(job.id).catch(() => job)))
  jobs.value = jobs.value.map(job => updated.find(item => item.id === job.id) || job)
}

const migrationPayload = () => {
  const topics = topicsText.value.split('\n').map(item => item.trim()).filter(Boolean)
  return { ...form.value, incremental_sync: form.value.incremental_sync && form.value.copy_data, overwrite_existing_topics: form.value.overwrite_existing_topics && form.value.create_topics, topics }
}

const sleep = (ms: number) => new Promise(resolve => window.setTimeout(resolve, ms))
const stepIcon = (status: string) => status === 'done' ? '✓' : status === 'warning' ? '!' : status === 'running' ? '…' : status === 'error' ? '!' : '•'
const closePreflightDialog = () => {
  if (checking.value) {
    preflightCancelled.value = true
    preflightAbortController?.abort()
    checking.value = false
    preflightPhase.value = 'idle'
  }
  preflightDialogOpen.value = false
}

const confirmStartMigration = async () => {
  if (!preflightCanProceed.value || startingJob.value) return
  if (preflight.value?.existing_topics?.length && form.value.overwrite_existing_topics) {
    overwriteConfirmOpen.value = true
    return
  }
  await startMigrationJob()
}

const startMigrationJob = async () => {
  if (startingJob.value) return
  startingJob.value = true
  try {
    const job = await createKafkaMigration(migrationPayload())
    jobs.value.unshift(job)
    overwriteConfirmOpen.value = false
    preflightDialogOpen.value = false
    await router.push({ name: 'migrationProgress', query: { job: job.id } })
  } finally {
    startingJob.value = false
  }
}

const checkTargetTopics = async () => {
  if (!canStart.value || checking.value) return null
  checking.value = true
  preflightDialogOpen.value = true
  preflight.value = null
  preflightError.value = ''
  preflightDetailOpen.value = false
  conflictListOpen.value = false
  overwriteConfirmOpen.value = false
  preflightCancelled.value = false
  preflightAbortController = new AbortController()
  preflightPhase.value = 'source'
  try {
    await sleep(180)
    if (preflightCancelled.value) return null
    preflightPhase.value = 'target'
    await sleep(180)
    if (preflightCancelled.value) return null
    preflightPhase.value = 'topics'
    preflight.value = await checkKafkaMigration(migrationPayload(), preflightAbortController.signal)
    if (preflightCancelled.value) return null
    preflightPhase.value = 'done'
    return preflight.value
  } catch (error: any) {
    if (preflightCancelled.value || error?.name === 'CanceledError' || error?.code === 'ERR_CANCELED') return null
    preflightPhase.value = 'error'
    preflightError.value = error?.response?.data?.detail || error?.response?.data?.message || error?.message || tr('检测失败，请检查 Kafka 实例配置。', 'Check failed. Please verify Kafka instance settings.')
    throw error
  } finally {
    checking.value = false
    preflightAbortController = null
  }
}

const startMigration = async () => {
  if (!canStart.value || starting.value) return
  starting.value = true
  try {
    let check: any = null
    try {
      check = await checkTargetTopics()
    } catch {
      return
    }
    if (check?.existing_topics?.length && !form.value.overwrite_existing_topics) {
      return
    }
  } finally {
    starting.value = false
  }
}

const stopMigrationJob = async (job: any) => {
  if (!canStopJob(job) || stoppingJobId.value) return
  const ok = window.confirm(tr('确认取消该迁移同步任务？已复制的数据不会自动回滚。', 'Stop this migration sync job? Copied data will not be rolled back automatically.'))
  if (!ok) return
  stoppingJobId.value = job.id
  try {
    await stopKafkaMigration(job.id)
    const updated = await getKafkaMigration(job.id).catch(() => ({ ...job, status: 'stopping', message: tr('正在停止迁移任务', 'Stopping migration job') }))
    jobs.value = jobs.value.map(item => item.id === job.id ? updated : item)
  } finally {
    stoppingJobId.value = ''
  }
}

onMounted(async () => {
  clusters.value = await listKafkaClusters()
  await loadJobs()
  timer = window.setInterval(pollRunningJobs, 1500)
})

onUnmounted(() => window.clearInterval(timer))
</script>

<style scoped>
.migration-page { padding: 24px 20px; width: 100%; }
.hero { display: flex; justify-content: space-between; gap: 20px; align-items: flex-end; padding: 30px; border: 1px solid var(--border-color); border-radius: 26px; background: radial-gradient(circle at 12% 0, rgba(34,211,238,.25), transparent 35%), var(--bg-secondary); margin-bottom: 20px; }
.hero-actions { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.eyebrow { color: var(--accent-secondary); font-size: 12px; letter-spacing: .14em; text-transform: uppercase; margin-bottom: 8px; }
h1 { font-size: 32px; margin-bottom: 8px; } h2 { font-size: 18px; margin: 0; } .hero p, .hint, .panel-title span { color: var(--text-muted); font-size: 13px; }
.config-page { display: grid; grid-template-columns: minmax(360px, 620px) minmax(280px, 1fr); gap: 16px; align-items: start; }
.progress-page { display: grid; gap: 16px; }
.panel { border: 1px solid var(--border-color); border-radius: 18px; background: var(--bg-secondary); padding: 18px; display: grid; gap: 12px; }
.panel-title { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
label { display: grid; gap: 6px; color: var(--text-secondary); font-size: 13px; }
input, select, textarea { width: 100%; background: var(--bg-tertiary); border: 1px solid var(--border-color); color: var(--text-primary); padding: 9px 11px; border-radius: 9px; }
textarea { min-height: 110px; resize: vertical; }
.two, .three { display: grid; gap: 10px; } .two { grid-template-columns: repeat(2, minmax(0, 1fr)); } .three { grid-template-columns: repeat(3, minmax(0, 1fr)); }
.option-row { display: flex; flex-wrap: wrap; gap: 10px; padding: 10px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-tertiary); }
.option-row label { display: flex; align-items: center; gap: 7px; } .option-row input { width: auto; }
.preflight-panel { display: grid; gap: 8px; padding: 12px; border-radius: 12px; border: 1px solid var(--border-color); font-size: 13px; }
.preflight-panel.ok { border-color: rgba(52,211,153,.36); background: rgba(52,211,153,.08); }
.preflight-panel.warning { border-color: rgba(251,191,36,.42); background: rgba(251,191,36,.08); }
.preflight-panel p { margin: 0; color: var(--text-secondary); }
.topic-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.topic-chips span { padding: 4px 8px; border-radius: 999px; background: rgba(248,113,113,.14); color: #fecaca; border: 1px solid rgba(248,113,113,.28); font-size: 12px; }
.topic-chips.neutral span { background: rgba(34,211,238,.1); color: #a5f3fc; border-color: rgba(34,211,238,.24); }
.topic-chips em { color: var(--text-muted); font-style: normal; font-size: 12px; }
.danger-text { color: #fca5a5 !important; }
button { border: 0; border-radius: 9px; padding: 9px 14px; cursor: pointer; font: inherit; } .primary { background: var(--accent-primary); color: white; } .ghost { background: var(--bg-tertiary); color: var(--text-primary); border: 1px solid var(--border-color); } .ghost.selected { color: #fff; background: var(--accent-primary); border-color: var(--accent-primary); } button:disabled { opacity: .55; cursor: not-allowed; }
.danger { background: rgba(248,113,113,.16); color: #fecaca; border: 1px solid rgba(248,113,113,.36); } .small { padding: 7px 10px; font-size: 12px; }
.modal-mask { position: fixed; inset: 0; z-index: 100; display: grid; place-items: center; padding: 20px; background: rgba(2,6,23,.68); backdrop-filter: blur(8px); }
.preflight-dialog { width: min(620px, 100%); max-height: calc(100vh - 44px); overflow-y: auto; display: grid; gap: 14px; padding: 20px; border: 1px solid rgba(34,211,238,.24); border-radius: 22px; background: linear-gradient(145deg, rgba(15,23,42,.98), rgba(30,41,59,.96)); box-shadow: 0 24px 80px rgba(0,0,0,.42); }
.dialog-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }
.dialog-desc { margin: 0; color: var(--text-secondary); line-height: 1.55; }
.check-progress { height: 10px; overflow: hidden; border-radius: 999px; background: rgba(148,163,184,.18); }
.check-progress i { display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, #22d3ee, #6366f1, #10b981); transition: width .35s ease; }
.check-percent { text-align: right; color: var(--accent-secondary); font-size: 12px; font-weight: 800; }
.check-steps { display: grid; gap: 9px; }
.check-step { display: grid; grid-template-columns: 30px 1fr; gap: 10px; align-items: center; padding: 11px; border: 1px solid var(--border-color); border-radius: 13px; background: rgba(15,23,42,.35); }
.check-step span { display: grid; place-items: center; width: 26px; height: 26px; border-radius: 999px; background: rgba(148,163,184,.14); color: var(--text-muted); font-weight: 900; }
.check-step strong { display: block; color: var(--text-primary); font-size: 13px; }
.check-step small { color: var(--text-muted); font-size: 12px; }
.check-step.running { border-color: rgba(34,211,238,.42); background: rgba(34,211,238,.08); }
.check-step.running span { background: rgba(34,211,238,.16); color: #67e8f9; }
.check-step.done { border-color: rgba(52,211,153,.36); }
.check-step.done span { background: rgba(52,211,153,.16); color: #86efac; }
.check-step.warning { border-color: rgba(251,191,36,.48); background: rgba(251,191,36,.08); }
.check-step.warning span { background: rgba(251,191,36,.18); color: #fde68a; }
.check-step.error { border-color: rgba(248,113,113,.45); }
.check-step.error span { background: rgba(248,113,113,.18); color: #fecaca; }
.detail-toggle { justify-self: start; }
.check-detail { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; padding: 12px; border: 1px solid rgba(34,211,238,.18); border-radius: 14px; background: rgba(34,211,238,.06); }
.check-detail div { display: grid; gap: 4px; min-width: 0; }
.check-detail span { color: var(--text-muted); font-size: 11px; }
.check-detail strong { color: var(--text-primary); font-size: 12px; overflow-wrap: anywhere; }
.check-detail .check-detail-topics { grid-column: 1 / -1; }
.dialog-conflicts { display: grid; gap: 8px; padding: 12px; border: 1px solid rgba(251,191,36,.36); border-radius: 14px; background: rgba(251,191,36,.08); }
.conflict-head { display: flex; justify-content: space-between; gap: 10px; align-items: center; }
.topic-conflict-list { display: grid; gap: 6px; max-height: 220px; overflow-y: auto; }
.topic-conflict-list span { display: flex; gap: 8px; align-items: flex-start; padding: 7px 9px; border: 1px solid rgba(251,191,36,.24); border-radius: 10px; background: rgba(15,23,42,.24); color: #fde68a; font-size: 12px; overflow-wrap: anywhere; }
.topic-conflict-list small { flex: 0 0 auto; color: #facc15; font-weight: 800; }
.overwrite-confirm { display: grid; gap: 10px; padding: 14px; border: 1px solid rgba(248,113,113,.42); border-radius: 16px; background: linear-gradient(145deg, rgba(127,29,29,.22), rgba(251,191,36,.08)); }
.overwrite-confirm strong { color: #fecaca; }
.overwrite-confirm p { margin: 0; color: var(--text-secondary); font-size: 13px; line-height: 1.55; }
.dialog-conflicts.error-box { border-color: rgba(248,113,113,.45); background: rgba(248,113,113,.1); }
.dialog-conflicts p { margin: 0; color: var(--text-secondary); font-size: 12px; line-height: 1.5; }
.dialog-actions { display: flex; justify-content: flex-end; gap: 10px; flex-wrap: wrap; }
.strategy-list { display: grid; gap: 10px; }
.strategy-list span { padding: 12px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-tertiary); color: var(--text-secondary); font-size: 13px; line-height: 1.55; }
.jobs { align-content: start; } .empty { color: var(--text-muted); padding: 24px; text-align: center; border: 1px dashed var(--border-color); border-radius: 14px; }
.job-card { display: grid; gap: 9px; padding: 14px; border: 1px solid var(--border-color); border-radius: 14px; background: var(--bg-tertiary); cursor: pointer; }
.job-card:hover { border-color: rgba(34,211,238,.36); background: rgba(34,211,238,.06); }
.job-card.completed { border-color: rgba(52,211,153,.35); } .job-card.failed { border-color: rgba(248,113,113,.38); }
.job-head, .job-meta { display: flex; justify-content: space-between; gap: 10px; flex-wrap: wrap; } .job-head strong { color: var(--text-primary); } .job-head span, .job-meta, .job-card small { color: var(--text-muted); font-size: 12px; }
.progress { height: 10px; overflow: hidden; border-radius: 999px; background: rgba(148,163,184,.18); } .progress i { display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, #22d3ee, #6366f1, #10b981); transition: width .35s ease; }
.live-topic { display: grid; grid-template-columns: 96px minmax(0, 1fr); gap: 8px; align-items: start; padding: 9px 10px; border: 1px solid rgba(34,211,238,.18); border-radius: 10px; background: rgba(34,211,238,.06); }
.live-topic span, .job-stage small, .current-message-progress span { color: var(--text-muted); font-size: 12px; }
.live-topic strong { color: var(--text-primary); font-size: 13px; overflow-wrap: anywhere; }
.job-stage { display: flex; justify-content: space-between; gap: 10px; flex-wrap: wrap; color: var(--text-secondary); font-size: 13px; }
.job-stage span { overflow-wrap: anywhere; }
.job-actions { display: flex; justify-content: flex-end; }
.current-message-progress { display: grid; gap: 7px; }
.mini-progress { height: 7px; overflow: hidden; border-radius: 999px; background: rgba(148,163,184,.14); }
.mini-progress i { display: block; height: 100%; border-radius: inherit; background: linear-gradient(90deg, #10b981, #22d3ee); transition: width .35s ease; }
.job-detail { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin-top: 4px; padding: 12px; border: 1px solid rgba(34,211,238,.22); border-radius: 12px; background: rgba(15,23,42,.18); cursor: default; }
.job-detail div { min-width: 0; display: grid; gap: 4px; }
.job-detail .job-detail-topics { grid-column: 1 / -1; }
.job-detail span { color: var(--text-muted); font-size: 11px; }
.job-detail strong { color: var(--text-primary); font-size: 12px; overflow-wrap: anywhere; }
.topic-detail-list { display: grid; gap: 7px; }
.topic-detail-list strong { display: flex; align-items: flex-start; gap: 8px; padding: 8px 10px; border: 1px solid rgba(34,211,238,.2); border-radius: 10px; background: rgba(34,211,238,.07); line-height: 1.45; }
.topic-detail-list strong.synced { border-color: rgba(52,211,153,.36); background: rgba(52,211,153,.08); }
.topic-detail-list strong.syncing { border-color: rgba(34,211,238,.42); background: rgba(34,211,238,.1); }
.topic-detail-list strong.failed { border-color: rgba(248,113,113,.42); background: rgba(248,113,113,.1); }
.topic-detail-list strong.pending { opacity: .72; }
.topic-detail-list small { flex: 0 0 auto; color: var(--accent-secondary); font-weight: 900; }
.topic-detail-list b { margin-left: auto; flex: 0 0 auto; padding: 2px 7px; border-radius: 999px; background: rgba(148,163,184,.14); color: var(--text-muted); font-size: 11px; }
.topic-detail-list strong.synced b { background: rgba(52,211,153,.18); color: #86efac; }
.topic-detail-list strong.syncing b { background: rgba(34,211,238,.18); color: #67e8f9; }
.topic-detail-list strong.failed b { background: rgba(248,113,113,.18); color: #fecaca; }
.topic-detail-list em { color: var(--text-muted); font-style: normal; }
.locked p { color: var(--text-secondary); }
@media (max-width: 980px) { .config-page, .two, .three, .job-detail { grid-template-columns: 1fr; } .hero { align-items: flex-start; flex-direction: column; } .hero-actions { justify-content: flex-start; } }
</style>
