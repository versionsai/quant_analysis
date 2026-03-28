<template>
  <n-card title="ETF池状态">
    <n-space vertical>
      <n-descriptions :column="2" label-placement="left">
        <n-descriptions-item label="数据源">{{ status?.source || 'N/A' }}</n-descriptions-item>
        <n-descriptions-item label="更新">{{ status?.updated_at || '--' }}</n-descriptions-item>
        <n-descriptions-item label="数量">{{ status?.item_count || 0 }} 只</n-descriptions-item>
        <n-descriptions-item label="状态">
          <n-tag :type="statusType">{{ statusLabel }}</n-tag>
        </n-descriptions-item>
      </n-descriptions>
    </n-space>
  </n-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { NCard, NSpace, NDescriptions, NDescriptionsItem, NTag } from 'naive-ui'

const props = defineProps<{
  data: any
}>()

const status = computed(() => props.data?.status || {})
const isFresh = computed(() => status.value?.is_fresh)
const exists = computed(() => status.value?.exists)

const statusLabel = computed(() => {
  if (!exists.value) return '无缓存'
  if (isFresh.value) return '新鲜'
  return '已过期'
})

const statusType = computed(() => {
  if (!exists.value) return 'warning'
  if (isFresh.value) return 'success'
  return 'error'
})
</script>
