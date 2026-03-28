<template>
  <n-card title="择时卖出复盘">
    <n-space vertical>
      <n-statistic label="择时次数">{{ summary?.total_count || 0 }}</n-statistic>
      <n-statistic label="正确率">{{ summary?.accuracy || '--' }}</n-statistic>
      <n-data-table
        :columns="columns"
        :data="records"
        :pagination="{ pageSize: 10 }"
        :bordered="false"
        size="small"
      />
    </n-space>
  </n-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { NCard, NSpace, NStatistic, NDataTable } from 'naive-ui'

const props = defineProps<{
  data: any
}>()

const summary = computed(() => props.data?.summary || {})
const records = computed(() => props.data?.records || [])

const columns = [
  { title: '日期', key: 'date', width: 90 },
  { title: '代码', key: 'code', width: 70 },
  { title: '信号', key: 'signal_type', width: 60 },
  { title: '结果', key: 'result', width: 60 },
]
</script>
