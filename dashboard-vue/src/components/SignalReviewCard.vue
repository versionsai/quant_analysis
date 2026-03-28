<template>
  <n-card title="信号质量复盘">
    <n-space vertical>
      <n-statistic label="胜率">{{ summary?.win_rate || '--' }}</n-statistic>
      <n-statistic label="盈利因子">{{ summary?.profit_factor || '--' }}</n-statistic>
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
  { title: '信号', key: 'signal_type', width: 50 },
  { title: '结果', key: 'result', width: 50 },
  { 
    title: '盈亏', 
    key: 'pnl_pct',
    render: (row: any) => `${row.pnl_pct?.toFixed(2) || '--'}%`
  },
]
</script>
