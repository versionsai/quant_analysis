<template>
  <n-card title="当前股票池">
    <n-data-table
      :columns="columns"
      :data="data || []"
      :pagination="false"
      :bordered="false"
      :max-height="300"
    />
    <template #footer>
      <span class="muted">共 {{ (data || []).length }} 只</span>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { h } from 'vue'
import { NCard, NDataTable } from 'naive-ui'

defineProps<{
  data: any[]
}>()

const columns = [
  { title: '代码', key: 'code', width: 80 },
  { title: '名称', key: 'name', width: 100 },
  { 
    title: '类型', 
    key: 'pool_type',
    render: (row: any) => h('n-tag', { size: 'small', type: row.pool_type === 'etf_lof' ? 'info' : 'default' }, 
      row.pool_type === 'etf_lof' ? 'ETF/LOF' : row.pool_type
    )
  },
]
</script>

<style scoped>
.muted {
  color: #6f7d95;
  font-size: 12px;
}
</style>
