<template>
  <n-card title="人工干预记录">
    <n-data-table
      :columns="columns"
      :data="history"
      :pagination="{ pageSize: 10 }"
      :bordered="false"
      size="small"
    />
  </n-card>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { NCard, NDataTable } from 'naive-ui'
import { getOverrideHistory } from '../api'

const history = ref<any[]>([])

const columns = [
  { title: '时间', key: 'timestamp', width: 150 },
  { title: '参数', key: 'param_key' },
  { title: '操作', key: 'action' },
  { title: '旧值', key: 'old_value' },
  { title: '新值', key: 'new_value' },
]

onMounted(async () => {
  try {
    const data = await getOverrideHistory()
    history.value = Array.isArray(data) ? data : []
  } catch (e) {
    history.value = []
  }
})
</script>
