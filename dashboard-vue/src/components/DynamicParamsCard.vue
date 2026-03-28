<template>
  <n-card title="动态参数与人工干预">
    <n-data-table
      :columns="columns"
      :data="params"
      :pagination="false"
      :bordered="false"
      size="small"
    />
    <template #footer>
      <n-button size="small" @click="$emit('refresh')">刷新</n-button>
    </template>
  </n-card>
</template>

<script setup lang="ts">
import { ref, onMounted, h } from 'vue'
import { NCard, NDataTable, NButton, NInput } from 'naive-ui'
import { getDynamicParams } from '../api'

defineEmits(['refresh'])

const params = ref<any[]>([])

const columns = [
  { title: '参数', key: 'key' },
  { title: '值', key: 'value' },
  { title: '来源', key: 'source' },
]

onMounted(async () => {
  try {
    const data = await getDynamicParams()
    params.value = Array.isArray(data) ? data : []
  } catch (e) {
    params.value = []
  }
})
</script>
