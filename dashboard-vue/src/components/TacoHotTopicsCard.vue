<template>
  <n-card title="TACO Hot Topics">
    <n-spin :show="loading">
      <n-space vertical>
        <n-card v-for="(item, idx) in items" :key="idx" size="small">
          <template #header>
            <n-space justify="space-between">
              <span>{{ item.name }}</span>
              <n-tag :type="getType(item.score)">score: {{ item.score?.toFixed(2) }}</n-tag>
            </n-space>
          </template>
          <n-space vertical>
            <div class="reason">{{ item.reason?.slice(0, 100) }}</div>
            <n-tag v-for="kw in (item.keywords || []).slice(0, 4)" :key="kw" size="small">
              {{ kw }}
            </n-tag>
          </n-space>
        </n-card>
      </n-space>
    </n-spin>
  </n-card>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { NCard, NSpin, NSpace, NTag } from 'naive-ui'
import { getTacoHotTopics } from '../api'

const loading = ref(false)
const data = ref<any[]>([])

const items = computed(() => data.value || [])

const getType = (score: number) => {
  if (score >= 80) return 'success'
  if (score >= 60) return 'warning'
  return 'default'
}

onMounted(async () => {
  loading.value = true
  try {
    data.value = await getTacoHotTopics()
  } catch (e) {
    console.error(e)
  } finally {
    loading.value = false
  }
})
</script>

<style scoped>
.reason {
  font-size: 12px;
  color: #666;
  margin-bottom: 8px;
}
</style>
