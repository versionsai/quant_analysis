<template>
  <n-config-provider :theme-overrides="themeOverrides">
    <n-message-provider>
      <n-dialog-provider>
        <n-notification-provider>
          <Dashboard />
        </n-notification-provider>
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { GlobalThemeOverrides } from 'naive-ui'
import Dashboard from './views/Dashboard.vue'

const themeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: '#2563eb',
    primaryColorHover: '#3b82f6',
    primaryColorPressed: '#1d4ed8',
  },
  Card: {
    borderRadius: '16px',
  },
  Button: {
    borderRadiusMedium: '8px',
  }
}
</script>

<style>
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: "PingFang SC", "Microsoft YaHei", "Helvetica Neue", Arial, sans-serif;
  background: linear-gradient(180deg, #eef4fb 0%, #f8fbff 48%, #f3f6fb 100%);
  min-height: 100vh;
}

.page {
  max-width: 1520px;
  margin: 0 auto;
  padding: 28px 24px 40px;
}
</style>
