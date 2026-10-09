<template>
  <div class="app-layout" :class="{ sidebarCollapsed: outerSidebarCollapsed }">
    <!-- Mobile menu toggle -->
    <button
      v-if="authenticated"
      class="mobile-menu-btn"
      @click="sidebarOpen = !sidebarOpen"
      :class="{ open: sidebarOpen }"
    >
      <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <line x1="3" y1="12" x2="21" y2="12" />
        <line x1="3" y1="6" x2="21" y2="6" />
        <line x1="3" y1="18" x2="21" y2="18" />
      </svg>
    </button>

    <!-- Sidebar overlay for mobile -->
    <div
      class="sidebar-overlay"
      v-if="sidebarOpen && authenticated"
      @click="sidebarOpen = false"
    ></div>

    <!-- Sidebar -->
    <aside class="sidebar-panel" :class="{ open: sidebarOpen }" v-if="authenticated">
      <Sidebar
        @navigate="handleNavigate"
        @logout="handleLogout"
        @sidebar-toggle="handleSidebarToggle"
      />
    </aside>

    <!-- Main content -->
    <main class="main-content">
      <div class="content-area">
        <router-view />
      </div>
    </main>


  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { isAuthenticated, removeToken } from './auth'
import Sidebar from './components/Sidebar.vue'

const router = useRouter()
const sidebarOpen = ref(false)
const outerSidebarCollapsed = ref(localStorage.getItem('sidebar_collapsed') === 'true')
const authFlag = ref(isAuthenticated())
const authenticated = computed(() => authFlag.value)

// 监听路由变化，登录后重新评估认证状态
onMounted(() => {
  router.afterEach(() => {
    authFlag.value = isAuthenticated()
  })
})

const handleSidebarToggle = (collapsed: boolean) => {
  outerSidebarCollapsed.value = collapsed
}

const handleLogout = () => {
  removeToken()
  authFlag.value = false
  router.push('/login')
}

const handleNavigate = (path: string) => {
  sidebarOpen.value = false
  router.push(path)
}
</script>
