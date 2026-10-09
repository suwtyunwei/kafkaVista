import { createRouter, createWebHistory } from 'vue-router'
import { isAdmin, isAuthenticated } from './auth'

const LAST_ROUTE_KEY = 'kafkavista_last_route'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/login',
      name: 'login',
      component: () => import('./views/LoginView.vue'),
    },
    {
      path: '/',
      redirect: '/kafka',
    },
    {
      path: '/kafka',
      name: 'kafka',
      component: () => import('./views/KafkaVista.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/settings',
      name: 'settings',
      component: () => import('./views/SettingsView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true },
    },
    {
      path: '/audit',
      name: 'audit',
      component: () => import('./views/AuditView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true },
    },
    {
      path: '/docs',
      name: 'docs',
      component: () => import('./views/DocsView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true },
    },
    {
      path: '/migration',
      name: 'migration',
      component: () => import('./views/MigrationView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true },
    },
    {
      path: '/migration/progress',
      name: 'migrationProgress',
      component: () => import('./views/MigrationView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true },
    },
    {
      path: '/settings/alert-rules',
      name: 'alertRules',
      component: () => import('./views/SettingsView.vue'),
      meta: { requiresAuth: true, requiresAdmin: true },
    },
    {
      path: '/:pathMatch(.*)*',
      redirect: '/kafka',
    },
  ],
})

router.beforeEach((to) => {
  if (to.meta.requiresAuth && !isAuthenticated()) {
    return { name: 'login', query: { redirect: to.fullPath } }
  }
  if (to.meta.requiresAdmin && !isAdmin()) return { name: 'kafka' }
  if (to.name === 'login' && isAuthenticated()) {
    const lastRoute = localStorage.getItem(LAST_ROUTE_KEY)
    if (lastRoute && lastRoute !== '/login') return lastRoute
    return { name: 'kafka' }
  }
})

router.afterEach((to) => {
  if (to.meta.requiresAuth && to.name !== 'login') {
    localStorage.setItem(LAST_ROUTE_KEY, to.fullPath)
  }
})

export default router
