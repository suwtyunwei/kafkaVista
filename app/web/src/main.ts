import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import { applyTheme, getTheme } from './theme'
import './style.css'

applyTheme(getTheme())

const app = createApp(App)
app.use(router)
app.mount('#app')
