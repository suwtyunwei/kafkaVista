export type ThemeMode = 'dark' | 'light'

const THEME_KEY = 'rag_theme'

export function getTheme(): ThemeMode {
  const saved = localStorage.getItem(THEME_KEY)
  return saved === 'light' ? 'light' : 'dark'
}

export function applyTheme(theme: ThemeMode): void {
  document.documentElement.dataset.theme = theme
  localStorage.setItem(THEME_KEY, theme)
}

export function toggleTheme(): ThemeMode {
  const next = getTheme() === 'dark' ? 'light' : 'dark'
  applyTheme(next)
  return next
}
