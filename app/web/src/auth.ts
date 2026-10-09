/**
 * 认证状态管理 — Token 存取 + 用户信息
 */

const TOKEN_KEY = '***'
const USER_KEY = 'rag_auth_user'

export interface UserInfo {
  username: string
  display_name: string
  role?: string
}

export function isAdmin(): boolean {
  return getUser()?.role === 'admin'
}

export function getToken(): string | null {
  // 优先查 localStorage（记住登录），然后 sessionStorage（会话登录）
  let token = localStorage.getItem(TOKEN_KEY)
  if (!token) token = sessionStorage.getItem(TOKEN_KEY)
  return token
}

export function setToken(token: string, remember: boolean = false): void {
  if (remember) {
    // 记住登录 → 存 localStorage
    localStorage.setItem(TOKEN_KEY, token)
  } else {
    // 不记住 → 存 sessionStorage
    sessionStorage.setItem(TOKEN_KEY, token)
  }
}

export function removeToken(): void {
  localStorage.removeItem(TOKEN_KEY)
  sessionStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(USER_KEY)
  sessionStorage.removeItem(USER_KEY)
}

export function getUser(): UserInfo | null {
  try {
    // 优先查 localStorage，然后 sessionStorage
    let raw = localStorage.getItem(USER_KEY)
    if (!raw) raw = sessionStorage.getItem(USER_KEY)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

export function setUser(user: UserInfo, remember: boolean = false): void {
  if (remember) {
    localStorage.setItem(USER_KEY, JSON.stringify(user))
  } else {
    sessionStorage.setItem(USER_KEY, JSON.stringify(user))
  }
}

export function isAuthenticated(): boolean {
  return !!getToken()
}
