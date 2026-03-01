import { createContext, useContext, useState, useEffect, useCallback } from 'react';

const STORAGE_KEY = 'cdm-theme';

const ThemeContext = createContext(null);

/**
 * Reads the initial theme:
 *  1. localStorage preference
 *  2. OS prefers-color-scheme
 *  3. Default: 'light'
 */
function getInitialTheme() {
  if (typeof window === 'undefined') return 'light';
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'dark' || stored === 'light') return stored;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/**
 * ThemeProvider — wraps the app and provides { theme, toggleTheme }.
 * Syncs theme to:
 *   - document.documentElement.dataset.theme  (CSS custom properties)
 *   - localStorage                             (persistence)
 */
export function ThemeProvider({ children }) {
  const [theme, setTheme] = useState(getInitialTheme);

  // Apply theme to DOM and persist whenever it changes
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  // Listen for OS preference changes (only when no stored preference)
  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = (e) => {
      if (!localStorage.getItem(STORAGE_KEY)) {
        setTheme(e.matches ? 'dark' : 'light');
      }
    };
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  const toggleTheme = useCallback(() => {
    setTheme((prev) => (prev === 'dark' ? 'light' : 'dark'));
  }, []);

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>
      {children}
    </ThemeContext.Provider>
  );
}

/**
 * useTheme — consume theme context.
 * Returns { theme: 'light' | 'dark', toggleTheme: () => void }
 */
export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error('useTheme must be used within ThemeProvider');
  return ctx;
}

