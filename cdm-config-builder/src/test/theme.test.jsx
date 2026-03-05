import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, useTheme } from '../context/ThemeContext.jsx';
import { SunIcon, MoonIcon } from '../components/ThemeToggleButton.jsx';

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Minimal consumer component to expose theme context values */
function ThemeConsumer() {
  const { theme, toggleTheme } = useTheme();
  return (
    <div>
      <span data-testid="theme-value">{theme}</span>
      <button data-testid="toggle-btn" onClick={toggleTheme}>
        Toggle
      </button>
    </div>
  );
}

// ── ThemeContext ──────────────────────────────────────────────────────────────

describe('ThemeContext', () => {
  beforeEach(() => {
    localStorage.clear();
    // Reset data-theme attribute
    delete document.documentElement.dataset.theme;
    // Reset matchMedia mock to light preference
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query) => ({
        matches: false, // default: light preference
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      })),
    });
  });

  afterEach(() => {
    localStorage.clear();
    delete document.documentElement.dataset.theme;
  });

  it('defaults to light theme when no stored preference and OS is light', () => {
    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-value').textContent).toBe('light');
  });

  it('defaults to dark theme when OS prefers dark and no stored preference', () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }));

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-value').textContent).toBe('dark');
  });

  it('reads stored theme from localStorage on mount', () => {
    localStorage.setItem('cdm-theme', 'dark');

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-value').textContent).toBe('dark');
  });

  it('stored preference overrides OS preference', () => {
    localStorage.setItem('cdm-theme', 'light');
    // OS says dark
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }));

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-value').textContent).toBe('light');
  });

  it('toggleTheme switches from light to dark', () => {
    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-value').textContent).toBe('light');

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(screen.getByTestId('theme-value').textContent).toBe('dark');
  });

  it('toggleTheme switches from dark to light', () => {
    localStorage.setItem('cdm-theme', 'dark');

    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-value').textContent).toBe('dark');

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(screen.getByTestId('theme-value').textContent).toBe('light');
  });

  it('persists theme to localStorage on toggle', () => {
    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(localStorage.getItem('cdm-theme')).toBe('dark');

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(localStorage.getItem('cdm-theme')).toBe('light');
  });

  it('sets data-theme attribute on documentElement', () => {
    render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );
    expect(document.documentElement.dataset.theme).toBe('light');

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(document.documentElement.dataset.theme).toBe('dark');
  });

  it('throws when useTheme is used outside ThemeProvider', () => {
    // Suppress React error boundary console output
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    expect(() => render(<ThemeConsumer />)).toThrow(
      'useTheme must be used within ThemeProvider'
    );
    consoleSpy.mockRestore();
  });

  it('registers and cleans up matchMedia change listener', () => {
    const addListener = vi.fn();
    const removeListener = vi.fn();
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      addEventListener: addListener,
      removeEventListener: removeListener,
    }));

    const { unmount } = render(
      <ThemeProvider>
        <ThemeConsumer />
      </ThemeProvider>
    );

    expect(addListener).toHaveBeenCalledWith('change', expect.any(Function));
    unmount();
    expect(removeListener).toHaveBeenCalledWith('change', expect.any(Function));
  });
});

// ── SunIcon / MoonIcon ────────────────────────────────────────────────────────

describe('SunIcon', () => {
  it('renders an SVG with aria-hidden', () => {
    const { container } = render(<SunIcon />);
    const svg = container.querySelector('svg');
    expect(svg).toBeTruthy();
    expect(svg.getAttribute('aria-hidden')).toBe('true');
    expect(svg.getAttribute('focusable')).toBe('false');
  });

  it('renders with correct dimensions', () => {
    const { container } = render(<SunIcon />);
    const svg = container.querySelector('svg');
    expect(svg.getAttribute('width')).toBe('20');
    expect(svg.getAttribute('height')).toBe('20');
  });
});

describe('MoonIcon', () => {
  it('renders an SVG with aria-hidden', () => {
    const { container } = render(<MoonIcon />);
    const svg = container.querySelector('svg');
    expect(svg).toBeTruthy();
    expect(svg.getAttribute('aria-hidden')).toBe('true');
    expect(svg.getAttribute('focusable')).toBe('false');
  });

  it('renders with correct dimensions', () => {
    const { container } = render(<MoonIcon />);
    const svg = container.querySelector('svg');
    expect(svg.getAttribute('width')).toBe('20');
    expect(svg.getAttribute('height')).toBe('20');
  });
});

