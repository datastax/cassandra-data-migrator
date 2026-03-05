/*
 Copyright DataStax, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

/**
 * ThemeToggleButton — SVG icon components for the theme switcher.
 *
 * SunIcon  — shown when in dark mode  (click → switch to light)
 * MoonIcon — shown when in light mode (click → switch to dark)
 *
 * These are used directly inside Carbon's HeaderGlobalAction which
 * renders its own <button> wrapper, so we export plain SVG components.
 */

export function SunIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="20"
      height="20"
      viewBox="0 0 32 32"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
    >
      <circle cx="16" cy="16" r="6" />
      <path
        d="M16 4v3M16 25v3M4 16H7M25 16h3M7.05 7.05l2.12 2.12M22.83 22.83l2.12 2.12M7.05 24.95l2.12-2.12M22.83 9.17l2.12-2.12"
        stroke="currentColor"
        strokeWidth="2.5"
        strokeLinecap="round"
        fill="none"
      />
    </svg>
  );
}

export function MoonIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="20"
      height="20"
      viewBox="0 0 32 32"
      fill="currentColor"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M13.502 5.414a15.075 15.075 0 0 0 11.594 17.194A11.113 11.113 0 0 1 11.5 27C5.701 27 1 22.299 1 16.5a11.113 11.113 0 0 1 12.502-11.086z" />
    </svg>
  );
}
