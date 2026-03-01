import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ConnectionSection } from '../components/ConnectionSection.jsx';

// Carbon components use CSS modules; css: false in vitest config handles this.
// Some Carbon components render complex DOM — we query by label text / role.

function renderSection(values = {}, onChange = vi.fn()) {
  return render(<ConnectionSection values={values} onChange={onChange} />);
}

describe('ConnectionSection', () => {
  // ── Renders ───────────────────────────────────────────────────────────────
  it('renders Origin Cluster and Target Cluster groups', () => {
    renderSection();
    expect(screen.getByText('Origin Cluster')).toBeInTheDocument();
    expect(screen.getByText('Target Cluster')).toBeInTheDocument();
  });

  it('renders Host / Port and Astra DB radio buttons for origin', () => {
    renderSection();
    const hostRadios = screen.getAllByLabelText('Host / Port');
    const astraRadios = screen.getAllByLabelText('Astra DB');
    expect(hostRadios.length).toBeGreaterThanOrEqual(1);
    expect(astraRadios.length).toBeGreaterThanOrEqual(1);
  });

  // ── Host / Port mode ──────────────────────────────────────────────────────
  it('shows Host and Port fields in host mode (default)', () => {
    renderSection();
    expect(screen.getAllByLabelText('Host').length).toBeGreaterThanOrEqual(1);
  });

  it('shows Username and Password fields in host mode', () => {
    renderSection();
    expect(screen.getAllByLabelText('Username').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByLabelText('Password').length).toBeGreaterThanOrEqual(1);
  });

  it('does not show Secure Connect Bundle path in host mode', () => {
    renderSection();
    expect(screen.queryByLabelText('Secure Connect Bundle path')).toBeNull();
  });

  it('does not show Astra DB sub-options in host mode', () => {
    renderSection();
    expect(screen.queryByText('Provide SCB')).toBeNull();
    expect(screen.queryByText('Auto-download SCB')).toBeNull();
  });

  // ── Astra DB mode ─────────────────────────────────────────────────────────
  it('shows Provide SCB / Auto-download SCB sub-radio when Astra DB selected', () => {
    renderSection({ originConnectionType: 'scb' });
    expect(screen.getAllByLabelText('Provide SCB').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByLabelText('Auto-download SCB').length).toBeGreaterThanOrEqual(1);
  });

  it('shows Secure Connect Bundle path when Astra DB + Provide SCB', () => {
    renderSection({ originConnectionType: 'scb', originScbMethod: 'provide' });
    expect(screen.getByLabelText('Secure Connect Bundle path')).toBeInTheDocument();
  });

  it('does not show SCB path field when Astra DB + Auto-download', () => {
    renderSection({ originConnectionType: 'scb', originScbMethod: 'auto' });
    expect(screen.queryByLabelText('Secure Connect Bundle path')).toBeNull();
  });

  it('shows Database UUID field in auto-download mode', () => {
    renderSection({ originConnectionType: 'scb', originScbMethod: 'auto' });
    expect(screen.getByLabelText('Database UUID')).toBeInTheDocument();
  });

  it('shows SCB Type dropdown in auto-download mode', () => {
    renderSection({ originConnectionType: 'scb', originScbMethod: 'auto' });
    expect(screen.getByLabelText('SCB Type')).toBeInTheDocument();
  });

  it('does not show SCB Region when SCB Type is default', () => {
    renderSection({
      originConnectionType: 'scb',
      originScbMethod: 'auto',
      originScbType: 'default',
    });
    expect(screen.queryByLabelText('SCB Region')).toBeNull();
  });

  it('shows SCB Region and SCB Custom Domain when SCB Type is custom', () => {
    renderSection({
      originConnectionType: 'scb',
      originScbMethod: 'auto',
      originScbType: 'custom',
    });
    expect(screen.getByLabelText('SCB Region')).toBeInTheDocument();
    expect(screen.getByLabelText('SCB Custom Domain')).toBeInTheDocument();
  });

  it('pre-fills SCB Custom Domain with default placeholder value', () => {
    renderSection({
      originConnectionType: 'scb',
      originScbMethod: 'auto',
      originScbType: 'custom',
      originScbCustomDomain: '',
    });
    const input = screen.getByLabelText('SCB Custom Domain');
    expect(input.value).toBe('your-custom-domain.example.com');
  });

  // ── Username field in Astra DB mode ───────────────────────────────────────
  it('shows Username label (not "Username / Client ID") in Astra DB mode', () => {
    renderSection({ originConnectionType: 'scb' });
    // Should have "Username" labels
    const usernameFields = screen.getAllByLabelText('Username');
    expect(usernameFields.length).toBeGreaterThanOrEqual(1);
  });

  it('Username field is read-only with value "token" in Astra DB mode', () => {
    renderSection({ originConnectionType: 'scb', originScbMethod: 'provide' });
    const usernameInputs = screen.getAllByLabelText('Username');
    // Find the one that belongs to origin (first one)
    const originUsername = usernameInputs[0];
    expect(originUsername.value).toBe('token');
    expect(originUsername.readOnly).toBe(true);
  });

  it('Password label includes AstraCS hint in Astra DB mode', () => {
    renderSection({ originConnectionType: 'scb' });
    expect(screen.getAllByText(/AstraCS/i).length).toBeGreaterThanOrEqual(1);
  });

  // ── UUID validation ───────────────────────────────────────────────────────
  it('shows UUID validation error for invalid input', () => {
    renderSection({
      originConnectionType: 'scb',
      originScbMethod: 'auto',
      originAstraDatabaseId: 'not-a-uuid',
    });
    expect(screen.getByText(/Must be a valid UUID/i)).toBeInTheDocument();
  });

  it('does not show UUID validation error for valid UUID', () => {
    renderSection({
      originConnectionType: 'scb',
      originScbMethod: 'auto',
      originAstraDatabaseId: '550e8400-e29b-41d4-a716-446655440000',
    });
    expect(screen.queryByText(/Must be a valid UUID/i)).toBeNull();
  });

  it('does not show UUID validation error for empty input', () => {
    renderSection({
      originConnectionType: 'scb',
      originScbMethod: 'auto',
      originAstraDatabaseId: '',
    });
    expect(screen.queryByText(/Must be a valid UUID/i)).toBeNull();
  });

  // ── onChange callbacks ────────────────────────────────────────────────────
  it('calls onChange when connection type radio changes', () => {
    const onChange = vi.fn();
    renderSection({ originConnectionType: 'host' }, onChange);
    const astraRadio = screen.getAllByLabelText('Astra DB')[0];
    fireEvent.click(astraRadio);
    expect(onChange).toHaveBeenCalled();
  });

  it('calls onChange when Host input changes', () => {
    const onChange = vi.fn();
    renderSection({ originConnectionType: 'host', originHost: '' }, onChange);
    const hostInputs = screen.getAllByLabelText('Host');
    fireEvent.change(hostInputs[0], { target: { value: 'new-host' } });
    expect(onChange).toHaveBeenCalledWith('originHost', 'new-host');
  });

  it('calls onChange when SCB path changes', () => {
    const onChange = vi.fn();
    renderSection({ originConnectionType: 'scb', originScbMethod: 'provide' }, onChange);
    const scbInput = screen.getByLabelText('Secure Connect Bundle path');
    fireEvent.change(scbInput, { target: { value: 'file:///new/path.zip' } });
    expect(onChange).toHaveBeenCalledWith('originScb', 'file:///new/path.zip');
  });

  it('calls onChange when Database UUID changes', () => {
    const onChange = vi.fn();
    renderSection({ originConnectionType: 'scb', originScbMethod: 'auto' }, onChange);
    const uuidInput = screen.getByLabelText('Database UUID');
    fireEvent.change(uuidInput, { target: { value: '550e8400-e29b-41d4-a716-446655440000' } });
    expect(onChange).toHaveBeenCalledWith('originAstraDatabaseId', '550e8400-e29b-41d4-a716-446655440000');
  });

  // ── Target cluster mirrors origin behavior ────────────────────────────────
  it('shows target Astra DB sub-options when target connection type is scb', () => {
    renderSection({ targetConnectionType: 'scb' });
    // Should have at least 2 "Provide SCB" radios (one per cluster)
    const provideRadios = screen.getAllByLabelText('Provide SCB');
    expect(provideRadios.length).toBeGreaterThanOrEqual(1);
  });

  it('shows target Database UUID in auto-download mode', () => {
    renderSection({ targetConnectionType: 'scb', targetScbMethod: 'auto' });
    const uuidInputs = screen.getAllByLabelText('Database UUID');
    expect(uuidInputs.length).toBeGreaterThanOrEqual(1);
  });
});

