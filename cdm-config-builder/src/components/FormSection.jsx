import { Tile } from '@carbon/react';

/**
 * Reusable section wrapper using Carbon Tile.
 * Provides consistent heading, optional description, and content area.
 */
export function FormSection({ title, description, children, className = '' }) {
  return (
    <Tile className={`form-section ${className}`}>
      <h3 className="form-section__title">{title}</h3>
      {description && <p className="form-section__description">{description}</p>}
      <div className="form-section__body">{children}</div>
    </Tile>
  );
}
