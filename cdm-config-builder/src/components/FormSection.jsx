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
