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

import { useState } from 'react';
import { Button, InlineNotification, Stack } from '@carbon/react';
import { Download, Copy, Checkmark } from '@carbon/icons-react';

/**
 * PropertiesPreview — displays the generated cdm.properties content
 * with Download and Copy to Clipboard actions.
 *
 * Props:
 *   content {string} - The generated properties file content
 */
export function PropertiesPreview({ content }) {
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState(null);

  const handleCopy = async () => {
    setCopyError(null);
    try {
      await navigator.clipboard.writeText(content);
      setCopied(true);
      setTimeout(() => setCopied(false), 2500);
    } catch {
      setCopyError('Could not copy to clipboard. Please select and copy manually.');
    }
  };

  const handleDownload = () => {
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'cdm.properties';
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="properties-preview">
      <div className="properties-preview__header">
        <h3 className="properties-preview__title">Generated cdm.properties</h3>
        <Stack gap={3} orientation="horizontal">
          <Button
            kind="ghost"
            size="sm"
            renderIcon={copied ? Checkmark : Copy}
            iconDescription={copied ? 'Copied!' : 'Copy to clipboard'}
            onClick={handleCopy}
            disabled={!content}
          >
            {copied ? 'Copied!' : 'Copy'}
          </Button>
          <Button
            kind="primary"
            size="sm"
            renderIcon={Download}
            iconDescription="Download cdm.properties"
            onClick={handleDownload}
            disabled={!content}
          >
            Download
          </Button>
        </Stack>
      </div>

      {copyError && (
        <InlineNotification
          kind="error"
          title="Copy failed: "
          subtitle={copyError}
          lowContrast
          hideCloseButton
        />
      )}

      <div className="properties-preview__code-wrapper">
        <pre className="properties-preview__code">
          <code>{content || '# Fill in the form to generate cdm.properties…'}</code>
        </pre>
      </div>
    </div>
  );
}
