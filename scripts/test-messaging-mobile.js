const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'messaging.html'), 'utf8');

test('attachment previews provide correctly scoped removal for new messages and replies', () => {
  const start = html.indexOf('    function fileChipMarkup(');
  const end = html.indexOf('\n    function ', start + 10);
  const context = vm.createContext({ esc: value => String(value ?? '').replace(/</g, '&lt;') });
  vm.runInContext(html.slice(start, end), context);
  const file = { fileName: 'photo.png', mimeType: 'image/png', fileSize: 1024, contentBase64: 'dGVzdA==' };
  const markup = context.fileChipMarkup([file]);
  assert.match(markup, /<img src="data:image\/png;base64,dGVzdA=="/);
  assert.match(markup, /data-attachment-remove="0" data-reply-file="false"/);
  assert.match(context.fileChipMarkup([file], true), /data-reply-file="true"/);
  assert.doesNotMatch(context.fileChipMarkup([{ ...file, mimeType: 'image/svg+xml' }]), /<img/);
});

test('production size limits and the real composer API remain in use', () => {
  assert.match(html, /file.size>600000/);
  assert.match(html, /file.size>250000/);
  assert.match(html, /api\('\/api\/internal-mail\/conversations',\{method:'POST'/);
  assert.match(html, /mobile-toolbar-host/);
  assert.match(html, /send.setAttribute\('form', 'composer-form'\)/);
});

for (const messagingOnly of [false, true]) {
  test(`mobile initialization reaches mailbox loading ${messagingOnly ? 'without the Safari ERP link' : 'with the ERP link'}`, () => {
    const nodes = new Map();
    function element() {
      return {
        dataset: {}, classList: { toggle() {}, remove() {}, contains: () => false },
        lastChild: {}, children: [], addEventListener() {}, setAttribute() {}, removeAttribute() {}, remove() {},
        focus() {}, insertBefore() {}, querySelectorAll: () => [],
        append(child) { this.children.push(child); },
        querySelector() { return element(); },
        cloneNode() { return element(); },
      };
    }
    const getNode = id => {
      if (!nodes.has(id)) nodes.set(id, element());
      return nodes.get(id);
    };
    const home = messagingOnly ? null : element();
    let loaded = false;
    const context = vm.createContext({
      $: getNode,
      state: { folder: 'inbox', category: '' },
      mailTeamMarkup: () => '',
      bootstrap: () => { loaded = true; },
      document: {
        addEventListener() {}, createElement: element,
        querySelector: selector => selector === '.erp-home-link' ? home : getNode(selector),
      },
      window: { matchMedia: () => ({ matches: true, addEventListener() {} }) },
    });
    const start = html.indexOf('    function initializeMobileMailbox()');
    const end = html.indexOf('    initializeMobileMailbox();', start);
    assert.ok(start > 0 && end > start);
    vm.runInContext(html.slice(start, end) + '\ninitializeMobileMailbox(); bootstrap();', context);
    assert.equal(loaded, true);
    assert.equal(getNode('.composer-head').children[0].textContent, 'Envoyer');
    if (home) assert.equal(home.children[0].textContent, 'ERP');
  });
}
