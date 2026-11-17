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
  assert.match(html, /mobile-toolbar-host/);
  assert.match(html, /send.setAttribute\('form', 'composer-form'\)/);
});

test('long recipient headers collapse without a second full recipient block', () => {
  const start = html.indexOf('    function compactRecipients(');
  const end = html.indexOf('    const recipientSelectId', start);
  const context = vm.createContext({ esc: value => String(value).replace(/</g, '&lt;') });
  vm.runInContext(html.slice(start, end), context);
  const recipients = Array.from({ length: 20 }, (_, index) => ({ fullName: 'Personne ' + (index + 1), recipientType: 'to' }));
  const markup = context.compactRecipients(recipients);
  const summary = markup.match(/<summary>([\s\S]*?)<\/summary>/)[1];
  assert.match(summary, /Personne 1, Personne 2/);
  assert.doesNotMatch(summary, /Personne 3/);
  assert.match(markup, /18 autre\(s\) · Voir plus/);
  assert.match(markup, /Voir moins/);
  assert.equal((markup.match(/<dd>/g) || []).length, 1);
  assert.doesNotMatch(context.compactRecipients(recipients.slice(0, 2)), /<details/);
  assert.match(context.compactRecipients([{ fullName: '<Copie>', recipientType: 'cc' }]), /&lt;Copie>/);
  const mixed = context.compactRecipients([{ fullName: 'Copie', recipientType: 'cc' }, { fullName: 'Principal', recipientType: 'to' }]);
  assert.match(mixed, /À : Principal<\/span>/);
  assert.doesNotMatch(mixed.match(/<summary>([\s\S]*?)<\/summary>/)[1], /À : Copie/);
});

test('overlapping autosaves create once and update the same draft with the newest content', async () => {
  let release;
  const firstResponse = new Promise(resolve => { release = resolve; });
  const calls = [];
  const composer = { open: true, id: 0, version: 1, savedVersion: 0, pendingFiles: 0, saving: null, timer: null };
  const context = vm.createContext({
    composer, clearTimeout() {}, hasComposerContent: () => true, cacheComposer() {}, draftStatus() {},
    composerPayload: status => ({ status, body: 'Version ' + composer.version }),
    $: () => ({ hidden: true }),
    api: (url, options) => {
      calls.push({ url, method: options.method, body: JSON.parse(options.body) });
      return calls.length === 1 ? firstResponse : Promise.resolve({ id: 42, updatedAt: 'revision-2' });
    },
  });
  const start = html.indexOf('    async function flushComposer(');
  const end = html.indexOf('    async function openComposer(', start);
  vm.runInContext(html.slice(start, end), context);
  const first = context.flushComposer();
  composer.version = 2;
  const second = context.flushComposer();
  release({ id: 42, updatedAt: 'revision-1' });
  await Promise.all([first, second]);
  assert.equal(calls[0].method, 'POST');
  assert.equal(calls[0].url, '/api/internal-mail/conversations');
  assert.equal(calls[1].method, 'PUT');
  assert.equal(calls[1].url, '/api/internal-mail/conversations/42/draft');
  assert.equal(calls[1].body.body, 'Version 2');
  assert.equal(composer.savedVersion, 2);
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
