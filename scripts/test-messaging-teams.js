const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'messaging.html'), 'utf8');

function extract(start, end) {
  const first = html.indexOf(start);
  const last = html.indexOf(end, first);
  assert.ok(first >= 0 && last > first, `Missing source: ${start}`);
  return html.slice(first, last);
}

function render(view, filter = '') {
  const host = { innerHTML: '' };
  const context = vm.createContext({
    state: {
      recipientView: view,
      currentUserRole: 'admin',
      users: [
        { id: 1, username: 'test_admin', fullName: 'Test Admin', department: 'Administration', role: 'admin' },
        { id: 2, username: 'test_rh', fullName: 'Test RH', department: 'RH', role: 'employe_standard' },
      ],
      projects: [{ id: 3, name: 'Projet test', site: 'Site test' }],
      recipientTeamIds: [10],
      teamId: 10,
      teams: [
        { id: 10, name: 'RH', isMember: true, recipientCount: 1, members: [
          { employeeId: 20, fullName: 'Test RH', userId: 2 },
          { employeeId: 21, fullName: 'Sans compte', userId: null },
        ] },
        { id: 11, name: 'Achats', isMember: false, recipientCount: 1, members: [
          { employeeId: 22, fullName: 'Test Achats', userId: 3 },
        ] },
        { id: 12, name: 'Vide', isMember: false, recipientCount: 0, members: [] },
      ],
    },
    document: {
      querySelector: selector => selector.includes('data-recipient-options') ? host : { value: filter },
    },
    esc: value => String(value ?? '').replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[char])),
    selectedRecipientIds: () => [],
    avatarStyle: () => '',
    initials: () => 'T',
    recentRecipientIds: () => [2],
    $: () => ({ checked: false }),
  });
  vm.runInContext(extract('    const mailTeamIcon =', '    function renderSelectedTeams('), context);
  vm.runInContext(extract('    function renderRecipientOptions(', '    function openRecipientPicker('), context);
  context.renderRecipientOptions('to');
  return view === 'sidebar' ? context.mailTeamMarkup() : host.innerHTML;
}

test('configured teams can be selected and their actual employee members are displayed', () => {
  const markup = render('teams');
  assert.match(markup, /data-recipient-team="10"/);
  assert.match(markup, /data-recipient-team="11"/);
  assert.match(markup, /Voir les membres/);
  assert.match(markup, /Test RH/);
  assert.match(markup, /Sans compte/);
  assert.match(markup, /Compte actif à lier/);
  assert.match(markup, /data-recipient-team="12" disabled/);
  assert.doesNotMatch(markup, /À configurer|data-recipient-all|data-category/);
});

test('team navigation is membership-scoped even for an administrator; address-book search does not guess groups from roles', () => {
  const sidebar = render('sidebar');
  assert.match(sidebar, /data-mail-team="10"/);
  assert.doesNotMatch(sidebar, /data-mail-team="11"|data-mail-team="12"/);
  assert.equal((render('teams', 'rh').match(/data-recipient-team=/g) || []).length, 1);
  const markup = render('teams', 'employe_standard');
  assert.match(markup, /Aucune équipe correspondant à la recherche/);
  assert.doesNotMatch(markup, /data-recipient-team=/);
});

test('individuals, departments, recent recipients and associated projects remain available', () => {
  const all = render('all');
  assert.match(all, /data-recipient-id="1"/);
  assert.match(all, /data-recipient-id="2"/);
  assert.match(all, /data-recipient-all/);
  const departments = render('departments');
  assert.match(departments, /Administration · 1/);
  assert.match(departments, /RH · 1/);
  assert.match(departments, /data-recipient-id="2"/);
  const recent = render('recent');
  assert.match(recent, /data-recipient-id="2"/);
  assert.doesNotMatch(recent, /data-recipient-id="1"/);
  assert.match(render('projects'), /data-recipient-project="3"/);
});

test('the app retains exactly five category folders and one envelope brand', () => {
  const sidebar = extract('      <aside class="sidebar">', '      <section class="list-panel">');
  assert.equal((sidebar.match(/class="folder" data-category=/g) || []).length, 5);
  for (const category of ['incident', 'general', 'project', 'meeting', 'administration']) {
    assert.ok(sidebar.includes(`data-category="${category}"`), category);
  }
  assert.match(sidebar, /Équipes destinataires/);
  assert.match(sidebar, /id="mail-teams"/);
  const topbar = extract('    <header class="topbar">', '    <main class="workspace">');
  assert.equal((topbar.match(/<img /g) || []).length, 1);
  assert.doesNotMatch(sidebar, /<img /);
  assert.match(topbar, /src="\/assets\/Messagerie%20interne.png"/);
  assert.doesNotMatch(topbar, /ryan-erp-logo-icon/);
});
