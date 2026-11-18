(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const base = '/api/admin/implementation-logbook';
  const state = { stages: [], entries: [], stageId: null, selectedId: null, filter: 'all', deployments: {}, environment: 'local' };
  const entryForm = $('entry-form');
  let editingEntry = null;
  let editorSnapshot = '';
  let saving = false;
  let loading = false;
  let checkingDeployment = false;
  let release = null;
  let deploymentStageId = null;
  let refreshTimer;

  function stageStatus(stage) {
    return { deployed: 'Déployé et vérifié', deploying: 'Publication en cours', failed: 'Échec de publication' }[stage?.status] || 'Local — non déployé';
  }

  function node(tag, className, text) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function notice(element, message, error = false) {
    element.textContent = message;
    element.className = `notice${error ? ' error' : ''}`;
    element.hidden = false;
  }

  async function api(url, method = 'GET', body) {
    const token = localStorage.getItem('erp_token');
    if (!token) throw new Error("Session absente. Reconnecte-toi à l'ERP pour ouvrir le carnet.");
    const response = await fetch(base + url, {
      method, cache: 'no-store',
      headers: { Authorization: `Bearer ${token}`, ...(body ? { 'Content-Type': 'application/json' } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || `Erreur serveur (${response.status}).`);
    return result;
  }

  function date(value, withTime = false) {
    return new Intl.DateTimeFormat('fr-CA', { dateStyle: 'medium', ...(withTime ? { timeStyle: 'short' } : {}) }).format(new Date(value));
  }

  function stageEntries() {
    return state.entries.filter(entry => entry.stageId === state.stageId);
  }

  function renderDetail(entry) {
    const detail = $('detail');
    detail.replaceChildren();
    detail.hidden = !entry;
    detail.parentElement.classList.toggle('has-detail', Boolean(entry));
    if (!entry) return;
    detail.append(
      node('p', 'detail-label', `${entry.kind === 'profile' ? 'Profil' : 'Module'} · Fiche ${entry.id} · Version ${entry.version}`),
      node('h3', '', entry.title),
      node('p', 'detail-summary', entry.summary),
    );
    const comparison = node('div', 'comparison');
    for (const [heading, text, className] of [['Avant', entry.beforeText, ''], ['Après (périmètre de l’étape)', entry.afterText, 'after']]) {
      const block = node('div', `spec-block ${className}`);
      block.append(node('strong', '', heading), node('p', '', text));
      comparison.append(block);
    }
    detail.append(comparison);
    const specifications = node('dl', 'spec-list');
    for (const [heading, text, wide] of [
      ['Profils concernés', entry.profiles], ['Modules concernés', entry.modules],
      ['Droits et restrictions', entry.rights], ['Visibilité, menus et écrans', entry.visibility],
      ['Vérifications', entry.validation || 'À renseigner.', true],
      ['État des vérifications', entry.validationStatus === 'verified' ? 'Vérifications effectuées' : 'À vérifier'],
      ["Demande d'origine / référence", entry.source || 'Non renseignée.', true],
      ['Statut de l’étape', stageStatus(state.stages.find(stage => stage.id === entry.stageId))],
    ]) {
      const row = node('div', `spec-row${wide ? ' wide' : ''}`);
      row.append(node('dt', '', heading), node('dd', '', text));
      specifications.append(row);
    }
    const edit = node('button', 'secondary', 'Modifier la fiche');
    edit.type = 'button';
    edit.addEventListener('click', () => openEditor(entry));
    detail.append(
      specifications,
      node('p', 'detail-foot', `Créée le ${date(entry.createdAt, true)} par ${entry.createdBy}. Dernière mise à jour le ${date(entry.updatedAt, true)} par ${entry.updatedBy}.`),
      edit,
    );
  }

  function renderEntries() {
    const entries = stageEntries();
    const term = $('search').value.trim().toLocaleLowerCase('fr');
    const visible = entries.filter(entry =>
      (state.filter === 'all' || entry.kind === state.filter) &&
      [entry.title, entry.summary, entry.profiles, entry.modules, entry.rights, entry.beforeText, entry.afterText, entry.visibility, entry.validation, entry.source]
        .join(' ').toLocaleLowerCase('fr').includes(term));
    if (!visible.some(entry => entry.id === state.selectedId)) state.selectedId = visible[0]?.id ?? null;
    const list = $('entries');
    list.replaceChildren();
    if (!visible.length) list.append(node('p', 'empty', entries.length ? 'Aucune fiche ne correspond à ces filtres.' : "Cette étape ne contient encore aucun changement. Les modules et profils à préparer seront ajoutés ici selon tes consignes."));
    for (const entry of visible) {
      const card = node('button', 'entry-card');
      card.type = 'button';
      card.dataset.entryId = entry.id;
      card.setAttribute('aria-pressed', String(entry.id === state.selectedId));
      const top = node('span', 'entry-top');
      const time = node('time', '', date(entry.updatedAt));
      time.dateTime = entry.updatedAt;
      top.append(node('span', `entry-kind ${entry.kind}`, entry.kind === 'profile' ? 'Profil' : 'Module'), time);
      const bottom = node('span', 'entry-bottom');
      bottom.append(node('span', '', stageStatus(state.stages.find(stage => stage.id === entry.stageId))), node('span', entry.validationStatus === 'verified' ? 'verified' : '', entry.validationStatus === 'verified' ? 'Fiche vérifiée' : 'À vérifier'));
      card.append(top, node('strong', '', entry.title), node('p', '', entry.summary), bottom);
      card.addEventListener('click', () => {
        state.selectedId = entry.id;
        list.querySelectorAll('.entry-card').forEach(button => button.setAttribute('aria-pressed', String(Number(button.dataset.entryId) === entry.id)));
        renderDetail(entry);
      });
      list.append(card);
    }
    renderDetail(visible.find(entry => entry.id === state.selectedId));
    for (const [id, count] of [
      ['metric-total', entries.length], ['metric-modules', entries.filter(entry => entry.kind === 'module').length],
      ['metric-profiles', entries.filter(entry => entry.kind === 'profile').length],
      ['metric-verified', entries.filter(entry => entry.validationStatus === 'verified').length],
    ]) $(id).textContent = String(count).padStart(2, '0');
    document.querySelectorAll('[data-filter]').forEach(button => {
      const filter = button.dataset.filter;
      button.setAttribute('aria-pressed', String(filter === state.filter));
      button.textContent = `${{ all: 'Tous', module: 'Modules', profile: 'Profils' }[filter]} · ${filter === 'all' ? entries.length : entries.filter(entry => entry.kind === filter).length}`;
    });
  }

  function render() {
    const stage = state.stages.find(item => item.id === state.stageId);
    $('folders').replaceChildren();
    for (const item of state.stages) {
      const folder = node('button', 'folder');
      folder.type = 'button';
      folder.setAttribute('aria-current', String(item.id === state.stageId));
      const icon = node('span', 'folder-icon', '▱');
      icon.append(node('small', '', `ÉTAPE ${String(item.number).padStart(2, '0')}`));
      folder.append(icon, node('strong', '', item.title), node('span', 'count', `${state.entries.filter(entry => entry.stageId === item.id).length} fiche(s)`), node('span', 'folder-status', `● ${stageStatus(item)}`));
      folder.addEventListener('click', () => { state.stageId = item.id; state.selectedId = null; render(); });
      $('folders').append(folder);
    }
    $('stage-title').textContent = stage.title;
    $('stage-number').textContent = `Dossier / Étape ${stage.number}`;
    $('stage-status').textContent = stageStatus(stage);
    $('stage-date').textContent = `Créé le ${date(stage.createdAt)}`;
    const deployment = state.deployments[state.stageId];
    $('open-deployment').textContent = `↑ Déployer l’étape ${stage.number}`;
    $('open-deployment').disabled = stage.status === 'deploying' || stage.status === 'deployed' || state.environment === 'production';
    $('deployment-hint').textContent = `${deployment.confirmationCode} requis · ${deployment.enabled ? 'livraison isolée du Carnet' : 'publication verrouillée'}`;
    $('workflow-confirmation').textContent = `Confirmation ${deployment.confirmationCode} → DigitalOcean`;
    $('publication-note').textContent = stage.number === 0
      ? 'Seul le clic confirmé avec CHADA0 lance la publication du Carnet. L’étape 1, les bases locales et les autres modifications ne sont jamais envoyées.'
      : 'CHADA1 reste verrouillé. La publication préalable du Carnet ne publie aucun changement RH de cette étape.';
    $('environment-label').textContent = state.environment === 'production' ? '● RyanERP en ligne · Admin' : '● Centre de publication localhost';
    $('environment-note').replaceChildren(
      node('strong', '', state.environment === 'production' ? 'Carnet en ligne' : 'Publication contrôlée'),
      document.createTextNode(state.environment === 'production'
        ? 'Les fiches de ce Carnet sont conservées dans la base du site. Les publications se lancent uniquement depuis localhost.'
        : 'Étape 0 : Carnet uniquement, après CHADA0. Étape 1 : publication distincte, toujours verrouillée. Aucun envoi au chargement du dossier.'),
    );
    const progress = $('deployment-progress');
    progress.hidden = !deployment.job;
    progress.replaceChildren();
    if (deployment.job) {
      progress.append(node('strong', '', stageStatus(stage)), node('p', '', deployment.job.message));
      if (deployment.job.commitSha) progress.append(node('p', '', `Version isolée : ${deployment.job.commitSha.slice(0, 12)}`));
      if (deployment.job.completedAt) progress.append(node('p', '', `Résultat enregistré le ${date(deployment.job.completedAt, true)}`));
    }
    renderEntries();
  }

  async function load() {
    if (loading) return;
    loading = true;
    $('refresh').disabled = true;
    try {
      const payload = await api('/');
      state.stages = payload.stages;
      state.entries = payload.entries;
      state.deployments = payload.deployments;
      state.environment = payload.environment;
      if (!state.stages.some(stage => stage.id === state.stageId)) state.stageId = state.stages[0]?.id;
      if (!state.stageId) throw new Error("Les dossiers d’implémentation sont absents. Vérifie l’initialisation du Carnet.");
      render();
      $('workspace').hidden = false;
    } catch (error) {
      notice($('notice'), error.message, true);
    } finally {
      $('loading').hidden = true;
      $('refresh').disabled = false;
      loading = false;
      clearTimeout(refreshTimer);
      if (state.stages.some(stage => stage.status === 'deploying')) refreshTimer = setTimeout(load, 4000);
    }
  }

  function formValues() {
    return Object.fromEntries(new FormData(entryForm));
  }

  function openEditor(entry = null) {
    editingEntry = entry;
    entryForm.reset();
    for (const field of entryForm.querySelectorAll('[name]')) field.value = entry?.[field.name] ?? (field.name === 'kind' ? 'module' : field.name === 'validationStatus' ? 'pending' : '');
    $('entry-dialog-title').textContent = entry ? 'Modifier la spécification' : 'Nouvelle fiche';
    $('entry-stage').textContent = state.stages.find(stage => stage.id === state.stageId).title;
    $('entry-error').hidden = true;
    editorSnapshot = JSON.stringify(formValues());
    $('entry-dialog').showModal();
  }

  function cancelEditor(event) {
    event?.preventDefault();
    if (saving) return;
    if (JSON.stringify(formValues()) !== editorSnapshot && !window.confirm('Fermer sans enregistrer cette fiche ?')) return;
    $('entry-dialog').close();
  }

  entryForm.addEventListener('submit', async event => {
    event.preventDefault();
    if (saving) return;
    saving = true;
    $('save-entry').disabled = true;
    $('entry-error').hidden = true;
    try {
      const values = formValues();
      if (editingEntry) values.version = editingEntry.version;
      const prefix = `/stages/${encodeURIComponent(state.stageId)}/entries`;
      const entry = await api(editingEntry ? `${prefix}/${editingEntry.id}` : prefix, editingEntry ? 'PUT' : 'POST', values);
      state.entries = [entry, ...state.entries.filter(item => item.id !== entry.id)];
      state.selectedId = entry.id;
      state.filter = 'all';
      $('search').value = '';
      $('entry-dialog').close();
      render();
      notice($('notice'), 'Fiche enregistrée dans ce Carnet. Aucun déploiement déclenché.');
    } catch (error) {
      notice($('entry-error'), error.message, true);
    } finally {
      saving = false;
      $('save-entry').disabled = false;
    }
  });

  function updateConfirmation() {
    const deployment = state.deployments[deploymentStageId];
    if (!deployment) return;
    const value = $('deployment-code').value;
    const valid = value === deployment.confirmationCode;
    $('deployment-code').setAttribute('aria-invalid', String(Boolean(value) && !valid));
    $('code-feedback').className = `code-feedback${value && !valid ? ' invalid' : ''}`;
    $('code-feedback').textContent = value && !valid ? `Code incorrect. Saisis exactement ${deployment.confirmationCode}.`
      : valid ? deployment.enabled ? 'Code reconnu. La confirmation déclenchera le déploiement réel du Carnet uniquement.' : 'Code reconnu. La publication reste verrouillée.'
        : 'La saisie doit correspondre exactement au code.';
    $('confirm-deployment').disabled = checkingDeployment || !valid || !$('scope-confirm').checked || !deployment.enabled || !release;
  }

  $('open-deployment').addEventListener('click', async () => {
    deploymentStageId = state.stageId;
    release = null;
    const stage = state.stages.find(item => item.id === deploymentStageId);
    const deployment = state.deployments[deploymentStageId];
    $('deployment-form').reset();
    $('deployment-error').hidden = true;
    $('deployment-title').textContent = `Déployer l’étape ${stage.number}`;
    $('deployment-warning').textContent = deployment.enabled
      ? 'Déploiement réel : seule une version isolée du Carnet sera envoyée. La branche de production et le résultat DigitalOcean seront contrôlés.'
      : 'Publication verrouillée : aucune publication de cette étape ne sera lancée.';
    $('deployment-scope').textContent = stage.number === 0
      ? 'Inclus : Carnet, menu Admin, API et tables dédiées. Exclus : modifications RH de l’étape 1, fiches locales, messages, bases, archives et secrets. Les données métier de production restent en place.'
      : 'L’étape 1 n’est pas incluse dans CHADA0. Sa version et sa procédure de publication doivent encore être validées.';
    $('deployment-code-label').textContent = `Code de confirmation de l’étape ${stage.number} : ${deployment.confirmationCode}`;
    $('scope-confirm-label').textContent = stage.number === 0
      ? 'Je confirme le déploiement réel du Carnet uniquement sur RyanERP DigitalOcean, sans l’étape 1 ni import des données locales.'
      : 'J’ai relu les fiches et le périmètre de l’étape 1.';
    $('confirm-deployment').textContent = deployment.enabled ? 'Déployer le Carnet maintenant' : 'Publication verrouillée';
    $('deployment-blockers').replaceChildren(...deployment.blockers.map(text => node('li', '', text)));
    checkingDeployment = deployment.enabled;
    updateConfirmation();
    $('deployment-dialog').showModal();
    $('deployment-code').focus();
    if (deployment.enabled) {
      try {
        release = await api(`/stages/${encodeURIComponent(deploymentStageId)}/deployment-check`);
        $('deployment-blockers').replaceChildren(
          node('li', '', `Production et branche vérifiées. Base : ${release.baseCommit.slice(0, 12)}.`),
          node('li', '', `${release.files.length} fichiers du Carnet uniquement : ${release.files.join(', ')}`),
        );
      } catch (error) {
        notice($('deployment-error'), error.message, true);
      } finally {
        checkingDeployment = false;
        updateConfirmation();
      }
    }
  });
  $('deployment-form').addEventListener('submit', async event => {
    event.preventDefault();
    if (checkingDeployment) return;
    const deployment = state.deployments[deploymentStageId];
    if (!deployment?.enabled || !release || $('deployment-code').value !== deployment.confirmationCode || !$('scope-confirm').checked) {
      notice($('deployment-error'), 'Vérifie la livraison, saisis le code exact et confirme le périmètre.', true);
      return;
    }
    checkingDeployment = true;
    updateConfirmation();
    try {
      const result = await api(`/stages/${encodeURIComponent(deploymentStageId)}/deploy`, 'POST', {
        code: $('deployment-code').value, confirmed: $('scope-confirm').checked, releaseDigest: release.digest,
      });
      if (!result.job?.id) throw new Error('Résultat de publication incomplet. Actualise le suivi avant toute nouvelle tentative.');
      $('deployment-dialog').close();
      notice($('notice'), 'Demande CHADA0 enregistrée. Consulte le suivi ci-dessous ; le succès ne sera annoncé qu’après vérification DigitalOcean et HTTP.');
      await load();
    } catch (error) {
      notice($('deployment-error'), error.message, true);
    } finally {
      checkingDeployment = false;
      updateConfirmation();
    }
  });
  for (const id of ['close-deployment', 'cancel-deployment']) $(id).addEventListener('click', () => { if (!checkingDeployment) $('deployment-dialog').close(); });
  $('deployment-dialog').addEventListener('cancel', event => { if (checkingDeployment) event.preventDefault(); });
  $('deployment-code').addEventListener('input', updateConfirmation);
  $('scope-confirm').addEventListener('change', updateConfirmation);
  for (const id of ['close-entry', 'cancel-entry']) $(id).addEventListener('click', cancelEditor);
  $('entry-dialog').addEventListener('cancel', cancelEditor);
  $('new-entry').addEventListener('click', () => openEditor());
  $('refresh').addEventListener('click', () => { $('notice').hidden = true; load(); });
  $('search').addEventListener('input', renderEntries);
  document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => { state.filter = button.dataset.filter; renderEntries(); }));
  load();
})();
