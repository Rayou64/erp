const { linkedEmployeeJoin, INTERNAL_MAIL_IDENTITIES } = require('./user-lifecycle');
const { createHash } = require('node:crypto');

function mailError(status, message) {
  return Object.assign(new Error(message), { status });
}

function ids(value = []) {
  if (!Array.isArray(value)) throw mailError(400, 'La liste de destinataires ou de membres est invalide.');
  const result = value.map(Number);
  if (result.some(id => !Number.isSafeInteger(id) || id <= 0)) throw mailError(400, 'Un identifiant est invalide.');
  return [...new Set(result)];
}

const memberAccess = `EXISTS (
  SELECT 1 FROM internal_mail_conversation_teams ct
  JOIN hr_team_members tm ON tm.teamId = ct.teamId
  JOIN hr_employees he ON he.id = tm.employeeId
  LEFT JOIN user_profile_states ps ON ps.employeeId = he.id AND ps.userId = viewer.id
  WHERE ct.conversationId = c.id
    AND LOWER(TRIM(he.username)) = LOWER(TRIM(viewer.username))
    AND COALESCE(ps.deactivatedAt, '') = '' AND COALESCE(ps.deletedAt, '') = ''
)`;
const recipientAccess = 'EXISTS (SELECT 1 FROM internal_mail_recipients r WHERE r.conversationId = c.id AND r.userId = viewer.id)';
const senderAccess = 'EXISTS (SELECT 1 FROM internal_mail_messages m WHERE m.conversationId = c.id AND m.senderId = viewer.id)';
const access = `(c.createdBy = viewer.id OR (c.status = 'sent' AND (${recipientAccess} OR ${senderAccess} OR ${memberAccess})))`;
const deletedForViewer = `COALESCE(org.deletedAt,
  NULLIF((SELECT r.deletedAt FROM internal_mail_recipients r
    WHERE r.conversationId = c.id AND r.userId = viewer.id ORDER BY r.messageId DESC, r.id DESC LIMIT 1), ''),
  CASE WHEN c.createdBy = viewer.id THEN c.deletedAt ELSE '' END, '')`;

function createInternalMail({ db, getTableColumns, notify }) {
  const change = operation => db.transaction(async tx => {
    // MAX(id)+1 allocations must also be serialized across PostgreSQL instances.
    if (tx.driver === 'postgres') await tx.get('SELECT pg_advisory_xact_lock(73291104)');
    return operation(tx);
  });
  const nextId = async (tx, table) => Number((await tx.get(`SELECT COALESCE(MAX(id), 0) + 1 AS nextId FROM ${table}`)).nextId);

  async function ensureSchema() {
    for (const [table, columns] of [
      ['internal_mail_conversations', { draftKey: "TEXT NOT NULL DEFAULT ''", sendToAll: 'INTEGER NOT NULL DEFAULT 0', sendFingerprint: "TEXT NOT NULL DEFAULT ''" }],
      ['internal_mail_recipients', { isTeamRecipient: 'INTEGER NOT NULL DEFAULT 0' }],
      ['internal_mail_user_organization', {
        deletedAt: 'TEXT', restoreFolder: "TEXT NOT NULL DEFAULT ''", isRead: 'INTEGER', isImportant: 'INTEGER',
      }],
    ]) {
      const existing = await getTableColumns(table);
      for (const [name, type] of Object.entries(columns)) {
        if (!existing.has(name)) await db.run(`ALTER TABLE ${table} ADD COLUMN ${name} ${type}`);
      }
    }
    await db.run(`CREATE TABLE IF NOT EXISTS hr_teams (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, nameKey TEXT NOT NULL UNIQUE,
      createdBy INTEGER NOT NULL, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL
    )`);
    await db.run(`CREATE TABLE IF NOT EXISTS hr_team_members (
      teamId INTEGER NOT NULL, employeeId INTEGER NOT NULL,
      PRIMARY KEY(teamId, employeeId),
      FOREIGN KEY(teamId) REFERENCES hr_teams(id) ON DELETE CASCADE,
      FOREIGN KEY(employeeId) REFERENCES hr_employees(id) ON DELETE CASCADE
    )`);
    await db.run(`CREATE TABLE IF NOT EXISTS internal_mail_conversation_teams (
      conversationId INTEGER NOT NULL, teamId INTEGER NOT NULL,
      PRIMARY KEY(conversationId, teamId),
      FOREIGN KEY(conversationId) REFERENCES internal_mail_conversations(id) ON DELETE CASCADE,
      FOREIGN KEY(teamId) REFERENCES hr_teams(id)
    )`);
    await db.run('CREATE INDEX IF NOT EXISTS idx_hr_team_employee ON hr_team_members(employeeId, teamId)');
    await db.run('CREATE INDEX IF NOT EXISTS idx_mail_team_conversation ON internal_mail_conversation_teams(teamId, conversationId)');
    await db.run('CREATE INDEX IF NOT EXISTS idx_mail_conversation_recipient ON internal_mail_recipients(conversationId, userId, messageId)');
    await db.run("CREATE UNIQUE INDEX IF NOT EXISTS idx_mail_draft_key ON internal_mail_conversations(createdBy, draftKey) WHERE draftKey <> ''");
  }

  async function getUser(userId, tx = db) {
    return tx.get(`SELECT u.id, u.username, u.role,
      COALESCE(NULLIF(TRIM(he.fullName), ''), u.username) AS fullName,
      COALESCE(NULLIF(TRIM(he.jobTitle), ''), u.role) AS department
      FROM users u ${linkedEmployeeJoin('u', 'he')}
      WHERE u.id = ? AND u.deactivatedAt = ''`, [Number(userId)]);
  }

  async function listTeams(userId, tx = db) {
    const teams = await tx.all('SELECT id, name, createdAt, updatedAt FROM hr_teams ORDER BY name ASC, id ASC');
    const members = await tx.all(`SELECT tm.teamId, he.id AS employeeId, he.fullName, he.jobTitle,
      CASE WHEN COALESCE(ps.deactivatedAt, '') = '' AND COALESCE(ps.deletedAt, '') = '' THEN u.id ELSE NULL END AS userId,
      he.username
      FROM hr_team_members tm JOIN hr_employees he ON he.id = tm.employeeId
      LEFT JOIN users u ON LOWER(TRIM(u.username)) = LOWER(TRIM(he.username)) AND u.deactivatedAt = ''
      LEFT JOIN user_profile_states ps ON ps.employeeId = he.id AND ps.userId = u.id
      ORDER BY he.fullName ASC, he.id ASC`);
    return teams.map(team => {
      const people = members.filter(member => Number(member.teamId) === Number(team.id)).map(member => ({
        employeeId: Number(member.employeeId), fullName: member.fullName, jobTitle: member.jobTitle,
        username: member.username, userId: member.userId == null ? null : Number(member.userId),
      }));
      return {
        ...team, id: Number(team.id), members: people, employeeIds: people.map(person => person.employeeId),
        recipientCount: new Set(people.filter(person => person.userId).map(person => person.userId)).size,
        isMember: people.some(person => person.userId === Number(userId)),
      };
    });
  }

  async function saveTeam(user, teamId, payload) {
    if (!['admin', 'directeur_rh'].includes(user.role)) throw mailError(403, 'Gestion des équipes réservée à l’administrateur et au directeur RH.');
    if (teamId !== null && (!Number.isSafeInteger(teamId) || teamId <= 0)) throw mailError(400, 'Identifiant d’équipe invalide.');
    const name = String(payload.name || '').trim();
    const nameKey = name.normalize('NFKC').toLocaleLowerCase('fr');
    const employeeIds = ids(payload.employeeIds);
    if (!name || name.length > 80) throw mailError(400, 'Le nom de l’équipe est obligatoire (80 caractères maximum).');
    const id = await change(async tx => {
      if (teamId && !(await tx.get('SELECT id FROM hr_teams WHERE id = ?', [teamId]))) throw mailError(404, 'Équipe introuvable.');
      if (await tx.get('SELECT id FROM hr_teams WHERE nameKey = ? AND id <> ?', [nameKey, teamId || 0])) throw mailError(409, 'Une équipe porte déjà ce nom.');
      if (employeeIds.length) {
        const found = await tx.all(`SELECT id FROM hr_employees WHERE id IN (${employeeIds.map(() => '?').join(',')})`, employeeIds);
        if (found.length !== employeeIds.length) throw mailError(400, 'Un employé sélectionné est introuvable.');
      }
      const now = new Date().toISOString();
      const id = teamId || await nextId(tx, 'hr_teams');
      if (teamId) await tx.run('UPDATE hr_teams SET name = ?, nameKey = ?, updatedAt = ? WHERE id = ?', [name, nameKey, now, id]);
      else await tx.run('INSERT INTO hr_teams (id, name, nameKey, createdBy, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?)', [id, name, nameKey, user.id, now, now]);
      await tx.run('DELETE FROM hr_team_members WHERE teamId = ?', [id]);
      for (const employeeId of employeeIds) await tx.run('INSERT INTO hr_team_members (teamId, employeeId) VALUES (?, ?)', [id, employeeId]);
      return id;
    });
    return (await listTeams(user.id)).find(team => team.id === id);
  }

  async function getAccess(conversationId, userId, tx = db) {
    return tx.get(`SELECT c.*, ${deletedForViewer} AS userDeletedAt,
      COALESCE(org.isStarred, (SELECT MAX(r.isStarred) FROM internal_mail_recipients r WHERE r.conversationId = c.id AND r.userId = viewer.id), 0) AS isStarred,
      COALESCE(org.restoreFolder, '') AS restoreFolder
      FROM internal_mail_conversations c JOIN users viewer ON viewer.id = ?
      LEFT JOIN internal_mail_user_organization org ON org.conversationId = c.id AND org.userId = viewer.id
      WHERE c.id = ? AND ${access}`, [Number(userId), Number(conversationId)]);
  }

  async function listConversations(userId, { folder = 'inbox', category = '', q = '', teamId = 0 } = {}) {
    if (!['inbox', 'sent', 'drafts', 'trash', 'starred', 'important', 'unread'].includes(folder)) throw mailError(400, 'Dossier de messagerie invalide.');
    if (!Number.isSafeInteger(Number(teamId)) || Number(teamId) < 0) throw mailError(400, 'Identifiant d’équipe invalide.');
    const teams = await listTeams(userId);
    if (teamId && !teams.some(team => team.id === Number(teamId) && team.isMember)) throw mailError(403, 'Vous ne faites pas partie de cette équipe.');
    const rows = await db.all(`SELECT c.id, c.subject, c.category, c.priority, c.status, c.projectId,
      c.updatedAt, c.createdBy, m.id AS messageId, m.body, m.senderId, m.createdAt,
      su.username AS senderUsername,
      COALESCE(NULLIF(TRIM(se.fullName), ''), su.username) AS senderName,
      COALESCE(NULLIF(TRIM(se.jobTitle), ''), su.role) AS senderDepartment,
      p.nomProjet AS projectName,
      COALESCE((SELECT MIN(r.isRead) FROM internal_mail_recipients r WHERE r.conversationId = c.id AND r.userId = viewer.id), org.isRead, 1) AS isRead,
      COALESCE(org.isStarred, (SELECT MAX(r.isStarred) FROM internal_mail_recipients r WHERE r.conversationId = c.id AND r.userId = viewer.id), 0) AS isStarred,
      COALESCE(org.isImportant, (SELECT MAX(r.isImportant) FROM internal_mail_recipients r WHERE r.conversationId = c.id AND r.userId = viewer.id), 0) AS isImportant,
      ${deletedForViewer} AS deletedAt,
      COALESCE(org.folderCategory, '') AS userFolder,
      CASE WHEN ${recipientAccess} OR (c.status = 'sent' AND ${memberAccess}) THEN 1 ELSE 0 END AS received,
      CASE WHEN ${senderAccess} THEN 1 ELSE 0 END AS sentByUser,
      (SELECT COUNT(*) FROM internal_mail_attachments a WHERE a.messageId = m.id) AS attachmentCount,
      (SELECT COUNT(*) FROM internal_mail_messages cm WHERE cm.conversationId = c.id) AS messageCount
      FROM internal_mail_conversations c JOIN users viewer ON viewer.id = ?
      JOIN internal_mail_messages m ON m.id = (
        SELECT lm.id FROM internal_mail_messages lm WHERE lm.conversationId = c.id ORDER BY lm.createdAt DESC, lm.id DESC LIMIT 1)
      LEFT JOIN internal_mail_user_organization org ON org.conversationId = c.id AND org.userId = viewer.id
      LEFT JOIN ${INTERNAL_MAIL_IDENTITIES} su ON su.id = m.senderId
      ${linkedEmployeeJoin('su', 'se')}
      LEFT JOIN projects p ON p.id = c.projectId
      WHERE ${access}
      ORDER BY c.updatedAt DESC, c.id DESC`, [Number(userId)]);
    const associations = await db.all('SELECT conversationId, teamId FROM internal_mail_conversation_teams');
    const query = String(q).trim().toLocaleLowerCase('fr');
    return rows.map(row => ({
      ...row, id: Number(row.id), teamIds: associations.filter(item => Number(item.conversationId) === Number(row.id)).map(item => Number(item.teamId)),
    })).filter(row => {
      const deleted = Boolean(String(row.deletedAt || '').trim());
      if (folder === 'trash') return deleted;
      if (deleted) return false;
      if (row.status === 'draft') return folder === 'drafts';
      if (folder === 'drafts') return false;
      if (teamId && !row.teamIds.includes(Number(teamId))) return false;
      if (category) return row.category === category || row.userFolder === category;
      if (folder === 'sent') return Number(row.sentByUser) === 1;
      if (folder === 'starred') return Number(row.isStarred) === 1;
      if (folder === 'important') return Number(row.isImportant) === 1;
      if (folder === 'unread') return Number(row.isRead) === 0;
      return teamId || Number(row.received) === 1;
    }).filter(row => !query || [row.subject, row.body, row.senderName, row.senderUsername, row.projectName, row.category]
      .some(value => String(value || '').toLocaleLowerCase('fr').includes(query)));
  }

  async function setOrganization(tx, userId, conversationId, fields) {
    const legacy = await tx.get('SELECT MAX(isStarred) AS starred FROM internal_mail_recipients WHERE userId = ? AND conversationId = ?', [Number(userId), Number(conversationId)]);
    const values = { isStarred: Number(legacy?.starred || 0), ...fields };
    const columns = Object.keys(values);
    await tx.run(`INSERT INTO internal_mail_user_organization (userId, conversationId, ${columns.join(', ')})
      VALUES (?, ?, ${columns.map(() => '?').join(', ')})
      ON CONFLICT(userId, conversationId) DO UPDATE SET ${Object.keys(fields).map(column => `${column} = excluded.${column}`).join(', ')}`,
    [Number(userId), Number(conversationId), ...Object.values(values)]);
  }

  async function readConversation(conversationId, userId) {
    const conversation = await getAccess(conversationId, userId);
    if (!conversation) throw mailError(404, 'Conversation introuvable ou inaccessible.');
    const messages = await db.all(`SELECT m.id, m.body, m.senderId, m.createdAt, su.username AS senderUsername,
      COALESCE(NULLIF(TRIM(se.fullName), ''), su.username) AS senderName,
      COALESCE(NULLIF(TRIM(se.jobTitle), ''), su.role) AS senderDepartment
      FROM internal_mail_messages m LEFT JOIN ${INTERNAL_MAIL_IDENTITIES} su ON su.id = m.senderId
      ${linkedEmployeeJoin('su', 'se')}
      WHERE m.conversationId = ? ORDER BY m.createdAt ASC, m.id ASC`, [Number(conversationId)]);
    for (const message of messages) {
      message.attachments = await db.all(`SELECT id, fileName, mimeType, fileSize${conversation.status === 'draft' ? ', contentBase64' : ''}
        FROM internal_mail_attachments WHERE messageId = ? ORDER BY id ASC`, [Number(message.id)]);
      message.recipients = await db.all(`SELECT u.id, u.username, r.recipientType, r.isTeamRecipient,
        COALESCE(NULLIF(TRIM(he.fullName), ''), u.username) AS fullName
        FROM internal_mail_recipients r JOIN ${INTERNAL_MAIL_IDENTITIES} u ON u.id = r.userId
        ${linkedEmployeeJoin('u', 'he')}
        WHERE r.messageId = ? AND (r.recipientType <> 'bcc' OR r.userId = ? OR ? = ?)
        ORDER BY fullName ASC`, [Number(message.id), Number(userId), Number(message.senderId), Number(userId)]);
    }
    const associations = await db.all(`SELECT t.id, t.name FROM internal_mail_conversation_teams ct
      JOIN hr_teams t ON t.id = ct.teamId WHERE ct.conversationId = ? ORDER BY t.name ASC`, [Number(conversationId)]);
    conversation.teamIds = associations.map(item => Number(item.id));
    conversation.teams = associations.map(item => ({ id: Number(item.id), name: item.name }));
    delete conversation.sendFingerprint;
    if (conversation.status !== 'draft') delete conversation.draftKey;
    if (conversation.status !== 'draft') await change(async tx => {
      await tx.run('UPDATE internal_mail_recipients SET isRead = 1 WHERE conversationId = ? AND userId = ?', [Number(conversationId), Number(userId)]);
      await setOrganization(tx, userId, conversationId, { isRead: 1 });
    });
    return { conversation, messages };
  }

  function prepareAttachments(value = []) {
    if (!Array.isArray(value)) throw mailError(400, 'Pièces jointes invalides.');
    return value.map(file => {
      const contentBase64 = String(file.contentBase64 || '').replace(/^data:[^;]+;base64,/, '');
      if (!contentBase64 || !/^[a-zA-Z0-9+/]*={0,2}$/.test(contentBase64)) throw mailError(400, 'Contenu de pièce jointe invalide.');
      const fileSize = Buffer.from(contentBase64, 'base64').length;
      if (fileSize > 600000) throw mailError(400, 'Une pièce jointe dépasse 600 Ko.');
      const mimeType = String(file.mimeType || 'application/octet-stream');
      if (/[\r\n]/.test(mimeType)) throw mailError(400, 'Type de pièce jointe invalide.');
      return { fileName: String(file.fileName || 'document').slice(0, 200), mimeType, fileSize, contentBase64 };
    });
  }

  async function writeRecipients(tx, conversationId, messageId, recipients, now) {
    for (const [userId, entry] of recipients) {
      await tx.run(`INSERT INTO internal_mail_recipients
        (id, conversationId, messageId, userId, recipientType, isRead, isStarred, isImportant, deletedAt, isTeamRecipient)
        VALUES (?, ?, ?, ?, ?, 0, 0, 0, '', ?)`,
      [await nextId(tx, 'internal_mail_recipients'), conversationId, messageId, userId, entry.type, entry.team ? 1 : 0]);
    }
  }

  async function writeAttachments(tx, messageId, attachments, now) {
    for (const file of attachments) await tx.run(`INSERT INTO internal_mail_attachments
      (id, messageId, fileName, mimeType, fileSize, contentBase64, createdAt) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [await nextId(tx, 'internal_mail_attachments'), messageId, file.fileName, file.mimeType, file.fileSize, file.contentBase64, now]);
  }

  async function save(userId, payload, draftId = null) {
    if (draftId !== null && (!Number.isSafeInteger(draftId) || draftId <= 0)) throw mailError(400, 'Identifiant de brouillon invalide.');
    const status = payload.status || 'sent';
    if (!['sent', 'draft'].includes(status)) throw mailError(400, 'État de message invalide.');
    const subject = String(payload.subject || '').trim();
    const body = String(payload.body || '').trim();
    const attachments = prepareAttachments(payload.attachments);
    const direct = ids(payload.recipientIds), cc = ids(payload.ccRecipientIds), bcc = ids(payload.bccRecipientIds);
    const teamIds = ids(payload.teamIds);
    const draftKey = String(payload.draftKey || '').trim();
    const fingerprint = createHash('sha256').update(JSON.stringify({
      subject, body, attachments, direct: [...direct].sort((a,b)=>a-b), cc: [...cc].sort((a,b)=>a-b),
      bcc: [...bcc].sort((a,b)=>a-b), teamIds: [...teamIds].sort((a,b)=>a-b),
      category: String(payload.category || 'general'), priority: String(payload.priority || 'normal'),
      projectId: Number(payload.projectId || 0), sendToAll: Boolean(payload.sendToAll),
    })).digest('hex');
    if (draftKey.length > 100) throw mailError(400, 'Clé de brouillon invalide.');
    if (status === 'sent' && !subject) throw mailError(400, 'Le sujet est obligatoire.');
    if (status === 'sent' && !body && !attachments.length) throw mailError(400, 'Écrivez un texte ou ajoutez une pièce jointe.');
    const result = await change(async tx => {
      const sender = await getUser(userId, tx);
      if (!sender) throw mailError(401, 'Profil expéditeur introuvable.');
      let existing = draftId
        ? await tx.get('SELECT * FROM internal_mail_conversations WHERE id = ? AND createdBy = ?', [Number(draftId), Number(userId)])
        : draftKey ? await tx.get('SELECT * FROM internal_mail_conversations WHERE createdBy = ? AND draftKey = ?', [Number(userId), draftKey]) : null;
      if (draftId && !existing) throw mailError(404, 'Brouillon introuvable.');
      if (existing?.status === 'sent') {
        if (status === 'sent' && draftKey && existing.draftKey === draftKey && existing.sendFingerprint === fingerprint) {
          return { id: Number(existing.id), alreadySent: true, updatedAt: existing.updatedAt };
        }
        throw mailError(409, 'Ce message est déjà envoyé.');
      }
      if (existing && payload.expectedUpdatedAt && payload.expectedUpdatedAt !== existing.updatedAt) {
        throw mailError(409, 'Ce brouillon a été modifié dans une autre fenêtre ou application. Rechargez-le ou enregistrez une copie.');
      }
      if (existing && (await getAccess(existing.id, userId, tx)).userDeletedAt) throw mailError(409, 'Restaurez le brouillon depuis la corbeille avant de le modifier.');
      const directory = await listTeams(userId, tx);
      if (teamIds.some(id => !directory.some(team => team.id === id))) throw mailError(400, 'Une équipe sélectionnée est introuvable.');
      const recipients = new Map(direct.map(id => [id, { type: 'to', team: false }]));
      for (const id of cc) if (!recipients.has(id)) recipients.set(id, { type: 'cc', team: false });
      for (const id of bcc) if (!recipients.has(id)) recipients.set(id, { type: 'bcc', team: false });
      for (const team of directory.filter(team => teamIds.includes(team.id))) {
        for (const member of team.members) if (member.userId && !recipients.has(member.userId)) recipients.set(member.userId, { type: 'to', team: true });
      }
      if (payload.sendToAll) {
        if (!['admin', 'dirigeant'].includes(sender.role)) throw mailError(403, 'L’envoi à tous n’est pas autorisé pour ce profil.');
        for (const user of await tx.all("SELECT id FROM users WHERE deactivatedAt = '' AND id <> ?", [Number(userId)])) {
          recipients.set(Number(user.id), { type: 'to', team: false });
        }
      }
      for (const id of recipients.keys()) {
        if (!(await tx.get("SELECT id FROM users WHERE id = ? AND deactivatedAt = ''", [id]))) {
          throw mailError(400, 'Un destinataire est désactivé ou introuvable. Actualisez la liste.');
        }
      }
      if (status === 'sent' && !recipients.size) throw mailError(400, 'Aucun destinataire actif. Choisissez un employé ou une équipe avec un compte lié.');
      const projectId = Number(payload.projectId || 0) || null;
      if (projectId && !(await tx.get('SELECT id FROM projects WHERE id = ?', [projectId]))) throw mailError(400, 'Projet associé introuvable.');
      const previousTime = existing ? Date.parse(existing.updatedAt) : 0;
      const now = new Date(Math.max(Date.now(), Number.isFinite(previousTime) ? previousTime + 1 : 0)).toISOString();
      const conversationId = existing ? Number(existing.id) : await nextId(tx, 'internal_mail_conversations');
      let messageId;
      if (existing) {
        messageId = Number((await tx.get('SELECT id FROM internal_mail_messages WHERE conversationId = ? ORDER BY id ASC LIMIT 1', [conversationId])).id);
        await tx.run(`UPDATE internal_mail_conversations SET subject = ?, category = ?, priority = ?, status = ?,
          projectId = ?, updatedAt = ?, sendToAll = ?, draftKey = ?, sendFingerprint = ? WHERE id = ?`,
        [subject, String(payload.category || 'general'), String(payload.priority || 'normal'), status, projectId, now, payload.sendToAll ? 1 : 0, draftKey || existing.draftKey, status === 'sent' ? fingerprint : '', conversationId]);
        await tx.run('UPDATE internal_mail_messages SET body = ?, createdAt = ? WHERE id = ?', [body, now, messageId]);
        await tx.run('DELETE FROM internal_mail_recipients WHERE conversationId = ?', [conversationId]);
        await tx.run('DELETE FROM internal_mail_attachments WHERE messageId = ?', [messageId]);
        await tx.run('DELETE FROM internal_mail_conversation_teams WHERE conversationId = ?', [conversationId]);
      } else {
        messageId = await nextId(tx, 'internal_mail_messages');
        await tx.run(`INSERT INTO internal_mail_conversations
          (id, subject, category, priority, status, projectId, incidentZone, incidentSite, incidentLevel, createdBy, createdAt, updatedAt, deletedAt, draftKey, sendToAll, sendFingerprint)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)`,
        [conversationId, subject, String(payload.category || 'general'), String(payload.priority || 'normal'), status, projectId,
          String(payload.incidentZone || ''), String(payload.incidentSite || ''), String(payload.incidentLevel || ''), Number(userId), now, now, draftKey, payload.sendToAll ? 1 : 0, status === 'sent' ? fingerprint : '']);
        await tx.run('INSERT INTO internal_mail_messages (id, conversationId, senderId, body, createdAt) VALUES (?, ?, ?, ?, ?)', [messageId, conversationId, Number(userId), body, now]);
      }
      await writeRecipients(tx, conversationId, messageId, recipients, now);
      await writeAttachments(tx, messageId, attachments, now);
      for (const teamId of teamIds) await tx.run('INSERT INTO internal_mail_conversation_teams (conversationId, teamId) VALUES (?, ?)', [conversationId, teamId]);
      return { id: conversationId, messageId, recipientIds: [...recipients.keys()], updatedAt: now };
    });
    if (status === 'sent' && !result.alreadySent) notify(userId, result.recipientIds, result.id, result.messageId, subject, body);
    return { id: result.id, updatedAt: result.updatedAt, alreadySent: Boolean(result.alreadySent), message: status === 'draft' ? 'Brouillon enregistré' : 'Message envoyé' };
  }

  async function reply(conversationId, userId, payload) {
    const conversation = await getAccess(conversationId, userId);
    if (!conversation || conversation.status !== 'sent' || conversation.userDeletedAt) throw mailError(404, 'Conversation introuvable ou inaccessible.');
    const body = String(payload.body || '').trim();
    const attachments = prepareAttachments(payload.attachments);
    if (!body && !attachments.length) throw mailError(400, 'Le message ou une pièce jointe est obligatoire.');
    const associations = await db.all('SELECT teamId FROM internal_mail_conversation_teams WHERE conversationId = ?', [Number(conversationId)]);
    const latest = await db.get('SELECT id, senderId FROM internal_mail_messages WHERE conversationId = ? ORDER BY createdAt DESC, id DESC LIMIT 1', [Number(conversationId)]);
    const mode = payload.mode === 'reply' ? 'reply' : 'reply-all';
    if (mode === 'reply' && associations.length) {
      let recipientId = Number(latest.senderId);
      if (recipientId === Number(userId)) {
        recipientId = Number((await db.get("SELECT userId FROM internal_mail_recipients WHERE messageId = ? AND userId <> ? AND recipientType <> 'bcc' ORDER BY id ASC LIMIT 1", [latest.id, Number(userId)]))?.userId || 0);
      }
      if (!recipientId) throw mailError(400, 'Aucun destinataire pour cette réponse privée.');
      const result = await save(userId, {
        status: 'sent', subject: /^re:/i.test(conversation.subject) ? conversation.subject : 'Re: ' + conversation.subject,
        body, attachments, recipientIds: [recipientId], category: conversation.category, priority: conversation.priority,
      });
      return { conversationId: result.id, message: 'Réponse privée envoyée' };
    }
    const result = await change(async tx => {
      if (!(await getAccess(conversationId, userId, tx))) throw mailError(404, 'Conversation introuvable ou inaccessible.');
      const teams = await listTeams(userId, tx);
      const recipients = new Map();
      const previous = await tx.all("SELECT userId, isTeamRecipient FROM internal_mail_recipients WHERE messageId = ? AND LOWER(recipientType) <> 'bcc'", [Number(latest.id)]);
      const participants = await tx.all('SELECT DISTINCT senderId AS userId FROM internal_mail_messages WHERE conversationId = ?', [Number(conversationId)]);
      if (mode === 'reply') {
        const target = Number(latest.senderId) === Number(userId) ? Number(previous.find(row => Number(row.userId) !== Number(userId))?.userId || 0) : Number(latest.senderId);
        if (target) recipients.set(target, { type: 'to', team: false });
      } else {
        for (const member of previous) if (!Number(member.isTeamRecipient)) recipients.set(Number(member.userId), { type: 'to', team: false });
        for (const participant of participants) recipients.set(Number(participant.userId), { type: 'to', team: false });
        for (const team of teams.filter(team => associations.some(item => Number(item.teamId) === team.id))) {
          for (const member of team.members) if (member.userId && !recipients.has(member.userId)) recipients.set(member.userId, { type: 'to', team: true });
        }
      }
      recipients.delete(Number(userId));
      for (const id of recipients.keys()) if (!(await tx.get("SELECT id FROM users WHERE id = ? AND deactivatedAt = ''", [id]))) recipients.delete(id);
      if (!recipients.size) throw mailError(400, 'Aucun destinataire actif dans cette conversation.');
      const now = new Date().toISOString();
      const messageId = await nextId(tx, 'internal_mail_messages');
      await tx.run('INSERT INTO internal_mail_messages (id, conversationId, senderId, body, createdAt) VALUES (?, ?, ?, ?, ?)', [messageId, Number(conversationId), Number(userId), body, now]);
      await writeRecipients(tx, Number(conversationId), messageId, recipients, now);
      await writeAttachments(tx, messageId, attachments, now);
      await tx.run('UPDATE internal_mail_conversations SET updatedAt = ? WHERE id = ?', [now, Number(conversationId)]);
      return { messageId, recipientIds: [...recipients.keys()] };
    });
    notify(userId, result.recipientIds, Number(conversationId), result.messageId, conversation.subject, body);
    return { conversationId: Number(conversationId), message: 'Réponse envoyée' };
  }

  async function update(conversationId, userId, payload) {
    return change(async tx => {
      const conversation = await getAccess(conversationId, userId, tx);
      if (!conversation) throw mailError(404, 'Conversation introuvable ou inaccessible.');
      const action = String(payload.action || '');
      let restoredFolder = '';
      if (['read', 'unread'].includes(action)) {
        const isRead = action === 'read' ? 1 : 0;
        await tx.run('UPDATE internal_mail_recipients SET isRead = ? WHERE conversationId = ? AND userId = ?', [isRead, Number(conversationId), Number(userId)]);
        await setOrganization(tx, userId, conversationId, { isRead });
      } else if (['star', 'unstar'].includes(action)) {
        await setOrganization(tx, userId, conversationId, { isStarred: action === 'star' ? 1 : 0 });
      } else if (['important', 'unimportant'].includes(action)) {
        await setOrganization(tx, userId, conversationId, { isImportant: action === 'important' ? 1 : 0 });
      } else if (action === 'copy-to-folder' || action === 'remove-from-folder') {
        const category = action === 'remove-from-folder' ? '' : String(payload.category || '');
        if (category && !['incident', 'general', 'project', 'meeting', 'administration'].includes(category)) throw mailError(400, 'Dossier de classement invalide.');
        await setOrganization(tx, userId, conversationId, { folderCategory: category });
      } else if (action === 'trash') {
        const fallback = conversation.status === 'draft' ? 'drafts' : Number(conversation.createdBy) === Number(userId) ? 'sent' : 'inbox';
        const source = ['inbox', 'sent'].includes(payload.sourceFolder) && conversation.status !== 'draft' ? payload.sourceFolder : fallback;
        if (!conversation.userDeletedAt) await setOrganization(tx, userId, conversationId, { deletedAt: new Date().toISOString(), restoreFolder: source });
      } else if (action === 'restore') {
        restoredFolder = conversation.restoreFolder || (conversation.status === 'draft' ? 'drafts' : Number(conversation.createdBy) === Number(userId) ? 'sent' : 'inbox');
        await setOrganization(tx, userId, conversationId, { deletedAt: '' });
      } else throw mailError(400, 'Action de messagerie inconnue.');
      return { message: 'Conversation mise à jour', restoredFolder };
    });
  }

  return { ensureSchema, getUser, listTeams, saveTeam, getAccess, listConversations, readConversation, save, reply, update };
}

module.exports = { createInternalMail };
