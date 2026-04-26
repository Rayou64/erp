const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

function formatTimestamp(date = new Date()) {
  const pad = value => String(value).padStart(2, '0');
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}-${pad(date.getHours())}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

async function pathExists(targetPath) {
  try {
    await fsp.access(targetPath, fs.constants.F_OK);
    return true;
  } catch (_error) {
    return false;
  }
}

async function ensureDir(dirPath) {
  await fsp.mkdir(dirPath, { recursive: true });
}

async function copyFileWithMeta(source, destination) {
  await ensureDir(path.dirname(destination));
  await fsp.copyFile(source, destination);

  try {
    const stats = await fsp.stat(source);
    await fsp.utimes(destination, stats.atime, stats.mtime);
  } catch (_error) {
    // Ignore metadata copy errors.
  }
}

async function copyDirectoryRecursive(sourceDir, destinationDir) {
  await ensureDir(destinationDir);
  const entries = await fsp.readdir(sourceDir, { withFileTypes: true });

  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    const destinationPath = path.join(destinationDir, entry.name);

    if (entry.isDirectory()) {
      await copyDirectoryRecursive(sourcePath, destinationPath);
    } else if (entry.isFile()) {
      await copyFileWithMeta(sourcePath, destinationPath);
    }
  }
}

async function listFilesRecursive(rootDir) {
  const results = [];

  async function walk(currentDir) {
    const entries = await fsp.readdir(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile()) {
        results.push(fullPath);
      }
    }
  }

  await walk(rootDir);
  return results;
}

async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', chunk => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

async function removeDirectorySafe(dirPath) {
  await fsp.rm(dirPath, { recursive: true, force: true });
}

async function applyRetentionPolicy(backupRoot, keepCount) {
  const entries = await fsp.readdir(backupRoot, { withFileTypes: true });
  const snapshots = entries
    .filter(entry => entry.isDirectory() && entry.name.startsWith('snapshot-'))
    .map(entry => entry.name)
    .sort();

  if (snapshots.length <= keepCount) {
    return [];
  }

  const toDelete = snapshots.slice(0, snapshots.length - keepCount);
  for (const name of toDelete) {
    await removeDirectorySafe(path.join(backupRoot, name));
  }

  return toDelete;
}

function getRuntimePaths(projectRoot) {
  const appDataDir = process.env.APP_DATA_DIR || projectRoot;
  const dbFile = process.env.DB_FILE || path.join(appDataDir, 'data.db');
  const archiveRoot = process.env.ARCHIVE_ROOT || path.join(appDataDir, 'archives');
  const backupRoot = process.env.BACKUP_DIR || path.join(appDataDir, 'backups');
  const keepCount = Number(process.env.BACKUP_KEEP || 20);

  return {
    appDataDir,
    dbFile,
    archiveRoot,
    backupRoot,
    keepCount: Number.isInteger(keepCount) && keepCount > 0 ? keepCount : 20,
  };
}

async function createBackup(projectRoot) {
  const runtime = getRuntimePaths(projectRoot);
  await ensureDir(runtime.backupRoot);

  const snapshotName = `snapshot-${formatTimestamp()}`;
  const snapshotDir = path.join(runtime.backupRoot, snapshotName);
  await ensureDir(snapshotDir);

  const copied = {
    db: false,
    archives: false,
  };

  if (await pathExists(runtime.dbFile)) {
    await copyFileWithMeta(runtime.dbFile, path.join(snapshotDir, 'data.db'));
    copied.db = true;
  }

  if (await pathExists(runtime.archiveRoot)) {
    await copyDirectoryRecursive(runtime.archiveRoot, path.join(snapshotDir, 'archives'));
    copied.archives = true;
  }

  const files = await listFilesRecursive(snapshotDir);
  const manifestFiles = [];
  let totalBytes = 0;

  for (const filePath of files) {
    const relativePath = path.relative(snapshotDir, filePath).replace(/\\/g, '/');
    const stats = await fsp.stat(filePath);
    const checksum = await sha256File(filePath);
    totalBytes += stats.size;
    manifestFiles.push({
      relativePath,
      size: stats.size,
      sha256: checksum,
      modifiedAt: stats.mtime.toISOString(),
    });
  }

  const manifest = {
    createdAt: new Date().toISOString(),
    snapshotName,
    snapshotDir,
    source: {
      appDataDir: runtime.appDataDir,
      dbFile: runtime.dbFile,
      archiveRoot: runtime.archiveRoot,
    },
    copied,
    fileCount: manifestFiles.length,
    totalBytes,
    files: manifestFiles,
  };

  const manifestPath = path.join(snapshotDir, 'manifest.json');
  await fsp.writeFile(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');

  const deletedSnapshots = await applyRetentionPolicy(runtime.backupRoot, runtime.keepCount);

  return {
    manifest,
    manifestPath,
    deletedSnapshots,
  };
}

async function getLatestSnapshotDir(backupRoot) {
  if (!(await pathExists(backupRoot))) {
    return null;
  }

  const entries = await fsp.readdir(backupRoot, { withFileTypes: true });
  const snapshots = entries
    .filter(entry => entry.isDirectory() && entry.name.startsWith('snapshot-'))
    .map(entry => entry.name)
    .sort();

  if (!snapshots.length) {
    return null;
  }

  return path.join(backupRoot, snapshots[snapshots.length - 1]);
}

async function verifySnapshot(snapshotDir) {
  const manifestPath = path.join(snapshotDir, 'manifest.json');
  if (!(await pathExists(manifestPath))) {
    throw new Error(`Manifest introuvable: ${manifestPath}`);
  }

  const rawManifest = await fsp.readFile(manifestPath, 'utf8');
  const manifest = JSON.parse(rawManifest);
  const mismatches = [];

  for (const fileEntry of manifest.files || []) {
    const targetPath = path.join(snapshotDir, fileEntry.relativePath);
    if (!(await pathExists(targetPath))) {
      mismatches.push({
        file: fileEntry.relativePath,
        reason: 'missing',
      });
      continue;
    }

    const stats = await fsp.stat(targetPath);
    const actualHash = await sha256File(targetPath);

    if (stats.size !== Number(fileEntry.size) || actualHash !== String(fileEntry.sha256 || '')) {
      mismatches.push({
        file: fileEntry.relativePath,
        reason: 'checksum-mismatch',
        expectedSize: Number(fileEntry.size),
        actualSize: stats.size,
      });
    }
  }

  return {
    snapshotDir,
    manifest,
    ok: mismatches.length === 0,
    mismatches,
  };
}

async function main() {
  const projectRoot = path.resolve(__dirname, '..');
  const args = new Set(process.argv.slice(2));
  const runtime = getRuntimePaths(projectRoot);

  if (args.has('--verify-latest')) {
    const latestSnapshotDir = await getLatestSnapshotDir(runtime.backupRoot);
    if (!latestSnapshotDir) {
      console.error(`Aucune sauvegarde trouvee dans ${runtime.backupRoot}`);
      process.exitCode = 1;
      return;
    }

    const result = await verifySnapshot(latestSnapshotDir);
    if (!result.ok) {
      console.error(`Sauvegarde invalide: ${latestSnapshotDir}`);
      for (const mismatch of result.mismatches) {
        console.error(` - ${mismatch.file}: ${mismatch.reason}`);
      }
      process.exitCode = 1;
      return;
    }

    console.log(`Sauvegarde verifiee: ${latestSnapshotDir}`);
    console.log(`Fichiers verifies: ${result.manifest.fileCount || 0}`);
    return;
  }

  const backup = await createBackup(projectRoot);
  console.log(`Sauvegarde creee: ${backup.manifest.snapshotDir}`);
  console.log(`Fichiers copies: ${backup.manifest.fileCount}`);
  console.log(`Taille totale: ${backup.manifest.totalBytes} octets`);
  console.log(`Manifest: ${backup.manifestPath}`);

  if (backup.deletedSnapshots.length) {
    console.log(`Anciennes sauvegardes supprimees (${backup.deletedSnapshots.length}):`);
    for (const snapshot of backup.deletedSnapshots) {
      console.log(` - ${snapshot}`);
    }
  }
}

main().catch(error => {
  console.error('Erreur sauvegarde:', error.message || error);
  process.exitCode = 1;
});
