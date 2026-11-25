/* Remmin Web
 * Notes, folders, calendar and drawing. All data is stored locally in the browser
 * (IndexedDB, with room for much more than the ~5 MB of local storage), so no server
 * or account is required. GitHub / a server can keep a synced copy.
 */

/* ---------------- storage: IndexedDB with a copy in memory ----------------
 * The app reads and writes through this small layer (synchronously, from memory);
 * every change is written to IndexedDB right after. Data that was still in local
 * storage (older versions) is moved over once. If IndexedDB can't be used (some
 * private windows), local storage is used as before. */
const RemminStore = (() => {
    const DB_NAME = 'remmin';
    const STORE = 'kv';
    const MIGRATED = '__migratedFromLocalStorage';
    // the app used to be called Remindate: its data is moved over once (the old copy is kept)
    const OLD_DB_NAME = 'remindate';
    const OLD_PREFIX = 'remindate.';
    const renameKey = (k) => (k.startsWith(OLD_PREFIX) ? `remmin.${k.slice(OLD_PREFIX.length)}` : k);
    const cache = new Map();
    const pending = new Map(); // key -> string (write) or null (delete)
    let db = null;
    let useLocal = false;
    let flushQueued = false;
    let onError = () => {};
    let onChange = () => {};

    const localKeys = () => {
        const keys = [];
        try {
            for (let i = 0; i < localStorage.length; i++) {
                const k = localStorage.key(i);
                if (k && (k.startsWith('remmin.') || k.startsWith(OLD_PREFIX))) keys.push(k);
            }
        } catch (_) { /* no local storage */ }
        return keys;
    };
    const loadFromLocal = () => {
        useLocal = true;
        const keys = localKeys();
        // new names first, then old (Remindate) names that have no new copy yet
        keys.filter((k) => !k.startsWith(OLD_PREFIX)).forEach((k) => cache.set(k, localStorage.getItem(k)));
        keys.filter((k) => k.startsWith(OLD_PREFIX)).forEach((k) => {
            const nk = renameKey(k);
            if (cache.has(nk)) return;
            const v = localStorage.getItem(k);
            cache.set(nk, v);
            try { localStorage.setItem(nk, v); localStorage.removeItem(k); } catch (_) { /* keep the old one */ }
        });
    };
    // everything in the old "remindate" IndexedDB (if there is one), without creating it
    const readOldDb = () => new Promise((resolve) => {
        const found = new Map();
        let req;
        try { req = indexedDB.open(OLD_DB_NAME); } catch (_) { resolve(found); return; }
        req.onupgradeneeded = () => { req.transaction.abort(); }; // it didn't exist: don't make it
        req.onerror = () => resolve(found);
        req.onblocked = () => resolve(found);
        req.onsuccess = () => {
            const old = req.result;
            if (!old.objectStoreNames.contains(STORE)) { old.close(); resolve(found); return; }
            const store = old.transaction(STORE, 'readonly').objectStore(STORE);
            Promise.all([reqDone(store.getAllKeys()), reqDone(store.getAll())])
                .then(([keys, values]) => { keys.forEach((k, i) => found.set(String(k), values[i])); })
                .catch(() => {})
                .finally(() => { old.close(); resolve(found); });
        };
        setTimeout(() => resolve(found), 4000);
    });
    const reqDone = (req) => new Promise((resolve, reject) => {
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
    const txDone = (tx) => new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
        tx.onabort = () => reject(tx.error || new Error('aborted'));
    });

    async function open() {
        if (!('indexedDB' in window)) throw new Error('no IndexedDB');
        const opened = new Promise((resolve, reject) => {
            const req = indexedDB.open(DB_NAME, 1);
            req.onupgradeneeded = () => { req.result.createObjectStore(STORE); };
            req.onsuccess = () => resolve(req.result);
            req.onerror = () => reject(req.error);
            req.onblocked = () => reject(new Error('blocked'));
        });
        const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 4000));
        db = await Promise.race([opened, timeout]);
        db.onversionchange = () => { db.close(); db = null; };

        const tx = db.transaction(STORE, 'readonly');
        const store = tx.objectStore(STORE);
        const [keys, values] = await Promise.all([reqDone(store.getAllKeys()), reqDone(store.getAll())]);
        keys.forEach((k, i) => cache.set(String(k), values[i]));

        if (!cache.has(MIGRATED)) {
            // move what older versions kept (the Remindate database, or local storage) in here (once)
            const oldDb = await readOldDb();
            const keysToMove = localKeys();
            const moved = new Map();
            oldDb.forEach((v, k) => { if (k !== MIGRATED) moved.set(renameKey(k), v); });
            keysToMove.forEach((k) => {
                const nk = renameKey(k);
                const v = localStorage.getItem(k);
                if (v !== null && !moved.has(nk)) moved.set(nk, v);
            });
            const wtx = db.transaction(STORE, 'readwrite');
            const ws = wtx.objectStore(STORE);
            moved.forEach((v, k) => {
                if (cache.has(k)) return;
                cache.set(k, v);
                ws.put(v, k);
            });
            ws.put('1', MIGRATED);
            cache.set(MIGRATED, '1');
            await txDone(wtx);
            // it is safely in IndexedDB now: free the small local storage
            keysToMove.forEach((k) => { try { localStorage.removeItem(k); } catch (_) { /* ignore */ } });
        }
        // ask the browser not to clear this data when the disk gets full
        try { if (navigator.storage && navigator.storage.persist) navigator.storage.persist(); } catch (_) { /* ignore */ }
    }

    function flush() {
        flushQueued = false;
        if (!pending.size) return;
        const batch = new Map(pending);
        pending.clear();
        if (useLocal || !db) {
            batch.forEach((v, k) => {
                try { if (v === null) localStorage.removeItem(k); else localStorage.setItem(k, v); } catch (err) { onError(err); }
            });
            return;
        }
        try {
            const tx = db.transaction(STORE, 'readwrite');
            const store = tx.objectStore(STORE);
            batch.forEach((v, k) => { if (v === null) store.delete(k); else store.put(v, k); });
            txDone(tx).catch((err) => {
                // put the failed changes back so a later save tries again
                batch.forEach((v, k) => { if (!pending.has(k)) pending.set(k, v); });
                onError(err);
            });
        } catch (err) {
            batch.forEach((v, k) => { if (!pending.has(k)) pending.set(k, v); });
            onError(err);
        }
    }
    const queue = () => {
        if (flushQueued) return;
        flushQueued = true;
        queueMicrotask(flush);
    };

    const ready = open().catch((err) => {
        console.warn('IndexedDB not available, using local storage:', err);
        db = null;
        cache.clear();
        loadFromLocal();
    });

    return {
        ready,
        get: (key) => (cache.has(key) ? cache.get(key) : null),
        set(key, value) { cache.set(key, value); pending.set(key, value); queue(); onChange(); },
        remove(key) { cache.delete(key); pending.set(key, null); queue(); onChange(); },
        // roughly how many bytes Remmin keeps on this device
        bytes() { let n = 0; cache.forEach((v, k) => { n += k.length + (v ? v.length : 0); }); return n; },
        onChange(fn) { onChange = fn; },
        flush,
        onError(fn) { onError = fn; },
        get usingIndexedDB() { return !!db && !useLocal; }
    };
})();

RemminStore.ready.then(() => {
    'use strict';

    const STORAGE = {
        notes: 'remmin.notes.v1',
        privatePass: 'remmin.privatePass.v1',
        folders: 'remmin.folders.v1',
        prefs: 'remmin.prefs.v1',
        alarms: 'remmin.alarms.v1',
        deleted: 'remmin.deleted.v1',
        trash: 'remmin.trash.v1',
        trashGone: 'remmin.trashGone.v1',
        media: 'remmin.media.v1',
        mediaOrig: 'remmin.mediaOrig.v1',
        updatedAt: 'remmin.updatedAt.v1',
        github: 'remmin.github.v1',
        server: 'remmin.server.v1'
    };


    // [color, name] in display order
    // colours in the order of the light spectrum (red → violet), then magenta / pink, then the greys
    const PALETTE_NAMED = [
        ['#fca5a5', 'Light red'], ['#e11d1d', 'Bright red'], ['#991b1b', 'Dark red'], ['#150404', 'Very dark red'],
        ['#fdba74', 'Light orange'], ['#f97316', 'Bright orange'], ['#c2410c', 'Dark orange'], ['#170a02', 'Very dark orange'],
        ['#facc15', 'Yellow'], ['#151202', 'Very dark yellow'],
        ['#86efac', 'Light green'], ['#22c55e', 'Bright green'], ['#166534', 'Dark green'], ['#04130a', 'Very dark green'],
        ['#14b8a6', 'Turquoise'], ['#22d3ee', 'Cyan'],
        ['#3b82f6', 'Blue'], ['#1e3a8a', 'Dark blue'], ['#040917', 'Very dark blue'], ['#6366f1', 'Blue-purple'],
        ['#c4b5fd', 'Light purple'], ['#8b5cf6', 'Purple'], ['#0e0518', 'Very dark purple'], ['#d946ef', 'Magenta'], ['#f472b6', 'Pink'],
        ['#000000', 'Black'], ['#404040', 'Dark grey'], ['#bfbfbf', 'Light grey'], ['#ffffff', 'White'] // dark grey = 75% black, light grey = 25% black
    ];
    // the first "very dark" colours were twice as light: drawings that use them get the new ones
    const OLD_VERY_DARK = { '#2b0808': '#150404', '#2e1505': '#170a02', '#2b2405': '#151202', '#082614': '#04130a', '#08132e': '#040917', '#1c0b30': '#0e0518' };
    const OLD_BRIGHT_RED = '#ef4444'; // the earlier, paler "Bright red"
    const NEW_BRIGHT_RED = '#e11d1d';
    const PALETTE = PALETTE_NAMED.map(([c]) => c);
    const DEFAULT_PEN_COLOR = '#bfbfbf';   // light grey
    const DEFAULT_CANVAS_BG = '#040917';   // very dark blue

    const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'];
    const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

    const state = {
        notes: [],
        folders: [],
        prefs: {
            sidebarWidth: 260,
            sidebarSide: 'left',
            sidebarCollapsed: false,
            activeTab: 'note',
            expandedFolders: [],
            scheduleCollapsed: true,
            mode: 'text'
        },
        alarms: {},
        // Deleted note/folder ids with the time they were deleted, so a sync
        // knows an item was removed instead of "missing on this device".
        deleted: { notes: {}, folders: {} },
        // Time of the last local change (ISO); compared with GitHub's version.
        updatedAt: null,
        // Recently deleted notes on this device: [{ note, deletedAt }]
        trash: [],
        // video files: id → data:video text (stored once, used by "media:<id>" in the notes)
        media: {},
        currentNoteId: null,
        currentFolderId: null,
        calendarCursor: new Date(),
        calendarDay: null,
        draw: {
            tool: 'pen',
            color: DEFAULT_PEN_COLOR,
            size: 6,
            strokes: [],
            drawing: false,
            current: null
        }
    };
//test2
    const $ = (id) => document.getElementById(id);
    const isMobile = () => window.matchMedia('(max-width: 760px)').matches;

    /* ---------------- opslag ---------------- */

    function readJSON(key, fallback) {
        try {
            const raw = RemminStore.get(key);
            return raw ? JSON.parse(raw) : fallback;
        } catch (err) {
            console.warn('Could not read data:', key, err);
            return fallback;
        }
    }

    function writeJSON(key, value) {
        try {
            RemminStore.set(key, JSON.stringify(value));
        } catch (err) {
            reportSaveError(err);
        }
    }
    let lastSaveError = 0;
    function reportSaveError(err) {
        console.error('Save failed:', err);
        if (Date.now() - lastSaveError < 10000) return; // don't repeat the message on every keystroke
        lastSaveError = Date.now();
        toast('Save failed: your browser storage is full or unavailable.');
    }
    RemminStore.onError(reportSaveError);

    function load() {
        state.media = validMedia(readJSON(STORAGE.media, {}));
        // originals of compressed videos (only on this device, for "Undo compression"; kept 7 days)
        state.mediaOrig = {};
        const origs = readJSON(STORAGE.mediaOrig, {});
        if (origs && typeof origs === 'object') Object.entries(origs).forEach(([k, o]) => {
            if (state.media[k] && o && typeof o.data === 'string' && Date.now() - (o.at || 0) < 7 * 86400000) state.mediaOrig[k] = o;
        });
        state.notes = readJSON(STORAGE.notes, []);
        state.folders = readJSON(STORAGE.folders, []);
        state.prefs = Object.assign(state.prefs, readJSON(STORAGE.prefs, {}));
        state.alarms = readJSON(STORAGE.alarms, {});

        if (!Array.isArray(state.notes)) state.notes = [];
        if (!Array.isArray(state.folders)) state.folders = [];
        if (!Array.isArray(state.prefs.expandedFolders)) state.prefs.expandedFolders = [];
        if (!state.alarms || typeof state.alarms !== 'object') state.alarms = {};
        state.deleted = normalizeDeleted(readJSON(STORAGE.deleted, null));
        state.updatedAt = readJSON(STORAGE.updatedAt, null);
        state.trash = readJSON(STORAGE.trash, []);
        if (!Array.isArray(state.trash)) state.trash = [];
        state.trashGone = readJSON(STORAGE.trashGone, {});
        if (!state.trashGone || typeof state.trashGone !== 'object') state.trashGone = {};

        state.notes.forEach(migrateNote);
        state.folders.forEach(migrateFolder);
        state.trash.forEach((t) => { if (t && t.note) migrateNoteMedia(t.note); });
        // forget video files that no note (or the trash) uses any more
        const kept = pickMedia(state.media, state.notes, state.trash);
        if (Object.keys(kept).length !== Object.keys(state.media).length) { state.media = kept; scheduleSaveMedia(); }
    }

    /* ---------------- video files (media) ----------------
     * A video file from your disk is stored once in state.media (id → data:video text) and synced in remmin.json.
     * Text, Collage and Draw only keep "media:<id>", so the same video in several places takes no extra space.
     * (Function declarations only: these are used while the notes are loaded.) */
    function mediaHash(str) {
        let h1 = 0x811c9dc5, h2 = 5381;
        for (let i = 0; i < str.length; i++) {
            const c = str.charCodeAt(i);
            h1 = Math.imul(h1 ^ c, 16777619);
            h2 = (Math.imul(h2, 33) + c) | 0;
        }
        return (h1 >>> 0).toString(36) + (h2 >>> 0).toString(36) + str.length.toString(36);
    }
    function mediaIdFor(dataUrl) {
        const id = `v${mediaHash(dataUrl)}`;
        if (!state.media[id]) { state.media[id] = dataUrl; scheduleSaveMedia(); }
        return `media:${id}`;
    }
    function isMediaRef(v) { return typeof v === 'string' && /^media:[a-z0-9]{4,40}$/.test(v); }
    function mediaData(ref) {
        if (!isMediaRef(ref)) return '';
        const id = ref.slice(6);
        if (!state.media[id] && state.mediaGone && state.mediaGone[id]) { // used again (undo): keep it after all
            state.media[id] = state.mediaGone[id];
            delete state.mediaGone[id];
            scheduleSaveMedia();
        }
        return state.media[id] || '';
    }
    // a little after every save: videos no note uses any more are removed from the storage
    // (kept in memory for this visit, so undo can still bring them back)
    function scheduleMediaGc() {
        clearTimeout(state.mediaGcTimer);
        state.mediaGcTimer = setTimeout(() => {
            state.mediaGone = state.mediaGone || {};
            const all = { ...state.mediaGone, ...state.media };
            const kept = pickMedia(all, state.notes, state.trash);
            const before = Object.keys(state.media).join();
            Object.keys(all).forEach((k) => {
                if (kept[k]) { state.media[k] = all[k]; delete state.mediaGone[k]; } else { state.mediaGone[k] = all[k]; delete state.media[k]; }
            });
            if (Object.keys(state.media).join() !== before) {
                scheduleSaveMedia();
                if (typeof updateStorageMeters === 'function') setTimeout(updateStorageMeters, 800);
            }
        }, 2500);
    }
    function scheduleSaveMedia() {
        clearTimeout(state.mediaTimer);
        state.mediaTimer = setTimeout(() => writeJSON(STORAGE.media, state.media), 400);
    }
    function validMedia(m) {
        const out = {};
        if (!m || typeof m !== 'object' || Array.isArray(m)) return out;
        Object.entries(m).forEach(([k, v]) => {
            if (/^[a-z0-9]{4,40}$/.test(k) && typeof v === 'string' && /^data:(?:video|audio)\/[\w.+-]+(?:;[\w=.+-]+)*;base64,/i.test(v.slice(0, 120))) out[k] = v;
        });
        return out;
    }
    // video data written straight into a note (the first version did that): move it to the media store
    function migrateNoteMedia(value) {
        if (Array.isArray(value)) { value.forEach((v, i) => { if (typeof v === 'string') value[i] = migrateMediaString(v); else if (v && typeof v === 'object') migrateNoteMedia(v); }); return; }
        if (!value || typeof value !== 'object') return;
        Object.keys(value).forEach((k) => {
            const v = value[k];
            if (typeof v === 'string') value[k] = migrateMediaString(v);
            else if (v && typeof v === 'object') migrateNoteMedia(v);
        });
    }
    function migrateMediaString(v) {
        if (v.startsWith('data:video/') || v.startsWith('data:audio/')) return mediaIdFor(v);
        if (v.length > 200 && v.includes('data-video="data:video/')) v = v.replace(/data-video="(data:video\/[^"]+)"/g, (m, d) => `data-video="${mediaIdFor(d)}"`);
        if (v.length > 200 && v.includes('data-voice="data:audio/')) v = v.replace(/data-voice="(data:audio\/[^"]+)"/g, (m, d) => `data-voice="${mediaIdFor(d)}"`);
        return v;
    }
    // the media used by these notes and trash; only those are kept and synced
    function pickMedia(media, notes, trash) {
        const used = new Set();
        const re = /media:([a-z0-9]{4,40})/g;
        const text = JSON.stringify([notes || [], (trash || []).map((t) => t && t.note)]);
        let m;
        while ((m = re.exec(text))) used.add(m[1]);
        const out = {};
        used.forEach((id) => { if (media && media[id]) out[id] = media[id]; });
        return out;
    }

    function normalizeDeleted(d) {
        return {
            notes: (d && typeof d.notes === 'object' && d.notes) || {},
            folders: (d && typeof d.folders === 'object' && d.folders) || {}
        };
    }

    function migrateFolder(folder) {
        folder.id = folder.id || uid();
        folder.name = folder.name || 'Folder';
        folder.modifiedAt = folder.modifiedAt || '2000-01-01T00:00:00.000Z';
        folder.parentId = folder.parentId || null; // null = top level
        if (folder.bgColor && OLD_BG_COLORS[folder.bgColor]) folder.bgColor = OLD_BG_COLORS[folder.bgColor];
    }

    // The folder a folder sits in (null = top level). A missing parent or a loop counts as top level.
    function parentOf(folder) {
        const pid = folder.parentId;
        if (!pid) return null;
        const seen = new Set([folder.id]);
        let cur = state.folders.find((f) => f.id === pid);
        if (!cur) return null;
        let walk = cur;
        while (walk) {
            if (seen.has(walk.id)) return null; // loop (e.g. after syncing two devices): show at top level
            seen.add(walk.id);
            walk = walk.parentId ? state.folders.find((f) => f.id === walk.parentId) : null;
        }
        return cur.id;
    }
    const childFolders = (parentId) => sortedFolders().filter((f) => parentOf(f) === (parentId || null));
    // true if folder `id` is `ancestorId` or somewhere inside it
    function isInside(id, ancestorId) {
        let f = state.folders.find((x) => x.id === id);
        const seen = new Set();
        while (f && !seen.has(f.id)) {
            if (f.id === ancestorId) return true;
            seen.add(f.id);
            const pid = parentOf(f);
            f = pid ? state.folders.find((x) => x.id === pid) : null;
        }
        return false;
    }
    // folders depth-first, with how deep they are
    function folderTree(parentId = null, depth = 0, out = []) {
        childFolders(parentId).forEach((f) => { out.push({ folder: f, depth }); folderTree(f.id, depth + 1, out); });
        return out;
    }
    function folderPath(id) {
        const names = [];
        let f = state.folders.find((x) => x.id === id);
        const seen = new Set();
        while (f && !seen.has(f.id)) {
            seen.add(f.id);
            names.unshift(f.name);
            const pid = parentOf(f);
            f = pid ? state.folders.find((x) => x.id === pid) : null;
        }
        return names.join(' / ');
    }
    function expandFolderPath(id) {
        let f = state.folders.find((x) => x.id === id);
        const seen = new Set();
        while (f && !seen.has(f.id)) {
            seen.add(f.id);
            if (!state.prefs.expandedFolders.includes(f.id)) state.prefs.expandedFolders.push(f.id);
            const pid = parentOf(f);
            f = pid ? state.folders.find((x) => x.id === pid) : null;
        }
    }

    // the pastel colours became 10% less white: update text that used the old ones
    const OLD_BG_COLORS = { '#ff9494': '#372020', '#ffca8c': '#372b20', '#fcff9c': '#373420', '#b5ffa6': '#203722',
        '#86b4ff': '#202937', '#c59cff': '#2b2037', '#2b2b2b': '#000000' };
    const OLD_PASTELS = [[/#ffadad|rgb\(255,\s*173,\s*173\)/gi, '#ff9494'], [/#ffd6a5|rgb\(255,\s*214,\s*165\)/gi, '#ffca8c'], [/#fdffb6|rgb\(253,\s*255,\s*182\)/gi, '#fcff9c'], [/#caffbf|rgb\(202,\s*255,\s*191\)/gi, '#b5ffa6'], [/#a0c4ff|rgb\(160,\s*196,\s*255\)/gi, '#86b4ff'], [/#d4b5ff|rgb\(212,\s*181,\s*255\)/gi, '#c59cff']];
    function migrateNote(note) {
        migrateNoteMedia(note);
        note.id = note.id || uid();
        note.title = note.title || '';
        note.content = note.content || '';
        note.createdAt = note.createdAt || new Date().toISOString();
        note.modifiedAt = note.modifiedAt || note.createdAt;
        note.scheduledAt = note.scheduledAt || null;
        note.folderId = note.folderId || null;
        if (note.color === OLD_BRIGHT_RED) note.color = NEW_BRIGHT_RED;
        if (typeof note.html === 'string' && /<img[^>]*width:\s*(?:12|24|48)%/i.test(note.html)) {
            // picture sizes are 1/8, 1/4, 1/2 and full width (older versions used 12%, 24% and 48%)
            note.html = note.html.replace(/(<img[^>]*width:\s*)(12|24|48)%/gi, (m, a, v) => `${a}${{ 12: 12.5, 24: 25, 48: 50 }[v]}%`);
        }
        // no spaces between pictures that stand next to each other
        if (typeof note.html === 'string' && /(<img[^>]*>)(?:\s|&nbsp;)+(?=<img)/i.test(note.html)) {
            note.html = note.html.replace(/(<img[^>]*>)(?:\s|&nbsp;)+(?=<img)/gi, '$1');
        }
        if (note.bgColor && OLD_BG_COLORS[note.bgColor]) note.bgColor = OLD_BG_COLORS[note.bgColor];
        if (typeof note.html === 'string' && /#(ffadad|ffd6a5|fdffb6|caffbf|a0c4ff|d4b5ff)|rgb\(\s*(255,\s*173|255,\s*214|253,\s*255|202,\s*255|160,\s*196|212,\s*181)/i.test(note.html)) {
            OLD_PASTELS.forEach(([re, color]) => { note.html = note.html.replace(re, color); });
        }
        note.alarmEnabled = !!note.alarmEnabled;
        note.emailEnabled = !!note.emailEnabled;
        note.emailTo = typeof note.emailTo === 'string' ? note.emailTo : '';
        note.color = /^#[0-9a-f]{6}$/i.test(note.color || '') ? note.color : '';
        note.alarmRepeatCount = Number(note.alarmRepeatCount) || 0;
        note.alarmSnoozeMinutes = Number(note.alarmSnoozeMinutes) || 5;
        note.repeatWeekdays = Array.isArray(note.repeatWeekdays) ? note.repeatWeekdays : [];
        if (!REPEAT_MODES.includes(note.repeatMode)) note.repeatMode = note.repeatWeekdays.length ? 'weekly' : 'none';
        note.repeatEndDate = note.repeatEndDate || '';
        note.canvasBackground = note.canvasBackground || DEFAULT_CANVAS_BG;
        note.strokes = Array.isArray(note.strokes) ? note.strokes : [];
        // the first, lighter "very dark" colours → the new ones
        if (OLD_VERY_DARK[note.canvasBackground]) note.canvasBackground = OLD_VERY_DARK[note.canvasBackground];
        // grids used to share Draw's background: grids with something on them keep that colour
        if (!note.gridBgs || typeof note.gridBgs !== 'object') {
            note.gridBgs = {};
            Object.entries(note.grids || {}).forEach(([k, cells]) => {
                if (cells && Object.keys(cells).length && note.canvasBackground) note.gridBgs[k] = note.canvasBackground;
            });
        } else Object.keys(note.gridBgs).forEach((k) => { if (OLD_VERY_DARK[note.gridBgs[k]]) note.gridBgs[k] = OLD_VERY_DARK[note.gridBgs[k]]; });
        note.strokes.forEach((st) => { if (st && OLD_VERY_DARK[st.color]) st.color = OLD_VERY_DARK[st.color]; });
        if (note.grids && typeof note.grids === 'object') {
            Object.values(note.grids).forEach((cells) => {
                if (cells && typeof cells === 'object') Object.keys(cells).forEach((k) => { if (OLD_VERY_DARK[cells[k]]) cells[k] = OLD_VERY_DARK[cells[k]]; });
            });
        }
        note.skipDates = Array.isArray(note.skipDates) ? note.skipDates : [];
        if (typeof note.html !== 'string') note.html = textToHtml(note.content);
        note.gridType = note.gridType || 'square';
        note.grids = note.grids && typeof note.grids === 'object' ? note.grids : {};
        note.private = !!note.private;
        // private notes show a 🔒 at the start of their title (like memos show 📝)
        if (note.private && !/^(?:📝\s*)?🔒/u.test((note.title || '').trim())) note.title = `🔒 ${(note.title || '').trim()}`.trim();
        if (note.private) note.title = note.title.replace(/^📝\s*🔒\s*/u, '🔒 📝 ');
    }

    let syncReady = false; // set once the sync code below has loaded
    // Every local change records "now" as the time of the latest local version
    function markChanged() {
        state.updatedAt = new Date().toISOString();
        writeJSON(STORAGE.updatedAt, state.updatedAt);
        writeJSON(STORAGE.deleted, state.deleted);
        if (syncReady && !githubBusy) updateGithubStatus();
    }
    const saveNotes = () => {
        writeJSON(STORAGE.notes, state.notes);
        scheduleMediaGc();
        markChanged();
        scheduleGithubSave();
    };
    const saveFolders = () => {
        writeJSON(STORAGE.folders, state.folders);
        markChanged();
        scheduleGithubSave();
    };
    const savePrefs = () => writeJSON(STORAGE.prefs, state.prefs);
    // Holiday choices and world clocks also go into remmin.json (the newest change wins)
    function settingsChanged() {
        state.prefs.settingsAt = new Date().toISOString();
        savePrefs();
        markChanged();
        scheduleGithubSave();
    }
    const ALARM_SOUND_KEY = 'remmin.alarmSound.v1';
    const EMAIL_ADDRESS_KEY = 'remmin.emailAddress.v1';
    function syncedSettings() {
        const p = state.prefs;
        const sound = RemminStore.get(ALARM_SOUND_KEY) || '';
        const email = RemminStore.get(EMAIL_ADDRESS_KEY) || '';
        if (!p.settingsAt && !Array.isArray(p.holidays) && !Array.isArray(p.clocks) && typeof p.broadcast !== 'string' &&
            !p.imgSize && !p.collageColors && !sound && !email) return null;
        return {
            holidays: Array.isArray(p.holidays) ? [...p.holidays] : null,
            clocks: Array.isArray(p.clocks) ? [...p.clocks] : null,
            ...(typeof p.broadcast === 'string' ? { broadcast: p.broadcast } : {}),
            ...(p.imgSize ? { imgSize: p.imgSize } : {}),
            ...(p.collageColors ? { collageColors: { ...p.collageColors } } : {}),
            email,
            alarmSound: sound, // '' = the standard sound
            at: p.settingsAt || null
        };
    }
    function validSettings(v) {
        if (!v || typeof v !== 'object') return null;
        const list = (a) => (Array.isArray(a) ? a.filter((x) => typeof x === 'string').slice(0, 100) : null);
        const out = { holidays: list(v.holidays), clocks: list(v.clocks), at: typeof v.at === 'string' ? v.at : null };
        if (typeof v.broadcast === 'string') out.broadcast = v.broadcast.slice(0, 300);
        if ([12, 24, 48, 12.5, 25, 50, 100].includes(Number(v.imgSize))) out.imgSize = ({ 12: 12.5, 24: 25, 48: 50 })[Number(v.imgSize)] || Number(v.imgSize);
        if (v.collageColors && typeof v.collageColors === 'object') {
            out.collageColors = {};
            ['sticky', 'frame', 'arrow'].forEach((k) => { if (/^#[0-9a-f]{6}$/i.test(v.collageColors[k] || '')) out.collageColors[k] = v.collageColors[k]; });
        }
        if (typeof v.email === 'string') out.email = v.email.slice(0, 200);
        if (typeof v.alarmSound === 'string' && (v.alarmSound === '' || /^data:audio\//.test(v.alarmSound))) out.alarmSound = v.alarmSound;
        return out.holidays || out.clocks || out.at || 'broadcast' in out ? out : null;
    }
    const saveAlarms = () => writeJSON(STORAGE.alarms, state.alarms);

    /* ---------------- hulpfuncties ---------------- */

    function uid() {
        if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
        return 'id-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 9);
    }

    function escapeHtml(text) {
        return String(text).replace(/[&<>"']/g, (c) => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }

    const pad = (n) => String(n).padStart(2, '0');

    function toLocalIso(date) {
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
            `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
    }

    function parseDate(value) {
        if (!value) return null;
        const d = new Date(value);
        return isNaN(d.getTime()) ? null : d;
    }

    function formatDateTime(value) {
        const d = parseDate(value);
        if (!d) return '';
        return `${d.getDate()} ${MONTHS[d.getMonth()].slice(0, 3)} ${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function formatTime(value) {
        const d = parseDate(value);
        if (!d) return '';
        return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function sameDay(a, b) {
        return !!a && !!b && a.getFullYear() === b.getFullYear() &&
            a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
    }

    function dayKey(d) {
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    }

    function keyToDate(key) {
        const [y, m, d] = key.split('-').map(Number);
        return new Date(y, m - 1, d);
    }

    /* ---------------- recurrence ----------------
     * scheduledAt = first occurrence (date + time).
     * repeatWeekdays = [0..6] (0 = Monday) on which the note repeats,
     * from the start date up to and including repeatEndDate (empty = no end).
     */
    // none | weekly (on repeatWeekdays) | monthlyDate (e.g. every 15th) |
    // monthlyWeekday (e.g. every 2nd Tuesday) | monthlyLastWeekday (e.g. last Friday) | yearly
    const REPEAT_MODES = ['none', 'weekly', 'monthlyDate', 'monthlyWeekday', 'monthlyLastWeekday', 'yearly'];

    function repeatModeOf(note) {
        const mode = REPEAT_MODES.includes(note.repeatMode) ? note.repeatMode : 'none';
        if (mode === 'weekly' || mode === 'none') {
            return Array.isArray(note.repeatWeekdays) && note.repeatWeekdays.length ? 'weekly' : 'none';
        }
        return mode;
    }

    function isRecurring(note) {
        return repeatModeOf(note) !== 'none';
    }

    const daysInMonth = (d) => new Date(d.getFullYear(), d.getMonth() + 1, 0).getDate();
    const nthOfMonth = (d) => Math.ceil(d.getDate() / 7);              // 1..5
    const isLastWeekdayOfMonth = (d) => d.getDate() + 7 > daysInMonth(d);

    // Does a repeating note (started on `base`) fall on `day`?
    function repeatMatches(note, base, day) {
        switch (repeatModeOf(note)) {
            case 'weekly':
                return note.repeatWeekdays.includes((day.getDay() + 6) % 7);
            case 'monthlyDate': // the 31st becomes the last day in shorter months
                return day.getDate() === Math.min(base.getDate(), daysInMonth(day));
            case 'monthlyWeekday':
                return day.getDay() === base.getDay() && nthOfMonth(day) === nthOfMonth(base);
            case 'monthlyLastWeekday':
                return day.getDay() === base.getDay() && isLastWeekdayOfMonth(day);
            case 'yearly': // 29 February becomes 28 February in other years
                return day.getMonth() === base.getMonth() && day.getDate() === Math.min(base.getDate(), daysInMonth(day));
            default:
                return false;
        }
    }

    function ordinal(n) {
        const s = ['th', 'st', 'nd', 'rd'];
        const v = n % 100;
        return n + (s[(v - 20) % 10] || s[v] || s[0]);
    }

    // Human readable label for a repeat mode, based on the start date
    function repeatLabel(mode, base, weekdays) {
        const wd = WEEKDAY_FULL[(base.getDay() + 6) % 7];
        switch (mode) {
            case 'weekly':
                return weekdays && weekdays.length
                    ? `Weekly on ${weekdays.slice().sort().map((i) => WEEKDAY_FULL[i].slice(0, 3)).join(', ')}`
                    : 'Weekly on selected days';
            case 'monthlyDate': return `Monthly on the ${ordinal(base.getDate())}`;
            case 'monthlyWeekday': return `Monthly on the ${ordinal(nthOfMonth(base))} ${wd}`;
            case 'monthlyLastWeekday': return `Monthly on the last ${wd}`;
            case 'yearly': return `Every year on ${base.getDate()} ${MONTHS[base.getMonth()]}`;
            default: return 'Does not repeat';
        }
    }

    // Returns the occurrence (Date with the note's time) on `day`, or null.
    function occurrenceOn(note, day) {
        const base = parseDate(note.scheduledAt);
        if (!base) return null;
        if (note.skipDates && note.skipDates.length && note.skipDates.includes(dayKey(day))) return null;
        const occ = new Date(day.getFullYear(), day.getMonth(), day.getDate(),
            base.getHours(), base.getMinutes(), base.getSeconds());
        if (sameDay(base, day)) return occ;
        if (!isRecurring(note)) return null;
        const key = dayKey(day);
        if (key < dayKey(base)) return null;
        if (note.repeatEndDate && key > note.repeatEndDate) return null;
        if (!repeatMatches(note, base, day)) return null;
        return occ;
    }

    // Most recent occurrence at or before `time` (ms).
    function latestOccurrence(note, time) {
        const base = parseDate(note.scheduledAt);
        if (!base) return null;
        if (!isRecurring(note)) return base.getTime() <= time ? base : null;
        const now = new Date(time);
        for (let i = 0; i < 8; i++) {
            const occ = occurrenceOn(note, new Date(now.getFullYear(), now.getMonth(), now.getDate() - i));
            if (occ && occ.getTime() <= time) return occ;
        }
        return null;
    }

    // First occurrence after `time` (ms).
    function nextOccurrence(note, time) {
        const base = parseDate(note.scheduledAt);
        if (!base) return null;
        if (!isRecurring(note)) return base.getTime() > time ? base : null;
        const from = new Date(Math.max(time, base.getTime() - 1));
        for (let i = 0; i < 800; i++) {
            const occ = occurrenceOn(note, new Date(from.getFullYear(), from.getMonth(), from.getDate() + i));
            if (occ && occ.getTime() > time) return occ;
        }
        return null;
    }

    const WEEKDAY_SHORT = ['M', 'T', 'W', 'T', 'F', 'S', 'S'];
    const WEEKDAY_FULL = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

    function repeatSummary(note) {
        if (!isRecurring(note)) return '';
        const base = parseDate(note.scheduledAt);
        if (!base) return '';
        const mode = repeatModeOf(note);
        const what = mode === 'weekly'
            ? `repeats ${note.repeatWeekdays.slice().sort().map((i) => WEEKDAY_FULL[i].slice(0, 2)).join(' ')}`
            : repeatLabel(mode, base).replace(/^Monthly/, 'monthly').replace(/^Every year/, 'every year');
        return `${what}${note.repeatEndDate ? ` until ${formatDateTime(note.repeatEndDate + 'T00:00:00').slice(0, -6)}` : ''}`;
    }

    const noteById = (id) => state.notes.find((n) => n.id === id) || null;

    function touch(note) {
        note.modifiedAt = new Date().toISOString();
        saveNotes();
    }

    /* ---------------- sidebar / folder tree ---------------- */

    const treeEl = $('tree');
    const emptyEl = $('emptyExplorer');

    function byModifiedDesc(a, b) {
        return (Date.parse(b.modifiedAt) || 0) - (Date.parse(a.modifiedAt) || 0);
    }

    // Manual order (drag & drop in the explorer). Notes without an order
    // (older data) get one based on their last-modified date.
    function ensureNoteOrder() {
        const missing = state.notes.filter((n) => typeof n.order !== 'number');
        if (!missing.length) return;
        const max = state.notes.reduce((m, n) => (typeof n.order === 'number' ? Math.max(m, n.order) : m), -1);
        missing.sort(byModifiedDesc).forEach((n, i) => { n.order = max + 1 + i; });
        saveNotes();
    }

    function sortedNotes(notes) {
        return notes.slice().sort((a, b) => {
            const ao = typeof a.order === 'number' ? a.order : Infinity;
            const bo = typeof b.order === 'number' ? b.order : Infinity;
            if (ao !== bo) return ao - bo;
            return byModifiedDesc(a, b);
        });
    }

    function topOrder() {
        return state.notes.reduce((m, n) => (typeof n.order === 'number' ? Math.min(m, n.order) : m), 0) - 1;
    }

    // in the Explorer a note only shows its title
    function noteItemHtml(note) {
        const bg = note.bgColor ? ` style="${bgStyle(note.bgColor)}"` : '';
        return `<div class="note-item ${note.id === state.currentNoteId ? 'active' : ''}${note.bgColor ? ' has-bg' : ''}${note.calendarOnly ? ' calendar-only' : ''}" data-note-id="${note.id}"${bg}>
            <button class="more-btn tree-menu-btn note-more" data-note-id="${note.id}" type="button" title="Menu" aria-label="Menu" aria-haspopup="menu">☰</button>
            <div class="note-item-title">${note.calendarOnly ? '<span class="hidden-mark" title="Only in Calendar (hidden in Explorer)">📆</span> ' : ''}${escapeHtml(note.title.trim() || 'Untitled')}</div>
        </div>`;
    }

    function sortedFolders() {
        return state.folders
            .map((f, i) => ({ f, i }))
            .sort((a, b) => {
                const ao = typeof a.f.order === 'number' ? a.f.order : a.i;
                const bo = typeof b.f.order === 'number' ? b.f.order : b.i;
                return ao - bo || a.i - b.i;
            })
            .map((x) => x.f);
    }

    /* ---- search in the Explorer ---- */
    const fold = (t) => String(t || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
    function noteMatches(note, words) {
        // private notes: only their title is searched
        const parts = [note.title];
        if (!note.private) {
            parts.push(note.content);
            if (note.project) (note.project.columns || []).forEach((c) => (c.cards || []).forEach((k) => parts.push(k.text)));
            if (note.collage) (note.collage.items || []).forEach((it) => parts.push(it.title, it.html && htmlToText(it.html)));
        }
        const folder = state.folders.find((f) => f.id === note.folderId);
        if (folder) parts.push(folder.name);
        const hay = fold(parts.join(' \n '));
        return words.every((w) => hay.includes(w));
    }
    function renderSearch(query) {
        const words = fold(query).split(/\s+/).filter(Boolean);
        const found = sortedNotes(state.notes.filter((n) => noteMatches(n, words))); // also notes hidden with 📆
        treeEl.innerHTML = found.length
            ? `<div class="search-count">${found.length} note${found.length === 1 ? '' : 's'} found</div><div class="folder-items search-results">${found.map(noteItemHtml).join('')}</div>`
            : '<div class="search-count">No notes found</div>';
        emptyEl.style.display = 'none';
        makeTreeNotesDraggable();
        updateTrashButton();
    }
    const searchInput = $('treeSearch');
    searchInput.addEventListener('input', () => {
        $('treeSearchClear').hidden = !searchInput.value;
        renderTree();
    });
    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') { searchInput.value = ''; $('treeSearchClear').hidden = true; renderTree(); searchInput.blur(); }
        else if (e.key === 'Enter') { const first = treeEl.querySelector('.note-item'); if (first) first.click(); }
    });
    // 📆 next to the search bar: list ONLY the notes that are hidden in the Explorer
    function updateShowHiddenBtn() {
        const on = !!state.prefs.showHiddenNotes;
        $('showHiddenBtn').classList.toggle('active', on);
        $('showHiddenBtn').setAttribute('aria-pressed', on ? 'true' : 'false');
        treeEl.classList.toggle('only-hidden', on);
    }
    $('showHiddenBtn').addEventListener('click', () => {
        state.prefs.showHiddenNotes = !state.prefs.showHiddenNotes;
        savePrefs();
        updateShowHiddenBtn();
        renderTree();
    });
    updateShowHiddenBtn();
    $('treeSearchClear').addEventListener('click', () => { searchInput.value = ''; $('treeSearchClear').hidden = true; renderTree(); searchInput.focus(); });

    function renderTree() {
        ensureNoteOrder();
        if (searchInput.value.trim()) { renderSearch(searchInput.value); return; }
        treeEl.innerHTML = '';
        // 📆 notes (only in the Calendar) are left out; with "show hidden" on, ONLY those are listed
        const onlyHidden = !!state.prefs.showHiddenNotes;
        const listed = (n) => (onlyHidden ? !!n.calendarOnly : !n.calendarOnly);
        const unfiled = sortedNotes(state.notes.filter((n) => !n.folderId && listed(n)));
        // in "only hidden" mode: skip folders without hidden notes or hidden folders, and open the rest
        const hasListed = (folderId) => state.notes.some((n) => n.folderId === folderId && listed(n)) || childFolders(folderId).some((f) => f.hidden || hasListed(f.id));

        // a hidden folder (📆) hides everything in it; with "show hidden" on, it is listed with all it holds
        const folderHtml = (folder, inHidden = false) => {
            const hid = inHidden || !!folder.hidden;
            if (!onlyHidden && folder.hidden) return '';
            if (onlyHidden && !hid && !hasListed(folder.id)) return '';
            const notes = sortedNotes(state.notes.filter((n) => n.folderId === folder.id && (hid && onlyHidden ? true : listed(n))));
            const subs = childFolders(folder.id);
            const expanded = onlyHidden || state.prefs.expandedFolders.includes(folder.id);
            const inner = subs.map((f) => folderHtml(f, hid)).join('') + notes.map(noteItemHtml).join('');
            return `<div class="folder">
                <div class="folder-header${folder.bgColor ? ' has-bg' : ''}${folder.hidden ? ' folder-hidden' : ''}" data-folder-id="${folder.id}" draggable="true" title="Drag to reorder, or onto another folder to put it inside"${folder.bgColor ? ` style="${bgStyle(folder.bgColor)}"` : ''}>
                    <span class="folder-chevron">${expanded ? '&#9662;' : '&#9656;'}</span>
                    <span class="folder-name">${folder.hidden ? '<span class="hidden-mark" title="Hidden in the Explorer">📆</span> ' : ''}${escapeHtml(folder.name || 'Folder')}</span>
                    <button class="more-btn tree-menu-btn folder-more" data-folder-id="${folder.id}" type="button" title="Menu" aria-label="Menu" aria-haspopup="menu">☰</button>
                    <button class="folder-add" data-folder-id="${folder.id}" title="New note in this folder">+</button>
                </div>
                ${expanded ? `<div class="folder-items">${inner || '<div class="folder-empty">Empty</div>'}</div>` : ''}
            </div>`;
        };
        treeEl.innerHTML = childFolders(null).map((f) => folderHtml(f)).join('');

        if (onlyHidden && !treeEl.innerHTML.trim() && !unfiled.length) {
            treeEl.innerHTML = '<div class="hidden-empty">No hidden notes or folders. Use the 📆 button in a note, or "Hide in Explorer" in a folder\'s ☰ menu.</div>';
        }
        if (unfiled.length) {
            const expanded = onlyHidden || state.prefs.expandedFolders.includes('__none');
            const group = document.createElement('div');
            group.className = 'folder';
            group.innerHTML = `
                <div class="folder-header" data-folder-id="__none">
                    <span class="folder-chevron">${expanded ? '&#9662;' : '&#9656;'}</span>
                    <span class="folder-name">No folder</span>
                    <button class="folder-add" data-folder-id="__none" title="New note without a folder">+</button>
                </div>
                ${expanded ? `<div class="folder-items">${unfiled.map(noteItemHtml).join('')}</div>` : ''}
            `;
            treeEl.appendChild(group);
        }

        emptyEl.style.display = state.notes.length ? 'none' : 'block';
        makeTreeNotesDraggable();
        updateTrashButton();
    }

    // 📆 hide a folder (with everything in it) in the Explorer, like "Show ONLY in Calendar" for a note
    function toggleFolderHidden(folder) {
        folder.hidden = !folder.hidden;
        if (!folder.hidden) delete folder.hidden;
        folder.modifiedAt = new Date().toISOString();
        saveFolders();
        renderTree();
        toast(folder.hidden ? `"${folder.name}" is hidden in the Explorer: click 📆 next to the search bar to see it`
            : `"${folder.name}" is shown in the Explorer again`);
    }

    /* ---- folder drag & drop (reorder) ---- */
    let draggedFolderId = null;

    treeEl.addEventListener('dragstart', (e) => {
        const header = e.target.closest && e.target.closest('.folder-header[draggable="true"]');
        if (!header || e.target.closest('.note-item')) return;
        draggedFolderId = header.dataset.folderId;
        e.dataTransfer.setData('application/x-remmin-folder', draggedFolderId);
        e.dataTransfer.effectAllowed = 'move';
        header.classList.add('dragging');
    });
    treeEl.addEventListener('dragend', () => {
        draggedFolderId = null;
        treeEl.querySelectorAll('.folder-header.dragging').forEach((el) => el.classList.remove('dragging'));
        clearTreeDropMarkers();
    });

    // Put a folder before/after another folder (in that folder's parent), or on "No folder" = top level, at the end
    function moveFolderNextTo(folderId, targetId, after) {
        if (folderId === targetId) return;
        const folder = state.folders.find((f) => f.id === folderId);
        const target = state.folders.find((f) => f.id === targetId);
        if (!folder || (target && isInside(target.id, folder.id))) return;
        const parentId = target ? parentOf(target) : null;
        const siblings = childFolders(parentId).filter((f) => f.id !== folderId);
        let index = target ? siblings.indexOf(target) + (after ? 1 : 0) : siblings.length;
        if (index < 0) index = siblings.length;
        siblings.splice(index, 0, folder);
        const now = new Date().toISOString();
        if ((folder.parentId || null) !== parentId) { folder.parentId = parentId; folder.modifiedAt = now; }
        siblings.forEach((f, i) => { if (f.order !== i) { f.order = i; f.modifiedAt = now; } });
        saveFolders();
        renderTree();
        refreshFolderSelect();
    }

    // Put a folder inside another folder (as its last sub-folder)
    function moveFolderInto(folderId, parentId) {
        const folder = state.folders.find((f) => f.id === folderId);
        if (!folder || folderId === parentId || isInside(parentId, folderId)) {
            if (folder && folderId !== parentId) toast("A folder can't go inside one of its own sub-folders");
            return;
        }
        const now = new Date().toISOString();
        const siblings = childFolders(parentId).filter((f) => f.id !== folderId);
        folder.parentId = parentId;
        folder.order = siblings.reduce((m, f) => Math.max(m, typeof f.order === 'number' ? f.order : 0), -1) + 1;
        folder.modifiedAt = now;
        expandFolderPath(parentId);
        savePrefs();
        saveFolders();
        renderTree();
        refreshFolderSelect();
    }
    // Phone-friendly: pick where a folder should go
    function chooseFolderDestination(folder) {
        const options = folderTree()
            .filter(({ folder: f }) => !isInside(f.id, folder.id) && f.id !== parentOf(folder))
            .map(({ folder: f, depth }) => ({ label: `${'\u00a0\u00a0'.repeat(depth)}📁 ${f.name}`, run: () => moveFolderInto(folder.id, f.id) }));
        if (parentOf(folder)) options.unshift({ label: '⬆ Top level', run: () => moveFolderNextTo(folder.id, '__none') });
        if (!options.length) { toast('There is no other folder to move it into'); return; }
        showActionSheet(`Move "${folder.name}" to`, options);
    }
    /* ---------- ☰ menus in the explorer ---------- */
    // Background colours for folders / notes: dark and greyish so they blend with the dark theme
    // (same lightness and greyness for every colour, red ≈ #361F1F)
    const BG_COLORS = [
        ['', 'No colour'],
        ['#372020', 'Red'], ['#372b20', 'Orange'], ['#373420', 'Yellow'], ['#2d3720', 'Lime'],
        ['#203722', 'Green'], ['#20372f', 'Teal'], ['#203437', 'Cyan'], ['#202937', 'Blue'],
        ['#212037', 'Indigo'], ['#2b2037', 'Purple'], ['#372037', 'Magenta'], ['#37202c', 'Pink'],
        ['#000000', 'Black'], ['#55565c', 'Grey'], ['#c9c9ce', 'White']
    ];

    // background colour of a tree item; dark text on light colours, light text on dark ones
    function bgStyle(color) {
        const r = parseInt(color.slice(1, 3), 16), g = parseInt(color.slice(3, 5), 16), b = parseInt(color.slice(5, 7), 16);
        const light = (0.299 * r + 0.587 * g + 0.114 * b) > 140;
        return `--item-bg:${color};--item-fg:${light ? '#14141c' : '#f5f5ff'};--item-dim:${light ? 'rgba(20,20,28,.65)' : 'rgba(245,245,255,.7)'};--item-hover:${light ? 'rgba(0,0,0,.07)' : 'rgba(255,255,255,.07)'}`;
    }
    function setItemBg(item, color, isFolder) {
        if ((item.bgColor || '') === (color || '')) return;
        if (color) item.bgColor = color; else delete item.bgColor;
        if (isFolder) { item.modifiedAt = new Date().toISOString(); saveFolders(); renderTree(); }
        else { touch(item); renderTree(); }
    }

    const menuEl = document.createElement('div');
    menuEl.className = 'tree-menu';
    menuEl.setAttribute('role', 'menu');
    menuEl.hidden = true;
    document.body.appendChild(menuEl);
    let menuActions = [];
    let menuAnchor = null;

    function placeMenu(anchor) {
        const r = anchor.getBoundingClientRect();
        menuEl.style.left = '0px';
        menuEl.style.top = '0px';
        const w = menuEl.offsetWidth, h = menuEl.offsetHeight;
        let left = Math.min(r.right - w, window.innerWidth - w - 8);
        left = Math.max(8, left);
        let top = r.bottom + 4;
        if (top + h > window.innerHeight - 8) top = Math.max(8, r.top - h - 4);
        menuEl.style.left = `${left}px`;
        menuEl.style.top = `${top}px`;
    }
    function closeMenu() {
        if (menuEl.hidden) return;
        menuEl.hidden = true;
        if (menuAnchor) menuAnchor.classList.remove('open');
        menuAnchor = null;
    }
    // Desktop: small drop-down under the ☰ button. Phone: the big action sheet.
    function showMenu(anchor, title, actions) {
        if (isMobile() || window.matchMedia('(hover: none)').matches) { showActionSheet(title, actions); return; }
        if (menuAnchor === anchor && !menuEl.hidden) { closeMenu(); return; }
        closeMenu();
        menuActions = actions;
        menuEl.innerHTML = actions.map((a, i) =>
            `<button type="button" role="menuitem" class="tree-menu-item${a.danger ? ' danger' : ''}" data-i="${i}"><span class="tree-menu-icon">${a.icon || ''}</span>${escapeHtml(a.label)}</button>`).join('');
        menuEl.hidden = false;
        menuAnchor = anchor;
        anchor.classList.add('open');
        placeMenu(anchor);
        menuEl.querySelector('button').focus({ preventScroll: true });
    }
    // Colour swatches (same colours as the text colours in notes)
    function pickBgColor(anchor, current, onPick) {
        closeMenu();
        menuActions = [];
        menuEl.innerHTML = `<div class="tree-menu-title">Background colour</div><div class="tree-menu-colors">${BG_COLORS.map(([c, name]) =>
            `<button type="button" class="text-color${c ? '' : ' reset'}${(current || '') === c ? ' selected' : ''}" data-bg="${c}" title="${c ? name : 'No colour'}" aria-label="${c ? name : 'No colour'}" style="${c ? `background:${c}` : ''}">${c ? '' : '∅'}</button>`).join('')}</div>`;
        menuEl._onPick = onPick;
        menuEl.hidden = false;
        // on a phone the button may be hidden behind the sheet: put the colours in the middle
        if (!anchor.isConnected || anchor.offsetParent === null) {
            menuEl.style.left = `${Math.max(8, (window.innerWidth - menuEl.offsetWidth) / 2)}px`;
            menuEl.style.top = `${Math.max(8, (window.innerHeight - menuEl.offsetHeight) / 2)}px`;
        } else {
            menuAnchor = anchor;
            anchor.classList.add('open');
            placeMenu(anchor);
        }
    }
    menuEl.addEventListener('click', (e) => {
        const color = e.target.closest('[data-bg]');
        if (color) {
            const onPick = menuEl._onPick;
            closeMenu();
            if (onPick) onPick(color.dataset.bg);
            return;
        }
        const item = e.target.closest('.tree-menu-item');
        if (!item) return;
        const action = menuActions[Number(item.dataset.i)];
        const anchor = menuAnchor;
        closeMenu();
        if (action) {
            // keep the anchor so a follow-up (the colour picker) can open next to it
            if (anchor) anchor.isConnected && anchor.classList.remove('open');
            action.run();
        }
    });
    document.addEventListener('pointerdown', (e) => {
        if (menuEl.hidden || menuEl.contains(e.target) || (menuAnchor && menuAnchor.contains(e.target))) return;
        closeMenu();
    }, true);
    document.addEventListener('keydown', (e) => {
        if (menuEl.hidden) return;
        if (e.key === 'Escape') { e.stopPropagation(); closeMenu(); return; }
        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
            const items = [...menuEl.querySelectorAll('button')];
            const i = items.indexOf(document.activeElement);
            const next = items[(i + (e.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length];
            if (next) { e.preventDefault(); next.focus(); }
        }
    }, true);
    window.addEventListener('resize', closeMenu);
    treeEl.addEventListener('scroll', closeMenu, { passive: true });

    function refreshFolderSelect() {
        if (state.currentNoteId) $('noteFolder').innerHTML = folderOptions(noteById(state.currentNoteId)?.folderId);
    }

    function makeTreeNotesDraggable() {
        treeEl.querySelectorAll('.note-item').forEach((item) => {
            item.setAttribute('draggable', 'true');
            item.addEventListener('dragstart', onTreeNoteDragStart);
            item.addEventListener('dragend', onTreeNoteDragEnd);
        });
    }

    let draggedTreeNoteId = null;

    function onTreeNoteDragStart(e) {
        const item = e.currentTarget;
        draggedTreeNoteId = item.dataset.noteId;
        e.dataTransfer.setData('text/plain', draggedTreeNoteId);
        e.dataTransfer.effectAllowed = 'copyMove';
        try {
            e.dataTransfer.setDragImage(item, 10, 10);
        } catch (err) {
            /* ignore */
        }
    }

    function clearTreeDropMarkers() {
        treeEl.querySelectorAll('.drop-before, .drop-after, .drop-inside').forEach((el) => el.classList.remove('drop-before', 'drop-after', 'drop-inside'));
    }

    function onTreeNoteDragEnd() {
        draggedTreeNoteId = null;
        clearTreeDropMarkers();
        document.querySelectorAll('.calendar-day.drag-over').forEach((el) => el.classList.remove('drag-over'));
    }

    // Move a note so it sits before/after another note (and in that note's folder).
    function moveNoteNextTo(noteId, targetId, after) {
        const note = noteById(noteId);
        const target = noteById(targetId);
        if (!note || !target || note.id === target.id) return;
        const folderId = target.folderId || null;
        const siblings = sortedNotes(state.notes.filter((n) => (n.folderId || null) === folderId && n.id !== note.id));
        const index = siblings.indexOf(target) + (after ? 1 : 0);
        siblings.splice(index, 0, note);
        const now = new Date().toISOString();
        siblings.forEach((n, i) => {
            if (n.order !== i) { n.order = i; n.modifiedAt = now; }
        });
        note.folderId = folderId;
        note.modifiedAt = now;
        if (note.id === state.currentNoteId) state.currentFolderId = folderId;
        saveNotes();
        renderTree();
        renderEditor();
    }

    treeEl.addEventListener('click', (e) => {
        const noteMore = e.target.closest('.note-more');
        if (noteMore) {
            e.stopPropagation();
            const note = noteById(noteMore.dataset.noteId);
            if (!note) return;
            showMenu(noteMore, note.title.trim() || 'Untitled', [
                ...(isMobile() ? [{ label: 'Open', icon: '📄', run: () => { selectNote(note.id); app.classList.remove('mobile-open'); } }] : []),
                { label: 'Rename', icon: '✏️', run: () => renameNote(note.id) },
                { label: 'Background colour', icon: '🎨', run: () => pickBgColor(noteMore, note.bgColor, (c) => setItemBg(note, c)) },
                { label: 'Delete', icon: '🗑', danger: true, run: () => deleteNote(note.id) }
            ]);
            return;
        }
        const folderMore = e.target.closest('.folder-more');
        if (folderMore) {
            e.stopPropagation();
            const folder = state.folders.find((f) => f.id === folderMore.dataset.folderId);
            if (!folder) return;
            showMenu(folderMore, folder.name, [
                { label: 'New note in this folder', icon: '📝', run: () => newNote({ folderId: folder.id }) },
                { label: '+ Folder (inside this one)', icon: '📁', run: () => createFolder(folder.id) },
                { label: 'Move to…', icon: '↪', run: () => chooseFolderDestination(folder) },
                { label: 'Rename', icon: '✏️', run: () => renameFolder(folder.id) },
                { label: 'Background colour', icon: '🎨', run: () => pickBgColor(folderMore, folder.bgColor, (c) => setItemBg(folder, c, true)) },
                { label: folder.hidden ? 'Show in Explorer again' : 'Hide in Explorer (the 📆 button shows it)', icon: '📆', run: () => toggleFolderHidden(folder) },
                { label: 'Delete', icon: '🗑', danger: true, run: () => deleteFolder(folder.id) }
            ]);
            return;
        }
        const noteRename = e.target.closest('.note-rename');
        if (noteRename) {
            e.stopPropagation();
            renameNote(noteRename.dataset.noteId);
            return;
        }
        const noteDelete = e.target.closest('.note-delete');
        if (noteDelete) {
            e.stopPropagation();
            deleteNote(noteDelete.dataset.noteId);
            return;
        }
        const noteEl = e.target.closest('.note-item');
        if (noteEl) {
            selectNote(noteEl.dataset.noteId);
            if (isMobile()) app.classList.remove('mobile-open');
            return;
        }
        const add = e.target.closest('.folder-add');
        if (add) {
            e.stopPropagation();
            newNote({ folderId: add.dataset.folderId === '__none' ? null : add.dataset.folderId });
            return;
        }
        const sub = e.target.closest('.folder-sub');
        if (sub) {
            e.stopPropagation();
            createFolder(sub.dataset.folderId);
            return;
        }
        const rename = e.target.closest('.folder-rename');
        if (rename) {
            e.stopPropagation();
            renameFolder(rename.dataset.folderId);
            return;
        }
        const del = e.target.closest('.folder-delete');
        if (del) {
            e.stopPropagation();
            deleteFolder(del.dataset.folderId);
            return;
        }
        const header = e.target.closest('.folder-header');
        if (header) {
            const id = header.dataset.folderId;
            const list = state.prefs.expandedFolders;
            const index = list.indexOf(id);
            if (index >= 0) list.splice(index, 1);
            else list.push(id);
            savePrefs();
            renderTree();
        }
    });

    // Double-click a note or folder name to rename it
    treeEl.addEventListener('dblclick', (e) => {
        if (e.target.closest('button')) return;
        const title = e.target.closest('.note-item-title');
        if (title) { renameNote(title.closest('.note-item').dataset.noteId); return; }
        const name = e.target.closest('.folder-name');
        const header = name && name.closest('.folder-header');
        if (header && header.dataset.folderId !== '__none') renameFolder(header.dataset.folderId);
    });

    treeEl.addEventListener('dragover', (e) => {
        if (draggedFolderId) {
            const target = e.target.closest('.folder-header');
            clearTreeDropMarkers();
            if (!target) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            const tid = target.dataset.folderId;
            if (tid === draggedFolderId || (tid !== '__none' && isInside(tid, draggedFolderId))) {
                e.dataTransfer.dropEffect = 'none';
                return;
            }
            if (tid === '__none') { target.classList.add('drop-before'); return; }
            // top quarter = before, bottom quarter = after, middle = inside
            const rect = target.getBoundingClientRect();
            const y = (e.clientY - rect.top) / rect.height;
            target.classList.add(y < 0.25 ? 'drop-before' : y > 0.75 ? 'drop-after' : 'drop-inside');
            return;
        }
        const item = e.target.closest('.note-item');
        if (item && draggedTreeNoteId) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            clearTreeDropMarkers();
            if (item.dataset.noteId === draggedTreeNoteId) return;
            const rect = item.getBoundingClientRect();
            const after = e.clientY > rect.top + rect.height / 2;
            item.classList.add(after ? 'drop-after' : 'drop-before');
            return;
        }
        const header = e.target.closest('.folder-header');
        if (!header) return;
        if (!draggedTreeNoteId && !Array.from(e.dataTransfer.types || []).includes('text/plain')) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        header.classList.add('drag-over');
    });
    treeEl.addEventListener('dragleave', (e) => {
        if (!treeEl.contains(e.relatedTarget)) clearTreeDropMarkers();
        const header = e.target.closest('.folder-header');
        if (header && !header.contains(e.relatedTarget)) header.classList.remove('drag-over');
    });
    treeEl.addEventListener('drop', (e) => {
        if (draggedFolderId) {
            const target = e.target.closest('.folder-header');
            const after = !!target && target.classList.contains('drop-after');
            const inside = !!target && target.classList.contains('drop-inside');
            const before = !!target && target.classList.contains('drop-before');
            const id = draggedFolderId;
            draggedFolderId = null;
            clearTreeDropMarkers();
            if (!target) return;
            e.preventDefault();
            if (inside) moveFolderInto(id, target.dataset.folderId);
            else if (before || after || target.dataset.folderId === '__none') {
                moveFolderNextTo(id, target.dataset.folderId, after);
            }
            return;
        }
        const item = e.target.closest('.note-item');
        if (item && draggedTreeNoteId) {
            e.preventDefault();
            const after = item.classList.contains('drop-after');
            clearTreeDropMarkers();
            moveNoteNextTo(draggedTreeNoteId, item.dataset.noteId, after);
            return;
        }
        const header = e.target.closest('.folder-header');
        if (!header) return;
        e.preventDefault();
        header.classList.remove('drag-over');
        const note = noteById(e.dataTransfer.getData('text/plain') || draggedTreeNoteId);
        if (!note) return;
        note.folderId = header.dataset.folderId === '__none' ? null : header.dataset.folderId;
        state.currentFolderId = note.folderId;
        touch(note);
        renderTree();
        renderEditor();
    });

    /* ---------------- notes ---------------- */

    function selectNote(id) {
        flushEditor();
        state.currentNoteId = id;
        state.currentFolderId = noteById(id)?.folderId || null;
        renderTree();
        renderEditor();
    }

    // opts: { folderId, scheduledAt }
    function newNote(opts = {}) {
        flushEditor();
        setTab('note');
        const now = new Date().toISOString();
        const folderId = opts.folderId && state.folders.some((f) => f.id === opts.folderId) ? opts.folderId : null;
        state.currentFolderId = folderId;
        if (folderId) expandFolderPath(folderId);
        const expandId = folderId || '__none';
        if (!state.prefs.expandedFolders.includes(expandId)) state.prefs.expandedFolders.push(expandId);
        state.draw.strokes = [];
        state.draw.tool = 'pen';
        state.draw.drawing = false;
        state.draw.current = null;
        state.prefs.mode = 'text';
        const note = {
            id: uid(),
            title: '',
            content: '',
            createdAt: now,
            modifiedAt: now,
            scheduledAt: opts.scheduledAt || null,
            folderId,
            alarmEnabled: false,
            emailEnabled: false,
            emailTo: '',
            color: '',
            alarmRepeatCount: 0,
            alarmSnoozeMinutes: 5,
            repeatWeekdays: [],
            repeatMode: 'none',
            repeatEndDate: '',
            canvasBackground: DEFAULT_CANVAS_BG,
            html: '',
            gridType: 'square',
            grids: {},
            strokes: [],
            windows: [], // a new note starts without windows: add them with the + button
            order: topOrder()
        };
        state.notes.push(note);
        saveNotes();
        state.currentNoteId = note.id;
        renderTree();
        renderEditor();
        $('noteTitle').focus();
        if (isMobile()) app.classList.remove('mobile-open');
    }

    async function deleteNote(id = state.currentNoteId) {
        flushEditor();
        const note = noteById(id);
        if (!note) return;
        const ok = await askConfirm('Delete note', `Delete "${note.title.trim() || 'Untitled'}"? This cannot be undone.`, 'Delete');
        if (!ok || !noteById(id)) return;
        state.notes = state.notes.filter((n) => n.id !== note.id);
        state.deleted.notes[note.id] = new Date().toISOString();
        addToTrash(note);
        delete state.alarms[note.id];
        saveNotes();
        saveAlarms();
        if (state.currentNoteId === note.id) {
            state.currentNoteId = sortedNotes(state.notes)[0]?.id || null;
            state.currentFolderId = noteById(state.currentNoteId)?.folderId || null;
            renderEditor();
        }
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
        toast('Note moved to trash', { label: 'Undo', onClick: () => restoreFromTrash(note.id) });
    }

    /* ---------------- trash ---------------- */

    const TRASH_DAYS = 30;

    function saveTrash(changed = false) {
        const cutoff = Date.now() - TRASH_DAYS * 86400000;
        state.trash = state.trash.filter((t) => (Date.parse(t.deletedAt) || 0) > cutoff).slice(0, 100);
        writeJSON(STORAGE.trash, state.trash);
        writeJSON(STORAGE.trashGone, state.trashGone);
        scheduleMediaGc();
        updateTrashButton();
        if (changed) { markChanged(); scheduleGithubSave(); } // the trash is synced too
    }
    // a note left the trash (restored or deleted forever): other devices should drop it as well
    function trashGone(id) { state.trashGone[id] = new Date().toISOString(); }
    // one trash from two lists: newest delete per note; leaves out what was restored or deleted forever
    function mergeTrash(a, b, gone, liveIds) {
        const byId = new Map();
        [...(a || []), ...(b || [])].forEach((t) => {
            if (!t || !t.note || !t.note.id) return;
            const have = byId.get(t.note.id);
            if (!have || (Date.parse(t.deletedAt) || 0) > (Date.parse(have.deletedAt) || 0)) byId.set(t.note.id, t);
        });
        const cutoff = Date.now() - TRASH_DAYS * 86400000;
        return [...byId.values()]
            .filter((t) => !liveIds.has(t.note.id))
            .filter((t) => !(gone[t.note.id] && (Date.parse(gone[t.note.id]) || 0) >= (Date.parse(t.deletedAt) || 0)))
            .filter((t) => (Date.parse(t.deletedAt) || 0) > cutoff)
            .sort((x, y) => (Date.parse(y.deletedAt) || 0) - (Date.parse(x.deletedAt) || 0))
            .slice(0, 100);
    }
    function mergeGone(a, b) {
        const out = {};
        const cutoff = Date.now() - 60 * 86400000;
        [a || {}, b || {}].forEach((g) => Object.entries(g).forEach(([id, at]) => {
            if ((Date.parse(at) || 0) < cutoff) return;
            if (!out[id] || (Date.parse(at) || 0) > (Date.parse(out[id]) || 0)) out[id] = at;
        }));
        return out;
    }

    function addToTrash(note) {
        state.trash = state.trash.filter((t) => t.note.id !== note.id);
        state.trash.unshift({ note: JSON.parse(JSON.stringify(note)), deletedAt: new Date().toISOString() });
        saveTrash(true);
    }

    function updateTrashButton() {
        const btn = $('trashBtn');
        if (btn) btn.textContent = `🗑 Trash${state.trash.length ? ` (${state.trash.length})` : ''}`;
    }

    function restoreFromTrash(id) {
        const entry = state.trash.find((t) => t.note.id === id);
        if (!entry) return;
        const note = entry.note;
        migrateNote(note);
        note.modifiedAt = new Date().toISOString(); // newer than the delete, so sync keeps it
        if (note.folderId && !state.folders.some((f) => f.id === note.folderId)) note.folderId = null;
        delete state.deleted.notes[id];
        state.notes = state.notes.filter((n) => n.id !== id);
        state.notes.push(note);
        state.trash = state.trash.filter((t) => t.note.id !== id);
        trashGone(id);
        saveTrash(true);
        resetAlarmRuntime(note);
        saveNotes();
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
        renderTrash();
        toast(`Restored "${note.title.trim() || 'Untitled'}"`);
    }

    function renderTrash() {
        const host = $('trashList');
        if (!host) return;
        $('emptyTrashBtn').disabled = !state.trash.length;
        if (!state.trash.length) {
            host.innerHTML = '<div class="calendar-hint">Trash is empty.</div>';
            return;
        }
        host.innerHTML = state.trash.map((t) => {
            const preview = t.note.private ? 'Private note' : (t.note.content || '').replace(/\s+/g, ' ').trim().slice(0, 70) ||
                ((t.note.strokes || []).length ? 'Drawing' : 'Empty note');
            return `<div class="trash-item">
                <div class="trash-text">
                    <div class="trash-title">${escapeHtml((t.note.title || '').trim() || 'Untitled')}</div>
                    <div class="trash-meta">Deleted ${formatDateTime(toLocalIso(new Date(t.deletedAt)))} · ${escapeHtml(preview)}</div>
                </div>
                <button class="folder-action-btn trash-restore" data-note-id="${t.note.id}">Restore</button>
                <button class="folder-action-btn folder-delete trash-forever" data-note-id="${t.note.id}">Delete forever</button>
            </div>`;
        }).join('');
    }

    $('trashBtn').addEventListener('click', () => {
        saveTrash();
        renderTrash();
        openModal('trashModal');
    });
    $('trashClose').addEventListener('click', closeModals);
    $('trashBackdrop').addEventListener('click', closeModals);
    $('trashList').addEventListener('click', async (e) => {
        const restore = e.target.closest('.trash-restore');
        if (restore) { restoreFromTrash(restore.dataset.noteId); return; }
        const forever = e.target.closest('.trash-forever');
        if (forever) {
            const ok = await askConfirm('Delete forever', 'This note will be gone for good. Continue?', 'Delete');
            if (!ok) return;
            state.trash = state.trash.filter((t) => t.note.id !== forever.dataset.noteId);
            trashGone(forever.dataset.noteId);
            saveTrash(true);
            renderTrash();
            openModal('trashModal');
        }
    });
    $('emptyTrashBtn').addEventListener('click', async () => {
        if (!state.trash.length) return;
        const ok = await askConfirm('Empty trash', `Permanently delete ${state.trash.length} note(s)?`, 'Delete');
        if (!ok) { openModal('trashModal'); return; }
        state.trash.forEach((t) => trashGone(t.note.id));
        state.trash = [];
        saveTrash(true);
        renderTrash();
        openModal('trashModal');
    });

    async function renameNote(id) {
        flushEditor();
        const note = noteById(id);
        if (!note) return;
        const name = await askText('Rename note', note.title, 'Rename');
        if (name === null || !noteById(id)) return;
        note.title = name.trim();
        touch(note);
        if (note.id === state.currentNoteId) $('noteTitle').value = note.title;
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    }

    function folderOptions(selected) {
        return '<option value="">No folder</option>' + folderTree().map(({ folder: f, depth }) =>
            `<option value="${f.id}" ${f.id === selected ? 'selected' : ''}>${'\u00a0\u00a0\u00a0'.repeat(depth)}${depth ? '↳ ' : ''}${escapeHtml(f.name)}</option>`
        ).join('');
    }

    $('noteFolder').addEventListener('change', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        note.folderId = $('noteFolder').value || null;
        state.currentFolderId = note.folderId;
        touch(note);
        renderTree();
    });

    function renderEditor() {
        const note = noteById(state.currentNoteId);
        const empty = !note;
        // remember the open note, so it opens again next time
        if (note && state.prefs.lastNoteId !== note.id) { state.prefs.lastNoteId = note.id; savePrefs(); }
        // opening another note locks a private note again
        if ((note ? note.id : null) !== lastEditorNoteId) unlockedNoteId = null;
        lastEditorNoteId = note ? note.id : null;
        // the pass screen never gets the "Show ONLY in Calendar" stripes
        if (isPrivateLocked(note)) { $('viewNote').classList.remove('calendar-only-note'); updateMainStripes(); }
        if (showPrivateLock(note)) return;

        $('noteTitle').disabled = empty;
        editorEl.contentEditable = empty ? 'false' : 'true';
        $('saveNoteBtn').disabled = empty;
        $('deleteNoteBtn').disabled = empty;
        $('noteFolder').disabled = empty;
        $('noteTitle').placeholder = empty ? 'No note selected' : 'Title';
        editorEl.dataset.placeholder = empty
            ? 'Create a note to get started.'
            : 'Write your note here... (you can paste or drag images in)';
        selectImage(null);
        textDirty = false;
        gridHistory = [];
        gridRedo = [];
        drawRedo = [];
        drawUndo = [];
        drawPick = [];
        gridPick = [];
        gridCopyMode = false;

        if (empty) {
            updatePrivateBtn(null);
            updateCalendarOnlyBtn(null);
            updateMemoBtn(null);
            $('noteTitle').value = '';
            editorEl.innerHTML = '';
            $('noteFolder').innerHTML = folderOptions('');
            state.draw.strokes = [];
            loadCollage(null);
            loadProject(null);
            updateScheduleUi(null);
            setMode('text');
            return;
        }

        $('noteTitle').value = note.title;
        updatePrivateBtn(note);
        updateCalendarOnlyBtn(note);
        updateMemoBtn(note);
        stopVoicePlayback();
        editorEl.innerHTML = sanitizeHtml(note.html);   // (also turns web addresses into links)
        resetTextHistory();
        decorateVoiceChips();
        $('gridType').value = currentGridType(note);
        updateGridControls(note);
        $('noteFolder').innerHTML = folderOptions(note.folderId);
        updateScheduleUi(note);

        drawSel = null;
        state.draw.strokes = note.strokes.map(copyStroke);
        loadCollage(note);
        loadProject(note);

        setMode(state.prefs.mode);
        fitCanvas();
        renderStrokes();
    }

    // A drawing item is a pen line, or a picture ({ image, x, y, w, h } as parts of the canvas size)
    function copyStroke(s) {
        if (s.image) {
            return {
                image: s.image, x: s.x, y: s.y, w: s.w, h: s.h, points: [], ...(s.fill ? { fill: true } : {}),
                ...(typeof s.video === 'string' && /^(?:media:[a-z0-9]{4,40}$|data:video\/[\w.+-]+;base64,)/i.test(s.video.slice(0, 80)) ? { video: s.video } : {}),
                ...(s.video && s.muted ? { muted: true } : {}),
                ...(s.video && s.loop === false ? { loop: false } : {}),
                ...(s.video && typeof s.volume === 'number' && s.volume >= 0 && s.volume < 1 ? { volume: s.volume } : {})
            };
        }
        return {
            color: s.color,
            thicknessRatio: s.thicknessRatio,
            points: (s.points || []).map((p) => ({ x: p.x, y: p.y }))
        };
    }

    /* ---------------- private notes ----------------
     * A private note asks for the "Private Note Pass" every time it is opened.
     * The pass is kept as a salted hash (never as plain text), on this device and in
     * remmin.json, so every synced device uses the same pass.
     * Note: this locks the note in the app; the note itself is not encrypted. */
    let privatePass = readJSON(STORAGE.privatePass, null);
    let unlockedNoteId = null;
    let lastEditorNoteId = null;
    const isPrivateLocked = (note) => !!(note && note.private && unlockedNoteId !== note.id);

    const toHex = (buf) => [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
    async function hashPass(pass, salt, iterations) {
        if (window.crypto && crypto.subtle) {
            const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(pass), 'PBKDF2', false, ['deriveBits']);
            const bits = await crypto.subtle.deriveBits(
                { name: 'PBKDF2', hash: 'SHA-256', salt: new TextEncoder().encode(salt), iterations }, key, 256);
            return 'pbkdf2:' + toHex(bits);
        }
        // very old browsers / file:// without crypto.subtle: a simple repeated hash
        let h = salt + pass;
        for (let i = 0; i < 2000; i++) {
            let h1 = 0xdeadbeef, h2 = 0x41c6ce57;
            for (let j = 0; j < h.length; j++) {
                const c = h.charCodeAt(j);
                h1 = Math.imul(h1 ^ c, 2654435761);
                h2 = Math.imul(h2 ^ c, 1597334677);
            }
            h = (h1 >>> 0).toString(16) + (h2 >>> 0).toString(16) + pass;
        }
        return 'simple:' + h.slice(0, 16);
    }
    async function checkPass(pass) {
        if (!privatePass) return false;
        return (await hashPass(pass, privatePass.salt, privatePass.iterations)) === privatePass.hash;
    }
    async function setPass(pass) {
        const salt = toHex(crypto.getRandomValues(new Uint8Array(16)));
        const iterations = 150000;
        privatePass = { salt, iterations, hash: await hashPass(pass, salt, iterations), setAt: new Date().toISOString() };
        writeJSON(STORAGE.privatePass, privatePass);
        markChanged();
        scheduleGithubSave(); // the (hashed) pass goes into remmin.json too
    }
    // a pass from remmin.json / a .json file is only used when it looks right
    const validPass = (p) => (p && typeof p === 'object' && typeof p.salt === 'string' && typeof p.hash === 'string' &&
        Number.isFinite(p.iterations) ? { salt: p.salt, iterations: p.iterations, hash: p.hash, ...(p.setAt ? { setAt: p.setAt } : {}) } : null);

    // Shows the lock screen for a private note; returns true when the note stays locked
    function showPrivateLock(note) {
        const locked = isPrivateLocked(note);
        const lock = $('privateLock');
        lock.hidden = !locked;
        [...$('viewNote').children].forEach((el) => {
            if (el === lock) return;
            if (locked) el.setAttribute('inert', ''); else el.removeAttribute('inert');
        });
        if (!locked) return false;
        // keep nothing of the note in the editor while it is locked
        stopVoicePlayback();
        loadCollage(null);
        loadProject(null);
        editorEl.innerHTML = '';
        $('noteTitle').value = '';
        state.draw.strokes = [];
        drawSel = null;
        gridHistory = []; gridRedo = []; drawRedo = []; drawUndo = []; drawPick = [];
        textDirty = false;
        $('privateLockTitle').textContent = note.title.trim() || 'Untitled';
        $('privateLockInput').value = '';
        $('privateLockError').textContent = '';
        $('privateLockForm').hidden = !privatePass;
        $('privateLockNoPass').hidden = !!privatePass;
        if (privatePass && !isMobile()) setTimeout(() => $('privateLockInput').focus(), 30);
        return true;
    }

    async function unlockCurrent() {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const pass = $('privateLockInput').value;
        if (!pass) { $('privateLockError').textContent = 'Enter your Private Note Pass'; return; }
        if (await checkPass(pass)) {
            unlockedNoteId = note.id;
            renderEditor();
        } else {
            $('privateLockError').textContent = 'Wrong pass, try again';
            $('privateLockInput').select();
            const box = $('privateLock').querySelector('.private-lock-box');
            box.classList.remove('shake'); void box.offsetWidth; box.classList.add('shake');
        }
    }
    $('privateLockBtn').addEventListener('click', unlockCurrent);
    $('privateLockInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); unlockCurrent(); } });
    $('privateLockNoPass').addEventListener('click', () => openSettings('pass'));

    // the striped background over the whole right side, while a "Show ONLY in Calendar" note is open
    function updateMainStripes() {
        const view = $('viewNote');
        document.querySelector('.main').classList.toggle('calendar-only-main', view.classList.contains('calendar-only-note') && view.style.display !== 'none'
            && !isPrivateLocked(noteById(state.currentNoteId)));
    }
    function updateCalendarOnlyBtn(note) {
        const btn = $('calendarOnlyBtn');
        btn.disabled = !note;
        const on = !!(note && note.calendarOnly);
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-pressed', on ? 'true' : 'false');
        $('viewNote').classList.toggle('calendar-only-note', on && !isPrivateLocked(note));
        updateMainStripes();
        btn.title = on ? 'Shown ONLY in Calendar (hidden in Explorer). Click to show it in the Explorer again.'
            : 'Show ONLY in Calendar (hidden in Explorer)';
    }
    $('calendarOnlyBtn').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        flushEditor();
        note.calendarOnly = !note.calendarOnly;
        if (!note.calendarOnly) delete note.calendarOnly;
        touch(note);
        updateCalendarOnlyBtn(note);
        updateMemoBtn(note);
        renderTree();
        if (note.calendarOnly) {
            toast(note.scheduledAt ? 'Hidden in Explorer: open this note from the Calendar'
                : 'Hidden in Explorer. Give it a date, or you can only find it in the Calendar if it has one');
        } else toast('This note is shown in the Explorer again');
    });

    /* ---- 📝 Collapse in Calendar: a small memo note (e.g. "pick up the kids") ----
     * In the month view a day only shows one small 📝 for all its memos; click the day to see them.
     * Memo notes always start their title with 📝. */
    const MEMO = '📝';
    const LOCK = '🔒';
    // the title with its marks in front: 🔒 for private, 📝 for memo
    const markTitle = (note, title) => {
        const base = String(title || '').replace(/^(?:[🔒📝]\s*)+/u, '').trim();
        return `${note.private ? `${LOCK} ` : ''}${note.memo ? `${MEMO} ` : ''}${base}`.trim();
    };
    function updateMemoBtn(note) {
        const btn = $('memoBtn');
        btn.disabled = !note;
        const on = !!(note && note.memo);
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-pressed', on ? 'true' : 'false');
        btn.title = on ? 'MEMO (collapsed in calendar). Click to make it a normal note again.'
            : 'Make MEMO (collapse note in calendar)';
        btn.setAttribute('aria-label', btn.title);
    }
    $('memoBtn').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        flushEditor();
        note.memo = !note.memo;
        if (!note.memo) delete note.memo;
        note.title = markTitle(note, note.title);
        $('noteTitle').value = note.title;
        touch(note);
        updateMemoBtn(note);
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
        toast(note.memo ? 'Collapsed in Calendar: shown as a small 📝' : 'Shown normally in the Calendar again');
    });

    function updatePrivateBtn(note) {
        const btn = $('privateBtn');
        btn.disabled = !note;
        const on = !!(note && note.private);
        btn.classList.toggle('active', on);
        btn.setAttribute('aria-pressed', on ? 'true' : 'false');
        btn.textContent = on ? '🔒' : '🔓';
        btn.title = on ? 'Private: this note asks for your Private Note Pass. Click to make it normal again.'
            : 'Make Private: ask for a pass every time this note is opened';
        btn.setAttribute('aria-label', on ? 'Private note' : 'Make Private');
    }
    $('privateBtn').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        if (!note.private && !privatePass) {
            toast("Set your 'Private Note Pass' in Settings (Top right)", { label: 'Open Settings', onClick: () => openSettings('pass') });
            return;
        }
        flushEditor();
        note.private = !note.private;
        if (note.private) unlockedNoteId = note.id; // stays open until you open it again
        note.title = markTitle(note, note.title);
        $('noteTitle').value = note.title;
        touch(note);
        if (state.prefs.activeTab === 'calendar') renderCalendar();
        updatePrivateBtn(note);
        updateCalendarOnlyBtn(note);
        updateMemoBtn(note);
        renderTree();
        toast(note.private ? 'Private: this note now asks for your pass when opened' : 'This note is no longer private');
    });

    // Settings → Private Note Pass
    function updatePassPanel() {
        $('passStatus').textContent = privatePass
            ? 'A pass is set on this device. To change it, enter the current pass first.'
            : 'No pass yet. Choose a short pass you can remember.';
        $('passCurrentRow').hidden = !privatePass;
        ['passCurrent', 'passNew', 'passRepeat'].forEach((id) => { $(id).value = ''; });
        $('passError').textContent = '';
        $('passSave').textContent = privatePass ? 'Change pass' : 'Set pass';
    }
    $('passSave').addEventListener('click', async () => {
        const err = $('passError');
        const next = $('passNew').value;
        if (privatePass && !(await checkPass($('passCurrent').value))) { err.textContent = 'The current pass is not right'; return; }
        if (next.length < 3) { err.textContent = 'Use at least 3 characters'; return; }
        if (next !== $('passRepeat').value) { err.textContent = 'The two new passes are not the same'; return; }
        await setPass(next);
        updatePassPanel();
        toast('Private Note Pass saved');
        if (isPrivateLocked(noteById(state.currentNoteId))) renderEditor();
    });
    $('passRepeat').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('passSave').click(); });

    let editorTimer = null;
    function queueSave() {
        clearTimeout(editorTimer);
        editorTimer = setTimeout(flushEditor, 600);
    }

    function flushEditor() {
        clearTimeout(editorTimer);
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        let title = $('noteTitle').value;
        if ((note.memo || note.private) && markTitle(note, title) !== title.trim()) { title = markTitle(note, title); $('noteTitle').value = title; } // memos keep their 📝, private notes their 🔒
        let html = note.html;
        let content = note.content;
        if (textDirty) {
            html = sanitizeHtml(editorEl.innerHTML);
            content = htmlToText(html);
        }
        const folderId = $('noteFolder').value || null;
        const strokes = state.draw.strokes.map(copyStroke);
        textDirty = false;
        const changed = note.title !== title || note.html !== html || note.folderId !== folderId ||
            JSON.stringify(note.strokes) !== JSON.stringify(strokes);
        if (!changed) return;
        note.title = title;
        note.html = html;
        note.content = content;
        note.folderId = folderId;
        note.strokes = strokes;
        note.modifiedAt = new Date().toISOString();
        saveNotes();
        renderTree();
    }

    $('noteTitle').addEventListener('input', queueSave);
    /* ---------------- rich text (text + images) ---------------- */

    const editorEl = $('noteContent');
    let textDirty = false;   // true once the user changed the text since it was loaded
    let selectedImg = null;

    const RICH_TAGS = new Set(['DIV', 'P', 'BR', 'B', 'STRONG', 'I', 'EM', 'U', 'UL', 'OL', 'LI',
        'IMG', 'SPAN', 'H1', 'H2', 'H3', 'BLOCKQUOTE', 'S', 'STRIKE', 'DEL', 'A', 'FONT']);
    const HEX_COLOR_RE = /^#[0-9a-f]{6}$/i;
    const SAFE_LINK_RE = /^(https?:\/\/|mailto:)/i;

    function cssColorToHex(value) {
        const v = String(value || '').trim();
        if (HEX_COLOR_RE.test(v)) return v.toLowerCase();
        const m = /^rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/i.exec(v);
        return m ? '#' + [m[1], m[2], m[3]].map((n) => Number(n).toString(16).padStart(2, '0')).join('') : '';
    }
    // embedded videos (YouTube / Vimeo players) and KLIPY GIFs shown from KLIPY
    const VIDEO_SRC_RE = /^https:\/\/(?:www\.youtube-nocookie\.com\/embed\/[A-Za-z0-9_-]{11}(?:\?start=\d+)?|player\.vimeo\.com\/video\/\d+)$/;
    const KLIPY_SRC_RE = /^https:\/\/(?:[a-z0-9-]+\.)*klipy\.(?:com|co)\/[^\s"'<>]+$/i;
    // video files from your own disk: kept in the media store, the notes hold "media:<id>"
    const VIDEO_MAX_MB = 25;
    const BIG_FILE_MB = 25; // videos, pictures and audio above this get the question to compress them first
    // a picture file above 25 MB: ask first (it is made smaller, then it fits)
    // a file above 25 MB: compress it to a size you choose (below 25 MB); null = not added
    async function askBigTarget(file, kind, extra = '') {
        const mb = file.size / 1048576;
        const answer = await askDialog({
            title: `${kind} too big`,
            message: `${file.name} is ${mb.toFixed(1)} MB (max ${BIG_FILE_MB} MB). Compress it so it fits in the note: how many MB may it be? (less than ${BIG_FILE_MB})${extra}`,
            input: '20',
            okLabel: 'Compress'
        });
        if (answer === null || answer === false) return null;
        const target = parseFloat(String(answer).replace(',', '.'));
        if (!Number.isFinite(target) || target <= 0 || target >= BIG_FILE_MB) { toast(`Enter a number of MB below ${BIG_FILE_MB}, for example 20`); return null; }
        return target * 1048576;
    }
    // a picture file → the picture text to store (big ones: to the size you choose); null = not added
    async function pictureFileToStorable(file) {
        if (typeof file === 'string') return file;
        if (!file || file.size <= BIG_FILE_MB * 1048576) return imageFileToDataUrl(file);
        const bytes = await askBigTarget(file, 'Picture', /gif$/i.test(file.type) ? ' A GIF this big stops moving.' : '');
        if (!bytes) return null;
        let src = await imageFileToDataUrl(file);
        if (sizeOfData(src) > bytes) src = await compressPictureData(src, { targetBytes: bytes });
        return src;
    }
    const isLocalVideo = (src) => isMediaRef(src);
    const isVideoSrc = (src) => VIDEO_SRC_RE.test(src || '') || isMediaRef(src);
    const resolveVideo = (v) => v;
    const videoRef = (v) => v;
    const isVideoFile = (f) => !!f && (/^video\//.test(f.type) || /\.(mp4|m4v|webm|mov|ogv)$/i.test(f.name || ''));
    const mbOf = (dataUrl) => (dataUrl.length * 0.75 / 1048576);
    function blobToDataUrl(blob) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(String(reader.result || ''));
            reader.onerror = () => reject(reader.error);
            reader.readAsDataURL(blob);
        });
    }
    async function videoFileToDataUrl(file) {
        let src = await blobToDataUrl(file);
        // some systems give .mov / .mkv files no type: name it a video so it can be played
        src = src.replace(/^data:(?:application\/octet-stream|);base64,/, `data:${/\.webm$/i.test(file.name) ? 'video/webm' : 'video/mp4'};base64,`);
        src = src.replace(/^data:video\/quicktime;/, 'data:video/mp4;'); // most .mov files play as mp4
        if (!/^data:video\/[\w.+-]+;base64,/i.test(src.slice(0, 80))) throw new Error('not a video');
        return src;
    }
    // reads dropped video files into the media store → their "media:<id>"; too big ones can be compressed first
    async function readVideoFiles(files) {
        const out = [];
        for (const f of files) {
            try {
                if (f.size > VIDEO_MAX_MB * 1048576) {
                    const bytes = await askBigTarget(f, 'Video', ' This takes about as long as the video lasts; keep this tab open.');
                    if (!bytes) continue;
                    const url = URL.createObjectURL(f);
                    try {
                        const data = await compressVideoToBytes(url, bytes, f.name);
                        if (data.length * 0.75 > VIDEO_MAX_MB * 1048576) { toast(`${f.name} is still too big after compressing`); continue; }
                        out.push(mediaIdFor(data));
                    } finally { URL.revokeObjectURL(url); }
                    continue;
                }
                out.push(mediaIdFor(await videoFileToDataUrl(f)));
            } catch (err) {
                if (err && err.message === 'cancelled') { toast('Compressing stopped'); continue; }
                toast(`Could not add ${f.name}${err && err.message === 'unsupported' ? ': this browser cannot compress videos' : ''}`);
            }
        }
        return out;
    }
    // data:…;base64 text → a file in memory (in pieces: works on phones too, also for big videos)
    function dataUrlToBlob(data) {
        const cut = data.indexOf(';base64,');
        const mime = data.slice(5, cut).split(';')[0] || 'application/octet-stream';
        const b64 = data.slice(cut + 8);
        const parts = [];
        const step = 4 * 1024 * 1024; // a multiple of 4
        for (let i = 0; i < b64.length; i += step) {
            const bin = atob(b64.slice(i, i + step));
            const u = new Uint8Array(bin.length);
            for (let j = 0; j < bin.length; j++) u[j] = bin.charCodeAt(j);
            parts.push(u);
        }
        return new Blob(parts, { type: mime });
    }
    // a playable address for a media video (made once per video)
    const mediaUrls = new Map(); // id → { url, len }
    async function mediaUrl(ref) {
        const data = mediaData(ref);
        if (!data) throw new Error('missing');
        const id = ref.slice(6);
        const c = mediaUrls.get(id);
        if (c && c.len === data.length) return c.url;
        const blob = dataUrlToBlob(data);
        const again = mediaUrls.get(id);
        if (again && again.len === data.length) return again.url;
        if (again) URL.revokeObjectURL(again.url);
        const url = URL.createObjectURL(blob);
        mediaUrls.set(id, { url, len: data.length });
        return url;
    }
    // a video's data changed (compressed / synced): reload every player of it
    function refreshMediaPlayers(id) {
        const c = mediaUrls.get(id);
        if (c) { URL.revokeObjectURL(c.url); mediaUrls.delete(id); }
        document.querySelectorAll(`video[data-media="media:${id}"]`).forEach((v) => {
            const err = v.nextElementSibling;
            if (err && err.classList.contains('video-error')) err.remove();
            v.removeAttribute('src'); v.load(); hydrateLocalVideos(v);
        });
        if (typeof dropDrawVideo === 'function') dropDrawVideo(`media:${id}`);
    }

    /* ---- compressing a video: played once (muted) into a smaller recording ---- */
    const COMPRESS_PRESETS = [
        { label: 'Good quality (720p)', maxH: 720, bitrate: 1600000 },
        { label: 'Smaller (480p)', maxH: 480, bitrate: 800000 },
        { label: 'Smallest (360p)', maxH: 360, bitrate: 420000 }
    ];
    let compressBox = null;
    function compressProgress(text, onCancel) {
        if (!compressBox) {
            compressBox = document.createElement('div');
            compressBox.className = 'compress-box';
            compressBox.innerHTML = '<span class="compress-text"></span><div class="compress-bar"><div></div></div><button type="button" class="btn compress-cancel">Stop</button>';
            document.body.appendChild(compressBox);
        }
        compressBox.hidden = false;
        compressBox.querySelector('.compress-text').textContent = text;
        compressBox.querySelector('.compress-bar > div').style.width = '0%';
        compressBox.querySelector('.compress-cancel').onclick = onCancel;
        return {
            set: (f, t) => { compressBox.querySelector('.compress-bar > div').style.width = `${Math.round(Math.min(1, Math.max(0, f)) * 100)}%`; if (t) compressBox.querySelector('.compress-text').textContent = t; },
            close: () => { compressBox.hidden = true; }
        };
    }
    // maxH 'auto': the resolution follows the bitrate (for "compress to … MB")
    async function compressVideoUrl(url, { maxH = 720, bitrate = 1600000, maxBytes = 0, label = 'video', audioBps = 96000 } = {}) {
        // MP4 (H.264) first: that plays on every phone; WebM when this browser can't make MP4
        const mime = ['video/mp4;codecs=avc1.42E01F,mp4a.40.2', 'video/mp4;codecs=avc1,mp4a.40.2', 'video/mp4', 'video/webm;codecs=vp9,opus', 'video/webm;codecs=vp8,opus', 'video/webm']
            .find((t) => window.MediaRecorder && MediaRecorder.isTypeSupported(t));
        if (!mime || !HTMLCanvasElement.prototype.captureStream) throw new Error('unsupported');
        const v = document.createElement('video');
        v.playsInline = true; v.preload = 'auto';
        v.src = url;
        await new Promise((resolve, reject) => { v.onloadedmetadata = resolve; v.onerror = () => reject(new Error('cannot play')); });
        const dur = Number.isFinite(v.duration) ? v.duration : 0;
        let rate = bitrate;
        if (maxBytes && dur) rate = Math.max(60000, Math.min(bitrate, (maxBytes * 8 * 0.9) / dur - audioBps));
        if (maxH === 'auto') maxH = rate > 2500000 ? 1080 : rate > 1000000 ? 720 : rate > 450000 ? 480 : rate > 200000 ? 360 : 240;
        const scale = Math.min(1, maxH / (v.videoHeight || maxH));
        const w = Math.max(2, Math.round((v.videoWidth || 640) * scale / 2) * 2), h = Math.max(2, Math.round((v.videoHeight || 360) * scale / 2) * 2);
        const c = document.createElement('canvas');
        c.width = w; c.height = h;
        const g = c.getContext('2d');
        const stream = c.captureStream(30);
        // the sound goes into the recording, not to the speakers
        let ac = null;
        try {
            ac = new (window.AudioContext || window.webkitAudioContext)();
            const node = ac.createMediaElementSource(v);
            const dest = ac.createMediaStreamDestination();
            node.connect(dest);
            dest.stream.getAudioTracks().forEach((t) => stream.addTrack(t));
            await ac.resume();
        } catch (_) { v.muted = true; }
        const rec = new MediaRecorder(stream, { mimeType: mime, videoBitsPerSecond: Math.round(rate), audioBitsPerSecond: audioBps });
        const chunks = [];
        rec.ondataavailable = (e) => { if (e.data && e.data.size) chunks.push(e.data); };
        const stopped = new Promise((resolve) => { rec.onstop = resolve; });
        let done = false, cancelled = false;
        const ui = compressProgress(`Compressing ${label}…`, () => { cancelled = true; finish(); });
        function finish() { if (done) return; done = true; v.pause(); if (rec.state !== 'inactive') rec.stop(); }
        const frame = () => {
            if (done) return;
            g.drawImage(v, 0, 0, w, h);
            if (dur) ui.set(v.currentTime / dur, `Compressing ${label}… ${Math.round(v.currentTime / dur * 100)}% (keep this tab open)`);
            if (v.requestVideoFrameCallback) v.requestVideoFrameCallback(frame); else requestAnimationFrame(frame);
        };
        v.onended = finish;
        try {
            rec.start(1000);
            await v.play();
            frame();
            await stopped;
        } finally {
            ui.close();
            if (ac) ac.close().catch(() => {});
            stream.getTracks().forEach((t) => t.stop());
        }
        if (cancelled) throw new Error('cancelled');
        const out = await blobToDataUrl(new Blob(chunks, { type: mime.split(';')[0] }));
        if (!/^data:video\//.test(out)) throw new Error('failed');
        return out;
    }
    // the original stays on this device for a week, so the compression can be undone
    function keepOriginal(id, data) {
        if (!state.mediaOrig[id]) state.mediaOrig[id] = { data, at: Date.now() };
        writeJSON(STORAGE.mediaOrig, state.mediaOrig);
    }
    // "Keep this size": the original is forgotten, the compressed version stays for good
    function keepCompressed(ref) {
        const id = ref.slice(6);
        if (!state.mediaOrig[id]) return;
        delete state.mediaOrig[id];
        writeJSON(STORAGE.mediaOrig, state.mediaOrig);
        toast(`Kept at ${mbOf(state.media[id] || '').toFixed(2)} MB (the original is removed)`);
    }
    // undo: the original comes back under a new id, and every note that uses the video points to it
    // (so the sync, where the smaller version would win, keeps the original)
    function undoCompression(ref) {
        const id = ref.slice(6);
        const orig = state.mediaOrig[id];
        if (!orig) { toast('The original of this video is not on this device any more'); return; }
        flushEditor();
        const newId = `${id.slice(0, 26)}r${Date.now().toString(36)}`;
        state.media[newId] = orig.data;
        delete state.mediaOrig[id];
        writeJSON(STORAGE.mediaOrig, state.mediaOrig);
        const from = `media:${id}`, to = `media:${newId}`;
        const swap = (v) => {
            if (typeof v === 'string') return v.includes(from) ? v.split(from).join(to) : v;
            if (Array.isArray(v)) { v.forEach((x, i) => { v[i] = swap(x); }); return v; }
            if (v && typeof v === 'object') Object.keys(v).forEach((k) => { v[k] = swap(v[k]); });
            return v;
        };
        state.notes.forEach((n) => {
            if (!JSON.stringify(n).includes(from)) return;
            swap(n);
            n.modifiedAt = new Date().toISOString();
        });
        state.trash.forEach((t) => { if (t && t.note) swap(t.note); });
        scheduleSaveMedia();
        saveNotes();
        writeJSON(STORAGE.trash, state.trash);
        markChanged();
        scheduleGithubSave();
        renderEditor();
        toast(`Compression undone: back to ${mbOf(orig.data).toFixed(1)} MB`);
    }
    // a video (address) compressed to at most `bytes`: a second, smaller try when the first one is too big
    async function compressVideoToBytes(url, bytes, label) {
        let out = await compressVideoUrl(url, { maxH: 'auto', bitrate: 8000000, maxBytes: bytes, audioBps: 64000, label: `${label} to ${(bytes / 1048576).toFixed(1)} MB` });
        const got = out.length * 0.75;
        if (got > bytes * 1.03) {
            const dur = await new Promise((res) => { const v = document.createElement('video'); v.preload = 'metadata'; v.onloadedmetadata = () => res(v.duration); v.onerror = () => res(0); v.src = url; });
            if (Number.isFinite(dur) && dur > 0) {
                const rate = Math.max(40000, ((bytes * 8 * 0.85) / dur - 32000) * Math.min(1, bytes / got));
                out = await compressVideoUrl(url, { maxH: 'auto', bitrate: rate, audioBps: 32000, label: `${label} (2nd try)` });
            }
        }
        return out;
    }
    // compress to (at most) a number of MB; a second, smaller try when the first one ends up too big
    async function compressMediaTo(ref) {
        const id = ref.slice(6);
        const before = state.media[id];
        if (!before) return;
        const now = mbOf(before);
        const answer = await askDialog({
            title: 'Compress to … MB',
            message: `How many MB may the video be? Now it is ${now.toFixed(1)} MB; enter a smaller number (for example ${Math.max(0.1, now / 2).toFixed(1)}).`,
            input: Math.max(0.1, now / 2).toFixed(1),
            okLabel: 'Compress'
        });
        if (answer === null || answer === false) return;
        const target = parseFloat(String(answer).replace(',', '.'));
        if (!Number.isFinite(target) || target <= 0) { toast('Enter a number of MB, for example 1.5'); return; }
        if (target >= now) { toast(`Enter a number below ${now.toFixed(1)} MB`); return; }
        const bytes = target * 1048576;
        const audioBps = target / now < 0.15 ? 32000 : 64000;
        let out;
        try {
            const url = await mediaUrl(ref);
            out = await compressVideoUrl(url, { maxH: 'auto', bitrate: 8000000, maxBytes: bytes, audioBps, label: `video to ${target} MB` });
            const got = out.length * 0.75;
            if (got > bytes * 1.03) {
                // a bit too big: once more, with a lower bitrate
                const dur = await new Promise((res) => { const v = document.createElement('video'); v.preload = 'metadata'; v.onloadedmetadata = () => res(v.duration); v.onerror = () => res(0); v.src = url; });
                if (Number.isFinite(dur) && dur > 0) {
                    const rate = Math.max(40000, ((bytes * 8 * 0.85) / dur - audioBps) * Math.min(1, bytes / got));
                    out = await compressVideoUrl(url, { maxH: 'auto', bitrate: rate, audioBps: Math.min(audioBps, 32000), label: `video to ${target} MB (2nd try)` });
                }
            }
        } catch (err) {
            toast(err && err.message === 'cancelled' ? 'Compressing stopped' : err && err.message === 'unsupported' ? 'This browser cannot compress videos' : 'Could not compress this video');
            return;
        }
        if (out.length >= before.length) { toast(`Not smaller after compressing: kept the original (${now.toFixed(1)} MB)`); return; }
        keepOriginal(id, before);
        state.media[id] = out;
        scheduleSaveMedia();
        refreshMediaPlayers(id);
        markChanged();
        scheduleGithubSave();
        const got = mbOf(out);
        toast(`Video compressed: ${now.toFixed(1)} MB → ${got.toFixed(2)} MB${got > target * 1.03 ? ` (a little above ${target} MB: this was the smallest possible)` : ''}`);
    }
    // Ogg pages around Opus packets (so the result is a normal .ogg audio file)
    const OGG_CRC = (() => {
        const t = new Uint32Array(256);
        for (let i = 0; i < 256; i++) { let r = i << 24; for (let j = 0; j < 8; j++) r = (r & 0x80000000) ? ((r << 1) ^ 0x04c11db7) : (r << 1); t[i] = r >>> 0; }
        return t;
    })();
    function oggOpusFile(packets, channels, inputRate, preSkip) {
        const serial = (Math.random() * 0xffffffff) >>> 0;
        const pages = [];
        let seq = 0;
        const page = (segs, granule, flags) => {
            const lacing = [];
            segs.forEach((p) => { let n = p.length; while (n >= 255) { lacing.push(255); n -= 255; } lacing.push(n); });
            const size = segs.reduce((t, p) => t + p.length, 0);
            const buf = new Uint8Array(27 + lacing.length + size);
            const dv = new DataView(buf.buffer);
            buf.set([0x4f, 0x67, 0x67, 0x53], 0); // OggS
            buf[5] = flags;
            dv.setUint32(6, granule % 4294967296, true); dv.setUint32(10, Math.floor(granule / 4294967296), true);
            dv.setUint32(14, serial, true); dv.setUint32(18, seq++, true);
            buf[26] = lacing.length;
            buf.set(lacing, 27);
            let o = 27 + lacing.length;
            segs.forEach((p) => { buf.set(p, o); o += p.length; });
            let crc = 0;
            for (let i = 0; i < buf.length; i++) crc = ((crc << 8) ^ OGG_CRC[((crc >>> 24) ^ buf[i]) & 0xff]) >>> 0;
            dv.setUint32(22, crc, true);
            pages.push(buf);
        };
        const head = new Uint8Array(19);
        head.set(new TextEncoder().encode('OpusHead'), 0);
        head[8] = 1; head[9] = channels;
        new DataView(head.buffer).setUint16(10, preSkip, true);
        new DataView(head.buffer).setUint32(12, inputRate, true);
        page([head], 0, 2);
        const vendor = new TextEncoder().encode('Remmin');
        const tags = new Uint8Array(8 + 4 + vendor.length + 4);
        tags.set(new TextEncoder().encode('OpusTags'), 0);
        new DataView(tags.buffer).setUint32(8, vendor.length, true);
        tags.set(vendor, 12);
        page([tags], 0, 0);
        let granule = preSkip, segs = [], lace = 0;
        packets.forEach((pk, i) => {
            const need = Math.floor(pk.data.length / 255) + 1;
            if (segs.length && lace + need > 255) { page(segs, granule, 0); segs = []; lace = 0; }
            segs.push(pk.data); lace += need; granule += pk.samples;
            if (i === packets.length - 1) page(segs, granule, 4);
        });
        if (!packets.length) page([], granule, 4);
        return new Blob(pages, { type: 'audio/ogg' });
    }
    // fast: decode the whole file, encode it to Opus with WebCodecs (much faster than playing it)
    async function compressAudioFast(url, targetBytes, label, fixedBps = 0) {
        if (!window.AudioEncoder || !window.AudioData || !window.OfflineAudioContext) return null;
        const ui = compressProgress(`Compressing ${label}…`, () => {});
        try {
            const raw = await (await fetch(url)).arrayBuffer();
            const ac = new OfflineAudioContext(1, 1, 48000);
            const decoded = await ac.decodeAudioData(raw);
            const dur = decoded.duration || 1;
            let bps = fixedBps || Math.round(Math.max(12000, Math.min(128000, (targetBytes * 8 * 0.9) / dur)));
            const channels = bps < 48000 ? 1 : Math.min(2, decoded.numberOfChannels);
            // AAC first (plays on every phone, also iPhones); Opus (.ogg) when this browser can't make AAC
            let cfg = { codec: 'mp4a.40.2', sampleRate: 48000, numberOfChannels: channels, bitrate: Math.max(32000, bps), aac: { format: 'adts' } };
            let sup = await AudioEncoder.isConfigSupported(cfg).catch(() => null);
            const aac = !!(sup && sup.supported);
            if (!aac) {
                if (!new Audio().canPlayType('audio/ogg; codecs=opus')) return null;
                cfg = { codec: 'opus', sampleRate: 48000, numberOfChannels: channels, bitrate: bps };
                sup = await AudioEncoder.isConfigSupported(cfg).catch(() => null);
                if (!sup || !sup.supported) return null;
            }
            ui.set(0.15, `Compressing ${label}… (reading)`);
            // to 48 kHz (what Opus uses)
            const off = new OfflineAudioContext(channels, Math.ceil(dur * 48000), 48000);
            const srcNode = off.createBufferSource();
            srcNode.buffer = decoded;
            srcNode.connect(off.destination);
            srcNode.start();
            const pcm = await off.startRendering();
            const packets = [];
            let failed = null;
            const enc = new AudioEncoder({
                output: (chunk) => {
                    const data = new Uint8Array(chunk.byteLength);
                    chunk.copyTo(data);
                    packets.push({ data, samples: Math.round((chunk.duration || 20000) * 48 / 1000) });
                },
                error: (e) => { failed = e; }
            });
            enc.configure(cfg);
            const total = pcm.length, step = 48000;
            for (let at = 0; at < total; at += step) {
                const n = Math.min(step, total - at);
                const planes = new Float32Array(n * channels);
                for (let c = 0; c < channels; c++) planes.set(pcm.getChannelData(c).subarray(at, at + n), c * n);
                enc.encode(new AudioData({ format: 'f32-planar', sampleRate: 48000, numberOfFrames: n, numberOfChannels: channels, timestamp: Math.round(at / 48 * 1000), data: planes }));
                if (enc.encodeQueueSize > 20) await new Promise((r) => setTimeout(r, 0));
                if ((at / step) % 20 === 0) { ui.set(0.15 + 0.8 * at / total, `Compressing ${label}… ${Math.round(at / total * 100)}%`); await new Promise((r) => setTimeout(r, 0)); }
            }
            await enc.flush();
            enc.close();
            if (failed || !packets.length) return null;
            const out = await blobToDataUrl(aac ? new Blob(packets.map((pk) => pk.data), { type: 'audio/aac' }) : oggOpusFile(packets, channels, decoded.sampleRate, 312));
            return /^data:audio\//.test(out) ? out : null;
        } catch (_) {
            return null;
        } finally { ui.close(); }
    }
    // audio: fast with WebCodecs when the browser can; otherwise played once (silently) into a smaller recording
    async function compressAudioUrl(url, { targetBytes, label = 'audio', bps = 0 }) {
        const fast = await compressAudioFast(url, targetBytes, label, bps);
        if (fast) return fast.replace(/^data:audio\/ogg;/, 'data:audio/ogg;');
        return compressAudioSlow(url, { targetBytes, label, fixedBps: bps });
    }
    async function compressAudioSlow(url, { targetBytes, label = 'audio', fixedBps = 0 }) {
        const mime = ['audio/webm;codecs=opus', 'audio/ogg;codecs=opus', 'audio/webm', 'audio/mp4']
            .find((t) => window.MediaRecorder && MediaRecorder.isTypeSupported(t));
        if (!mime) throw new Error('unsupported');
        const a = document.createElement('audio');
        a.preload = 'auto';
        a.src = url;
        await new Promise((resolve, reject) => { a.onloadedmetadata = resolve; a.onerror = () => reject(new Error('cannot play')); });
        const dur = Number.isFinite(a.duration) ? a.duration : 0;
        const bps = fixedBps || Math.round(Math.max(12000, Math.min(128000, dur ? (targetBytes * 8 * 0.9) / dur : 64000)));
        const ac = new (window.AudioContext || window.webkitAudioContext)();
        const node = ac.createMediaElementSource(a);
        const dest = ac.createMediaStreamDestination();
        node.connect(dest);
        await ac.resume();
        const rec = new MediaRecorder(dest.stream, { mimeType: mime, audioBitsPerSecond: bps });
        const chunks = [];
        rec.ondataavailable = (e) => { if (e.data && e.data.size) chunks.push(e.data); };
        const stopped = new Promise((resolve) => { rec.onstop = resolve; });
        let done = false, cancelled = false;
        const ui = compressProgress(`Compressing ${label}…`, () => { cancelled = true; finish(); });
        function finish() { if (done) return; done = true; a.pause(); if (rec.state !== 'inactive') rec.stop(); }
        a.ontimeupdate = () => { if (dur) ui.set(a.currentTime / dur, `Compressing ${label}… ${Math.round(a.currentTime / dur * 100)}% (keep this tab open)`); };
        a.onended = finish;
        try {
            rec.start(1000);
            await a.play();
            await stopped;
        } finally {
            ui.close();
            ac.close().catch(() => {});
        }
        if (cancelled) throw new Error('cancelled');
        const out = await blobToDataUrl(new Blob(chunks, { type: mime.split(';')[0] }));
        if (!/^data:audio\//.test(out)) throw new Error('failed');
        return out;
    }
    // re-record a WebM video as MP4 (about the same quality), everywhere it is used; can be undone
    async function convertToMp4(ref) {
        const id = ref.slice(6);
        const before = state.media[id];
        if (!before) return;
        let out;
        try {
            const url = await mediaUrl(ref);
            const dur = await new Promise((res) => { const v = document.createElement('video'); v.preload = 'metadata'; v.onloadedmetadata = () => res(v.duration); v.onerror = () => res(0); v.src = url; });
            const rate = Number.isFinite(dur) && dur > 0 ? Math.max(500000, Math.min(6000000, (before.length * 0.75 * 8) / dur)) : 2500000;
            out = await compressVideoUrl(url, { maxH: 1080, bitrate: rate, label: 'video to MP4' });
        } catch (err) { toast(err && err.message === 'cancelled' ? 'Converting stopped' : 'Could not convert this video'); return; }
        if (!/^data:video\/mp4/.test(out)) { toast('This browser cannot make MP4 videos (try Chrome or Edge)'); return; }
        keepOriginal(id, before);
        state.media[id] = out;
        scheduleSaveMedia();
        refreshMediaPlayers(id);
        markChanged();
        scheduleGithubSave();
        toast(`Converted to MP4 (${mbOf(out).toFixed(1)} MB): it now plays on phones too`);
    }
    // compress a video that is already in a note (all its places get the smaller version)
    async function compressMedia(ref, preset) {
        const id = ref.slice(6);
        const before = state.media[id];
        if (!before) return;
        let out;
        try { out = await compressVideoUrl(await mediaUrl(ref), { ...preset, label: 'video' }); }
        catch (err) { toast(err && err.message === 'cancelled' ? 'Compressing stopped' : err && err.message === 'unsupported' ? 'This browser cannot compress videos' : 'Could not compress this video'); return; }
        if (out.length >= before.length * 0.95) { toast(`Not smaller after compressing (${mbOf(before).toFixed(1)} MB): kept the original`); return; }
        keepOriginal(id, before);
        state.media[id] = out;
        scheduleSaveMedia();
        refreshMediaPlayers(id);
        markChanged();
        scheduleGithubSave();
        toast(`Video compressed: ${mbOf(before).toFixed(1)} MB → ${mbOf(out).toFixed(1)} MB`);
    }

    /* ---- compressing a picture (Text, Collage, Draw): smaller size and/or lower quality ---- */
    const IMG_PRESETS = [
        { label: 'Good quality (max 1920 px)', maxDim: 1920, quality: 0.85 },
        { label: 'Smaller (max 1280 px)', maxDim: 1280, quality: 0.75 },
        { label: 'Smallest (max 800 px)', maxDim: 800, quality: 0.6 }
    ];
    const IMG_ORIG_KEY = 'remmin.imgOrig.v1';
    let imgOrig = null; // hash of the compressed picture → { data, at } (originals, only on this device, 7 days)
    function imgOrigs() {
        if (!imgOrig) {
            imgOrig = {};
            const saved = readJSON(IMG_ORIG_KEY, {});
            if (saved && typeof saved === 'object') Object.entries(saved).forEach(([k, o]) => {
                if (o && typeof o.data === 'string' && Date.now() - (o.at || 0) < 7 * 86400000) imgOrig[k] = o;
            });
        }
        return imgOrig;
    }
    const sizeOfData = (d) => Math.round((d || '').length * 0.75);
    const isPictureData = (src) => typeof src === 'string' && /^data:image\/(?:png|jpe?g|webp|bmp|avif)/i.test(src.slice(0, 30));
    function loadImg(src) {
        return new Promise((resolve, reject) => { const im = new Image(); im.onload = () => resolve(im); im.onerror = () => reject(new Error('bad picture')); im.src = src; });
    }
    function hasAlpha(im) {
        const c = document.createElement('canvas');
        const w = c.width = Math.min(64, im.naturalWidth), h = c.height = Math.min(64, im.naturalHeight);
        const g = c.getContext('2d');
        g.drawImage(im, 0, 0, w, h);
        const d = g.getImageData(0, 0, w, h).data;
        for (let i = 3; i < d.length; i += 4) if (d[i] < 250) return true;
        return false;
    }
    function encodeImg(im, maxDim, quality, alpha) {
        const scale = Math.min(1, maxDim / Math.max(im.naturalWidth, im.naturalHeight));
        const c = document.createElement('canvas');
        c.width = Math.max(1, Math.round(im.naturalWidth * scale));
        c.height = Math.max(1, Math.round(im.naturalHeight * scale));
        const g = c.getContext('2d');
        if (!alpha) { g.fillStyle = '#fff'; g.fillRect(0, 0, c.width, c.height); }
        g.drawImage(im, 0, 0, c.width, c.height);
        const webp = c.toDataURL('image/webp', quality);
        if (/^data:image\/webp/.test(webp)) return webp;
        return alpha ? c.toDataURL('image/png') : c.toDataURL('image/jpeg', quality);
    }
    // preset: { maxDim, quality } or { targetBytes }
    async function compressPictureData(src, preset) {
        const im = await loadImg(src);
        const alpha = hasAlpha(im);
        if (!preset.targetBytes) return encodeImg(im, preset.maxDim, preset.quality, alpha);
        // to a size: lower the quality first, then the size, until it fits
        let dim = Math.max(im.naturalWidth, im.naturalHeight);
        let best = null;
        for (let round = 0; round < 8; round++) {
            for (const q of [0.85, 0.7, 0.55, 0.4]) {
                const out = encodeImg(im, dim, q, alpha);
                if (!best || out.length < best.length) best = out;
                if (sizeOfData(out) <= preset.targetBytes) return out;
            }
            dim = Math.max(32, Math.round(dim * 0.75));
        }
        return best;
    }
    // compress one picture; place(newSrc) puts the result where the picture is
    async function compressPicture(src, preset, place) {
        if (!isPictureData(src)) { toast(/^data:image\/gif/i.test(src || '') ? 'GIFs are not compressed (they would stop moving)' : 'This picture cannot be compressed'); return; }
        let out;
        try { out = await compressPictureData(src, preset); } catch (_) { toast('Could not compress this picture'); return; }
        if (!out || out.length >= src.length * 0.97) { toast(`Not smaller after compressing: kept the original (${fmtSize(sizeOfData(src))})`); return; }
        const origs = imgOrigs();
        const key = mediaHash(out);
        origs[key] = { data: (origs[mediaHash(src)] || {}).data || src, at: Date.now() }; // after compressing twice: still the first original
        writeJSON(IMG_ORIG_KEY, origs);
        place(out);
        const t = preset.targetBytes;
        toast(`Picture compressed: ${fmtSize(sizeOfData(src))} → ${fmtSize(sizeOfData(out))}${t && sizeOfData(out) > t ? ' (the smallest possible)' : ''}`);
    }
    /* ---- Ctrl+C / Ctrl+V of pictures, videos and audio between Text, Collage and Draw (several at once) ----
     * The items go on the clipboard as JSON (own type), the pictures also as html for other programs. */
    const ITEMS_TYPE = 'application/x-remmin-items';
    const ITEMS_MARK = '[Remmin items]';
    function cleanClipItem(it) {
        if (!it || typeof it !== 'object' || typeof it.src !== 'string') return null;
        if (it.kind === 'image' && /^data:image\//i.test(it.src.slice(0, 20))) return { kind: 'image', src: it.src };
        if (it.kind === 'video' && isVideoSrc(it.src)) {
            const w = Math.round(Number(it.w));
            return { kind: 'video', src: it.src, loop: !!it.loop, ...(w >= 10 && w <= 100 ? { w } : {}) };
        }
        if (it.kind === 'audio' && isVoiceSrc(it.src)) {
            const vol = Math.round(Number(it.vol));
            return { kind: 'audio', src: it.src, name: String(it.name || 'Voice note').slice(0, 200), duration: Number(it.duration) || 0, loop: !!it.loop, vol: Number.isFinite(vol) && vol >= 0 && vol <= 100 ? vol : 100 };
        }
        return null;
    }
    let lastClip = null; // what Remmin copied last (the Paste button can't read Remmin's own clipboard type)
    function writeClipItems(e, items, plain = ITEMS_MARK) {
        const list = items.map(cleanClipItem).filter(Boolean);
        if (!list.length || !e.clipboardData) return false;
        lastClip = { list, plain };
        e.clipboardData.setData(ITEMS_TYPE, JSON.stringify(list));
        const pics = list.filter((it) => it.kind === 'image');
        if (pics.length) e.clipboardData.setData('text/html', pics.map((it) => `<img src="${it.src.replace(/"/g, '&quot;')}">`).join(''));
        e.clipboardData.setData('text/plain', plain);
        return true;
    }
    // the copied items; fromHtml: also pictures / videos / audio in html copied from the text (when there is no other text)
    function readClipItems(e, fromHtml = false) {
        const dt = e.clipboardData;
        if (!dt) return [];
        const raw = dt.getData(ITEMS_TYPE);
        if (raw) {
            try { const list = JSON.parse(raw); return Array.isArray(list) ? list.map(cleanClipItem).filter(Boolean) : []; } catch (_) { return []; }
        }
        if (!fromHtml) return [];
        const html = dt.getData('text/html');
        if (!html || !/data-video=|data-voice=|<img/i.test(html)) return [];
        const doc = new DOMParser().parseFromString(html, 'text/html');
        const out = [];
        doc.body.querySelectorAll('img, [data-video], span[data-voice]').forEach((el) => {
            if (el.matches('img')) { if (!el.closest('[data-video]')) out.push({ kind: 'image', src: el.getAttribute('src') || '' }); }
            else if (el.hasAttribute('data-video')) out.push({ kind: 'video', src: el.getAttribute('data-video'), loop: el.getAttribute('data-loop') === '1', w: el.getAttribute('data-w') });
            else out.push({ kind: 'audio', src: el.getAttribute('data-voice'), name: el.getAttribute('data-name'), duration: el.getAttribute('data-duration'), loop: el.getAttribute('data-loop') === '1', vol: el.getAttribute('data-vol') === null ? 100 : el.getAttribute('data-vol') });
            el.remove();
        });
        if (doc.body.textContent.replace(/[\s\u00a0⠿▶⏸⏪⏩🔁🔊✕🎤]/g, '').replace(/\d+:\d+/g, '').trim()) return []; // there is text too: the normal paste
        return out.map(cleanClipItem).filter(Boolean);
    }
    // paste the items in the text (at the cursor, in order)
    async function pasteItemsInText(items) {
        for (const it of items) {
            if (it.kind === 'image') await insertImageFiles([it.src]);
            else if (it.kind === 'video') {
                const box = insertVideoInText(it.src);
                if (box) {
                    if (it.loop && isMediaRef(it.src)) { box.setAttribute('data-loop', '1'); const v = box.querySelector('video'); if (v) v.loop = true; }
                    if (it.w) { box.setAttribute('data-w', String(it.w)); box.style.width = `${it.w}%`; }
                }
            } else insertVoiceChip(it.src, it.name, it.duration, it.loop, it.vol);
        }
        markTextChanged();
    }

    // copy a picture so it can be pasted in Text, Collage or Draw
    const PIC_MARK = 'remmin-picture-copy';
    const picCopyHtml = (src) => `<img data-remmin-pic="1" src="${src.replace(/"/g, '&quot;')}">`;
    // the picture as a PNG (what Paint, Discord, Word… understand)
    async function pictureToPng(src) {
        const img = new Image();
        img.src = src;
        await img.decode();
        const c = document.createElement('canvas');
        c.width = img.naturalWidth || 1;
        c.height = img.naturalHeight || 1;
        c.getContext('2d').drawImage(img, 0, 0);
        return new Promise((resolve, reject) => c.toBlob((b) => (b ? resolve(b) : reject(new Error('png'))), 'image/png'));
    }
    // copy a picture: Remmin pastes the picture itself (no second copy is stored); other apps get the real picture
    function copyPictureSrc(src, quiet = false) {
        const old = () => {
            const onCopy = (e) => {
                e.clipboardData.setData('text/html', picCopyHtml(src));
                e.clipboardData.setData('text/plain', PIC_MARK);
                e.preventDefault();
                e.stopImmediatePropagation();
            };
            document.addEventListener('copy', onCopy, true);
            let ok = false;
            try { ok = document.execCommand('copy'); } catch (_) { ok = false; }
            document.removeEventListener('copy', onCopy, true);
            if (!quiet) toast(ok ? 'Picture copied: paste it (Ctrl+V) in Text, Collage or Draw' : 'Could not copy the picture');
        };
        if (!(window.ClipboardItem && navigator.clipboard && navigator.clipboard.write) || !/^data:image\//.test(src)) { if (!quiet) old(); return; }
        navigator.clipboard.write([new ClipboardItem({
            'image/png': pictureToPng(src),
            'text/html': new Blob([picCopyHtml(src)], { type: 'text/html' })
        })]).then(() => { if (!quiet) toast('Picture copied: paste it in Text, Collage or Draw, or in another app (Paint, Discord…)'); },
            () => { if (!quiet) old(); });
    }
    /* ---- the Paste button: reads the clipboard and pastes it like Ctrl+V (phones have no Ctrl+V) ---- */
    async function clipboardAsTransfer() {
        const dt = new DataTransfer();
        let got = false;
        try {
            for (const item of await navigator.clipboard.read()) {
                for (const type of item.types) {
                    const blob = await item.getType(type);
                    if (type === 'text/plain' || type === 'text/html') { dt.setData(type, await blob.text()); got = true; }
                    else if (/^(image|audio|video)\//.test(type)) { dt.items.add(new File([blob], `pasted.${type.split('/')[1].replace('jpeg', 'jpg')}`, { type })); got = true; }
                }
            }
        } catch (err) {
            try { const t = await navigator.clipboard.readText(); if (t) { dt.setData('text/plain', t); got = true; } } catch (_) { /* not allowed */ }
        }
        if (!got) return null;
        if (lastClip && dt.getData('text/plain') === lastClip.plain) dt.setData(ITEMS_TYPE, JSON.stringify(lastClip.list));
        return dt;
    }
    async function pasteButton() {
        const mode = state.prefs.mode;
        if (!noteById(state.currentNoteId) || isPrivateLocked(noteById(state.currentNoteId))) return;
        const sel = window.getSelection();
        const range = sel && sel.rangeCount ? sel.getRangeAt(0).cloneRange() : null;
        if (!navigator.clipboard) { toast('This browser does not let websites read the clipboard: use Ctrl+V'); return; }
        const dt = await clipboardAsTransfer();
        if (!dt) { toast('Nothing to paste (or the browser did not allow reading the clipboard)'); return; }
        let target = document.body;
        if (mode === 'text') {
            editorEl.focus({ preventScroll: true });
            if (range && editorEl.contains(range.startContainer)) { sel.removeAllRanges(); sel.addRange(range); }
            target = editorEl;
        } else if (mode === 'collage' && cb.editing) {
            const el = cbEls.get(cb.editing);
            const t = el && el.querySelector('.cb-text');
            if (t) { t.focus({ preventScroll: true }); if (range && t.contains(range.startContainer)) { sel.removeAllRanges(); sel.addRange(range); } target = t; }
        }
        const ev = new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true });
        if (!ev.clipboardData) Object.defineProperty(ev, 'clipboardData', { value: dt });
        target.dispatchEvent(ev);
        // plain text that nobody handled: type it in
        const text = dt.getData('text/plain');
        if (!ev.defaultPrevented && text && target !== document.body) document.execCommand('insertText', false, text);
    }
    // a picture copied with Remmin's Copy (it also carries a PNG: use the original picture instead)
    function ownPictureCopy(e) {
        const dt = e.clipboardData;
        if (!dt) return [];
        const html = dt.getData('text/html');
        return /data-remmin-pic=/.test(html || '') || dt.getData('text/plain') === PIC_MARK ? pastedPictureSrcs(html) : [];
    }
    // all data: pictures in pasted html
    const pastedPictureSrcs = (html) => [...String(html || '').matchAll(/<img[^>]+src="(data:image\/[^"]+)"/gi)].map((m) => m[1].replace(/&quot;/g, '"'));
    // the options of a picture: compress / undo, download, remove
    function pictureMenuActions(src, { place, remove, x, y }) {
        const items = [{ label: 'Copy picture', icon: '⧉', run: () => copyPictureSrc(src) }];
        const now = fmtSize(sizeOfData(src));
        if (isPictureData(src)) {
            items.push({
                label: `Compress picture… (now ${now})`, icon: '🗜',
                run: () => setTimeout(() => menuAt(x != null ? x : parseFloat(pointAnchor.style.left) || 200, y != null ? y : parseFloat(pointAnchor.style.top) || 200, 'Compress to', [
                    { label: `Compress to … KB (now ${now})`, icon: '✎', run: async () => {
                        const cur = sizeOfData(src) / 1024;
                        const answer = await askDialog({ title: 'Compress to … KB', message: `How many KB may the picture be? Now it is ${now}; enter a smaller number (1000 KB = about 1 MB).`, input: String(Math.max(10, Math.round(cur / 2))), okLabel: 'Compress' });
                        if (answer === null || answer === false) return;
                        const kb = parseFloat(String(answer).replace(',', '.'));
                        if (!Number.isFinite(kb) || kb <= 0) { toast('Enter a number of KB, for example 300'); return; }
                        if (kb >= cur) { toast(`Enter a number below ${Math.round(cur)} KB`); return; }
                        compressPicture(src, { targetBytes: kb * 1024 }, place);
                    } },
                    ...IMG_PRESETS.map((pr) => ({ label: pr.label, icon: '🗜', run: () => compressPicture(src, pr, place) }))
                ]), 0)
            });
            const orig = imgOrigs()[mediaHash(src)];
            if (orig) {
                items.push({ label: `Undo compression (back to ${fmtSize(sizeOfData(orig.data))})`, icon: '↺', run: () => {
                    delete imgOrigs()[mediaHash(src)];
                    writeJSON(IMG_ORIG_KEY, imgOrigs());
                    place(orig.data);
                    toast('Compression undone');
                } });
                items.push({ label: 'Keep this size (forget the original)', icon: '✔', run: () => {
                    delete imgOrigs()[mediaHash(src)];
                    writeJSON(IMG_ORIG_KEY, imgOrigs());
                    toast(`Kept at ${fmtSize(sizeOfData(src))} (the original is removed)`);
                } });
            }
        } else if (/^data:image\/gif/i.test(src || '')) items.push({ label: 'GIFs cannot be compressed (they would stop moving)', icon: 'ℹ', run: () => {} });
        items.push({ label: 'Download picture', icon: '⬇', run: () => {
            const a = document.createElement('a');
            a.href = src;
            a.download = `picture.${((/^data:image\/([\w+]+)/.exec(src) || [])[1] || 'png').replace('jpeg', 'jpg').replace('svg+xml', 'svg')}`;
            if (!/^data:/.test(src)) a.target = '_blank';
            document.body.appendChild(a); a.click(); a.remove();
        } });
        if (remove) items.push({ label: 'Remove picture', icon: '🗑', danger: true, run: remove });
        return items;
    }

    /* ---- copy a video to paste it in Text, Collage or Draw (also in another note) ---- */
    const VIDEO_COPY_RE = /^\[Remmin video\] (\S+)$/;
    function copyVideo(src) {
        const text = `[Remmin video] ${src}`;
        const done = () => toast('Video copied: paste it (Ctrl+V) in Text, Collage or Draw, in any note');
        if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done, () => toast('Could not copy the video'));
        else toast('Could not copy the video');
    }
    function pastedVideoRef(e) {
        const t = e.clipboardData ? e.clipboardData.getData('text/plain').trim() : '';
        const m = VIDEO_COPY_RE.exec(t);
        return m && isVideoSrc(m[1]) ? m[1] : null;
    }
    function downloadVideo(ref) {
        mediaUrl(ref).then((url) => {
            const a = document.createElement('a');
            a.href = url;
            a.download = `video.${/^data:video\/webm/.test(mediaData(ref)) ? 'webm' : 'mp4'}`;
            document.body.appendChild(a); a.click(); a.remove();
        }).catch(() => toast('This video is not on this device yet'));
    }
    // a menu at a point on the screen (right-click)
    const pointAnchor = document.createElement('span');
    pointAnchor.className = 'point-anchor';
    document.body.appendChild(pointAnchor);
    function menuAt(x, y, title, actions) {
        pointAnchor.style.left = `${x}px`;
        pointAnchor.style.top = `${y}px`;
        closeMenu();
        showMenu(pointAnchor, title, actions);
    }

    /* ---- a small bar with the options of what you clicked (picture, video, audio, sticky note…), right above it ----
     * It lives inside the window's toolbar (so the toolbar's own buttons keep working in it) but floats on the screen. */
    function rectOf(els) {
        let r = null;
        els.forEach((el) => {
            if (!el || !el.isConnected) return;
            const b = el.getBoundingClientRect();
            if (!b.width && !b.height) return;
            r = r ? { left: Math.min(r.left, b.left), top: Math.min(r.top, b.top), right: Math.max(r.right, b.right), bottom: Math.max(r.bottom, b.bottom) }
                : { left: b.left, top: b.top, right: b.right, bottom: b.bottom };
        });
        if (r) { r.width = r.right - r.left; r.height = r.bottom - r.top; }
        return r;
    }
    function makeCtxBar(toolbar, groups = []) {
        const bar = document.createElement('div');
        bar.className = 'ctx-bar';
        bar.hidden = true;
        bar.innerHTML = '<div class="ctx-slots"></div><div class="ctx-acts"></div>';
        toolbar.appendChild(bar);
        const slots = bar.firstChild, acts = bar.lastChild;
        const park = document.createElement('div');
        park.hidden = true;
        toolbar.appendChild(park);
        groups.forEach((g) => park.appendChild(g));
        let anchor = null, actions = [], key = null, raf = 0, busy = null, after = null;
        ['pointerdown', 'mousedown'].forEach((t) => bar.addEventListener(t, (e) => { if (e.target.closest('button')) e.preventDefault(); }));
        acts.addEventListener('click', (e) => {
            const b = e.target.closest('[data-ai]');
            if (!b) return;
            e.stopPropagation();
            const a = actions[Number(b.dataset.ai)];
            if (!a) return;
            const r = b.getBoundingClientRect();
            pointAnchor.style.left = `${r.left}px`;
            pointAnchor.style.top = `${r.bottom}px`;
            closeMenu();
            key = null; // its labels may change (loop on / off…)
            a.run();
            if (after) setTimeout(after, 0);
        });
        function place() {
            if (bar.hidden) { raf = 0; return; }
            raf = requestAnimationFrame(place);
            const r = anchor && anchor();
            const off = !r || (busy && busy()) || toolbar.offsetParent === null || r.bottom < 0 || r.top > window.innerHeight;
            bar.style.visibility = off ? 'hidden' : '';
            if (off) return;
            bar.style.left = '0px';
            bar.style.top = '0px';
            const o = bar.getBoundingClientRect();
            const tb = toolbar.getBoundingClientRect();
            const minTop = Math.max(8, tb.bottom + 6);
            const x = Math.max(8, Math.min(window.innerWidth - o.width - 8, r.left + r.width / 2 - o.width / 2));
            let y = r.top - o.height - 8;
            if (y < minTop) y = r.bottom + 8;
            if (y + o.height > window.innerHeight - 8) y = Math.max(minTop, Math.min(r.top + 8, window.innerHeight - o.height - 8));
            bar.style.left = `${x - o.left}px`;
            bar.style.top = `${y - o.top}px`;
        }
        function renderActs(list) {
            const count = {};
            list.forEach((a) => { count[a.icon || ''] = (count[a.icon || ''] || 0) + 1; });
            acts.innerHTML = list.map((a, i) => {
                const words = a.text ? `<span class="ctx-txt">${escapeHtml(a.text)}</span>` : !a.icon || count[a.icon] > 1 ? `<span class="ctx-txt">${escapeHtml(a.label.replace(/\s*\(.*?\)/g, '').replace(/…/g, '').replace(/^Size /, '').trim())}</span>` : '';
                return `<button type="button" class="tool-btn ctx-act${a.danger ? ' danger' : ''}" data-ai="${i}" title="${escapeHtml(a.label)}" aria-label="${escapeHtml(a.label)}">${a.icon ? `<span class="ctx-ico">${a.icon}</span>` : ''}${words}</button>`;
            }).join('');
        }
        return {
            show(o) {
                anchor = o.anchor; busy = o.busy || null; after = o.after || null;
                if (o.key !== key || bar.hidden) {
                    key = o.key;
                    const want = o.groups || [];
                    [...slots.children].forEach((g) => { if (!want.includes(g)) park.appendChild(g); });
                    want.forEach((g) => slots.appendChild(g));
                    actions = o.actions ? o.actions() : [];
                    renderActs(actions);
                }
                if (bar.hidden) { bar.hidden = false; bar.style.visibility = 'hidden'; }
                if (!raf) raf = requestAnimationFrame(place);
            },
            hide() {
                if (bar.hidden) return;
                bar.hidden = true;
                key = null; anchor = null;
            }
        };
    }
    // "+ Add": one button with the ways to add something
    function addMenu(btn, items) {
        btn.addEventListener('click', () => {
            const sel = window.getSelection();
            const range = sel && sel.rangeCount && editorEl.contains(sel.anchorNode) ? sel.getRangeAt(0).cloneRange() : null;
            showMenu(btn, 'Add', items.map(([label, icon, id]) => ({ label, icon, run: () => {
                if (range) { editorEl.focus({ preventScroll: true }); sel.removeAllRanges(); sel.addRange(range); }
                $(id).click();
            } })));
        });
    }
    // the same video options in Text, Collage and Draw
    function videoMenuActions(src, { remove, sizes, sizeNow, sound, playing, loop } = {}) {
        const items = [{ label: 'Copy video', icon: '⧉', run: () => copyVideo(src) }];
        if (isMediaRef(src)) {
            items.push({ label: 'Download video', icon: '⬇', run: () => downloadVideo(src) });
            const data = mediaData(src);
            items.push({
                label: `Compress video…${data ? ` (now ${mbOf(data).toFixed(1)} MB)` : ''}`, icon: '🗜',
                run: () => setTimeout(() => menuAt(parseFloat(pointAnchor.style.left) || 200, parseFloat(pointAnchor.style.top) || 200, 'Compress to',
                    [{ label: `Compress to … MB${data ? ` (now ${mbOf(data).toFixed(1)} MB)` : ''}`, icon: '✎', run: () => compressMediaTo(src) },
                        ...COMPRESS_PRESETS.map((pr) => ({ label: pr.label, icon: '🗜', run: () => compressMedia(src, pr) }))]), 0)
            });
            if (data && !/^data:video\/mp4/.test(data) && window.MediaRecorder && MediaRecorder.isTypeSupported('video/mp4')) {
                items.push({ label: 'Convert to MP4 (plays on every phone)', icon: '📱', run: () => convertToMp4(src) });
            }
            const orig = state.mediaOrig && state.mediaOrig[src.slice(6)];
            if (orig) {
                items.push({ label: `Undo compression (back to ${mbOf(orig.data).toFixed(1)} MB)`, icon: '↺', run: () => undoCompression(src) });
                items.push({ label: 'Keep this size (forget the original)', icon: '✔', run: () => keepCompressed(src) });
            }
        } else items.push({ label: 'Open on the website', icon: '↗', run: () => window.open(videoWatchUrl(src), '_blank', 'noopener') });
        if (sizes) [[25, '1/4'], [50, '1/2'], [75, '3/4'], [100, 'Full width']].forEach(([w, name]) =>
            items.push({ label: `Size ${name}${sizeNow === w ? ' ✓' : ''}`, icon: '↔', run: () => sizes(w) }));
        if (playing) items.push({ label: playing.on ? 'Pause' : 'Play', icon: playing.on ? '⏸' : '▶', run: playing.toggle });
        if (loop && isMediaRef(src)) items.push({ label: loop.on ? 'Loop off' : 'Loop on (play again and again)', icon: '🔁', run: loop.toggle });
        if (sound) items.push({ label: sound.on ? 'Sound off' : 'Sound on', icon: sound.on ? '🔇' : '🔊', run: sound.toggle });
        if (remove) items.push({ label: 'Remove video', icon: '🗑', danger: true, run: remove });
        return items;
    }
    const DROP_TAGS = new Set(['SCRIPT', 'STYLE', 'IFRAME', 'OBJECT', 'EMBED', 'TEMPLATE', 'META', 'LINK', 'TITLE', 'svg', 'SVG']);

    // "Default colour": the normal (white) text colour of the notes
    const COLOR_MARK = '#010203';
    let plainTextHexCache = '';
    function plainTextHex() {
        if (!plainTextHexCache) {
            const ed = document.getElementById('noteContent');
            plainTextHexCache = ed ? cssColorToHex(getComputedStyle(ed).color) : '';
        }
        return plainTextHexCache;
    }

    // Keep only safe, simple formatting + embedded (data:) images
    function sanitizeHtml(html) {
        const doc = new DOMParser().parseFromString(`<div>${html || ''}</div>`, 'text/html');
        const root = doc.body.firstElementChild || doc.body;
        (function clean(node) {
            [...node.childNodes].forEach((child) => {
                if (child.nodeType === 3) return;
                if (child.nodeType !== 1 || DROP_TAGS.has(child.tagName)) { child.remove(); return; }
                // an embedded YouTube / Vimeo video (only these two players are allowed)
                if (child.tagName === 'DIV' && child.hasAttribute('data-video')) {
                    const src = child.getAttribute('data-video') || '';
                    if (isVideoSrc(src)) child.replaceWith(videoBlock(doc, src, Number(child.getAttribute('data-w')), child.getAttribute('data-loop') === '1')); else child.remove();
                    return;
                }
                clean(child);
                if (!RICH_TAGS.has(child.tagName)) { child.replaceWith(...child.childNodes); return; }
                if (child.tagName === 'SPAN' && child.hasAttribute('data-voice')) {
                    const src = child.getAttribute('data-voice') || '';
                    if (!isVoiceSrc(src)) { child.remove(); return; }
                    const name = (child.getAttribute('data-name') || 'Voice note').slice(0, 200);
                    const dur = String(Math.max(0, Math.round(Number(child.getAttribute('data-duration')) || 0)));
                    const loop = child.getAttribute('data-loop') === '1';
                    const vol = Math.round(Number(child.getAttribute('data-vol')));
                    [...child.attributes].forEach((attr) => child.removeAttribute(attr.name));
                    if (loop) child.setAttribute('data-loop', '1');
                    if (Number.isFinite(vol) && vol >= 0 && vol < 100) child.setAttribute('data-vol', String(vol));
                    child.setAttribute('data-voice', src);
                    child.setAttribute('data-name', name);
                    child.setAttribute('data-duration', dur);
                    child.textContent = `🎤 ${name}`;
                    return;
                }
                // <font color> / <span style="color"> → <span style="color:#..."> (text colour)
                if (child.tagName === 'FONT' || child.tagName === 'SPAN') {
                    const styleColor = /(?:^|;)\s*color\s*:\s*([^;]+)/i.exec(child.getAttribute('style') || '');
                    const color = cssColorToHex(child.tagName === 'FONT' ? child.getAttribute('color') : (styleColor && styleColor[1]));
                    // the normal text colour (or the old "back to normal" marker) needs no colour tag
                    if (!color || color === COLOR_MARK || color === plainTextHex()) { child.replaceWith(...child.childNodes); return; }
                    const span = doc.createElement('span');
                    span.setAttribute('style', `color:${color}`);
                    span.append(...child.childNodes);
                    child.replaceWith(span);
                    return;
                }
                // strikethrough
                if (child.tagName === 'STRIKE' || child.tagName === 'DEL') {
                    const s = doc.createElement('s');
                    s.append(...child.childNodes);
                    child.replaceWith(s);
                    return;
                }
                // links: only web and mail links, always opened in a new tab
                if (child.tagName === 'A') {
                    const href = (child.getAttribute('href') || '').trim();
                    if (!SAFE_LINK_RE.test(href)) { child.replaceWith(...child.childNodes); return; }
                    [...child.attributes].forEach((attr) => child.removeAttribute(attr.name));
                    child.setAttribute('href', href);
                    child.setAttribute('target', '_blank');
                    child.setAttribute('rel', 'noopener noreferrer');
                    return;
                }
                [...child.attributes].forEach((attr) => {
                    const keep = child.tagName === 'IMG' && (attr.name === 'style' || attr.name === 'alt' ||
                        (attr.name === 'src' && (/^data:image\/(png|jpe?g|gif|webp);base64,/i.test(attr.value) || KLIPY_SRC_RE.test(attr.value))));
                    if (!keep) child.removeAttribute(attr.name);
                });
                if (child.tagName === 'IMG') {
                    if (!child.getAttribute('src')) { child.remove(); return; }
                    const m = /width:\s*(\d{1,3}(?:\.\d+)?)%/.exec(child.getAttribute('style') || '');
                    if (m) child.setAttribute('style', `width:${Math.min(100, Number(m[1]))}%`);
                    else child.removeAttribute('style');
                }
            });
        })(root);
        linkifyNode(root);
        return root.innerHTML;
    }

    /* ---- clickable links ---- */
    const URL_RE = /\b((?:https?:\/\/|www\.)[^\s<>"']+)/gi;

    function trimUrl(url) {
        // "see https://x.nl." → leave the final dot/bracket out of the link
        let u = url.replace(/[.,;:!?'"]+$/, '');
        if (u.endsWith(')') && !u.includes('(')) u = u.slice(0, -1);
        return u;
    }

    // Turn plain-text web addresses inside `root` into <a> links
    function linkifyNode(root, skipNode) {
        const doc = root.ownerDocument;
        const walker = doc.createTreeWalker(root, NodeFilter.SHOW_TEXT);
        const targets = [];
        while (walker.nextNode()) {
            const t = walker.currentNode;
            if (t === skipNode || t.parentElement.closest('a, .voice-chip, span[data-voice]')) continue;
            URL_RE.lastIndex = 0;
            if (URL_RE.test(t.data)) targets.push(t);
        }
        targets.forEach((t) => {
            const frag = doc.createDocumentFragment();
            let last = 0;
            t.data.replace(URL_RE, (match, _g, offset) => {
                const url = trimUrl(match);
                frag.append(t.data.slice(last, offset));
                const a = doc.createElement('a');
                a.href = /^www\./i.test(url) ? `https://${url}` : url;
                a.target = '_blank';
                a.rel = 'noopener noreferrer';
                a.textContent = url;
                frag.append(a);
                last = offset + url.length;
                return match;
            });
            frag.append(t.data.slice(last));
            t.replaceWith(frag);
        });
    }

    function textToHtml(text) {
        return escapeHtml(text || '').replace(/\n/g, '<br>');
    }

    // Plain text version (for previews, alarms and search)
    function htmlToText(html) {
        const body = new DOMParser().parseFromString(html || '', 'text/html').body;
        let out = '';
        (function walk(node) {
            node.childNodes.forEach((ch) => {
                if (ch.nodeType === 3) { out += ch.nodeValue; return; }
                if (ch.nodeType !== 1) return;
                if (ch.tagName === 'BR') { out += '\n'; return; }
                if (ch.tagName === 'IMG') { if (out && !out.endsWith('\n')) out += '\n'; return; }
                if (ch.tagName === 'SPAN' && ch.hasAttribute('data-voice')) { out += `🎤 ${ch.getAttribute('data-name') || 'Voice note'}`; return; }
                const block = /^(DIV|P|LI|H1|H2|H3|BLOCKQUOTE|UL|OL)$/.test(ch.tagName);
                if (block && out && !out.endsWith('\n')) out += '\n';
                walk(ch);
                if (block && out && !out.endsWith('\n')) out += '\n';
            });
        })(body);
        return out.replace(/ /g, ' ').replace(/\n{3,}/g, '\n\n').trim();
    }

    function hasVoice(note) {
        return /data-voice=/i.test(note.html || '');
    }

    function hasImages(note) {
        return /<img\b/i.test(note.html || '');
    }

    function markTextChanged() {
        textDirty = true;
        queueSave();
        updateTextUndoButtons();
        if (textCtx) requestAnimationFrame(updateTextCtx);
    }

    /* ---------------- undo / redo in the text ----------------
     * Our own history (the browser's undo can't undo pictures that were added, moved
     * or removed). Typing is grouped per word / pause; every button action is one step. */
    let textUndo = [], textRedo = [];
    let lastTypeAt = 0, lastTypeKind = '';
    function caretOffset() {
        const sel = window.getSelection();
        if (!sel.rangeCount || !editorEl.contains(sel.anchorNode)) return null;
        const r = document.createRange();
        r.selectNodeContents(editorEl);
        r.setEnd(sel.anchorNode, sel.anchorOffset);
        return r.toString().length;
    }
    function setCaretOffset(n) {
        if (n === null || n === undefined) return;
        const walker = document.createTreeWalker(editorEl, NodeFilter.SHOW_TEXT);
        let node, left = n;
        while ((node = walker.nextNode())) {
            if (left <= node.length) {
                const r = document.createRange();
                r.setStart(node, left);
                r.collapse(true);
                const sel = window.getSelection();
                sel.removeAllRanges();
                sel.addRange(r);
                return;
            }
            left -= node.length;
        }
    }
    const textSnap = () => ({ html: editorEl.innerHTML, caret: caretOffset() });
    // call BEFORE changing the text
    function recordText() {
        if (!noteById(state.currentNoteId)) return;
        const snap = textSnap();
        const top = textUndo[textUndo.length - 1];
        if (top && top.html === snap.html) return;
        textUndo.push(snap);
        if (textUndo.length > 200) textUndo.shift();
        textRedo = [];
        updateTextUndoButtons();
    }
    function applyTextSnap(snap) {
        editorEl.innerHTML = snap.html;
        decorateVoiceChips();
        selectImage(null);
        editorEl.focus({ preventScroll: true });
        setCaretOffset(snap.caret);
        lastTypeKind = '';
        markTextChanged();
    }
    function undoText() {
        const prev = textUndo.pop();
        if (!prev) return;
        textRedo.push(textSnap());
        applyTextSnap(prev);
    }
    function redoText() {
        const next = textRedo.pop();
        if (!next) return;
        textUndo.push(textSnap());
        applyTextSnap(next);
    }
    function resetTextHistory() { textUndo = []; textRedo = []; lastTypeKind = ''; updateTextUndoButtons(); }
    function updateTextUndoButtons() {
        if (!$('textUndo')) return;
        $('textUndo').disabled = !textUndo.length;
        $('textRedo').disabled = !textRedo.length;
    }
    editorEl.addEventListener('beforeinput', (e) => {
        if (e.inputType === 'historyUndo' || e.inputType === 'historyRedo') {
            e.preventDefault();
            if (e.inputType === 'historyUndo') undoText(); else redoText();
            return;
        }
        // a new step when you start deleting after typing (or the other way round),
        // after a pause, at a new word or a new line
        const kind = e.inputType.startsWith('delete') ? 'delete' : e.inputType === 'insertText' ? 'type' : e.inputType;
        const now = Date.now();
        if (kind !== lastTypeKind || now - lastTypeAt > 1500 || e.data === ' ' || kind === 'insertParagraph') recordText();
        lastTypeAt = now;
        lastTypeKind = kind;
    });
    editorEl.addEventListener('keydown', (e) => {
        if (!(e.ctrlKey || e.metaKey) || e.altKey) return;
        const key = e.key.toLowerCase();
        if (key === 'z' && !e.shiftKey) { e.preventDefault(); undoText(); }
        else if (key === 'y' || (key === 'z' && e.shiftKey)) { e.preventDefault(); redoText(); }
    });

    editorEl.addEventListener('input', markTextChanged);

    // A web address becomes a link as soon as you type a space or Enter after it
    editorEl.addEventListener('input', (e) => {
        if (!((e.inputType === 'insertText' && e.data === ' ') || e.inputType === 'insertParagraph')) return;
        const sel = window.getSelection();
        if (!sel.rangeCount) return;
        let node = sel.anchorNode;
        // after Enter the caret is on the new line: look at the end of the previous line
        if (e.inputType === 'insertParagraph') {
            const block = (node.nodeType === 1 ? node : node.parentElement).closest('div, p, li');
            const prev = block && block.previousSibling;
            if (!prev) return;
            linkifyNode(prev.nodeType === 3 ? prev.parentNode : prev);
            return;
        }
        if (!node || node.nodeType !== 3 || node.parentElement.closest('a')) return;
        const offset = sel.anchorOffset;
        const before = node.data.slice(0, offset - 1);
        const m = /((?:https?:\/\/|www\.)[^\s<>"']+)$/i.exec(before);
        if (!m) return;
        const url = trimUrl(m[1]);
        const start = before.length - m[1].length;
        const range = document.createRange();
        range.setStart(node, start);
        range.setEnd(node, start + url.length);
        const a = document.createElement('a');
        a.href = /^www\./i.test(url) ? `https://${url}` : url;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        range.surroundContents(a);
        // keep typing after the link, not inside it
        const after = a.nextSibling;
        const caret = document.createRange();
        if (after && after.nodeType === 3) caret.setStart(after, Math.min(after.length, 1 + (m[1].length - url.length)));
        else caret.setStartAfter(a);
        caret.collapse(true);
        sel.removeAllRanges();
        sel.addRange(caret);
        markTextChanged();
    });
    editorEl.addEventListener('blur', () => {
        const before = editorEl.innerHTML;
        linkifyNode(editorEl);
        if (editorEl.innerHTML !== before) markTextChanged();
    });

    /* ---- text colours ---- */
    const TEXT_COLORS = [
        ['', 'Default colour'],
        ['#ff9494', 'Pastel red'], ['#ffca8c', 'Pastel orange'], ['#fcff9c', 'Pastel yellow'],
        ['#b5ffa6', 'Pastel green'], ['#86b4ff', 'Pastel blue'], ['#c59cff', 'Pastel purple'],
        ['#000000', 'Black']
    ];
    $('textColors').innerHTML = TEXT_COLORS.map(([c, name]) =>
        `<button type="button" class="text-color${c ? '' : ' reset'}" data-text-color="${c}" title="${name}" style="${c ? `background:${c}` : ''}">${c ? '' : 'A'}</button>`
    ).join('');

    function applyTextColor(color) {
        if (!noteById(state.currentNoteId)) return;
        editorEl.focus();
        try { document.execCommand('styleWithCSS', false, false); } catch (_) {}
        const sel = window.getSelection();
        if (color) {
            document.execCommand('foreColor', false, color);
        } else if (!sel.rangeCount || sel.isCollapsed) {
            // nothing selected: type on in the normal (white) colour (the tag is removed when saving)
            document.execCommand('foreColor', false, plainTextHex() || '#ffffff');
        } else {
            // back to the normal colour: colour with a marker, then remove those colour tags
            const MARK = COLOR_MARK;
            document.execCommand('foreColor', false, MARK);
            editorEl.querySelectorAll('font[color], span[style*="color"]').forEach((el) => {
                const c = el.tagName === 'FONT' ? el.getAttribute('color') : cssColorToHex(el.style.color);
                if (cssColorToHex(c) !== MARK) return;
                el.querySelectorAll('font[color], span[style*="color"]').forEach((inner) => inner.replaceWith(...inner.childNodes));
                el.replaceWith(...el.childNodes);
            });
        }
        markTextChanged();
    }

    /* ---- heading (subkopje) ---- */
    function currentBlockTag() {
        const sel = window.getSelection();
        const node = sel.rangeCount ? sel.anchorNode : null;
        if (!node || !editorEl.contains(node)) return '';
        const el = (node.nodeType === 1 ? node : node.parentElement).closest('h1, h2, h3, li');
        return el && editorEl.contains(el) ? el.tagName : '';
    }

    function toggleHeading() {
        if (!noteById(state.currentNoteId)) return;
        editorEl.focus();
        const inHeading = /^H[123]$/.test(currentBlockTag());
        document.execCommand('formatBlock', false, inHeading ? '<div>' : '<h2>');
        markTextChanged();
        updateToolbarState();
    }

    $('headingBtn').addEventListener('click', toggleHeading);
    // Heading works like a switch: Enter on a heading line starts another heading line,
    // until you click Heading again
    function headingAt(container) {
        const sel = window.getSelection();
        const node = sel.rangeCount ? sel.anchorNode : null;
        if (!node || !container.contains(node)) return null;
        const h = (node.nodeType === 1 ? node : node.parentElement).closest('h1, h2, h3');
        return h && container.contains(h) ? h : null;
    }
    function keepHeadingOnEnter(e, container, after) {
        if (e.key !== 'Enter' || e.shiftKey || e.ctrlKey || e.metaKey || e.isComposing) return;
        if (!headingAt(container)) return;
        setTimeout(() => {
            if (!headingAt(container)) document.execCommand('formatBlock', false, '<h2>');
            if (after) after();
        }, 0);
    }
    editorEl.addEventListener('keydown', (e) => keepHeadingOnEnter(e, editorEl, () => { markTextChanged(); updateToolbarState(); }));
    editorEl.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.altKey && e.key === '2') { e.preventDefault(); toggleHeading(); }
    });

    // Light up the buttons for the formatting where the cursor is (bold, list, heading, ...)
    function updateToolbarState() {
        const sel = window.getSelection();
        const inside = sel.rangeCount && editorEl.contains(sel.anchorNode);
        $('textToolbar').querySelectorAll('[data-cmd]').forEach((btn) => {
            let on = false;
            try { on = inside && document.queryCommandState(btn.dataset.cmd); } catch (_) { /* ignore */ }
            btn.classList.toggle('active', !!on);
        });
        $('headingBtn').classList.toggle('active', !!inside && /^H[123]$/.test(currentBlockTag()));
    }
    document.addEventListener('selectionchange', () => {
        if (state.prefs.mode === 'text') updateToolbarState();
    });

    $('textColors').addEventListener('click', (e) => {
        const btn = e.target.closest('[data-text-color]');
        if (btn) applyTextColor(btn.dataset.textColor);
    });
    editorEl.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase() === 'x') {
            e.preventDefault();
            document.execCommand('strikeThrough', false, null);
            markTextChanged();
        }
    });

    // Typing "1. " or "- " / "* " at the start of a line turns it into a list (like Word)
    editorEl.addEventListener('input', (e) => {
        if (e.inputType !== 'insertText' || e.data !== ' ') return;
        const sel = window.getSelection();
        if (!sel.rangeCount || !sel.isCollapsed) return;
        const node = sel.anchorNode;
        if (!node || node.nodeType !== 3 || !editorEl.contains(node)) return;
        if (node.parentElement.closest('li')) return; // already in a list
        const before = node.data.slice(0, sel.anchorOffset).replace(/\u00a0/g, ' ');
        const lineStart = !node.previousSibling || node.previousSibling.nodeName === 'BR';
        if (!lineStart) return;
        let cmd = null;
        if (/^\s*1[.)] $/.test(before)) cmd = 'insertOrderedList';
        else if (/^\s*[-*•] $/.test(before)) cmd = 'insertUnorderedList';
        if (!cmd) return;
        // remove the typed "1. " and start the list on this line
        const rest = node.data.slice(sel.anchorOffset);
        const range = document.createRange();
        if (rest) {
            node.data = rest;
            range.setStart(node, 0);
        } else {
            // keep the (now empty) line alive so the list starts here, not on the line above
            const br = document.createElement('br');
            node.replaceWith(br);
            range.setStartBefore(br);
        }
        range.collapse(true);
        sel.removeAllRanges();
        sel.addRange(range);
        document.execCommand(cmd, false, null);
        markTextChanged();
    });

    // Shrink big photos so they fit in browser storage (and sync quickly)
    const IMAGE_MAX_SIDE = 1400;
    function imageFileToDataUrl(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onerror = () => reject(reader.error);
            reader.onload = () => {
                const original = reader.result;
                if (file.type === 'image/gif' && file.size < 8e6) { resolve(original); return; } // keep the animation
                const img = new Image();
                img.onerror = () => reject(new Error('not an image'));
                img.onload = () => {
                    const scale = Math.min(1, IMAGE_MAX_SIDE / Math.max(img.width, img.height));
                    if (scale === 1 && file.size < 350000 && /^data:image\/(png|jpe?g|webp)/.test(original)) {
                        resolve(original);
                        return;
                    }
                    const c = document.createElement('canvas');
                    c.width = Math.max(1, Math.round(img.width * scale));
                    c.height = Math.max(1, Math.round(img.height * scale));
                    c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
                    let out = c.toDataURL('image/webp', 0.85);
                    if (!out.startsWith('data:image/webp')) out = c.toDataURL('image/jpeg', 0.85);
                    resolve(out);
                };
                img.src = original;
            };
            reader.readAsDataURL(file);
        });
    }

    function placeCaretAtPoint(x, y) {
        let range = null;
        if (document.caretRangeFromPoint) range = document.caretRangeFromPoint(x, y);
        else if (document.caretPositionFromPoint) {
            const pos = document.caretPositionFromPoint(x, y);
            if (pos) { range = document.createRange(); range.setStart(pos.offsetNode, pos.offset); }
        }
        if (range && editorEl.contains(range.startContainer)) {
            const sel = window.getSelection();
            sel.removeAllRanges();
            sel.addRange(range);
        }
    }

    function ensureCaretInEditor() {
        const sel = window.getSelection();
        const inside = sel.rangeCount && editorEl.contains(sel.getRangeAt(0).commonAncestorContainer);
        editorEl.focus();
        if (inside) {
            // don't let a new image inherit bold/italic/underline
            ['bold', 'italic', 'underline'].forEach((cmd) => {
                if (document.queryCommandState(cmd)) document.execCommand(cmd, false, null);
            });
            return;
        }
        const range = document.createRange();
        range.selectNodeContents(editorEl);
        range.collapse(false);
        sel.removeAllRanges();
        sel.addRange(range);
    }

    function insertNodeAtCaret(node) {
        const sel = window.getSelection();
        let range = sel.rangeCount ? sel.getRangeAt(0) : null;
        if (!range || !editorEl.contains(range.commonAncestorContainer)) {
            range = document.createRange();
            range.selectNodeContents(editorEl);
            range.collapse(false);
        }
        range.deleteContents();
        range.insertNode(node);
        range.setStartAfter(node);
        range.collapse(true);
        sel.removeAllRanges();
        sel.addRange(range);
    }

    // The top-level lines (blocks) of the text, with where they are on screen
    function textBlocks() {
        const out = [];
        editorEl.childNodes.forEach((n) => {
            if (n.nodeType === 3 && !n.textContent.trim()) return;
            let rect;
            if (n.nodeType === 1) rect = n.getBoundingClientRect();
            else { const r = document.createRange(); r.selectNodeContents(n); rect = r.getBoundingClientRect(); }
            if (rect.height || rect.width) out.push({ node: n, rect });
        });
        return out;
    }
    // a line with only pictures in it (pictures can stand side by side there)
    function isPictureLine(n) {
        return !!n && n.nodeType === 1 && (n.nodeName === 'DIV' || n.nodeName === 'P') && !!n.querySelector(':scope > img')
            && [...n.childNodes].every((c) => c.nodeName === 'IMG' || c.nodeName === 'BR' || (c.nodeType === 3 && !c.textContent.trim()));
    }
    // all picture lines (also ones nested in another line), after giving loose pictures a line of their own
    function pictureLines() {
        [...editorEl.childNodes].forEach((n) => {
            if (n.nodeName !== 'IMG') return;
            const line = document.createElement('div');
            editorEl.insertBefore(line, n);
            line.appendChild(n);
        });
        return [...editorEl.querySelectorAll('div, p')].filter(isPictureLine);
    }
    // Pointer at the height of a picture line (anywhere left/right): next to the pictures on it
    function sideBySidePoint(x, y) {
        for (const line of pictureLines()) {
            const imgs = [...line.querySelectorAll(':scope > img')];
            const rects = imgs.map((i) => i.getBoundingClientRect());
            const top = Math.min(...rects.map((r) => r.top)), bottom = Math.max(...rects.map((r) => r.bottom));
            if (y < top - 4 || y > bottom + 4) continue;
            // the picture whose middle is right of the pointer: the new one goes before it; else at the end
            const i = rects.findIndex((r) => y >= r.top - 4 && y <= r.bottom + 4 && x < r.left + r.width / 2);
            const last = rects[rects.length - 1];
            return {
                inline: true, line, beforeImg: i < 0 ? null : imgs[i],
                x: i < 0 ? last.right : rects[i].left - 4, y: i < 0 ? last.top : rects[i].top,
                h: i < 0 ? last.height : rects[i].height, end: i < 0
            };
        }
        return null;
    }
    // Where a picture lands: next to another picture, or between two lines (the nearest one)
    function dropTarget(x, y) { return sideBySidePoint(x, y) || dropPoint(y); }
    function dropPoint(y) {
        const blocks = textBlocks();
        for (const b of blocks) {
            if (y < b.rect.top + b.rect.height / 2) return { before: b.node, y: b.rect.top };
        }
        const last = blocks[blocks.length - 1];
        const er = editorEl.getBoundingClientRect();
        return { before: null, y: last ? last.rect.bottom : er.top + parseFloat(getComputedStyle(editorEl).paddingTop || 0) };
    }
    // Where a picture added with the button / Ctrl+V goes: after the line with the caret
    function caretPoint() {
        const sel = window.getSelection();
        if (!sel.rangeCount || !editorEl.contains(sel.anchorNode)) return { before: null };
        let n = sel.anchorNode;
        if (n === editorEl) return { before: editorEl.childNodes[sel.anchorOffset] || null };
        while (n.parentNode !== editorEl) n = n.parentNode;
        // an empty line: put the picture there (before it), so you keep typing below it
        if (n.nodeType === 1 && !n.textContent.trim() && !n.querySelector('img, audio, [data-voice]')) return { before: n };
        return { before: n.nextSibling };
    }
    // the size you picked last time (S/M/L/Full) is used for new pictures
    const lastImgSize = () => { const v = ({ 12: 12.5, 24: 25, 48: 50 })[Number(state.prefs.imgSize)] || Number(state.prefs.imgSize); return [12.5, 25, 50, 100].includes(v) ? v : 25; };
    function pictureLine(src) {
        const line = document.createElement('div');
        const img = document.createElement('img');
        img.src = src;
        img.alt = '';
        img.style.width = `${lastImgSize()}%`; // new pictures get the size you used last
        line.appendChild(img);
        return line;
    }
    // put a line in the text; there is always an empty line after the last picture to type in
    function placeLine(line, point) {
        const before = point.before && point.before.parentNode === editorEl ? point.before : null;
        editorEl.insertBefore(line, before);
        if (!line.nextSibling) {
            const blank = document.createElement('div');
            blank.appendChild(document.createElement('br'));
            editorEl.appendChild(blank);
        }
    }

    // put a picture into a picture line, before `beforeImg` (or at the end)
    function placeInline(img, point) {
        const old = img.parentNode;
        point.line.insertBefore(img, point.beforeImg && point.beforeImg.parentNode === point.line ? point.beforeImg : null);
        // no spaces between pictures, so 4 × M fit exactly on one line
        [...point.line.childNodes].forEach((n) => { if (n.nodeType === 3 && !n.textContent.trim()) n.remove(); });
        // the line it came from is now empty: remove it
        if (old && old !== point.line && old.parentNode === editorEl && !old.querySelector('img') && !old.textContent.trim()) old.remove();
    }

    let insertingImages = false;
    async function insertImageFiles(files, point) {
        if (!noteById(state.currentNoteId)) return;
        const images = [...files].filter((f) => typeof f === 'string' || f.type.startsWith('image/'));
        if (!images.length || insertingImages) return;
        insertingImages = true; // never handle the same drop / paste twice
        recordText();
        const at = point || caretPoint();
        let lastImg = null;
        try {
            for (const file of images) {
                try {
                    const picSrc = await pictureFileToStorable(file);
                    if (!picSrc) continue;
                    const line = pictureLine(picSrc);
                    if (at.inline) {
                        const img = line.querySelector('img');
                        placeInline(img, at); // next to the other picture(s)
                        at.beforeImg = img.nextSibling && img.nextSibling.nodeName === 'IMG' ? img.nextSibling : null;
                        lastImg = img;
                    } else {
                        placeLine(line, at);
                        lastImg = line.querySelector('img');
                    }
                } catch (err) {
                    toast(`Could not load image: ${file.name}`);
                }
            }
        } finally {
            insertingImages = false;
        }
        if (lastImg) {
            // caret on the line after the picture
            const next = lastImg.parentNode.nextSibling;
            if (next) {
                const r = document.createRange();
                r.selectNodeContents(next);
                r.collapse(true);
                const sel = window.getSelection();
                sel.removeAllRanges();
                sel.addRange(r);
            }
        }
        textDirty = true;
        flushEditor();
        updateTextUndoButtons();
    }

    // dashed outline showing where a dragged picture will land
    const dropOutline = document.createElement('div');
    dropOutline.className = 'drop-outline';
    dropOutline.hidden = true;
    document.body.appendChild(dropOutline);
    function showDropOutline(point, aspect, widthPct) {
        const er = editorEl.getBoundingClientRect();
        const cs = getComputedStyle(editorEl);
        const left = er.left + parseFloat(cs.paddingLeft || 0);
        const width = (er.width - parseFloat(cs.paddingLeft || 0) - parseFloat(cs.paddingRight || 0)) * (widthPct || 24) / 100;
        const height = Math.max(24, width * (aspect || 0.66));
        dropOutline.classList.toggle('inline', !!point.inline);
        if (point.inline && point.end) {
            // the picture's own size, right next to the last picture on that line
            dropOutline.style.left = `${point.x}px`;
            dropOutline.style.top = `${point.y + Math.max(0, (point.h || height) - height)}px`;
            dropOutline.style.width = `${width}px`;
            dropOutline.style.height = `${height}px`;
        } else if (point.inline) {
            // between two pictures: a bar where it slides in
            dropOutline.style.left = `${point.x - 3}px`;
            dropOutline.style.top = `${point.y}px`;
            dropOutline.style.width = '8px';
            dropOutline.style.height = `${point.h || height}px`;
        } else {
            dropOutline.style.left = `${left}px`;
            dropOutline.style.top = `${point.y + 2}px`;
            dropOutline.style.width = `${width}px`;
            dropOutline.style.height = `${height}px`;
        }
        dropOutline.hidden = false;
    }
    const hideDropOutline = () => { dropOutline.hidden = true; };

    // Ctrl+C on a clicked picture copies the picture (also when no text is selected)
    // videos and audio selected in the text (click their bar / handle; Shift+click: one more)
    let textMediaSel = [];
    function selectTextMedia(el, add = false) {
        textMediaSel = textMediaSel.filter((x) => editorEl.contains(x));
        if (add && el) textMediaSel = textMediaSel.includes(el) ? textMediaSel.filter((x) => x !== el) : [...textMediaSel, el];
        else textMediaSel = el ? [el] : [];
        editorEl.querySelectorAll('.media-selected').forEach((x) => x.classList.remove('media-selected'));
        textMediaSel.forEach((x) => x.classList.add('media-selected'));
        updateTextCtx();
    }
    // everything selected in the text, in the order of the text
    function textSelectedItems() {
        const els = [...selectedImgs, ...textMediaSel].filter((x) => editorEl.contains(x));
        els.sort((a, b) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1));
        return els.map((el) => (el.matches('img') ? { kind: 'image', src: el.getAttribute('src') || '' }
            : el.classList.contains('video-embed') ? { kind: 'video', src: el.getAttribute('data-video'), loop: el.getAttribute('data-loop') === '1', w: el.getAttribute('data-w') }
                : { kind: 'audio', src: el.getAttribute('data-voice'), name: el.getAttribute('data-name'), duration: el.getAttribute('data-duration'), loop: el.getAttribute('data-loop') === '1', vol: chipVolume(el) }));
    }
    ['copy', 'cut'].forEach((type) => editorEl.addEventListener(type, (e) => {
        if (!e.clipboardData || state.prefs.mode !== 'text') return;
        const sel = window.getSelection();
        if (sel && !sel.isCollapsed && sel.toString().trim()) return; // copying text: normal copy
        const items = textSelectedItems();
        if (!items.length) return;
        e.preventDefault();
        writeClipItems(e, items);
        if (items.length === 1 && items[0].kind === 'image') setTimeout(() => copyPictureSrc(items[0].src, true), 0);
        if (type === 'cut') {
            recordText();
            [...selectedImgs, ...textMediaSel].forEach((x) => x.remove());
            selectImage(null); selectTextMedia(null);
            markTextChanged();
        }
        toast(`${items.length > 1 ? `${items.length} items` : items[0].kind === 'image' ? 'Picture' : items[0].kind === 'video' ? 'Video' : 'Audio'} ${type === 'cut' ? 'cut' : 'copied'}: paste with Ctrl+V in Text, Collage or Draw`);
    }));

    editorEl.addEventListener('paste', (e) => {
        const dt = e.clipboardData;
        if (!dt) return;
        // in Draw / Grid the text is hidden (but may still have the focus): leave the paste to them
        if (state.prefs.mode !== 'text') return;
        // pictures, videos and audio copied with Ctrl+C in Text, Collage or Draw
        const clipItems = readClipItems(e);
        if (clipItems.length) { e.preventDefault(); pasteItemsInText(clipItems); return; }
        // a video copied with "Copy video"
        const copiedVid = pastedVideoRef(e);
        if (copiedVid) { e.preventDefault(); insertVideoInText(copiedVid); return; }
        const copiedAud = pastedAudio(e);
        if (copiedAud) { e.preventDefault(); insertVoiceChip(copiedAud.src, copiedAud.name, copiedAud.duration, copiedAud.loop, copiedAud.vol); return; }
        // a YouTube / Vimeo link on its own: embed the video
        const pastedText = dt.getData('text/plain');
        const pastedVideo = !dt.files.length && pastedText && !/\s/.test(pastedText.trim()) && videoEmbedUrl(pastedText);
        if (pastedVideo) { e.preventDefault(); insertVideoInText(pastedVideo); return; }
        const ownPics = ownPictureCopy(e);
        if (ownPics.length) { e.preventDefault(); insertImageFiles(ownPics); return; }
        const files = [...dt.files].filter((f) => f.type.startsWith('image/'));
        const audio = [...dt.files].filter((f) => f.type.startsWith('audio/'));
        if (files.length || audio.length) {
            e.preventDefault();
            if (files.length) insertImageFiles(files);
            if (audio.length) insertAudioFiles(audio);
            return;
        }
        const html = dt.getData('text/html');
        // pictures copied from the Collage or with "Copy picture"
        if ((pastedText === PIC_MARK || pastedText === CB_MARK) && pastedPictureSrcs(html).length) {
            e.preventDefault();
            insertImageFiles(pastedPictureSrcs(html));
            return;
        }
        // a piece copied from the drawing or the grid: insert it like any other picture
        const own = html && /<img[^>]*data-(?:remmin|remindate)-copy[^>]*src="(data:image\/[^"]+)"/i.exec(html);
        if (own) {
            e.preventDefault();
            fetch(own[1]).then((r) => r.blob())
                .then((blob) => insertImageFiles([new File([blob], 'drawing.png', { type: blob.type || 'image/png' })]))
                .catch(() => toast('Could not paste that picture'));
            return;
        }
        if (html) {
            e.preventDefault();
            document.execCommand('insertHTML', false, sanitizeHtml(html));
            markTextChanged();
        }
    });

    const draggingAppItem = () => !!(draggedTreeNoteId || draggedFolderId ||
        (typeof draggedCalendarNote !== 'undefined' && draggedCalendarNote));
    editorEl.addEventListener('dragover', (e) => {
        if (draggingAppItem()) { e.dataTransfer.dropEffect = 'none'; return; }
        if (!movingNoteImg && !movingNoteVideo && !Array.from(e.dataTransfer.types || []).includes('Files')) return;
        e.preventDefault();
        if (movingNoteVideo) {
            e.dataTransfer.dropEffect = 'move';
            const full = editorEl.clientWidth || 1;
            showDropOutline(dropTarget(e.clientX, e.clientY), 9 / 16, Math.min(100, movingNoteVideo.getBoundingClientRect().width / full * 100));
            return;
        }
        const aspect = movingNoteImg && movingNoteImg.naturalWidth ? movingNoteImg.naturalHeight / movingNoteImg.naturalWidth : 0.66;
        const pct = movingNoteImg ? parseFloat(movingNoteImg.style.width) || 24 : lastImgSize();
        showDropOutline(dropTarget(e.clientX, e.clientY), aspect, pct);
    });
    editorEl.addEventListener('dragleave', (e) => {
        if (!e.relatedTarget || !editorEl.contains(e.relatedTarget)) hideDropOutline();
    });
    document.addEventListener('dragend', hideDropOutline);
    document.addEventListener('drop', hideDropOutline, true);
    // Dragging a picture that is already in the note: Chrome also hands it over as a "file",
    // which used to add a second copy. Remember the picture and just move it instead.
    let movingNoteImg = null;
    editorEl.addEventListener('dragstart', (e) => {
        const video = e.target && e.target.closest && e.target.closest('.video-embed');
        if (video && editorEl.contains(video)) {
            movingNoteVideo = video;
            movingNoteImg = null;
            e.dataTransfer.setData('text/plain', '');
            return;
        }
        const img = e.target && e.target.closest && e.target.closest('img');
        movingNoteImg = img && editorEl.contains(img) ? img : null;
    });
    editorEl.addEventListener('dragend', () => {
        if (movingNoteVideo) { movingNoteVideo.draggable = false; videoMovedAt = Date.now(); }
        movingNoteImg = null; movingNoteVideo = null; hideDropOutline();
    });

    editorEl.addEventListener('drop', (e) => {
        if (draggingAppItem()) { e.preventDefault(); return; }
        if (movingNoteVideo) {
            const video = movingNoteVideo;
            movingNoteVideo = null;
            video.draggable = false;
            e.preventDefault();
            e.stopPropagation();
            hideDropOutline();
            if (!editorEl.contains(video)) return;
            const point = dropTarget(e.clientX, e.clientY);
            const target = point.inline ? { before: point.line && point.line.nextSibling } : point;
            if (target.before === video || target.before === video.nextSibling) return; // same place
            recordText();
            placeLine(video, target);
            markTextChanged();
            return;
        }
        if (movingNoteImg) {
            const img = movingNoteImg;
            movingNoteImg = null;
            e.preventDefault();
            hideDropOutline();
            if (!editorEl.contains(img)) return;
            const point = dropTarget(e.clientX, e.clientY);
            if (point.inline) {
                if (point.beforeImg === img || (point.line === img.parentNode && (point.beforeImg === img.nextSibling
                    || (!point.beforeImg && !img.nextElementSibling)))) return; // same place
                recordText();
                placeInline(img, point);
                selectImage(img);
                markTextChanged();
                return;
            }
            // the picture's own line moves along (one node, so never a copy)
            let line = img.parentNode;
            const alone = line && line.parentNode === editorEl && line.nodeName === 'DIV'
                && [...line.childNodes].every((n) => n === img || (n.nodeType === 3 && !n.textContent.trim()) || n.nodeName === 'BR');
            if (point.before === line || (alone && point.before === line.nextSibling)) return; // same place
            recordText();
            if (!alone) { line = document.createElement('div'); line.appendChild(img); }
            placeLine(line, point);
            selectImage(img);
            markTextChanged();
            return;
        }
        const files = [...(e.dataTransfer.files || [])].filter((f) => f.type.startsWith('image/'));
        const audio = [...(e.dataTransfer.files || [])].filter((f) => f.type.startsWith('audio/'));
        const videos = [...(e.dataTransfer.files || [])].filter(isVideoFile);
        if (!files.length && !audio.length && !videos.length) return;
        e.preventDefault();
        e.stopPropagation();
        hideDropOutline();
        if (videos.length) {
            const at = dropTarget(e.clientX, e.clientY);
            readVideoFiles(videos).then((srcs) => srcs.forEach((src) => insertVideoInText(src, at)));
        }
        if (files.length) insertImageFiles(files, dropTarget(e.clientX, e.clientY));
        if (audio.length) { placeCaretAtPoint(e.clientX, e.clientY); insertAudioFiles(audio); }
    });

    // the selected pictures (Shift + click adds or removes one); selectedImg is the last one clicked
    let selectedImgs = [];
    function selectImage(img, add = false) {
        selectedImgs = selectedImgs.filter((i) => editorEl.contains(i));
        if (add && img) {
            if (selectedImgs.includes(img)) selectedImgs = selectedImgs.filter((i) => i !== img);
            else selectedImgs.push(img);
        } else selectedImgs = img ? [img] : [];
        editorEl.querySelectorAll('img.selected').forEach((i) => i.classList.remove('selected'));
        selectedImgs.forEach((i) => i.classList.add('selected'));
        selectedImg = selectedImgs[selectedImgs.length - 1] || null;
        $('imageTools').hidden = !selectedImg;
        placeImgHandle();
        $('imageTools').dataset.count = selectedImgs.length > 1 ? `${selectedImgs.length} pictures` : '';
        updateTextCtx();
    }
    // the options of what is selected in the text, right above it
    var textCtx = null;
    function updateTextCtx() {
        if (!textCtx) return;
        const imgs = selectedImgs.filter((i) => editorEl.contains(i));
        const media = textMediaSel.filter((x) => editorEl.contains(x));
        if (imgs.length && !media.length) {
            const one = imgs.length === 1 ? imgs[0] : null;
            const src = one ? one.getAttribute('src') || '' : '';
            $('imageDownload').hidden = !!one;
            $('imageRemove').hidden = !!one;
            textCtx.show({
                key: one ? `img:${src.length}:${src.slice(-40)}` : `imgs:${imgs.length}`,
                anchor: () => rectOf(imgs),
                groups: [$('imageTools')],
                after: updateTextCtx,
                actions: () => (one ? pictureMenuActions(src, {
                    x: null, y: null,
                    place: (nsrc) => { if (!editorEl.contains(one)) return; recordText(); one.src = nsrc; markTextChanged(); updateTextCtx(); },
                    remove: () => { recordText(); selectImage(null); one.remove(); markTextChanged(); }
                }) : [])
            });
        } else if (media.length === 1 && !imgs.length) {
            const el = media[0];
            const isVideo = el.classList.contains('video-embed');
            textCtx.show({
                key: `${isVideo ? 'v' : 'a'}:${(el.getAttribute(isVideo ? 'data-video' : 'data-voice') || '').length}:${el.getAttribute('data-loop')}:${el.getAttribute('data-w')}:${el.getAttribute('data-name')}`,
                anchor: () => rectOf([el]),
                after: updateTextCtx,
                actions: () => (isVideo ? textVideoActions(el) : textAudioActions(el).filter((a) => !['▶', '⏸', '🔁'].includes(a.icon)))
            });
        } else textCtx.hide();
    }

    // the options of a video in the text
    function textVideoActions(video) {
        const src = video.getAttribute('data-video');
        const player = video.querySelector('video');
        return videoMenuActions(src, {
            loop: {
                on: video.getAttribute('data-loop') === '1',
                toggle: () => {
                    const on = video.getAttribute('data-loop') !== '1';
                    recordText();
                    if (on) video.setAttribute('data-loop', '1'); else video.removeAttribute('data-loop');
                    if (player) player.loop = on;
                    markTextChanged();
                    toast(on ? 'Loop on: the video plays again and again' : 'Loop off');
                }
            },
            remove: () => { recordText(); video.remove(); markTextChanged(); },
            sizeNow: Number(video.getAttribute('data-w')) || 0,
            sizes: (w) => { recordText(); video.setAttribute('data-w', String(w)); video.style.width = `${w}%`; markTextChanged(); }
        });
    }
    editorEl.addEventListener('contextmenu', (e) => {
        const img = e.target.closest && e.target.closest('img');
        if (img && editorEl.contains(img) && !img.closest('.video-embed')) {
            e.preventDefault();
            menuAt(e.clientX, e.clientY, 'Picture', pictureMenuActions(img.getAttribute('src') || '', {
                x: e.clientX, y: e.clientY,
                place: (src) => { if (!editorEl.contains(img)) return; recordText(); img.src = src; markTextChanged(); },
                remove: () => { recordText(); selectImage(null); img.remove(); markTextChanged(); }
            }));
            return;
        }
        const video = e.target.closest && e.target.closest('.video-embed');
        if (!video || !editorEl.contains(video)) return;
        e.preventDefault();
        menuAt(e.clientX, e.clientY, 'Video', textVideoActions(video));
    });
    /* move a video in the text: drag its top bar; resize: drag the corner */
    let movingNoteVideo = null;
    let videoMovedAt = 0;
    editorEl.addEventListener('pointerdown', (e) => {
        const video = e.target.closest && e.target.closest('.video-embed');
        if (!video || !editorEl.contains(video) || e.button !== 0) return;
        const handle = e.target.closest('.ve-handle');
        if (handle) {
            e.preventDefault();
            e.stopPropagation();
            const style = getComputedStyle(editorEl);
            const full = editorEl.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
            const startW = video.getBoundingClientRect().width;
            const x0 = e.clientX;
            let changed = false;
            recordText();
            try { handle.setPointerCapture(e.pointerId); } catch (_) { /* ignore */ }
            const move = (ev) => {
                let pct = Math.max(15, Math.min(100, (startW + ev.clientX - x0) / full * 100));
                const snap = [25, 50, 75, 100].find((s) => Math.abs(s - pct) < 3);
                if (snap) pct = snap;
                pct = Math.round(pct);
                video.style.width = `${pct}%`;
                video.setAttribute('data-w', String(pct));
                changed = true;
            };
            const up = () => {
                handle.removeEventListener('pointermove', move);
                handle.removeEventListener('pointerup', up);
                handle.removeEventListener('pointercancel', up);
                videoMovedAt = Date.now();
                if (changed) markTextChanged();
            };
            handle.addEventListener('pointermove', move);
            handle.addEventListener('pointerup', up);
            handle.addEventListener('pointercancel', up);
            return;
        }
        // the bar above the player (where "▶ Video" is) moves the video
        video.draggable = e.clientY - video.getBoundingClientRect().top < 24;
    });
    editorEl.addEventListener('click', (e) => {
        const chipHit = e.target.closest('.voice-chip');
        if (chipHit && editorEl.contains(chipHit) && (e.target === chipHit || e.target.closest('.voice-handle, .voice-time, .voice-vol-icon'))) {
            // click on the audio itself (not a button): select it (Shift: one more)
            if (!e.shiftKey) selectImage(null);
            selectTextMedia(chipHit, e.shiftKey);
        }
        if (chipHit) { handleVoiceChipClick(e); return; }

        const link = e.target.closest('a[href]');
        if (link && editorEl.contains(link) && !window.getSelection().toString()) {
            e.preventDefault();
            if (SAFE_LINK_RE.test(link.getAttribute('href'))) window.open(link.getAttribute('href'), '_blank', 'noopener');
            return;
        }
        // click the bar of a video: select it (Shift: one more); right-click: its options
        const video = e.target.closest('.video-embed');
        if (video && editorEl.contains(video) && e.target.tagName !== 'VIDEO' && !e.target.closest('.ve-handle')) {
            if (Date.now() - videoMovedAt < 400) return;
            // already the only one selected: a second click / tap opens its options (handy on phones)
            if (!e.shiftKey && textMediaSel.length === 1 && textMediaSel[0] === video) {
                const r = video.getBoundingClientRect();
                menuAt(Math.min(e.clientX, r.right), e.clientY, 'Video', textVideoActions(video));
                return;
            }
            if (!e.shiftKey) selectImage(null);
            selectTextMedia(video, e.shiftKey);
            return;
        }
        const img = e.target.closest('img');
        if (e.shiftKey && img && editorEl.contains(img)) { e.preventDefault(); selectImage(img, true); return; }
        if (e.shiftKey && !img && (selectedImgs.length || textMediaSel.length)) return; // Shift + click on text keeps the selection
        selectTextMedia(null);
        selectImage(img && editorEl.contains(img) ? img : null);
    });
    // Tab / Shift+Tab inside a list: indent / outdent the item
    editorEl.addEventListener('keydown', (e) => {
        if (e.key !== 'Tab') return;
        const sel = window.getSelection();
        const node = sel.rangeCount ? sel.anchorNode : null;
        const li = node && (node.nodeType === 1 ? node : node.parentElement).closest('li');
        if (!li || !editorEl.contains(li)) return;
        e.preventDefault();
        document.execCommand(e.shiftKey ? 'outdent' : 'indent', false, null);
        markTextChanged();
    });
    editorEl.addEventListener('keydown', (e) => {
        textMediaSel = textMediaSel.filter((x) => editorEl.contains(x));
        if ((selectedImg || textMediaSel.length) && (e.key === 'Delete' || e.key === 'Backspace')) {
            e.preventDefault();
            recordText();
            selectedImgs.forEach((i) => i.remove());
            textMediaSel.forEach((x) => { if (voicePlayingChip === x) stopVoicePlayback(); x.remove(); });
            selectImage(null);
            selectTextMedia(null);
            markTextChanged();
        }
    });

    try { document.execCommand('styleWithCSS', false, false); } catch (_) { /* ignore */ }
    try { document.execCommand('defaultParagraphSeparator', false, 'div'); } catch (_) { /* ignore */ }
    // Toolbar buttons must not take the focus away from the text
    $('textToolbar').addEventListener('mousedown', (e) => {
        if (e.target.closest('button')) e.preventDefault();
    });
    $('textToolbar').addEventListener('click', (e) => {
        const b = e.target.closest('button');
        if (b && b.id !== 'textUndo' && b.id !== 'textRedo' && !b.classList.contains('ctx-act') && b.id !== 'textAddBtn' && b.id !== 'textPasteBtn') recordText();
    }, true);
    $('textUndo').addEventListener('click', undoText);
    $('textRedo').addEventListener('click', redoText);
    $('textToolbar').addEventListener('click', (e) => {
        const cmd = e.target.closest('[data-cmd]');
        if (cmd) {
            if (!noteById(state.currentNoteId)) return;
            editorEl.focus();
            document.execCommand(cmd.dataset.cmd, false, null);
            markTextChanged();
            updateToolbarState();
            return;
        }
        const size = e.target.closest('[data-img-size]');
        if (size && selectedImg) {
            selectedImgs.filter((i) => editorEl.contains(i)).forEach((i) => { i.style.width = `${size.dataset.imgSize}%`; });
            requestAnimationFrame(placeImgHandle);
            state.prefs.imgSize = Number(size.dataset.imgSize);
            settingsChanged();
            markTextChanged();
        }
    });

    /* ---------------- download the text as PDF or Word ---------------- */
    // the text part, cleaned up for exporting (voice notes become a short line)
    function exportContent() {
        flushEditor();
        const box = document.createElement('div');
        box.innerHTML = editorEl.innerHTML;
        box.querySelectorAll('[data-voice]').forEach((v) => { v.replaceWith(document.createTextNode('🎤 [voice note]')); });
        box.querySelectorAll('[data-video]').forEach((v) => {
            if (isMediaRef(v.getAttribute('data-video'))) { v.replaceWith(document.createTextNode('▶ [video]')); return; }
            const url = videoWatchUrl(v.getAttribute('data-video'));
            const line = document.createElement('div');
            const a = document.createElement('a');
            a.href = url; a.textContent = `▶ Video: ${url}`;
            line.appendChild(a);
            v.replaceWith(line);
        });
        box.querySelectorAll('.img-selected, .selected').forEach((n) => n.classList.remove('img-selected', 'selected'));
        return box;
    }
    const exportName = () => {
        const note = noteById(state.currentNoteId);
        return ((note && note.title.trim()) || 'Note').replace(/[\\/:*?"<>|]+/g, '').slice(0, 80) || 'Note';
    };

    // PDF: a clean page in the print window → "Save as PDF"
    function downloadPdf() {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const body = exportContent();
        const frame = document.createElement('iframe');
        frame.style.cssText = 'position:fixed;right:0;bottom:0;width:0;height:0;border:0;';
        document.body.appendChild(frame);
        const doc = frame.contentDocument;
        doc.open();
        doc.write(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>${escapeHtml(exportName())}</title><style>
            @page { margin: 18mm; }
            body { font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #111; }
            h1.note-title { font-size: 20pt; margin: 0 0 12pt; }
            h1, h2, h3 { line-height: 1.25; } h2 { font-size: 1.8em; margin: .4em 0; }
            img { max-width: 100%; vertical-align: bottom; border-radius: 4px; }
            div > img + img { margin-left: 0; }
            a { color: #2563eb; }
            ul, ol { padding-left: 1.4em; }
            blockquote { border-left: 3px solid #ccc; margin: 0; padding-left: 10px; color: #444; }
            img, div:has(> img) { break-inside: avoid; }
        </style></head><body><h1 class="note-title">${escapeHtml(note.title.trim() || 'Untitled')}</h1>${body.innerHTML}</body></html>`);
        doc.close();
        const go = () => {
            try { frame.contentWindow.focus(); frame.contentWindow.print(); } catch (err) { toast('Could not open the print window'); }
            setTimeout(() => frame.remove(), 60000);
        };
        // wait for the pictures before printing
        const imgs = [...doc.images].filter((i) => !i.complete);
        if (!imgs.length) setTimeout(go, 50);
        else Promise.all(imgs.map((i) => new Promise((r) => { i.onload = i.onerror = r; }))).then(go);
        toast('Choose "Save as PDF" in the print window');
    }

    // ----- Word (.docx), built right here: text, formatting, headings, lists, pictures -----
    const xmlEsc = (t) => String(t).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    const CRC_TABLE = (() => {
        const t = new Uint32Array(256);
        for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; }
        return t;
    })();
    function crc32(bytes) {
        let c = 0xFFFFFFFF;
        for (let i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 255] ^ (c >>> 8);
        return (c ^ 0xFFFFFFFF) >>> 0;
    }
    // a simple zip file (stored, not compressed) — a .docx is a zip with XML files in it
    function makeZip(files) {
        const enc = new TextEncoder();
        const parts = [], central = [];
        let offset = 0;
        files.forEach(({ name, data }) => {
            const bytes = typeof data === 'string' ? enc.encode(data) : data;
            const nameBytes = enc.encode(name);
            const crc = crc32(bytes);
            const local = new DataView(new ArrayBuffer(30));
            local.setUint32(0, 0x04034b50, true); local.setUint16(4, 20, true); local.setUint16(6, 0x0800, true);
            local.setUint16(8, 0, true); local.setUint32(14, crc, true);
            local.setUint32(18, bytes.length, true); local.setUint32(22, bytes.length, true);
            local.setUint16(26, nameBytes.length, true);
            parts.push(new Uint8Array(local.buffer), nameBytes, bytes);
            const cen = new DataView(new ArrayBuffer(46));
            cen.setUint32(0, 0x02014b50, true); cen.setUint16(4, 20, true); cen.setUint16(6, 20, true); cen.setUint16(8, 0x0800, true);
            cen.setUint32(16, crc, true); cen.setUint32(20, bytes.length, true); cen.setUint32(24, bytes.length, true);
            cen.setUint16(28, nameBytes.length, true); cen.setUint32(42, offset, true);
            central.push(new Uint8Array(cen.buffer), nameBytes);
            offset += 30 + nameBytes.length + bytes.length;
        });
        const cenSize = central.reduce((a, p) => a + p.length, 0);
        const end = new DataView(new ArrayBuffer(22));
        end.setUint32(0, 0x06054b50, true); end.setUint16(8, files.length, true); end.setUint16(10, files.length, true);
        end.setUint32(12, cenSize, true); end.setUint32(16, offset, true);
        return new Blob([...parts, ...central, new Uint8Array(end.buffer)], { type: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' });
    }
    function cssColorHex(c) {
        if (!c) return null;
        const m = /rgba?\(\s*(\d+)[,\s]+(\d+)[,\s]+(\d+)/i.exec(c);
        if (m) return [m[1], m[2], m[3]].map((v) => (+v).toString(16).padStart(2, '0')).join('').toUpperCase();
        const h = /^#([0-9a-f]{6})$/i.exec(c.trim());
        return h ? h[1].toUpperCase() : null;
    }
    // a picture as PNG bytes (Word can't show every web format)
    async function imagePng(img) {
        const el = new Image();
        el.src = img.getAttribute('src');
        await (el.decode ? el.decode().catch(() => {}) : new Promise((r) => { el.onload = el.onerror = r; }));
        if (!el.naturalWidth) return null;
        const c = document.createElement('canvas');
        c.width = el.naturalWidth; c.height = el.naturalHeight;
        c.getContext('2d').drawImage(el, 0, 0);
        const blob = await new Promise((r) => c.toBlob(r, 'image/png'));
        return blob ? { bytes: new Uint8Array(await blob.arrayBuffer()), w: el.naturalWidth, h: el.naturalHeight } : null;
    }

    async function downloadWord() {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        toast('Making the Word file…');
        const root = exportContent();
        const media = [];   // { name, bytes }
        const paras = [];   // finished <w:p> elements
        const PAGE_EMU = 5731510; // text width of an A4 page with 2.54 cm margins
        let pending = [];   // runs of the paragraph being built
        let listInfo = null;

        const runXml = (text, f) => {
            const rp = [f.b && '<w:b/>', f.i && '<w:i/>', f.u && '<w:u w:val="single"/>', f.s && '<w:strike/>',
                f.color && `<w:color w:val="${f.color}"/>`, f.size && `<w:sz w:val="${f.size}"/><w:szCs w:val="${f.size}"/>`].filter(Boolean).join('');
            return `<w:r>${rp ? `<w:rPr>${rp}</w:rPr>` : ''}<w:t xml:space="preserve">${xmlEsc(text)}</w:t></w:r>`;
        };
        const flushPara = (pPr = '') => {
            if (!pending.length && !pPr) { paras.push('<w:p/>'); return; }
            let prefix = '';
            if (listInfo) {
                prefix = runXml(listInfo.ordered ? `${listInfo.n}.\t` : '•\t', {});
                pPr += '<w:ind w:left="720" w:hanging="360"/>';
            }
            paras.push(`<w:p>${pPr ? `<w:pPr>${pPr}</w:pPr>` : ''}${prefix}${pending.join('')}</w:p>`);
            pending = [];
        };

        async function walk(node, f) {
            if (node.nodeType === 3) {
                const t = node.textContent.replace(/\s+/g, ' ');
                if (t) pending.push(runXml(t, f));
                return;
            }
            if (node.nodeType !== 1) return;
            const tag = node.tagName;
            const style = node.getAttribute('style') || '';
            const nf = { ...f };
            if (tag === 'B' || tag === 'STRONG' || /font-weight:\s*(bold|[6-9]00)/i.test(style)) nf.b = true;
            if (tag === 'I' || tag === 'EM' || /font-style:\s*italic/i.test(style)) nf.i = true;
            if (tag === 'U' || /text-decoration[^;]*underline/i.test(style)) nf.u = true;
            if (tag === 'S' || tag === 'STRIKE' || tag === 'DEL' || /line-through/i.test(style)) nf.s = true;
            if (tag === 'A') { nf.u = true; nf.color = '2563EB'; }
            const col = cssColorHex(node.style && node.style.color) || (tag === 'FONT' && cssColorHex(node.getAttribute('color')));
            if (col) nf.color = col;
            if (tag === 'BR') { pending.push('<w:r><w:br/></w:r>'); return; }
            if (tag === 'IMG') {
                const png = await imagePng(node);
                if (!png) return;
                const id = media.length + 1;
                media.push({ name: `image${id}.png`, bytes: png.bytes });
                const pct = parseFloat(node.style.width) || 100;
                const cx = Math.round(PAGE_EMU * Math.min(100, pct) / 100);
                const cy = Math.round(cx * png.h / png.w);
                pending.push(`<w:r><w:drawing><wp:inline distT="0" distB="0" distL="0" distR="0"><wp:extent cx="${cx}" cy="${cy}"/><wp:docPr id="${id}" name="Picture ${id}"/><wp:cNvGraphicFramePr><a:graphicFrameLocks xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" noChangeAspect="1"/></wp:cNvGraphicFramePr><a:graphic xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"><a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/picture"><pic:pic xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture"><pic:nvPicPr><pic:cNvPr id="${id}" name="image${id}.png"/><pic:cNvPicPr/></pic:nvPicPr><pic:blipFill><a:blip r:embed="rIdImg${id}"/><a:stretch><a:fillRect/></a:stretch></pic:blipFill><pic:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="${cx}" cy="${cy}"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></pic:spPr></pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing></w:r>`);
                return;
            }
            if (tag === 'UL' || tag === 'OL') {
                if (pending.length) flushPara();
                const prev = listInfo;
                let n = 0;
                for (const li of node.children) {
                    if (li.tagName !== 'LI') continue;
                    listInfo = { ordered: tag === 'OL', n: ++n };
                    for (const c of li.childNodes) await walk(c, nf);
                    flushPara();
                }
                listInfo = prev;
                return;
            }
            const block = ['DIV', 'P', 'H1', 'H2', 'H3', 'BLOCKQUOTE', 'LI'].includes(tag);
            if (block && pending.length) flushPara();
            if (/^H[1-3]$/.test(tag)) { nf.b = true; nf.size = { H1: 44, H2: 36, H3: 28 }[tag]; }
            for (const c of node.childNodes) await walk(c, nf);
            if (block) {
                const empty = !pending.length;
                if (tag === 'BLOCKQUOTE') flushPara('<w:ind w:left="567"/>');
                else if (!empty || !node.querySelector('img, ul, ol, div, p, h1, h2, h3')) flushPara();
            }
        }

        try {
            for (const c of root.childNodes) await walk(c, {});
            if (pending.length) flushPara();
        } catch (err) {
            console.error(err);
            toast('Could not make the Word file');
            return;
        }
        const title = runXml(note.title.trim() || 'Untitled', { b: true, size: 40 });
        const W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main';
        const docXml = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="${W}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"><w:body><w:p><w:pPr><w:spacing w:after="240"/></w:pPr>${title}</w:p>${paras.join('')}<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="708" w:footer="708" w:gutter="0"/></w:sectPr></w:body></w:document>`;
        const rels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">${media.map((m, i) => `<Relationship Id="rIdImg${i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/${m.name}"/>`).join('')}<Relationship Id="rIdStyles" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`;
        const styles = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="${W}"><w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" w:eastAsia="Calibri" w:cs="Calibri"/><w:sz w:val="22"/><w:szCs w:val="22"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:spacing w:after="80" w:line="276" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults></w:styles>`;
        const blob = makeZip([
            { name: '[Content_Types].xml', data: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Default Extension="png" ContentType="image/png"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/><Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/></Types>` },
            { name: '_rels/.rels', data: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>` },
            { name: 'word/document.xml', data: docXml },
            { name: 'word/styles.xml', data: styles },
            { name: 'word/_rels/document.xml.rels', data: rels },
            ...media.map((m) => ({ name: `word/media/${m.name}`, data: m.bytes }))
        ]);
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `${exportName()}.docx`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 10000);
    }

    $('textDownload').addEventListener('click', (e) => {
        if (!noteById(state.currentNoteId)) return;
        showMenu(e.currentTarget, 'Download this note', [
            { label: 'PDF', icon: '📄', run: downloadPdf },
            { label: 'Word (.docx)', icon: '📝', run: downloadWord }
        ]);
    });

    /* ---------------- videos (YouTube / Vimeo) in Text and Collage ---------------- */
    function videoEmbedUrl(text) {
        const url = String(text || '').trim();
        let m = /^(?:https?:\/\/)?(?:www\.|m\.|music\.)?(?:youtube\.com\/(?:watch\?(?:[^#\s]*&)?v=|shorts\/|embed\/|live\/|v\/)|youtu\.be\/)([A-Za-z0-9_-]{11})(\S*)$/i.exec(url);
        if (m) {
            const t = /[?&#](?:t|start)=(?:(\d+)h)?(?:(\d+)m)?(\d+)s?/.exec(m[2] || '');
            const start = t ? (Number(t[1] || 0) * 3600 + Number(t[2] || 0) * 60 + Number(t[3] || 0)) : 0;
            return `https://www.youtube-nocookie.com/embed/${m[1]}${start ? `?start=${start}` : ''}`;
        }
        m = /^(?:https?:\/\/)?(?:www\.|player\.)?vimeo\.com\/(?:video\/)?(\d+)/i.exec(url);
        if (m) return `https://player.vimeo.com/video/${m[1]}`;
        return null;
    }
    // the normal web address of an embedded video (for "open", PDF / Word)
    function videoWatchUrl(src) {
        let m = /youtube-nocookie\.com\/embed\/([A-Za-z0-9_-]{11})(?:\?start=(\d+))?/.exec(src);
        if (m) return `https://www.youtube.com/watch?v=${m[1]}${m[2] ? `&t=${m[2]}s` : ''}`;
        m = /player\.vimeo\.com\/video\/(\d+)/.exec(src);
        return m ? `https://vimeo.com/${m[1]}` : src;
    }
    function videoFrameHtml(src, loop = false) {
        if (isMediaRef(src)) return `<video data-media="${escapeHtml(src)}" controls playsinline preload="metadata"${loop ? ' loop' : ''}></video>`;
        return `<iframe src="${escapeHtml(src)}" title="Video" loading="lazy" allowfullscreen ` +
            'allow="accelerometer; encrypted-media; gyroscope; picture-in-picture; fullscreen" referrerpolicy="strict-origin-when-cross-origin"></iframe>';
    }
    // w: width in % of the note (empty = the normal size)
    function videoBlock(doc, src, w, loop = false) {
        const box = doc.createElement('div');
        box.className = `video-embed${isMediaRef(src) ? ' local' : ''}`;
        box.setAttribute('data-video', src);
        box.setAttribute('contenteditable', 'false');
        const pct = Math.round(Number(w));
        if (pct >= 10 && pct <= 100) { box.setAttribute('data-w', String(pct)); box.style.width = `${pct}%`; }
        if (loop && isMediaRef(src)) box.setAttribute('data-loop', '1');
        box.innerHTML = `${videoFrameHtml(src, loop)}<span class="ve-handle" title="Drag to resize"></span>`;
        return box;
    }
    // players of video files get their video when they are shown
    function hydrateLocalVideos(root) {
        if (!root || !root.querySelectorAll) return;
        const list = root.matches && root.matches('video[data-media]') ? [root] : root.querySelectorAll('video[data-media]');
        list.forEach((v) => {
            if (!v.dataset.errWatch) {
                v.dataset.errWatch = '1';
                v.addEventListener('error', () => showUnplayable(v));
            }
            if (v.getAttribute('src')) return;
            const ref = v.getAttribute('data-media');
            mediaUrl(ref).then((url) => { if (!v.getAttribute('src')) v.src = url; })
                .catch(() => { v.classList.add('missing'); v.title = 'This video is not on this device yet (sync to get it)'; });
        });
    }
    function showUnplayable(v) {
        if (!v.getAttribute('src') || (v.nextElementSibling && v.nextElementSibling.classList.contains('video-error'))) return;
        const fmt = (/^data:video\/([\w.+-]+)/.exec(mediaData(v.getAttribute('data-media')) || '') || [])[1] || 'this';
        const msg = document.createElement('div');
        msg.className = 'video-error';
        msg.contentEditable = 'false';
        msg.textContent = `This device cannot play ${fmt.toUpperCase()} videos. On a computer: right-click the video → "Convert to MP4", then it plays everywhere.`;
        v.insertAdjacentElement('afterend', msg);
    }
    new MutationObserver((list) => {
        list.forEach((m) => m.addedNodes.forEach((n) => { if (n.nodeType === 1) hydrateLocalVideos(n); }));
    }).observe(document.body, { childList: true, subtree: true });
    // let the user choose files (resolves [] when nothing was chosen)
    function pickFiles(accept, multiple = true) {
        return new Promise((resolve) => {
            const inp = document.createElement('input');
            inp.type = 'file';
            inp.accept = accept;
            inp.multiple = multiple;
            inp.hidden = true;
            const done = (files) => { resolve(files); inp.remove(); };
            inp.addEventListener('change', () => done([...(inp.files || [])]));
            inp.addEventListener('cancel', () => done([]));
            document.body.appendChild(inp);
            inp.click();
        });
    }
    // a YouTube / Vimeo link (a string), or { files } when video files were chosen
    async function askVideoUrl() {
        const url = await askDialog({ title: 'Add a video', message: 'Paste a YouTube or Vimeo link, or upload a video file.', input: '', okLabel: 'Embed link', altLabel: 'Upload video file' });
        if (url === 'alt') {
            const files = (await pickFiles('video/*')).filter(isVideoFile);
            return files.length ? { files } : null;
        }
        if (!url) return null;
        const src = videoEmbedUrl(url);
        if (!src) toast('That is not a YouTube or Vimeo link');
        return src;
    }
    function insertVideoInText(src, at) {
        if (!noteById(state.currentNoteId) || !src) return;
        recordText();
        const point = at || caretPoint();
        const block = videoBlock(document, src);
        placeLine(block, point.inline ? { before: point.line && point.line.nextSibling } : point);
        markTextChanged();
        return block;
    }
    $('insertVideoEmbedBtn').addEventListener('click', async () => {
        if (!noteById(state.currentNoteId)) return;
        const at = caretPoint();
        const src = await askVideoUrl();
        if (src && src.files) readVideoFiles(src.files).then((srcs) => srcs.forEach((s) => insertVideoInText(s, at)));
        else if (src) insertVideoInText(src, at);
    });

    /* ---------------- KLIPY GIFs ----------------
     * Needs a free KLIPY API key (klipy.com/developers), kept on this device. */
    const KLIPY_KEY = 'remmin.klipyKey.v1';
    const KLIPY_USER = 'remmin.klipyUser.v1';
    let gifPicker = null;
    function klipyUser() {
        let id = RemminStore.get(KLIPY_USER);
        if (!id) { id = uid(); RemminStore.set(KLIPY_USER, id); }
        return id;
    }
    function buildGifPicker() {
        const el = document.createElement('div');
        el.className = 'gif-picker-overlay';
        el.innerHTML = `
            <div class="gif-picker" role="dialog" aria-label="GIFs from KLIPY">
                <div class="gif-head">
                    <input type="search" class="gif-search" placeholder="Search KLIPY GIFs…" autocomplete="off">
                    <button type="button" class="gif-close" title="Close" aria-label="Close">✕</button>
                </div>
                <div class="gif-key" hidden>
                    <p>To use KLIPY GIFs, you need a free API key. Make one at
                    <a href="https://klipy.com/developers" target="_blank" rel="noopener noreferrer">klipy.com/developers</a>
                    and paste it here. It is kept on this device only.</p>
                    <div class="gif-key-row"><input type="text" class="gif-key-input" placeholder="KLIPY API key" autocomplete="off" spellcheck="false"><button type="button" class="btn btn-primary gif-key-save">Save</button></div>
                </div>
                <div class="gif-grid"></div>
                <div class="gif-foot"><button type="button" class="btn gif-upload">Upload GIF file</button><span>Powered by <b>KLIPY</b></span><button type="button" class="gif-more" hidden>More</button><button type="button" class="gif-change-key">API key</button></div>
            </div>`;
        document.body.appendChild(el);
        const q = el.querySelector('.gif-search'), grid = el.querySelector('.gif-grid'), keyBox = el.querySelector('.gif-key');
        const more = el.querySelector('.gif-more');
        let page = 1, timer = null, token = 0;
        const close = () => { el.classList.remove('active'); gifPicker.onPick = null; gifPicker.onFiles = null; };
        el.querySelector('.gif-upload').addEventListener('click', async () => {
            const onFiles = gifPicker.onFiles;
            const files = (await pickFiles('image/gif')).filter((f) => f.type.startsWith('image/'));
            if (!files.length) return;
            close();
            if (onFiles) onFiles(files);
        });
        const showKey = (on) => { keyBox.hidden = !on; grid.hidden = on; if (on) el.querySelector('.gif-key-input').value = RemminStore.get(KLIPY_KEY) || ''; };
        async function load(reset) {
            const key = (RemminStore.get(KLIPY_KEY) || '').trim();
            if (!key) { showKey(true); return; }
            showKey(false);
            if (reset) { page = 1; grid.innerHTML = '<div class="gif-msg">Loading…</div>'; }
            const my = ++token;
            const term = q.value.trim();
            const url = `https://api.klipy.com/api/v1/${encodeURIComponent(key)}/gifs/${term ? 'search' : 'trending'}` +
                `?page=${page}&per_page=24&customer_id=${encodeURIComponent(klipyUser())}&locale=${encodeURIComponent((navigator.language || 'en').slice(0, 2))}` +
                (term ? `&q=${encodeURIComponent(term)}` : '');
            try {
                const res = await fetch(url);
                if (my !== token) return;
                if (res.status === 401 || res.status === 403) { grid.innerHTML = '<div class="gif-msg">KLIPY refused the API key. Check it with "API key" below.</div>'; return; }
                if (!res.ok) throw new Error(`KLIPY error ${res.status}`);
                const json = await res.json();
                const box = (json && json.data) || {};
                const items = Array.isArray(box.data) ? box.data : [];
                if (reset) grid.innerHTML = '';
                items.forEach((it) => {
                    const f = it.file || {};
                    const pick = (sizes) => { for (const s of sizes) { const g = f[s] && f[s].gif; if (g && g.url) return g; } return null; };
                    const thumb = pick(['sm', 'xs', 'md']), full = pick(['md', 'sm', 'hd']);
                    if (!thumb || !full) return;
                    const b = document.createElement('button');
                    b.type = 'button';
                    b.className = 'gif-item';
                    b.title = it.title || 'GIF';
                    b.innerHTML = `<img loading="lazy" alt="" src="${escapeHtml(thumb.url)}">`;
                    b.addEventListener('click', () => { const pickFn = gifPicker.onPick; close(); if (pickFn) pickFn(full.url); });
                    grid.appendChild(b);
                });
                if (reset && !grid.children.length) grid.innerHTML = '<div class="gif-msg">No GIFs found</div>';
                more.hidden = !box.has_next;
            } catch (err) {
                if (my !== token) return;
                grid.innerHTML = `<div class="gif-msg">Could not reach KLIPY (${escapeHtml(err.message || 'network error')}).</div>`;
            }
        }
        q.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(() => load(true), 350); });
        q.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });
        more.addEventListener('click', () => { page++; load(false); });
        el.querySelector('.gif-close').addEventListener('click', close);
        el.addEventListener('pointerdown', (e) => { if (e.target === el) close(); });
        el.querySelector('.gif-change-key').addEventListener('click', () => showKey(keyBox.hidden));
        el.querySelector('.gif-key-save').addEventListener('click', () => {
            const v = el.querySelector('.gif-key-input').value.trim();
            if (v) RemminStore.set(KLIPY_KEY, v); else RemminStore.remove(KLIPY_KEY);
            load(true);
        });
        el.querySelector('.gif-key-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') el.querySelector('.gif-key-save').click(); });
        return { el, q, load, onPick: null };
    }
    function openGifPicker(onPick, onFiles) {
        if (!gifPicker) gifPicker = buildGifPicker();
        gifPicker.onPick = onPick;
        gifPicker.onFiles = onFiles || null;
        gifPicker.el.classList.add('active');
        gifPicker.q.value = '';
        gifPicker.load(true);
        setTimeout(() => gifPicker.q.focus(), 30);
    }
    // the GIF itself (kept in the note, so it keeps working offline); null when KLIPY doesn't allow that
    async function fetchGifFile(url) {
        try {
            const res = await fetch(url);
            if (!res.ok) return null;
            const blob = await res.blob();
            if (!blob.type.startsWith('image/')) return null;
            return new File([blob], 'klipy.gif', { type: blob.type });
        } catch (err) { return null; }
    }
    $('insertGifBtn').addEventListener('click', () => {
        if (!noteById(state.currentNoteId)) return;
        const at = caretPoint();
        openGifPicker(async (url) => {
            const file = await fetchGifFile(url);
            if (file) { insertImageFiles([file], at); return; }
            // could not download it: show it from KLIPY instead
            recordText();
            placeLine(pictureLine(url), at.inline ? { before: at.line && at.line.nextSibling } : at);
            markTextChanged();
        }, (files) => insertImageFiles(files, at));
    });
    $('toolGif').addEventListener('click', () => {
        if (!noteById(state.currentNoteId)) return;
        openGifPicker(async (url) => {
            const file = await fetchGifFile(url);
            if (file) addDrawImages([file]);
            else toast('This GIF could not be downloaded, so it can\'t go in a drawing');
        }, (files) => addDrawImages(files));
    });

    // Download the selected picture (also possible with right-click → Save image as)
    $('imageDownload').addEventListener('click', () => {
        if (!selectedImg) return;
        selectedImgs.filter((i) => editorEl.contains(i)).forEach((img, k) => setTimeout(() => downloadPicture(img), k * 250));
    });
    function downloadPicture(selectedImg) {
        const src = selectedImg.getAttribute('src') || '';
        const mime = (/^data:([^;,]+)/.exec(src) || [])[1] || 'image/png';
        const ext = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif', 'image/svg+xml': 'svg' }[mime] || 'png';
        const note = noteById(state.currentNoteId);
        const base = ((note && note.title.trim()) || 'picture').replace(/[\\/:*?"<>|]+/g, '').slice(0, 60) || 'picture';
        const pics = [...editorEl.querySelectorAll('img')];
        const n = pics.length > 1 ? `-${pics.indexOf(selectedImg) + 1}` : '';
        const a = document.createElement('a');
        a.href = src;
        a.download = `${base}${n}.${ext}`;
        document.body.appendChild(a);
        a.click();
        a.remove();
    }
    // put the selected picture next to the picture(s) on the line above it
    $('imageSideBySide').addEventListener('click', () => {
        if (!selectedImg) return;
        const line = selectedImg.parentNode;
        let prev = line && line.parentNode === editorEl ? line.previousSibling : null;
        while (prev && prev.nodeType === 3 && !prev.textContent.trim()) prev = prev.previousSibling;
        if (!isPictureLine(prev)) { toast('There is no picture on the line above to put it next to'); return; }
        placeInline(selectedImg, { line: prev, beforeImg: null });
        selectImage(selectedImg);
        markTextChanged();
    });
    // put the selected picture on a line of its own (below the line it is on now)
    $('imageOwnLine').addEventListener('click', () => {
        if (!selectedImg) return;
        const line = selectedImg.parentNode;
        if (!line || line.parentNode !== editorEl) return;
        if (isPictureLine(line) && line.querySelectorAll(':scope > img').length === 1) { toast('This picture already has its own line'); return; }
        const own = document.createElement('div');
        editorEl.insertBefore(own, line.nextSibling);
        own.appendChild(selectedImg);
        if (!line.querySelector('img') && !line.textContent.trim()) line.remove();
        selectImage(selectedImg);
        markTextChanged();
    });
    /* ---- corner handle on the selected picture(s): drag to make them all bigger / smaller ---- */
    const imgHandle = document.createElement('div');
    imgHandle.className = 'img-scale-handle';
    imgHandle.title = 'Drag to make the selected picture(s) bigger / smaller';
    imgHandle.hidden = true;
    document.body.appendChild(imgHandle);
    function placeImgHandle() {
        const img = selectedImg && editorEl.contains(selectedImg) && state.prefs.mode === 'text' && editorEl.offsetParent ? selectedImg : null;
        imgHandle.hidden = !img;
        if (!img) return;
        const r = img.getBoundingClientRect();
        imgHandle.style.left = `${r.right - 7}px`;
        imgHandle.style.top = `${r.bottom - 7}px`;
    }
    window.addEventListener('scroll', placeImgHandle, true);
    window.addEventListener('resize', placeImgHandle);
    editorEl.addEventListener('input', () => requestAnimationFrame(placeImgHandle));
    let imgScale = null;
    imgHandle.addEventListener('pointerdown', (e) => {
        if (!selectedImg) return;
        e.preventDefault();
        imgHandle.setPointerCapture(e.pointerId);
        recordText();
        const imgs = selectedImgs.filter((i) => editorEl.contains(i));
        imgScale = { x: e.clientX, w0: selectedImg.getBoundingClientRect().width || 1, pct0: parseFloat(selectedImg.style.width) || 100,
            orig: imgs.map((i) => [i, parseFloat(i.style.width) || 100]) };
    });
    imgHandle.addEventListener('pointermove', (e) => {
        if (!imgScale) return;
        let f = Math.max(0.05, (imgScale.w0 + e.clientX - imgScale.x) / imgScale.w0);
        // snap to S / M / L / Full when close
        const want = imgScale.pct0 * f;
        const snap = [12.5, 25, 50, 100].find((v) => Math.abs(v - want) < 1.5);
        if (snap) f = snap / imgScale.pct0;
        imgScale.orig.forEach(([img, pct]) => { img.style.width = `${Math.round(Math.min(100, Math.max(3, pct * f)) * 10) / 10}%`; });
        placeImgHandle();
    });
    const endImgScale = () => {
        if (!imgScale) return;
        imgScale = null;
        markTextChanged();
        placeImgHandle();
    };
    imgHandle.addEventListener('pointerup', endImgScale);
    imgHandle.addEventListener('pointercancel', endImgScale);

    $('imageRemove').addEventListener('click', () => {
        if (!selectedImg) return;
        selectedImgs.forEach((i) => i.remove());
        selectImage(null);
        markTextChanged();
    });
    $('insertImageBtn').addEventListener('click', () => {
        if (!noteById(state.currentNoteId)) return;
        $('imageFile').click();
    });
    $('imageFile').addEventListener('change', (e) => {
        if (e.target.files.length) insertImageFiles(e.target.files);
        e.target.value = '';
    });

    /* ---------------- voice notes ----------------
     * Stored inside the note text as
     *   <span data-voice="data:audio/...;base64,..." data-name="..." data-duration="12">🎤 name</span>
     * and shown in the editor as a small player chip.
     */

    const VOICE_SRC_RE = /^data:audio\/[\w.+-]+(;[\w=.+-]+)*;base64,[A-Za-z0-9+/=]+$/i;
    // audio is kept once in the media store ("media:<id>"); older notes may still hold the data itself
    const isVoiceSrc = (s) => isMediaRef(s) || VOICE_SRC_RE.test(s || '');
    const resolveAudio = (s) => (isMediaRef(s) ? mediaData(s) : s);
    // the stored form of an audio: its media reference (the same file twice = stored once)
    const audioRef = (s) => (typeof s === 'string' && s.startsWith('data:audio/') ? mediaIdFor(s) : s);
    const VOICE_MAX_SECONDS = 10 * 60;
    const VOICE_MAX_CHARS = 25 * 1048576 * 4 / 3 + 200; // 25 MB of audio (stored as text)

    function formatSeconds(sec) {
        sec = Math.max(0, Math.round(Number(sec) || 0));
        return `${Math.floor(sec / 60)}:${pad(sec % 60)}`;
    }

    function voiceChipHtml(src, name, duration, loop = false, vol = 100) {
        const span = document.createElement('span');
        span.setAttribute('data-voice', src);
        if (loop) span.setAttribute('data-loop', '1');
        if (vol < 100) span.setAttribute('data-vol', String(vol));
        span.setAttribute('data-name', name || 'Voice note');
        span.setAttribute('data-duration', String(Math.round(duration || 0)));
        span.textContent = `🎤 ${name || 'Voice note'}`;
        return span.outerHTML;
    }

    // Turn the stored <span data-voice> elements into playable chips
    function decorateVoiceChips(root = editorEl) {
        root.querySelectorAll('span[data-voice]').forEach((chip) => {
            if (chip.classList.contains('voice-chip')) return;
            chip.classList.add('voice-chip');
            chip.contentEditable = 'false';
            const name = chip.getAttribute('data-name') || 'Voice note';
            chip.innerHTML = `<span class="voice-handle" title="Drag to move">⠿</span>` +
                `<button class="voice-back" type="button" title="15 seconds back">⏪</button>` +
                `<button class="voice-play" type="button" title="Play">▶</button>` +
                `<button class="voice-fwd" type="button" title="15 seconds forward">⏩</button>` +
                `<input class="voice-seek" type="range" min="0" max="1000" value="0" title="Click or drag to go to a moment">` +
                `<button class="voice-loop${chip.getAttribute('data-loop') === '1' ? ' on' : ''}" type="button" title="Loop: play again and again">🔁</button>` +
                `<span class="voice-vol-icon" title="Volume">🔊</span><input class="voice-vol" type="range" min="0" max="100" value="${chipVolume(chip)}" title="Volume">` +
                `<span class="voice-name" title="Click to rename">🎤 ${escapeHtml(name)}</span>` +
                `<span class="voice-time">${formatSeconds(chip.getAttribute('data-duration'))}</span>` +
                `<button class="voice-remove" type="button" title="Remove voice note">✕</button>`;
        });
    }

    // the volume of an audio chip (0–100, 100 when not set)
    function chipVolume(chip) {
        const v = chip.getAttribute('data-vol');
        const n = v === null ? 100 : Math.round(Number(v));
        return Number.isFinite(n) ? Math.max(0, Math.min(100, n)) : 100;
    }

    /* ---- playback ---- */
    let voiceAudio = null;
    let voicePlayingChip = null;

    function stopVoicePlayback() {
        if (voiceAudio) { voiceAudio.pause(); voiceAudio = null; }
        if (voicePlayingChip) {
            const btn = voicePlayingChip.querySelector('.voice-play');
            if (btn) btn.textContent = '▶';
            const time = voicePlayingChip.querySelector('.voice-time');
            if (time) time.textContent = formatSeconds(voicePlayingChip.getAttribute('data-duration'));
            const seek = voicePlayingChip.querySelector('.voice-seek');
            if (seek) seek.value = '0';
            voicePlayingChip.classList.remove('playing');
        }
        voicePlayingChip = null;
    }

    function toggleVoicePlayback(chip) {
        if (voicePlayingChip === chip && voiceAudio) {
            if (voiceAudio.paused) { voiceAudio.play(); chip.querySelector('.voice-play').textContent = '⏸'; }
            else { voiceAudio.pause(); chip.querySelector('.voice-play').textContent = '▶'; }
            return;
        }
        stopVoicePlayback();
        voicePlayingChip = chip;
        voiceAudio = new Audio(resolveAudio(chip.getAttribute('data-voice')));
        voiceAudio.loop = chip.getAttribute('data-loop') === '1';
        voiceAudio.volume = chipVolume(chip) / 100;
        const total = Number(chip.getAttribute('data-duration')) || 0;
        const timeEl = chip.querySelector('.voice-time');
        const seekEl = chip.querySelector('.voice-seek');
        voiceAudio.addEventListener('timeupdate', () => {
            if (voicePlayingChip !== chip) return;
            timeEl.textContent = `${formatSeconds(voiceAudio.currentTime)} / ${formatSeconds(total || voiceAudio.duration)}`;
            const len = voiceLength(chip);
            if (seekEl && !seekEl.matches(':active') && len) seekEl.value = String(Math.round(Math.min(1, voiceAudio.currentTime / len) * 1000));
        });
        voiceAudio.addEventListener('ended', stopVoicePlayback);
        voiceAudio.play().catch(() => { toast('This voice note cannot be played in this browser'); stopVoicePlayback(); });
        chip.querySelector('.voice-play').textContent = '⏸';
        chip.classList.add('playing');
    }

    // the length of an audio (the browser's, or the stored one: recordings often have no length in the file)
    function voiceLength(chip) {
        const d = voicePlayingChip === chip && voiceAudio ? voiceAudio.duration : NaN;
        return Number.isFinite(d) && d > 0 ? d : (Number(chip.getAttribute('data-duration')) || 0);
    }
    // play the chip (if it is not playing yet), then run fn with its audio once it can jump
    function withVoicePlaying(chip, fn) {
        if (voicePlayingChip !== chip || !voiceAudio) toggleVoicePlayback(chip);
        const a = voiceAudio;
        if (!a) return;
        if (a.readyState >= 1) fn(a); else a.addEventListener('loadedmetadata', () => fn(a), { once: true });
    }
    function voiceSkip(chip, delta) {
        withVoicePlaying(chip, (a) => {
            const len = voiceLength(chip);
            a.currentTime = Math.max(0, Math.min(len ? len - 0.05 : a.currentTime + delta, a.currentTime + delta));
        });
    }
    function voiceSeekTo(chip, fraction) {
        withVoicePlaying(chip, (a) => { a.currentTime = Math.max(0, fraction * voiceLength(chip)); });
    }
    // chip buttons must not move the text cursor (the sliders must still be draggable)
    editorEl.addEventListener('mousedown', (e) => {
        if (e.target.closest('.voice-chip') && !e.target.closest('.voice-vol, .voice-seek')) e.preventDefault();
    });
    // the volume slider of an audio chip (Text and Collage)
    function chipVolumeInput(slider, done) {
        const chip = slider.closest('.voice-chip');
        if (!chip) return;
        const v = Math.round(Number(slider.value));
        if (v >= 100) chip.removeAttribute('data-vol'); else chip.setAttribute('data-vol', String(v));
        if (voicePlayingChip === chip && voiceAudio) voiceAudio.volume = v / 100;
        if (done) done(v);
    }
    editorEl.addEventListener('input', (e) => {
        if (!e.target.classList) return;
        if (e.target.classList.contains('voice-seek')) {
            e.stopImmediatePropagation();
            const chip = e.target.closest('.voice-chip');
            if (chip) voiceSeekTo(chip, Number(e.target.value) / 1000);
            return;
        }
        if (!e.target.classList.contains('voice-vol')) return;
        e.stopImmediatePropagation();
        chipVolumeInput(e.target);
    }, true);
    editorEl.addEventListener('change', (e) => {
        if (e.target.classList && e.target.classList.contains('voice-vol')) markTextChanged();
    });

    /* ---- move a voice note by dragging its handle (mouse and touch) ---- */
    let voiceDrag = null; // { chip, pointerId, range }
    const dropMarker = document.createElement('div');
    dropMarker.className = 'voice-drop-marker';
    document.body.appendChild(dropMarker);

    function caretRangeAt(x, y) {
        let range = null;
        if (document.caretRangeFromPoint) range = document.caretRangeFromPoint(x, y);
        else if (document.caretPositionFromPoint) {
            const pos = document.caretPositionFromPoint(x, y);
            if (pos) { range = document.createRange(); range.setStart(pos.offsetNode, pos.offset); range.collapse(true); }
        }
        return range;
    }

    // Where would the chip land if dropped at (x, y)?
    function voiceDropRange(x, y, chip) {
        const over = document.elementFromPoint(x, y);
        const otherChip = over && over.closest && over.closest('.voice-chip');
        let range;
        if (otherChip && editorEl.contains(otherChip)) {
            // over a chip (also itself): drop before or after it
            if (otherChip === chip) return null;
            const r = otherChip.getBoundingClientRect();
            range = document.createRange();
            if (x < r.left + r.width / 2) range.setStartBefore(otherChip); else range.setStartAfter(otherChip);
            range.collapse(true);
            return range;
        }
        range = caretRangeAt(x, y);
        if (!range || !editorEl.contains(range.startContainer) || chip.contains(range.startContainer)) {
            const r = editorEl.getBoundingClientRect();
            if (x < r.left || x > r.right || y < r.top || y > r.bottom) return null;
            range = document.createRange();               // below the last line: put it at the end
            range.selectNodeContents(editorEl);
            range.collapse(false);
        }
        return range;
    }

    function showDropMarker(range) {
        if (!range) { dropMarker.style.display = 'none'; return; }
        let rect = range.getClientRects()[0] || range.getBoundingClientRect();
        if (!rect || (!rect.height && !rect.width)) {
            const node = range.startContainer.nodeType === 1 ? range.startContainer : range.startContainer.parentElement;
            const child = node && node.childNodes[range.startOffset];
            const el = child && child.nodeType === 1 ? child : node;
            rect = el ? el.getBoundingClientRect() : null;
            if (rect && child && child.nodeType === 1) rect = { left: rect.left, top: rect.top, height: rect.height || 18 };
            else if (rect) rect = { left: rect.right, top: rect.top, height: Math.min(rect.height || 18, 24) };
        }
        if (!rect) { dropMarker.style.display = 'none'; return; }
        dropMarker.style.display = 'block';
        dropMarker.style.left = `${rect.left - 1}px`;
        dropMarker.style.top = `${rect.top}px`;
        dropMarker.style.height = `${Math.max(16, rect.height || 18)}px`;
    }

    editorEl.addEventListener('pointerdown', (e) => {
        const handle = e.target.closest('.voice-handle');
        if (!handle) return;
        const chip = handle.closest('.voice-chip');
        if (!chip || !editorEl.contains(chip)) return;
        e.preventDefault();
        e.stopPropagation();
        try { handle.setPointerCapture(e.pointerId); } catch (_) { /* ignore */ }
        voiceDrag = { chip, handle, pointerId: e.pointerId, range: null };
        chip.classList.add('dragging');
        document.body.classList.add('voice-dragging');
    });

    editorEl.addEventListener('pointermove', (e) => {
        if (!voiceDrag || e.pointerId !== voiceDrag.pointerId) return;
        e.preventDefault();
        // scroll the page when you drag near the top/bottom edge
        const scroller = editorEl.closest('.content');
        if (scroller) {
            const r = scroller.getBoundingClientRect();
            if (e.clientY < r.top + 40) scroller.scrollTop -= 12;
            else if (e.clientY > r.bottom - 40) scroller.scrollTop += 12;
        }
        voiceDrag.range = voiceDropRange(e.clientX, e.clientY, voiceDrag.chip);
        showDropMarker(voiceDrag.range);
    });

    function endVoiceDrag(e, cancelled) {
        if (!voiceDrag || (e && e.pointerId !== voiceDrag.pointerId)) return;
        const { chip, range } = voiceDrag;
        voiceDrag = null;
        chip.classList.remove('dragging');
        document.body.classList.remove('voice-dragging');
        dropMarker.style.display = 'none';
        if (cancelled || !range || !editorEl.contains(range.startContainer)) return;
        // dropping right next to where it already is: nothing to do
        if ((range.startContainer === chip.parentNode &&
            (chip.parentNode.childNodes[range.startOffset] === chip || chip.parentNode.childNodes[range.startOffset - 1] === chip))) return;
        range.insertNode(chip);
        // keep a space on both sides so you can still type around it
        const nbsp = () => document.createTextNode('\u00a0');
        if (!chip.previousSibling || (chip.previousSibling.nodeType === 1 && chip.previousSibling.matches('.voice-chip'))) chip.before(nbsp());
        if (!chip.nextSibling || (chip.nextSibling.nodeType === 1 && chip.nextSibling.matches('.voice-chip'))) chip.after(nbsp());
        markTextChanged();
        flushEditor();
    }
    editorEl.addEventListener('pointerup', (e) => endVoiceDrag(e, false));
    editorEl.addEventListener('pointercancel', (e) => endVoiceDrag(e, true));
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && voiceDrag) endVoiceDrag(null, true); });

    async function handleVoiceChipClick(e) {
        const chip = e.target.closest('.voice-chip');
        if (!chip || !editorEl.contains(chip)) return false;
        e.preventDefault();
        if (e.target.closest('.voice-handle')) return true;
        if (e.target.closest('.voice-play')) { toggleVoicePlayback(chip); return true; }
        if (e.target.closest('.voice-back')) { voiceSkip(chip, -15); return true; }
        if (e.target.closest('.voice-fwd')) { voiceSkip(chip, 15); return true; }
        if (e.target.closest('.voice-seek, .voice-vol')) return true;
        if (e.target.closest('.voice-loop')) {
            const on = chip.getAttribute('data-loop') !== '1';
            textChipSetLoop(chip, on);
            toast(on ? 'Loop on: it plays again and again' : 'Loop off');
            return true;
        }
        if (e.target.closest('.voice-remove')) {
            const ok = await askConfirm('Remove voice note', `Remove "${chip.getAttribute('data-name')}" from this note?`, 'Delete');
            if (!ok) return true;
            if (voicePlayingChip === chip) stopVoicePlayback();
            chip.remove();
            markTextChanged();
            return true;
        }
        if (e.target.closest('.voice-name')) {
            const name = await askText('Rename voice note', chip.getAttribute('data-name'), 'Rename');
            if (name === null || !name.trim() || !editorEl.contains(chip)) return true;
            chip.setAttribute('data-name', name.trim());
            chip.querySelector('.voice-name').textContent = `🎤 ${name.trim()}`;
            markTextChanged();
            return true;
        }
        if (e.target.closest('.voice-time')) toggleVoicePlayback(chip);
        return true;
    }

    /* ---- inserting ---- */
    let savedEditorRange = null;

    function saveEditorSelection() {
        const sel = window.getSelection();
        savedEditorRange = sel.rangeCount && editorEl.contains(sel.getRangeAt(0).commonAncestorContainer)
            ? sel.getRangeAt(0).cloneRange() : null;
    }

    function restoreEditorSelection() {
        editorEl.focus();
        const sel = window.getSelection();
        sel.removeAllRanges();
        if (savedEditorRange) sel.addRange(savedEditorRange);
        else {
            const range = document.createRange();
            range.selectNodeContents(editorEl);
            range.collapse(false);
            sel.addRange(range);
        }
    }

    function insertVoiceChip(src, name, duration, loop = false, vol = 100) {
        if (!noteById(state.currentNoteId)) return;
        if (src.length > VOICE_MAX_CHARS) {
            toast('This recording is too large to store. Keep voice notes under a few minutes.');
            return;
        }
        ensureCaretInEditor();
        src = audioRef(src);
        const html = voiceChipHtml(src, name, duration, loop, vol) + '&nbsp;';
        if (!document.execCommand('insertHTML', false, html)) {
            const tmp = document.createElement('span');
            tmp.innerHTML = html;
            insertNodeAtCaret(tmp.firstChild);
        }
        decorateVoiceChips();
        textDirty = true;
        flushEditor();
    }

    function blobToDataUrl(blob) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result);
            reader.onerror = () => reject(reader.error);
            reader.readAsDataURL(blob);
        });
    }

    function audioDuration(src) {
        return new Promise((resolve) => {
            const a = new Audio();
            const done = (v) => resolve(Number.isFinite(v) ? v : 0);
            a.addEventListener('loadedmetadata', () => done(a.duration));
            a.addEventListener('error', () => done(0));
            setTimeout(() => done(0), 4000);
            a.src = src;
        });
    }

    // an audio file as text to store; more than 25 MB: compress it first (when you want)
    async function audioFileToStorable(file) {
        if (file.size <= BIG_FILE_MB * 1048576) {
            const src = await blobToDataUrl(file);
            if (src.length <= VOICE_MAX_CHARS) return src;
        }
        const bytes = await askBigTarget(file, 'Audio');
        if (!bytes) return null;
        const url = URL.createObjectURL(file);
        try {
            const out = await compressAudioUrl(url, { targetBytes: bytes, label: file.name });
            if (out.length > VOICE_MAX_CHARS) { toast(`${file.name} is still too big after compressing (too long)`); return null; }
            return out;
        } catch (err) {
            toast(err && err.message === 'cancelled' ? 'Compressing stopped' : err && err.message === 'unsupported' ? 'This browser cannot compress audio' : `Could not compress ${file.name}`);
            return null;
        } finally { URL.revokeObjectURL(url); }
    }
    async function insertAudioFiles(files) {
        for (const file of [...files].filter((f) => f.type.startsWith('audio/'))) {
            try {
                const src = await audioFileToStorable(file);
                if (!src) continue;
                if (!isVoiceSrc(src)) throw new Error('unsupported');
                insertVoiceChip(src, file.name.replace(/\.[^.]+$/, ''), await audioDuration(src));
            } catch (_) {
                toast(`Could not add audio: ${file.name}`);
            }
        }
    }

    /* ---- the options of an audio chip (Text and Collage), copy / paste ---- */
    const AUDIO_COPY_RE = /^\[Remmin audio\] (\{[\s\S]*\})$/;
    function copyAudio(a) {
        const text = `[Remmin audio] ${JSON.stringify({ name: a.name || 'Voice note', duration: Math.round(a.duration || 0), loop: !!a.loop, vol: a.vol, src: a.src })}`;
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(() => toast('Audio copied: paste it (Ctrl+V) in Text or Collage, in any note'), () => toast('Could not copy the audio'));
        } else toast('Could not copy the audio');
    }
    function pastedAudio(e) {
        const t = e.clipboardData ? e.clipboardData.getData('text/plain').trim() : '';
        const m = AUDIO_COPY_RE.exec(t);
        if (!m) return null;
        try {
            const a = JSON.parse(m[1]);
            const vol = Math.round(Number(a.vol));
            return a && typeof a.src === 'string' && isVoiceSrc(a.src) ? { src: a.src, name: String(a.name || 'Voice note').slice(0, 200), duration: Number(a.duration) || 0, loop: !!a.loop, vol: Number.isFinite(vol) && vol >= 0 && vol < 100 ? vol : 100 } : null;
        } catch (_) { return null; }
    }
    function downloadAudio(src, name) {
        src = resolveAudio(src);
        if (!src) { toast('This audio is not on this device yet'); return; }
        const ext = (/^data:audio\/([\w]+)/.exec(src) || [])[1] || 'webm';
        const a = document.createElement('a');
        a.href = src;
        a.download = `${(name || 'audio').replace(/[\\/:*?"<>|]+/g, '_')}.${ext === 'mpeg' ? 'mp3' : ext}`;
        document.body.appendChild(a); a.click(); a.remove();
    }
    // chip: the chip on screen; o: { loop, setLoop, rename, remove }
    function audioMenuActions(chip, o) {
        const playing = voicePlayingChip === chip && voiceAudio && !voiceAudio.paused;
        const a = { src: chip.getAttribute('data-voice'), name: chip.getAttribute('data-name'), duration: Number(chip.getAttribute('data-duration')) || 0, loop: o.loop, vol: chipVolume(chip) };
        return [
            { label: playing ? 'Pause' : 'Play', icon: playing ? '⏸' : '▶', run: () => toggleVoicePlayback(chip) },
            { label: o.loop ? 'Loop off' : 'Loop on (play again and again)', icon: '🔁', run: o.setLoop },
            { label: 'Copy audio', icon: '⧉', run: () => copyAudio(a) },
            { label: 'Download audio', icon: '⬇', run: () => downloadAudio(a.src, a.name) },
            ...audioCompressActions(a.src, o.x, o.y),
            { label: 'Rename', icon: '✎', run: o.rename },
            { label: 'Remove audio', icon: '🗑', danger: true, run: o.remove }
        ];
    }
    /* ---- compressing audio that is already in a note (all its places get the smaller version) ---- */
    const AUDIO_PRESETS = [
        { label: 'Good quality (96 kbps)', bps: 96000 },
        { label: 'Smaller (48 kbps)', bps: 48000 },
        { label: 'Smallest (24 kbps)', bps: 24000 }
    ];
    function audioCompressActions(src, x, y) {
        if (!isMediaRef(src)) return [];
        const data = mediaData(src);
        const now = data ? ` (now ${mbOf(data).toFixed(2)} MB)` : '';
        const items = [{
            label: `Compress audio…${now}`, icon: '🗜',
            run: () => setTimeout(() => menuAt(x || parseFloat(pointAnchor.style.left) || 200, y || parseFloat(pointAnchor.style.top) || 200, 'Compress to', [
                { label: `Compress to … MB${now}`, icon: '✎', run: async () => {
                    const cur = mbOf(mediaData(src));
                    const answer = await askDialog({ title: 'Compress to … MB', message: `How many MB may the audio be? Now it is ${cur.toFixed(2)} MB; enter a smaller number.`, input: (cur / 2).toFixed(2), okLabel: 'Compress' });
                    if (answer === null || answer === false) return;
                    const mb = parseFloat(String(answer).replace(',', '.'));
                    if (!Number.isFinite(mb) || mb <= 0) { toast('Enter a number of MB, for example 0.5'); return; }
                    if (mb >= cur) { toast(`Enter a number below ${cur.toFixed(2)} MB`); return; }
                    compressAudioMedia(src, { targetBytes: mb * 1048576 });
                } },
                ...AUDIO_PRESETS.map((pr) => ({ label: pr.label, icon: '🗜', run: () => compressAudioMedia(src, { bps: pr.bps }) }))
            ]), 0)
        }];
        const orig = state.mediaOrig && state.mediaOrig[src.slice(6)];
        if (orig) {
            items.push({ label: `Undo compression (back to ${mbOf(orig.data).toFixed(2)} MB)`, icon: '↺', run: () => { stopVoicePlayback(); undoCompression(src); } });
            items.push({ label: 'Keep this size (forget the original)', icon: '✔', run: () => keepCompressed(src) });
        }
        return items;
    }
    async function compressAudioMedia(ref, { targetBytes = 0, bps = 0 }) {
        const id = ref.slice(6);
        const before = state.media[id];
        if (!before) return;
        let out;
        try { out = await compressAudioUrl(await mediaUrl(ref), { targetBytes: targetBytes || 1048576, bps, label: 'audio' }); }
        catch (err) { toast(err && err.message === 'cancelled' ? 'Compressing stopped' : err && err.message === 'unsupported' ? 'This browser cannot compress audio' : 'Could not compress this audio'); return; }
        if (!out || out.length >= before.length * 0.97) { toast(`Not smaller after compressing (${mbOf(before).toFixed(2)} MB): kept the original`); return; }
        if (voicePlayingChip && voicePlayingChip.getAttribute('data-voice') === ref) stopVoicePlayback();
        keepOriginal(id, before);
        state.media[id] = out;
        scheduleSaveMedia();
        refreshMediaPlayers(id);
        markChanged();
        scheduleGithubSave();
        toast(`Audio compressed: ${mbOf(before).toFixed(2)} MB → ${mbOf(out).toFixed(2)} MB`);
    }
    function textChipSetLoop(chip, on) {
        if (on) chip.setAttribute('data-loop', '1'); else chip.removeAttribute('data-loop');
        const b = chip.querySelector('.voice-loop');
        if (b) b.classList.toggle('on', on);
        if (voicePlayingChip === chip && voiceAudio) voiceAudio.loop = on;
        markTextChanged();
    }
    editorEl.addEventListener('contextmenu', (e) => {
        const chip = e.target.closest && e.target.closest('.voice-chip');
        if (!chip || !editorEl.contains(chip)) return;
        e.preventDefault();
        menuAt(e.clientX, e.clientY, 'Audio', textAudioActions(chip, e.clientX, e.clientY));
    });
    function textAudioActions(chip, x, y) {
        return audioMenuActions(chip, {
            x, y,
            loop: chip.getAttribute('data-loop') === '1',
            setLoop: () => textChipSetLoop(chip, chip.getAttribute('data-loop') !== '1'),
            rename: async () => {
                const name = await askText('Rename audio', chip.getAttribute('data-name'), 'Rename');
                if (name === null || !name.trim() || !editorEl.contains(chip)) return;
                chip.setAttribute('data-name', name.trim());
                chip.querySelector('.voice-name').textContent = `🎤 ${name.trim()}`;
                markTextChanged();
            },
            remove: () => { if (voicePlayingChip === chip) stopVoicePlayback(); recordText(); chip.remove(); selectTextMedia(null); markTextChanged(); }
        });
    }

    /* ---- recorder ---- */
    const voice = { recorder: null, stream: null, chunks: [], startedAt: 0, timer: null,
        result: null, duration: 0, analyser: null, source: null, levelRaf: 0 };

    function pickVoiceMime() {
        if (!window.MediaRecorder) return '';
        return ['audio/mp4;codecs=mp4a.40.2', 'audio/mp4', 'audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus', 'audio/aac']
            .find((t) => MediaRecorder.isTypeSupported && MediaRecorder.isTypeSupported(t)) || '';
    }

    function setVoiceUi(stateName) {
        const rec = $('voiceRecBtn');
        rec.classList.toggle('recording', stateName === 'recording');
        rec.textContent = stateName === 'recording' ? '■' : '●';
        rec.title = stateName === 'recording' ? 'Stop recording' : 'Start recording';
        $('voiceInsert').disabled = stateName !== 'ready';
        $('voicePreview').hidden = stateName !== 'ready';
        $('voiceStatus').textContent = {
            idle: 'Press the red button to start recording',
            recording: 'Recording… press ■ to stop',
            ready: 'Listen back, give it a name and press Insert. Press ● to record again.',
            denied: 'No access to the microphone. Allow it in your browser settings.'
        }[stateName] || '';
    }

    function releaseVoiceStream() {
        cancelAnimationFrame(voice.levelRaf);
        $('voiceLevelBar').style.width = '0%';
        try { voice.source && voice.source.disconnect(); } catch (_) {}
        voice.source = null;
        voice.analyser = null;
        if (voice.stream) voice.stream.getTracks().forEach((t) => t.stop());
        voice.stream = null;
    }

    function stopVoiceRecording(discard) {
        clearInterval(voice.timer);
        if (voice.recorder && voice.recorder.state !== 'inactive') {
            if (discard) voice.recorder.ondataavailable = null;
            if (discard) voice.recorder.onstop = null;
            voice.recorder.stop();
        }
        if (discard) voice.recorder = null;
        releaseVoiceStream();
    }

    function resetVoiceModal() {
        stopVoiceRecording(true);
        const preview = $('voicePreview');
        preview.pause();
        if (preview.src && preview.src.startsWith('blob:')) URL.revokeObjectURL(preview.src);
        preview.removeAttribute('src');
        voice.result = null;
        voice.duration = 0;
        $('voiceTimer').textContent = '0:00';
        setVoiceUi('idle');
    }

    let voiceTarget = 'text'; // where the recording / audio file goes: 'text' or 'collage'
    function openVoiceModal(target) {
        if (!noteById(state.currentNoteId)) return;
        voiceTarget = target === 'collage' ? 'collage' : 'text';
        saveEditorSelection();
        resetVoiceModal();
        $('voiceName').value = `Voice note ${formatTime(toLocalIso(new Date()))}`;
        if (!window.MediaRecorder || !navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
            $('voiceStatus').textContent = 'Recording is not supported in this browser. You can still upload an audio file.';
            $('voiceRecBtn').disabled = true;
        } else {
            $('voiceRecBtn').disabled = false;
        }
        openModal('voiceModal');
    }

    async function startVoiceRecording() {
        resetVoiceModal();
        try {
            voice.stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true } });
        } catch (err) {
            setVoiceUi('denied');
            return;
        }
        if (!$('voiceModal').classList.contains('active')) { releaseVoiceStream(); return; }
        const mimeType = pickVoiceMime();
        try {
            voice.recorder = new MediaRecorder(voice.stream, { ...(mimeType ? { mimeType } : {}), audioBitsPerSecond: 32000 });
        } catch (_) {
            voice.recorder = new MediaRecorder(voice.stream);
        }
        voice.chunks = [];
        voice.recorder.ondataavailable = (e) => { if (e.data && e.data.size) voice.chunks.push(e.data); };
        voice.recorder.onstop = async () => {
            const blob = new Blob(voice.chunks, { type: (voice.recorder && voice.recorder.mimeType) || mimeType || 'audio/webm' });
            voice.recorder = null;
            if (!blob.size) { setVoiceUi('idle'); return; }
            const src = await blobToDataUrl(blob);
            voice.result = src.replace(/^data:audio\/([\w.+-]+);codecs=[^;,]+/i, 'data:audio/$1');
            $('voicePreview').src = URL.createObjectURL(blob);
            setVoiceUi('ready');
        };
        voice.recorder.start(250);
        voice.startedAt = Date.now();
        setVoiceUi('recording');
        voice.timer = setInterval(() => {
            voice.duration = (Date.now() - voice.startedAt) / 1000;
            $('voiceTimer').textContent = formatSeconds(voice.duration);
            if (voice.duration >= VOICE_MAX_SECONDS) {
                toast('Maximum length reached (10 minutes)');
                stopVoiceRecording(false);
            }
        }, 200);

        // little level meter so you can see the microphone works
        try {
            const actx = getAudioContext();
            voice.analyser = actx.createAnalyser();
            voice.analyser.fftSize = 512;
            voice.source = actx.createMediaStreamSource(voice.stream);
            voice.source.connect(voice.analyser);
            const data = new Uint8Array(voice.analyser.fftSize);
            const tick = () => {
                if (!voice.analyser) return;
                voice.analyser.getByteTimeDomainData(data);
                let peak = 0;
                for (let i = 0; i < data.length; i++) peak = Math.max(peak, Math.abs(data[i] - 128));
                $('voiceLevelBar').style.width = `${Math.min(100, (peak / 128) * 160)}%`;
                voice.levelRaf = requestAnimationFrame(tick);
            };
            tick();
        } catch (_) { /* meter is optional */ }
    }

    $('insertVoiceBtn').addEventListener('click', () => openVoiceModal('text'));
    textCtx = makeCtxBar($('textToolbar'), [$('imageTools')]);
    ['textPasteBtn', 'cbPasteBtn', 'drawPasteBtn'].forEach((id) => $(id).addEventListener('click', pasteButton));
    addMenu($('textAddBtn'), [['Picture', '🖼', 'insertImageBtn'], ['GIF', '🎞', 'insertGifBtn'], ['Video file / link', '▶', 'insertVideoEmbedBtn'], ['Audio / voice note', '🎤', 'insertVoiceBtn']]);
    $('voiceRecBtn').addEventListener('click', () => {
        if (voice.recorder && voice.recorder.state === 'recording') {
            voice.duration = (Date.now() - voice.startedAt) / 1000;
            stopVoiceRecording(false);
        } else {
            startVoiceRecording();
        }
    });
    $('voiceInsert').addEventListener('click', () => {
        if (!voice.result) return;
        const name = $('voiceName').value.trim() || 'Voice note';
        const { result, duration } = voice;
        $('voiceModal').classList.remove('active');
        resetVoiceModal();
        if (voiceTarget === 'collage') { cbAddAudio({ src: result, name, duration }); return; }
        restoreEditorSelection();
        insertVoiceChip(result, name, duration);
    });
    const closeVoiceModal = () => { resetVoiceModal(); $('voiceModal').classList.remove('active'); };
    $('voiceCancel').addEventListener('click', closeVoiceModal);
    $('voiceClose').addEventListener('click', closeVoiceModal);
    $('voiceBackdrop').addEventListener('click', closeVoiceModal);
    $('voiceUploadBtn').addEventListener('click', () => $('voiceFile').click());
    $('voiceFile').addEventListener('change', async (e) => {
        const files = [...e.target.files];
        e.target.value = '';
        if (!files.length) return;
        closeVoiceModal();
        if (voiceTarget === 'collage') { cbAddAudioFiles(files); return; }
        restoreEditorSelection();
        await insertAudioFiles(files);
    });



    $('saveNoteBtn').addEventListener('click', () => {
        flushEditor();
        toast('Note saved');
    });

    $('deleteNoteBtn').addEventListener('click', () => deleteNote());


    $('newNoteBtn').addEventListener('click', () => newNote());

    /* ---------------- folders ---------------- */

    $('newFolderBtn').addEventListener('click', () => createFolder(null));

    async function createFolder(parentId) {
        const parent = parentId ? state.folders.find((f) => f.id === parentId) : null;
        const name = await askText(parent ? `New folder in "${parent.name}"` : 'New folder', 'New folder', 'Create');
        if (!name || !name.trim()) return;
        const pid = parent && state.folders.includes(parent) ? parent.id : null;
        const folder = { id: uid(), name: name.trim(), modifiedAt: new Date().toISOString(), parentId: pid,
            order: childFolders(pid).reduce((m, f) => Math.max(m, typeof f.order === 'number' ? f.order : 0), -1) + 1 };
        state.folders.push(folder);
        state.prefs.expandedFolders.push(folder.id);
        if (pid) expandFolderPath(pid);
        saveFolders();
        savePrefs();
        renderTree();
        refreshFolderSelect();
    }

    async function renameFolder(id) {
        const folder = state.folders.find((f) => f.id === id);
        if (!folder) return;
        const name = await askText('Rename folder', folder.name, 'Rename');
        if (!name || !name.trim() || !state.folders.includes(folder)) return;
        folder.name = name.trim();
        folder.modifiedAt = new Date().toISOString();
        saveFolders();
        renderTree();
        if (noteById(state.currentNoteId)?.folderId === id) {
            $('noteFolder').innerHTML = folderOptions(id);
        }
    }

    async function deleteFolder(id) {
        flushEditor();
        const folder = state.folders.find((f) => f.id === id);
        if (!folder) return;
        const parentId = parentOf(folder);
        const parent = parentId ? state.folders.find((f) => f.id === parentId) : null;
        const where = parent ? `"${parent.name}"` : '"No folder"';
        const hasSubs = childFolders(id).length > 0;
        const ok = await askConfirm('Delete folder', `Delete folder "${folder.name}"? The notes${hasSubs ? ' and folders' : ''} in it are kept and moved to ${where}.`, 'Delete');
        if (!ok || !state.folders.includes(folder)) return;
        const now = new Date().toISOString();
        childFolders(id).forEach((f) => { f.parentId = parentId; f.modifiedAt = now; });
        state.folders = state.folders.filter((f) => f.id !== id);
        state.deleted.folders[id] = now;
        state.notes.forEach((n) => { if (n.folderId === id) { n.folderId = parentId; n.modifiedAt = now; } });
        state.prefs.expandedFolders = state.prefs.expandedFolders.filter((f) => f !== id);
        if (state.currentFolderId === id) state.currentFolderId = parentId;
        saveFolders();
        saveNotes();
        savePrefs();
        renderTree();
        renderEditor();
    }

    /* ---------------- planning ---------------- */

    // "Scheduled" is folded whenever a note is opened (also after a reload); it stays open only
    // for the note you unfolded it in
    let scheduleOpenFor = null, scheduleNoteShown = null;
    function applyScheduleCollapse() {
        if (state.currentNoteId !== scheduleNoteShown) { scheduleNoteShown = state.currentNoteId; scheduleOpenFor = null; } // another note: folded
        const collapsed = !state.currentNoteId || scheduleOpenFor !== state.currentNoteId;
        // the header says when the note is planned, so it is useful while folded too
        const note = noteById(state.currentNoteId);
        const d = note && parseDate(note.scheduledAt);
        $('scheduleHeader').firstElementChild.textContent = !d ? 'Scheduled'
            : `Scheduled · ${d.toLocaleDateString(undefined, { weekday: 'short', day: 'numeric', month: 'short', year: d.getFullYear() === new Date().getFullYear() ? undefined : 'numeric' })} ${formatTime(note.scheduledAt)}` +
              `${note.alarmEnabled ? ' · ⏰' : ''}${note.emailEnabled ? ' · ✉' : ''}${isRecurring(note) ? ' · ↻' : ''}`;
        $('scheduleBody').style.display = collapsed ? 'none' : 'flex';
        $('scheduleChevron').textContent = String.fromCharCode(collapsed ? 9656 : 9662);
    }

    $('scheduleHeader').addEventListener('click', (e) => {
        if (e.target.closest('#unscheduleBtn')) return;
        scheduleOpenFor = scheduleOpenFor === state.currentNoteId ? null : state.currentNoteId;
        applyScheduleCollapse();
    });

    function updateScheduleUi(note) {
        const body = $('scheduleBody');
        if (!note) {
            $('scheduleDate').value = '';
            $('scheduleTime').value = '';
            $('alarmEnabled').checked = false;
            $('snoozeMinutes').value = 5;
            $('snoozeMinutesLabel').textContent = '5';
            $('scheduleDate').disabled = true;
            $('scheduleTime').disabled = true;
            $('alarmEnabled').disabled = true;
            $('emailEnabled').checked = false;
            $('emailEnabled').disabled = true;
            $('emailToLabel').textContent = '';
            updateEditorRepeatUi(null);
            body.style.opacity = '0.5';
            applyScheduleCollapse();
            return;
        }

        $('scheduleDate').disabled = false;
        $('scheduleTime').disabled = false;
        $('alarmEnabled').disabled = false;
        body.style.opacity = '1';

        const d = parseDate(note.scheduledAt);
        $('scheduleDate').value = d
            ? `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` : '';
        $('scheduleTime').value = d
            ? `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}` : '';
        $('alarmEnabled').checked = note.alarmEnabled;
        $('emailEnabled').disabled = false;
        $('emailEnabled').checked = note.emailEnabled;
        $('emailToLabel').textContent = note.emailEnabled && note.emailTo ? `→ ${note.emailTo}` : '';
        $('snoozeMinutes').value = note.alarmSnoozeMinutes;
        $('snoozeMinutesLabel').textContent = note.alarmSnoozeMinutes;
        updateEditorRepeatUi(note);
        applyScheduleCollapse();
    }

    /* ---- repeat settings + overview inside the note's Scheduled card ---- */

    // the day buttons (M T W T F S S): hold the mouse button / finger and slide over them to select (or unselect) several
    function dragSelectDays(box) {
        let paint = null, pointerId = null;
        const set = (input, on) => {
            if (input.checked === on) return;
            input.checked = on;
            input.dispatchEvent(new Event('change', { bubbles: true }));
        };
        box.addEventListener('pointerdown', (e) => {
            const chip = e.target.closest('.weekday-chip');
            if (!chip || (e.pointerType === 'mouse' && e.button !== 0)) return;
            e.preventDefault();
            const input = chip.querySelector('input');
            paint = !input.checked;
            pointerId = e.pointerId;
            set(input, paint);
        });
        document.addEventListener('pointermove', (e) => {
            if (paint === null || e.pointerId !== pointerId) return;
            const el = document.elementFromPoint(e.clientX, e.clientY);
            const chip = el && el.closest && el.closest('.weekday-chip');
            if (chip && box.contains(chip)) set(chip.querySelector('input'), paint);
        });
        const stop = (e) => { if (e.pointerId === pointerId) { paint = null; pointerId = null; } };
        document.addEventListener('pointerup', stop);
        document.addEventListener('pointercancel', stop);
        // the press already switched the day: no second switch from the click (the keyboard still works)
        box.addEventListener('click', (e) => { if (e.detail > 0 && e.target.closest('.weekday-chip')) e.preventDefault(); });
    }

    $('edWeekdays').innerHTML = WEEKDAY_SHORT.map((letter, index) =>
        `<label class="weekday-chip" title="${WEEKDAY_FULL[index]}"><input type="checkbox" value="${index}" aria-label="${WEEKDAY_FULL[index]}"><span>${letter}</span></label>`
    ).join('');

    function availableRepeatModes(base) {
        const modes = ['none', 'weekly', 'monthlyDate', 'monthlyWeekday'];
        if (isLastWeekdayOfMonth(base)) modes.push('monthlyLastWeekday');
        modes.push('yearly');
        return modes;
    }

    function formatOccurrence(d, withYear) {
        return `${WEEKDAY_FULL[(d.getDay() + 6) % 7].slice(0, 3)} ${d.getDate()} ${MONTHS[d.getMonth()].slice(0, 3)}` +
            `${withYear || d.getFullYear() !== new Date().getFullYear() ? ' ' + d.getFullYear() : ''} at ${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function updateEditorRepeatUi(note) {
        const select = $('edRepeatMode');
        const base = note ? parseDate(note.scheduledAt) : null;
        const ref = base || new Date();
        const mode = base ? repeatModeOf(note) : 'none';
        const weekdays = note && mode === 'weekly' ? note.repeatWeekdays : [];
        const modes = availableRepeatModes(ref);
        if (!modes.includes(mode)) modes.splice(modes.length - 1, 0, mode);
        select.innerHTML = modes.map((m) => `<option value="${m}">${escapeHtml(repeatLabel(m, ref, weekdays))}</option>`).join('');
        select.value = mode;
        select.disabled = !base;
        select.title = base ? '' : 'Pick a date first';
        $('edWeekdays').querySelectorAll('input').forEach((input) => {
            input.checked = weekdays.includes(Number(input.value));
        });
        $('edWeeklyOptions').hidden = mode !== 'weekly';
        $('edRepeatEndRow').hidden = mode === 'none';
        $('edRepeatEnd').value = (note && note.repeatEndDate) || '';
        $('edRepeatEnd').min = base ? dayKey(base) : '';

        const skipped = note && mode !== 'none' ? (note.skipDates || []).slice().sort() : [];
        $('edSkippedRow').hidden = !skipped.length;
        $('edSkipped').innerHTML = skipped.map((key) => {
            const d = keyToDate(key);
            return `<span class="skipped-chip">${WEEKDAY_FULL[(d.getDay() + 6) % 7].slice(0, 3)} ${d.getDate()} ${MONTHS[d.getMonth()].slice(0, 3)} ${d.getFullYear()}` +
                `<button type="button" class="skipped-remove" data-key="${key}" title="Bring this day back">✕</button></span>`;
        }).join('');

        $('scheduleSummary').innerHTML = scheduleSummaryHtml(note);
        renderNoteColors(note);
    }

    // in the order of the light spectrum, then pink, then the greys
    const NOTE_COLORS = ['#fca5a5', '#e11d1d', '#f97316', '#facc15', '#86efac', '#22c55e', '#14b8a6', '#22d3ee',
        '#3b82f6', '#1e3a8a', '#8b5cf6', '#d946ef', '#f472b6', '#4b5563', '#d1d5db'];

    function renderNoteColors(note) {
        renderColorPicker($('noteColors'), note ? note.color : '', !!note, 'noteColorCustom');
    }

    function renderColorPicker(host, current, enabled, customId) {
        const note = enabled;
        const custom = current && !NOTE_COLORS.includes(current);
        host.innerHTML =
            `<button type="button" class="note-color auto${!current ? ' selected' : ''}" data-color="" title="Default colour" ${note ? '' : 'disabled'}></button>` +
            NOTE_COLORS.map((c) => {
                const name = (PALETTE_NAMED.find(([pc]) => pc === c) || [, c])[1];
                return `<button type="button" class="note-color${c === current ? ' selected' : ''}" data-color="${c}" style="background:${c}" title="${name}" ${note ? '' : 'disabled'}></button>`;
            }).join('') +
            `<label class="note-color custom${custom ? ' selected' : ''}" title="Custom colour" style="${custom ? `background:${current}` : ''}">` +
            `<input type="color" id="${customId}" value="${current || '#8b5cf6'}" ${note ? '' : 'disabled'}></label>`;
    }

    function setNoteColor(color) {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        note.color = /^#[0-9a-f]{6}$/i.test(color) ? color.toLowerCase() : '';
        touch(note);
        renderNoteColors(note);
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    }

    $('noteColors').addEventListener('click', (e) => {
        const btn = e.target.closest('button.note-color');
        if (btn) setNoteColor(btn.dataset.color);
    });
    $('noteColors').addEventListener('change', (e) => {
        if (e.target.id === 'noteColorCustom') setNoteColor(e.target.value);
    });

    // "When will this note show up?" overview
    function scheduleSummaryHtml(note) {
        if (!note) return '';
        const base = parseDate(note.scheduledAt);
        if (!base) return '<div class="summary-line dim">Not in the calendar yet. Pick a date to schedule this note.</div>';
        const alarm = (note.alarmEnabled
            ? '🔔 Alarm on (rings every minute until you stop it)'
            : '🔕 No alarm') + (note.emailEnabled && note.emailTo ? ` · ✉️ Email to ${escapeHtml(note.emailTo)}` : '');
        if (!isRecurring(note)) {
            const passed = base.getTime() < Date.now();
            return `<div class="summary-line">📅 ${escapeHtml(formatOccurrence(base, true))}${passed ? ' <span class="dim">(passed)</span>' : ''}</div>` +
                `<div class="summary-line dim">${alarm}</div>`;
        }
        const next = [];
        let t = Date.now();
        for (let i = 0; i < 4; i++) {
            const occ = nextOccurrence(note, t);
            if (!occ) break;
            next.push(occ);
            t = occ.getTime();
        }
        const until = note.repeatEndDate ? ` until ${formatDateTime(note.repeatEndDate + 'T00:00:00').slice(0, -6)}` : ', no end date';
        return `<div class="summary-line">↻ ${escapeHtml(repeatLabel(repeatModeOf(note), base, note.repeatWeekdays))}${escapeHtml(until)}</div>` +
            `<div class="summary-line dim">Starts ${escapeHtml(formatOccurrence(base, true))} · ${alarm}</div>` +
            (next.length
                ? `<div class="summary-line">Next: ${next.map((d) => `<span class="next-chip">${escapeHtml(formatOccurrence(d))}</span>`).join(' ')}</div>`
                : '<div class="summary-line dim">No upcoming dates (the repeat has ended).</div>');
    }

    $('edRepeatMode').addEventListener('change', scheduleChanged);
    $('edWeekdays').addEventListener('change', scheduleChanged);
    dragSelectDays($('edWeekdays'));
    $('edRepeatEnd').addEventListener('change', scheduleChanged);
    $('edSkipped').addEventListener('click', (e) => {
        const btn = e.target.closest('.skipped-remove');
        const note = noteById(state.currentNoteId);
        if (!btn || !note) return;
        note.skipDates = (note.skipDates || []).filter((k) => k !== btn.dataset.key);
        resetAlarmRuntime(note);
        touch(note);
        updateScheduleUi(note);
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    });

    function scheduleChanged() {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const date = $('scheduleDate').value;
        const time = $('scheduleTime').value || '09:00:00'; // only a date picked: default to 9 in the morning

        if (!date) {
            note.scheduledAt = null;
        } else {
            const parts = time.split(':');
            const dt = new Date(`${date}T${pad(parts[0] || 0)}:${pad(parts[1] || 0)}:${pad(parts[2] || 0)}`);
            note.scheduledAt = isNaN(dt.getTime()) ? null : toLocalIso(dt);
        }

        note.alarmEnabled = $('alarmEnabled').checked;
        note.alarmSnoozeMinutes = Number($('snoozeMinutes').value);

        const base = parseDate(note.scheduledAt);
        let mode = base ? ($('edRepeatMode').value || 'none') : 'none';
        if (!REPEAT_MODES.includes(mode)) mode = 'none';
        if (mode === 'monthlyLastWeekday' && base && !isLastWeekdayOfMonth(base)) mode = 'monthlyWeekday';
        let weekdays = [...$('edWeekdays').querySelectorAll('input:checked')].map((el) => Number(el.value));
        if (mode === 'weekly' && !weekdays.length) weekdays = [(base.getDay() + 6) % 7];
        let end = mode === 'none' ? '' : $('edRepeatEnd').value;
        if (end && base && end < dayKey(base)) {
            toast('End date is before the start date');
            end = '';
        }
        note.repeatMode = mode;
        note.repeatWeekdays = mode === 'weekly' ? weekdays : [];
        note.repeatEndDate = end;

        resetAlarmRuntime(note);
        touch(note);
        updateScheduleUi(note);
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    }

    $('scheduleDate').addEventListener('change', scheduleChanged);
    $('scheduleTime').addEventListener('change', scheduleChanged);
    $('alarmEnabled').addEventListener('change', scheduleChanged);

    /* ---- email notification (sent by the GitHub Action, see EMAIL-SETUP.md) ---- */
    const EMAIL_KEY = 'remmin.emailAddress.v1';
    const EMAIL_RE = /^[^\s@<>"']+@[^\s@<>"']+\.[^\s@<>"']{2,}$/;
    const defaultEmail = () => RemminStore.get(EMAIL_KEY) || '';

    // Who is the email popup for? The open note, or the "Schedule note" window.
    let emailTarget = null;

    function editorEmailTarget() {
        const note = noteById(state.currentNoteId);
        if (!note) return null;
        return {
            note,
            address: () => note.emailTo,
            enabled: () => note.emailEnabled,
            save(address) {
                note.emailTo = address;
                note.emailEnabled = true;
                touch(note);
                updateScheduleUi(note);
                renderTree();
                if (state.prefs.activeTab === 'calendar') renderCalendar();
            },
            off() {
                note.emailEnabled = false;
                touch(note);
                updateScheduleUi(note);
                if (state.prefs.activeTab === 'calendar') renderCalendar();
            },
            sync() { $('emailEnabled').checked = !!(noteById(note.id) || {}).emailEnabled; }
        };
    }

    function openEmailModal(target) {
        emailTarget = target && target.address ? target : editorEmailTarget();
        if (!emailTarget) return;
        const note = emailTarget.note;
        $('emailAddress').value = emailTarget.address() || defaultEmail();
        $('emailTurnOff').hidden = !emailTarget.enabled();
        const sync = githubConfigured();
        const notes = [];
        if (!emailTarget.isPlan && !note.scheduledAt) notes.push('This note has no date yet: pick a date and time in the Scheduled card.');
        if (!sync) notes.push('Emails are sent via GitHub using your synced notes, so turn on <b>GitHub Sync</b> (with automatic sync).');
        else if (!githubConfig.autoSync) notes.push('Turn on <b>automatic sync</b> in GitHub Sync settings, or press “Sync now” after changes, so GitHub knows about this note.');
        notes.push('Emails are sent by the “Remmin email reminders” Google Apps Script, which checks your synced notes every minute (see <b>EMAIL-SETUP.md</b>).');
        $('emailStatus').innerHTML = notes.map((t) => `<div>• ${t}</div>`).join('');
        openModal('emailModal');
        setTimeout(() => $('emailAddress').focus(), 30);
    }

    function closeEmailModal() {
        $('emailModal').classList.remove('active');
        if (emailTarget) emailTarget.sync();
        emailTarget = null;
    }

    function saveEmailSettings() {
        if (!emailTarget) return;
        const address = $('emailAddress').value.trim();
        if (!EMAIL_RE.test(address)) {
            toast('Please enter a valid email address');
            $('emailAddress').focus();
            return;
        }
        if ($('emailRemember').checked && address !== defaultEmail()) { RemminStore.set(EMAIL_KEY, address); settingsChanged(); }
        emailTarget.save(address);
        closeEmailModal();
        toast(`Email notification on → ${address}`);
    }

    $('emailEnabled').addEventListener('change', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        if ($('emailEnabled').checked) {
            const address = note.emailTo || defaultEmail();
            if (!address) { openEmailModal(); return; }   // ask for the address first
            note.emailTo = address;
            note.emailEnabled = true;
        } else {
            note.emailEnabled = false;
        }
        touch(note);
        updateScheduleUi(note);
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    });
    $('emailSettingsLink').addEventListener('click', () => openEmailModal());

    /* ---- "How to setup: Google Apps Script Emails" ---- */
    let emailScriptCode = null;

    // The script lives next to the app (scripts/email-reminders.gs). The repo it reads
    // is filled in from your GitHub Sync settings, so the copied code is ready to use.
    async function loadEmailScriptCode() {
        if (emailScriptCode === null) {
            try {
                const res = await fetch('scripts/email-reminders.gs', { cache: 'no-store' });
                if (!res.ok) throw new Error(String(res.status));
                emailScriptCode = await res.text();
            } catch (_) {
                emailScriptCode = '';
            }
        }
        let code = emailScriptCode;
        if (code && githubConfigured()) {
            code = code
                .replace(/dataRepo: '[^']*'/, `dataRepo: '${githubConfig.owner}/${githubConfig.repo}'`)
                .replace(/dataPath: '[^']*'/, `dataPath: '${githubConfig.path || 'remmin.json'}'`)
                .replace(/appUrl: '[^']*'/, `appUrl: '${location.protocol.startsWith('http') ? location.href.split(/[?#]/)[0] : 'https://vharixce.github.io/Remmin/'}'`);
        }
        return code;
    }

    async function openEmailHelp() {
        $('emailHelpRepo').innerHTML = githubConfigured()
            ? `✔ GitHub Sync is set up: <code>${escapeHtml(githubConfig.owner)}/${escapeHtml(githubConfig.repo)}/${escapeHtml(githubConfig.path || 'remmin.json')}</code>. The code you copy below already uses this repo.`
            : '⚠ GitHub Sync is not set up on this device yet. Do that first, then come back here, so the copied code uses your repo.';
        $('emailHelpCopyStatus').textContent = '';
        $('emailHelpCode').hidden = true;
        $('emailHelpToggleCode').textContent = 'Show code';
        openModal('emailHelpModal');
        const code = await loadEmailScriptCode();
        $('emailHelpCode').value = code || '// Could not load the code here.\n// Open scripts/email-reminders.gs in the Remmin folder and copy everything in it.';
        $('emailHelpCopy').disabled = !code;
    }

    async function copyEmailScriptCode() {
        const code = await loadEmailScriptCode();
        if (!code) return;
        let ok = false;
        try {
            await navigator.clipboard.writeText(code);
            ok = true;
        } catch (_) {
            // older browsers / no clipboard permission: copy via a selected text box
            const box = $('emailHelpCode');
            const wasHidden = box.hidden;
            box.hidden = false;
            box.value = code;
            box.focus();
            box.select();
            try { ok = document.execCommand('copy'); } catch (__) { ok = false; }
            box.hidden = wasHidden;
        }
        $('emailHelpCopyStatus').textContent = ok ? '✔ Copied! Paste it in the Apps Script editor.' : 'Select the code below and copy it with Ctrl+C.';
        if (!ok) { $('emailHelpCode').hidden = false; $('emailHelpToggleCode').textContent = 'Hide code'; }
    }

    $('emailHelpBtn').addEventListener('click', openEmailHelp);
    $('emailHelpCopy').addEventListener('click', copyEmailScriptCode);
    $('emailHelpToggleCode').addEventListener('click', () => {
        const box = $('emailHelpCode');
        box.hidden = !box.hidden;
        $('emailHelpToggleCode').textContent = box.hidden ? 'Show code' : 'Hide code';
    });
    const closeEmailHelp = () => $('emailHelpModal').classList.remove('active');
    $('emailHelpClose').addEventListener('click', closeEmailHelp);
    $('emailHelpDone').addEventListener('click', closeEmailHelp);
    $('emailHelpBackdrop').addEventListener('click', closeEmailHelp);
    $('emailSave').addEventListener('click', saveEmailSettings);
    $('emailAddress').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); saveEmailSettings(); } });
    $('emailCancel').addEventListener('click', closeEmailModal);
    $('emailClose').addEventListener('click', closeEmailModal);
    $('emailBackdrop').addEventListener('click', closeEmailModal);
    $('emailTurnOff').addEventListener('click', () => {
        if (emailTarget) emailTarget.off();
        closeEmailModal();
    });
    $('snoozeMinutes').addEventListener('input', () => {
        $('snoozeMinutesLabel').textContent = $('snoozeMinutes').value;
    });
    $('snoozeMinutes').addEventListener('change', scheduleChanged);

    $('unscheduleBtn').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        note.scheduledAt = null;
        note.alarmEnabled = false;
        note.emailEnabled = false;
        note.repeatWeekdays = [];
        note.repeatMode = 'none';
        note.repeatEndDate = '';
        note.skipDates = [];
        resetAlarmRuntime(note);
        touch(note);
        updateScheduleUi(note);
        renderTree();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    });

    /* ---------------- tabs ---------------- */

    function setTab(tab) {
        flushEditor();
        if (tab !== 'note' && unlockedNoteId) { unlockedNoteId = null; renderEditor(); }
        state.prefs.activeTab = tab;
        savePrefs();
        $('tabNote').classList.toggle('active', tab === 'note');
        $('tabCalendar').classList.toggle('active', tab === 'calendar');
        $('viewNote').style.display = tab === 'note' ? 'flex' : 'none';
        $('viewCalendar').style.display = tab === 'calendar' ? 'flex' : 'none';
        updateMainStripes();
        applyCalendarMode(); // the calendar, or the world clocks
    }

    $('tabNote').addEventListener('click', () => setTab('note'));
    $('tabCalendar').addEventListener('click', () => setTab('calendar'));

    /* ---------------- calendar ---------------- */

    $('calendarWeekdays').innerHTML = WEEKDAYS
        .map((d) => `<div class="calendar-weekday">${d}</div>`).join('');

    function renderCalendar() {
        if (state.prefs.calendarMode === 'clock') return; // the world clocks are showing
        const cursor = state.calendarCursor;
        const year = cursor.getFullYear();
        const month = cursor.getMonth();
        $('calendarMonth').textContent = `${MONTHS[month]} ${year}`;

        const grid = $('calendarGrid');
        grid.innerHTML = '';

        const first = new Date(year, month, 1);
        const startOffset = (first.getDay() + 6) % 7;
        const start = new Date(year, month, 1 - startOffset);
        const today = new Date();

        for (let i = 0; i < 42; i++) {
            const day = new Date(start.getFullYear(), start.getMonth(), start.getDate() + i);
            const inMonth = day.getMonth() === month;
            const onDay = notesOnDay(day);
            const memos = onDay.filter((n) => n.memo);
            const scheduled = onDay.filter((n) => !n.memo);
            const isToday = sameDay(day, today);
            const isSelected = sameDay(day, state.calendarDay);

            const cell = document.createElement('div');
            cell.className = 'calendar-day' + (inMonth ? '' : ' other-month') +
                (isToday ? ' today' : '') + (isSelected ? ' selected' : '');
            cell.dataset.date = dayKey(day);
            const holidays = holidaysOn(day);
            const items = [
                ...holidays.map((h) => `<div class="holiday-chip" title="${escapeHtml(holidayTitle(h))}">${escapeHtml(h.name)}</div>`),
                ...scheduled.map((n) =>
                    `<div class="calendar-chip${isRecurring(n) ? ' recurring' : ''}" draggable="true" data-note-id="${n.id}" style="${noteChipStyle(n)}" title="${escapeHtml(n.title.trim() || 'Untitled')} – click to open, drag to another day">${escapeHtml(n.title.trim() || 'Untitled')}</div>`)
            ];
            if (holidays.length) cell.classList.add('has-holiday');
            if (items.length > 2) cell.classList.add('has-more'); // phones: dot top-right for "more than 2"
            const memoMark = memos.length
                ? `<span class="calendar-memo" title="${escapeHtml(memos.map((m) => m.title.trim() || 'Untitled').join('\n'))}">${MEMO}</span>` : '';
            cell.innerHTML = `<div class="calendar-day-number">${day.getDate()}${memoMark}</div>` +
                (items.length
                    ? `<div class="calendar-chips">${items.slice(0, 2).join('')}${items.length > 2 ? `<div class="calendar-more">+${items.length - 2} more</div>` : ''}</div>${scheduled.length ? '<div class="calendar-dot"></div>' : ''}`
                    : '');
            grid.appendChild(cell);
        }

        renderCalendarDay();
    }

    // readable text on a coloured chip
    function textColorFor(hex) {
        const v = parseInt(hex.slice(1), 16);
        const lum = (0.299 * ((v >> 16) & 255) + 0.587 * ((v >> 8) & 255) + 0.114 * (v & 255)) / 255;
        return lum > 0.6 ? '#111827' : '#ffffff';
    }

    function noteChipStyle(note) {
        return note.color ? `background:${note.color};color:${textColorFor(note.color)}` : '';
    }

    function notesOnDay(day) {
        return state.notes
            .filter((n) => occurrenceOn(n, day))
            .sort((a, b) => occurrenceOn(a, day) - occurrenceOn(b, day));
    }

    function renderCalendarDay() {
        const host = $('calendarDayNotes');
        const day = state.calendarDay;
        if (!day) {
            host.innerHTML = '<div class="calendar-hint">Click a day to view scheduled notes.</div>';
            return;
        }
        const notes = notesOnDay(day);
        const holidays = holidaysOn(day);
        const label = `${day.getDate()} ${MONTHS[day.getMonth()]} ${day.getFullYear()}`;
        const holidayHtml = holidays.map((h) => `<div class="holiday-item">
                    <div class="holiday-name">${escapeHtml(h.name)}${h.locals.length ? ` <span class="holiday-local">${escapeHtml(h.locals.join(' / '))}</span>` : ''}</div>
                    <div class="calendar-note-time">${escapeHtml(h.sets.join(', '))} ${h.obs ? 'observance (not a day off)' : 'holiday'}</div>
                </div>`).join('');
        host.innerHTML = `<div class="calendar-day-head"><div class="calendar-day-title">${label}</div>` +
            `<div class="calendar-day-buttons">` +
            `<button class="btn calendar-pick-note" id="calendarPickNote" type="button" title="Put an existing note on this day">Add existing</button>` +
            `<button class="btn btn-primary calendar-add-note" id="calendarAddNote" type="button" title="New note on this day">+ Note</button></div></div>` +
            holidayHtml + (notes.length
            ? notes.map((n) => `<div class="calendar-note-item${n.color ? ' colored' : ''}" draggable="true" data-note-id="${n.id}" style="${n.color ? `border-left-color:${n.color}` : ''}" title="Click to open – drag to another day">
                    <button class="more-btn cal-more" data-note-id="${n.id}" type="button" title="More" aria-label="More">⋯</button>
                    <span class="item-actions">
                        <button class="folder-action-btn note-unschedule" data-note-id="${n.id}" title="Remove this note/alarm from this date">Remove from date</button>
                        <button class="folder-action-btn note-rename" data-note-id="${n.id}" title="Rename note">Rename</button>
                        <button class="folder-action-btn folder-delete note-delete" data-note-id="${n.id}" title="Delete note">Delete</button>
                    </span>
                    <div>${escapeHtml(markTitle(n, n.title) || 'Untitled')}</div>
                    <div class="calendar-note-time">${formatTime(n.scheduledAt)}${n.alarmEnabled ? ' · alarm' : ''}${n.emailEnabled ? ' · email' : ''}${isRecurring(n) ? ' · ' + repeatSummary(n) : ''}</div>
                </div>`).join('')
            : (holidays.length ? '' : '<div class="calendar-hint">No notes scheduled for this day.</div>'));
    }
    /* ---------------- holidays (calculated in the app, no internet needed) ---------------- */

    const HOLIDAY_SETS = [
        { code: 'INT', label: 'International', short: 'Intl' },
        { code: 'SKY', label: 'Seasons & clock changes', short: 'Seasons' },
        { code: 'MOON', label: 'Moon phases & eclipses', short: 'Moon' },
        { code: 'GB', label: 'British', short: 'UK' },
        { code: 'NL', label: 'Dutch', short: 'NL' },
        { code: 'FR', label: 'French', short: 'FR' },
        { code: 'SE', label: 'Swedish', short: 'SE' },
        { code: 'DE', label: 'German', short: 'DE' },
        { code: 'JP', label: 'Japanese', short: 'JP' },
        { code: 'US', label: 'American', short: 'US' },
        { code: 'CN', label: 'Chinese', short: 'CN' },
        { code: 'IN', label: 'Indian', short: 'IN' }
    ];

    /* ---------------- astronomy (all calculated, no internet needed) ----------------
     * Moon phases: Meeus, Astronomical Algorithms ch. 49 (accurate to about a minute)
     * Eclipses:    Meeus ch. 54 (which full/new moons give an eclipse, and what kind)
     * Sun rise/set: the standard sunrise equation (accurate to a minute or two) */
    const DEG = Math.PI / 180;
    const sinD = (x) => Math.sin(x * DEG), cosD = (x) => Math.cos(x * DEG);
    const jdToDate = (jde) => new Date((jde - 2440587.5) * 86400000 - 69000); // TT -> real time (~69 s)

    const PHASE_ARGS = (M, Mp, F, O) => [
        Mp, M, 2 * Mp, 2 * F, Mp - M, Mp + M, 2 * M, Mp - 2 * F, Mp + 2 * F, 2 * Mp + M, 3 * Mp, M + 2 * F, M - 2 * F,
        2 * Mp - M, O, Mp + 2 * M, 2 * Mp - 2 * F, 3 * M, Mp + M - 2 * F, 2 * Mp + 2 * F, Mp + M + 2 * F, Mp - M + 2 * F,
        Mp - M - 2 * F, 3 * Mp + M, 4 * Mp
    ];
    // [coefficient, power of E] for new moon / full moon
    const NEW_TERMS = [[-0.40720, 0], [0.17241, 1], [0.01608, 0], [0.01039, 0], [0.00739, 1], [-0.00514, 1], [0.00208, 2],
        [-0.00111, 0], [-0.00057, 0], [0.00056, 1], [-0.00042, 0], [0.00042, 1], [0.00038, 1], [-0.00024, 1], [-0.00017, 0],
        [-0.00007, 0], [0.00004, 0], [0.00004, 0], [0.00003, 0], [0.00003, 0], [-0.00003, 0], [0.00003, 0], [-0.00002, 0],
        [-0.00002, 0], [0.00002, 0]];
    const FULL_TERMS = [[-0.40614, 0], [0.17302, 1], [0.01614, 0], [0.01043, 0], [0.00734, 1], [-0.00515, 1], [0.00209, 2],
        [-0.00111, 0], [-0.00057, 0], [0.00056, 1], [-0.00042, 0], [0.00042, 1], [0.00038, 1], [-0.00024, 1], [-0.00017, 0],
        [-0.00007, 0], [0.00004, 0], [0.00004, 0], [0.00003, 0], [0.00003, 0], [-0.00003, 0], [0.00003, 0], [-0.00002, 0],
        [-0.00002, 0], [0.00002, 0]];

    function lunation(k) { // k whole = new moon, k + 0.5 = full moon
        const T = k / 1236.85;
        const full = Math.abs(k % 1) > 0.25;
        const E = 1 - 0.002516 * T - 0.0000074 * T * T;
        const M = 2.5534 + 29.10535670 * k - 0.0000014 * T * T - 0.00000011 * T ** 3;
        const Mp = 201.5643 + 385.81693528 * k + 0.0107582 * T * T + 0.00001238 * T ** 3 - 0.000000058 * T ** 4;
        const F = 160.7108 + 390.67050284 * k - 0.0016118 * T * T - 0.00000227 * T ** 3 + 0.000000011 * T ** 4;
        const O = 124.7746 - 1.56375588 * k + 0.0020672 * T * T + 0.00000215 * T ** 3;
        const jde0 = 2451550.09766 + 29.530588861 * k + 0.00015437 * T * T - 0.000000150 * T ** 3 + 0.00000000073 * T ** 4;
        const args = PHASE_ARGS(M, Mp, F, O);
        let jde = jde0;
        (full ? FULL_TERMS : NEW_TERMS).forEach(([c, e], i) => { jde += c * E ** e * sinD(args[i]); });
        // planetary corrections (largest ones)
        const A = [299.77 + 0.107408 * k - 0.009173 * T * T, 251.88 + 0.016321 * k, 251.83 + 26.651886 * k,
            349.42 + 36.412478 * k, 84.66 + 18.206239 * k, 141.74 + 53.303771 * k, 207.14 + 2.453732 * k];
        [0.000325, 0.000165, 0.000164, 0.000126, 0.000110, 0.000062, 0.000060].forEach((c, i) => { jde += c * sinD(A[i]); });
        return { jde, full, T, E, M, Mp, F, O };
    }

    // Is there an eclipse at this new/full moon? (Meeus ch. 54)
    function eclipseAt(k) {
        const { full, T, E, M, Mp, F, O } = lunation(k);
        if (Math.abs(sinD(F)) > 0.36) return null;
        const F1 = F - 0.02665 * sinD(O);
        const A1 = 299.77 + 0.107408 * k - 0.009173 * T * T;
        let jde = 2451550.09766 + 29.530588861 * k + 0.00015437 * T * T - 0.000000150 * T ** 3 + 0.00000000073 * T ** 4;
        jde += (full ? -0.4065 : -0.4075) * sinD(Mp) + (full ? 0.1727 : 0.1721) * E * sinD(M)
            + 0.0161 * sinD(2 * Mp) - 0.0097 * sinD(2 * F1) + 0.0073 * E * sinD(Mp - M) - 0.0050 * E * sinD(Mp + M)
            - 0.0023 * sinD(Mp - 2 * F1) + 0.0021 * E * sinD(2 * M) + 0.0012 * sinD(Mp + 2 * F1) + 0.0006 * E * sinD(2 * Mp + M)
            - 0.0004 * sinD(3 * Mp) - 0.0003 * E * sinD(M + 2 * F1) + 0.0003 * sinD(A1) - 0.0002 * E * sinD(M - 2 * F1)
            - 0.0002 * E * sinD(2 * Mp - M) - 0.0002 * sinD(O);
        const P = 0.2070 * E * sinD(M) + 0.0024 * E * sinD(2 * M) - 0.0392 * sinD(Mp) + 0.0116 * sinD(2 * Mp)
            - 0.0073 * E * sinD(Mp + M) + 0.0067 * E * sinD(Mp - M) + 0.0118 * sinD(2 * F1);
        const Q = 5.2207 - 0.0048 * E * cosD(M) + 0.0020 * E * cosD(2 * M) - 0.3299 * cosD(Mp)
            - 0.0060 * E * cosD(Mp + M) + 0.0041 * E * cosD(Mp - M);
        const W = Math.abs(cosD(F1));
        const gamma = (P * cosD(F1) + Q * sinD(F1)) * (1 - 0.0048 * W);
        const u = 0.0059 + 0.0046 * E * cosD(M) - 0.0182 * cosD(Mp) + 0.0004 * cosD(2 * Mp) - 0.0005 * cosD(M + Mp);
        const g = Math.abs(gamma);
        let kind = null;
        if (!full) {
            if (g > 1.5433 + u) return null;
            if (g < 0.9972) kind = u < 0 ? 'Total solar eclipse' : u > 0.0047 ? 'Annular solar eclipse'
                : (u < 0.00464 * Math.sqrt(1 - gamma * gamma) ? 'Hybrid solar eclipse' : 'Annular solar eclipse');
            else kind = g < 0.9972 + Math.abs(u) ? (u < 0 ? 'Total solar eclipse' : 'Annular solar eclipse') : 'Partial solar eclipse';
        } else {
            const pen = (1.5573 + u - g) / 0.5450;
            const umb = (1.0128 - u - g) / 0.5450;
            if (pen < 0) return null;
            kind = umb < 0 ? 'Penumbral lunar eclipse' : umb >= 1 ? 'Total lunar eclipse' : 'Partial lunar eclipse';
        }
        return { kind, date: jdToDate(jde), solar: !full };
    }

    // All new moons, full moons and eclipses whose moment falls in year y (in this device's time zone)
    function moonEvents(y) {
        const out = [];
        const k0 = Math.floor((y - 2000) * 12.3685) - 2;
        for (let k = k0; k < k0 + 16; k++) {
            [k, k + 0.5].forEach((kk) => {
                const { jde, full } = lunation(kk);
                const date = jdToDate(jde);
                if (date.getFullYear() === y) out.push({ type: full ? 'full' : 'new', date });
                const ecl = eclipseAt(kk);
                if (ecl && ecl.date.getFullYear() === y) out.push({ type: 'eclipse', ...ecl });
            });
        }
        return out.sort((a, b) => a.date - b.date);
    }

    // Sunrise and sunset for a place on a calendar day (y, m 1-12, d); null = sun stays up / down
    function sunTimes(lat, lon, y, m, d) {
        const n = Math.round((Date.UTC(y, m - 1, d, 12) - Date.UTC(2000, 0, 1, 12)) / 86400000);
        const Js = n - lon / 360;
        const M = (357.5291 + 0.98560028 * Js) % 360;
        const C = 1.9148 * sinD(M) + 0.0200 * sinD(2 * M) + 0.0003 * sinD(3 * M);
        const lambda = (M + C + 180 + 102.9372) % 360;
        const transit = 2451545.0 + Js + 0.0053 * sinD(M) - 0.0069 * sinD(2 * lambda);
        const sinDec = sinD(lambda) * sinD(23.4397);
        const cosDec = Math.cos(Math.asin(sinDec));
        const cosW = (sinD(-0.833) - sinD(lat) * sinDec) / (cosD(lat) * cosDec);
        const toDate = (jd) => new Date((jd - 2440587.5) * 86400000);
        if (cosW > 1) return { polar: 'night', noon: toDate(transit) };
        if (cosW < -1) return { polar: 'day', noon: toDate(transit) };
        const w = Math.acos(cosW) / DEG;
        return { rise: toDate(transit - w / 360), set: toDate(transit + w / 360), noon: toDate(transit) };
    }

    // Easter Sunday (Gregorian calendar, "anonymous" algorithm)
    function easterSunday(y) {
        const a = y % 19, b = Math.floor(y / 100), c = y % 100, d = Math.floor(b / 4), e = b % 4;
        const f = Math.floor((b + 8) / 25), g = Math.floor((b - f + 1) / 3);
        const h = (19 * a + b - d - g + 15) % 30, i = Math.floor(c / 4), k = c % 4;
        const l = (32 + 2 * e + 2 * i - h - k) % 7, m = Math.floor((a + 11 * h + 22 * l) / 451);
        const month = Math.floor((h + l - 7 * m + 114) / 31), day = ((h + l - 7 * m + 114) % 31) + 1;
        return new Date(y, month - 1, day);
    }

    const D = (y, m, d) => new Date(y, m - 1, d);
    const addDays = (date, n) => new Date(date.getFullYear(), date.getMonth(), date.getDate() + n);
    // n-th given weekday (0 = Sunday) of a month
    const nthWeekday = (y, m, wd, n) => { const first = D(y, m, 1); return D(y, m, 1 + ((wd - first.getDay() + 7) % 7) + (n - 1) * 7); };
    const lastWeekday = (y, m, wd) => { const last = D(y, m + 1, 0); return addDays(last, -((last.getDay() - wd + 7) % 7)); };
    // first given weekday on or after a date
    const weekdayFrom = (date, wd) => addDays(date, (wd - date.getDay() + 7) % 7);

    // Equinoxes and solstices (Meeus, Astronomical Algorithms ch. 27; accurate to about a minute)
    const SEASON_TERMS = [
        [485, 324.96, 1934.136], [203, 337.23, 32964.467], [199, 342.08, 20.186], [182, 27.85, 445267.112],
        [156, 73.14, 45036.886], [136, 171.52, 22518.443], [77, 222.54, 65928.934], [74, 296.72, 3034.906],
        [70, 243.58, 9037.513], [58, 119.81, 33718.147], [52, 297.17, 150.678], [50, 21.02, 2281.226],
        [45, 247.54, 29929.562], [44, 325.15, 31555.956], [29, 60.93, 4443.417], [18, 155.12, 67555.328],
        [17, 288.79, 4562.452], [16, 198.04, 62894.029], [14, 199.76, 31436.921], [12, 95.39, 14577.848],
        [12, 287.11, 31931.756], [12, 320.81, 34777.259], [9, 227.73, 1222.114], [8, 15.45, 16859.074]
    ];
    function seasonMoment(y, which) { // which: 0 March, 1 June, 2 September, 3 December
        const Y = (y - 2000) / 1000;
        const c = [
            [2451623.80984, 365242.37404, 0.05169, -0.00411, -0.00057],
            [2451716.56767, 365241.62603, 0.00325, 0.00888, -0.00030],
            [2451810.21715, 365242.01767, -0.11575, 0.00337, 0.00078],
            [2451900.05952, 365242.74049, -0.06223, -0.00823, 0.00032]
        ][which];
        const jde0 = c[0] + c[1] * Y + c[2] * Y * Y + c[3] * Y ** 3 + c[4] * Y ** 4;
        const T = (jde0 - 2451545.0) / 36525;
        const rad = Math.PI / 180;
        const W = (35999.373 * T - 2.47) * rad;
        const dl = 1 + 0.0334 * Math.cos(W) + 0.0007 * Math.cos(2 * W);
        const S = SEASON_TERMS.reduce((sum, [A, B, C]) => sum + A * Math.cos((B + C * T) * rad), 0);
        const jde = jde0 + 0.00001 * S / dl;
        // Julian day (Terrestrial Time) -> real time (about 69 s difference)
        return new Date((jde - 2440587.5) * 86400000 - 69000);
    }

    // Days the clocks change, read from this device's own time zone
    function clockChanges(y) {
        const out = [];
        for (let d = D(y, 1, 1); d.getFullYear() === y; d = addDays(d, 1)) {
            const before = d.getTimezoneOffset();
            const after = addDays(d, 1).getTimezoneOffset();
            if (before !== after) out.push({ date: d, forward: after < before, minutes: Math.abs(after - before) });
        }
        return out;
    }

    /* ---------------- Indian (Hindu) festivals: calculated like a panchang ----------------
     * tithi (lunar day) = angle between Moon and Sun / 12°; lunar month (amanta) from the
     * sidereal sign of the Sun at the new moon that starts it. Reference place: New Delhi (IST). */
    const DELHI = { lat: 28.61, lon: 77.21 };
    const MOON_TERMS = [ // D, M, M', F, coefficient (1e-6 degree)
        [0, 0, 1, 0, 6288774], [2, 0, -1, 0, 1274027], [2, 0, 0, 0, 658314], [0, 0, 2, 0, 213618],
        [0, 1, 0, 0, -185116], [0, 0, 0, 2, -114332], [2, 0, -2, 0, 58793], [2, -1, -1, 0, 57066],
        [2, 0, 1, 0, 53322], [2, -1, 0, 0, 45758], [0, 1, -1, 0, -40923], [1, 0, 0, 0, -34720],
        [0, 1, 1, 0, -30383], [2, 0, 0, -2, 15327], [0, 0, 1, 2, -12528], [0, 0, 1, -2, 10980],
        [4, 0, -1, 0, 10675], [0, 0, 3, 0, 10034], [4, 0, -2, 0, 8548], [2, 1, -1, 0, -7888],
        [2, 1, 0, 0, -6766], [1, 0, -1, 0, -5163], [1, 1, 0, 0, 4987], [2, -1, 1, 0, 4036],
        [2, 0, 2, 0, 3994], [4, 0, 0, 0, 3861], [2, 0, -3, 0, 3665], [0, 1, -2, 0, -2689],
        [2, 0, -1, 2, -2602], [2, -1, -2, 0, 2390], [1, 0, 1, 0, -2348], [2, -2, 0, 0, 2236],
        [0, 1, 2, 0, -2120], [0, 2, 0, 0, -2069], [2, -2, -1, 0, 2048], [2, 0, 1, -2, -1773],
        [2, 0, 0, 2, -1595], [4, -1, -1, 0, 1215], [0, 0, 2, 2, -1110], [3, 0, -1, 0, -892],
        [2, 1, 1, 0, -810], [4, -1, -2, 0, 759], [0, 2, -1, 0, -713], [2, 2, -1, 0, -700]
    ];
    const norm360 = (a) => ((a % 360) + 360) % 360;
    const centuries = (date) => (date.getTime() / 86400000 + 2440587.5 + 69 / 86400 - 2451545.0) / 36525;
    function sunLongitude(date) {
        const T = centuries(date);
        const L0 = 280.46646 + 36000.76983 * T + 0.0003032 * T * T;
        const M = 357.52911 + 35999.05029 * T - 0.0001537 * T * T;
        const C = (1.914602 - 0.004817 * T) * sinD(M) + (0.019993 - 0.000101 * T) * sinD(2 * M) + 0.000289 * sinD(3 * M);
        return norm360(L0 + C - 0.00569);
    }
    function moonLongitude(date) {
        const T = centuries(date);
        const Lp = 218.3164477 + 481267.88123421 * T - 0.0015786 * T * T + T ** 3 / 538841;
        const Dm = 297.8501921 + 445267.1114034 * T - 0.0018819 * T * T + T ** 3 / 545868;
        const M = 357.5291092 + 35999.0502909 * T - 0.0001536 * T * T;
        const Mp = 134.9633964 + 477198.8675055 * T + 0.0087414 * T * T + T ** 3 / 69699;
        const F = 93.2720950 + 483202.0175233 * T - 0.0036539 * T * T;
        const E = 1 - 0.002516 * T - 0.0000074 * T * T;
        let sum = 0;
        MOON_TERMS.forEach(([d, m, mp, f, c]) => { sum += c * E ** Math.abs(m) * sinD(d * Dm + m * M + mp * Mp + f * F); });
        sum += 3958 * sinD(119.75 + 131.849 * T) + 1962 * sinD(Lp - F) + 318 * sinD(53.09 + 479264.29 * T);
        return norm360(Lp + sum / 1e6);
    }
    // tithi 1..30 (1–15 bright half, 16–30 dark half, 30 = new moon day)
    const tithiAt = (date) => Math.floor(norm360(moonLongitude(date) - sunLongitude(date)) / 12) + 1;
    const ayanamsa = (date) => 23.853 + 0.013968 * (date.getUTCFullYear() + date.getUTCMonth() / 12 - 2000);
    const siderealSign = (date) => Math.floor(norm360(sunLongitude(date) - ayanamsa(date)) / 30); // 0 = Mesha
    const newMoonMoment = (k) => jdToDate(lunation(k).jde);
    // lunar month (amanta) at a moment: 0 Chaitra … 11 Phalguna; leap = adhika (extra) month
    function lunarMonthAt(date) {
        let k = Math.floor((date.getTime() / 86400000 - 10957.75) / 29.530588861) + 1; // rough index from 2000
        while (newMoonMoment(k) > date) k--;
        while (newMoonMoment(k + 1) <= date) k++;
        const s0 = siderealSign(newMoonMoment(k)), s1 = siderealSign(newMoonMoment(k + 1));
        return { month: (s0 + 1) % 12, leap: s0 === s1 };
    }
    // the IST day (y, m, d) on which `tithi` in lunar `month` holds at the reference moment of that day
    const IST = 330; // minutes
    function istDay(y, m, d) { return new Date(Date.UTC(y, m - 1, d) - IST * 60000); } // 00:00 IST
    function refMoment(y, m, d, when) {
        const sun = sunTimes(DELHI.lat, DELHI.lon, y, m, d);
        const len = sun.set - sun.rise;
        switch (when) {
            case 'sunrise': return new Date(sun.rise.getTime() + 60000);
            case 'midday': return new Date(sun.rise.getTime() + len / 2);               // madhyahna
            case 'afternoon': return new Date(sun.rise.getTime() + len * 0.7);          // aparahna
            case 'evening': return new Date(sun.set.getTime() + 45 * 60000);            // pradosh
            case 'midnight': return new Date(istDay(y, m, d).getTime() + 24 * 3600000 + 5 * 60000); // nishita
            default: return sun.rise;
        }
    }
    const elongationAt = (date) => norm360(moonLongitude(date) - sunLongitude(date));
    // full moon day where the full moon still holds 2.4 h (3 muhurtas) after sunrise;
    // otherwise the day before (used for Raksha Bandhan)
    function purnimaUdaya(month, from, to) {
        for (let t = from.getTime(); t <= to.getTime(); t += 86400000) {
            const g = new Date(t);
            const yy = g.getFullYear(), mm = g.getMonth() + 1, dd = g.getDate();
            const rise = sunTimes(DELHI.lat, DELHI.lon, yy, mm, dd).rise;
            const at = new Date(rise.getTime() + 144 * 60000);
            if (tithiAt(at) === 15) { const lm = lunarMonthAt(at); if (lm.month === month && !lm.leap) return D(yy, mm, dd); continue; }
            const aft = refMoment(yy, mm, dd, 'afternoon');
            const nRise = sunTimes(DELHI.lat, DELHI.lon, yy, mm, dd + 1).rise;
            if (tithiAt(aft) === 15 && tithiAt(new Date(nRise.getTime() + 144 * 60000)) !== 15) {
                const lm = lunarMonthAt(aft); if (lm.month === month && !lm.leap) return D(yy, mm, dd);
            }
        }
        return null;
    }
    // Holika Dahan: full moon in the evening, but not during Bhadra (the first half of the full-moon tithi).
    // If Bhadra only ends after midnight, the bonfire moves to the next evening.
    function holikaDahan(from, to) {
        const day = festivalDay(null, 11, 15, 'evening', from, to);
        if (!day) return null;
        const y = day.getFullYear(), m = day.getMonth() + 1, d = day.getDate();
        const ev = refMoment(y, m, d, 'evening');
        const e = elongationAt(ev);
        if (e >= 174) return day; // Bhadra is already over
        const rate = (elongationAt(new Date(ev.getTime() + 3600000)) - e) || 0.5; // degrees per hour
        const bhadraEnd = new Date(ev.getTime() + ((174 - e) / rate) * 3600000);
        const midnight = new Date(istDay(y, m, d).getTime() + 24 * 3600000);
        if (bhadraEnd <= midnight) return day; // bonfire later that same night
        // Bhadra lasts past midnight: the next evening, if the full moon lasts until about sunset that day
        const next = addDays(day, 1);
        const nSet = sunTimes(DELHI.lat, DELHI.lon, next.getFullYear(), next.getMonth() + 1, next.getDate()).set;
        return tithiAt(new Date(nSet.getTime() - 2 * 3600000)) === 15 ? next : day;
    }

    function festivalDay(y, month, tithi, when, from, to) {
        for (let t = from.getTime(); t <= to.getTime(); t += 86400000) {
            const g = new Date(t);
            const yy = g.getFullYear(), mm = g.getMonth() + 1, dd = g.getDate();
            const at = refMoment(yy, mm, dd, when);
            if (tithiAt(at) !== tithi) continue;
            const lm = lunarMonthAt(at);
            if (lm.month === month && !lm.leap) return D(yy, mm, dd);
        }
        return null;
    }
    // [name, lunar month, tithi, when, search window (month, day) → (month, day)]
    const HINDU_FESTIVALS = [
        ['Maha Shivaratri', 10, 29, 'midnight', [2, 1], [3, 20]],
        ['Holika Dahan', 11, 15, 'evening', [2, 20], [4, 5]],
        ['Ram Navami', 0, 9, 'midday', [3, 15], [4, 30]],
        ['Raksha Bandhan', 4, 15, 'afternoon', [7, 20], [9, 5]],
        ['Krishna Janmashtami', 4, 23, 'midnight', [8, 5], [9, 15]],
        ['Ganesh Chaturthi', 5, 4, 'midday', [8, 15], [9, 30]],
        ['Navratri begins', 6, 1, 'sunrise', [9, 15], [10, 31]],
        ['Dussehra', 6, 10, 'afternoon', [9, 25], [11, 5]],
        ['Diwali', 6, 30, 'evening', [10, 10], [11, 20]]
    ];
    const hinduCache = new Map();
    function hinduFestivals(y) {
        if (hinduCache.has(y)) return hinduCache.get(y);
        const out = [];
        HINDU_FESTIVALS.forEach(([name, month, tithi, when, a, b]) => {
            const from = D(y, a[0], a[1]), to = D(y, b[0], b[1]);
            const day = name === 'Holika Dahan' ? holikaDahan(from, to)
                : name === 'Raksha Bandhan' ? purnimaUdaya(month, from, to)
                    : festivalDay(y, month, tithi, when, from, to);
            if (!day) return;
            if (name === 'Holika Dahan') { out.push([day, 'Holika Dahan']); out.push([addDays(day, 1), 'Holi']); }
            else out.push([day, name]);
        });
        hinduCache.set(y, out);
        return out;
    }

    // the day (in a time zone, as an offset in minutes) on which the Sun reaches a longitude
    // (tropical, or sidereal for Indian sankrantis), searching day `from` (y, m, d) for `days` days
    function solarTermDay(y, m, d, days, targetLon, tzMin, sidereal) {
        const lonAt = (t) => norm360(sunLongitude(t) - (sidereal ? ayanamsa(t) : 0));
        for (let i = 0; i < days; i++) {
            const start = new Date(Date.UTC(y, m - 1, d + i) - tzMin * 60000);
            const end = new Date(start.getTime() + 86400000);
            const a = norm360(lonAt(start) - targetLon), b = norm360(lonAt(end) - targetLon);
            if (a > 300 && b < 60) { const g = new Date(Date.UTC(y, m - 1, d + i)); return D(g.getUTCFullYear(), g.getUTCMonth() + 1, g.getUTCDate()); }
        }
        return null;
    }

    // Chinese lunar dates (the browser knows the Chinese calendar): every day of year y with its lunar month/day
    const chineseCache = new Map();
    function chineseLunarDays(y) {
        if (chineseCache.has(y)) return chineseCache.get(y);
        const out = [];
        let fmt = null;
        try { fmt = new Intl.DateTimeFormat('en-u-ca-chinese', { month: 'numeric', day: 'numeric', timeZone: 'Asia/Shanghai' }); } catch (err) { /* old browser */ }
        if (fmt) {
            for (let dt = D(y, 1, 1); dt.getFullYear() === y; dt = addDays(dt, 1)) {
                const parts = {};
                fmt.formatToParts(new Date(Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate(), 4))).forEach((p) => { parts[p.type] = p.value; });
                const leap = /bis/.test(parts.month);
                out.push({ date: dt, month: parseInt(parts.month, 10), day: parseInt(parts.day, 10), leap });
            }
        }
        chineseCache.set(y, out);
        return out;
    }
    const lunarDates = (y, month, day) => chineseLunarDays(y).filter((x) => !x.leap && x.month === month && x.day === day).map((x) => x.date);

    // Islamic dates (Umm al-Qura calendar); in India the day can be one later (moon sighting)
    function islamicDates(y, month, day) {
        let fmt = null;
        try { fmt = new Intl.DateTimeFormat('en-u-ca-islamic-umalqura', { month: 'numeric', day: 'numeric', timeZone: 'UTC' }); } catch (err) { return []; }
        const out = [];
        for (let dt = D(y, 1, 1); dt.getFullYear() === y; dt = addDays(dt, 1)) {
            const parts = {};
            fmt.formatToParts(new Date(Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate(), 12))).forEach((p) => { parts[p.type] = p.value; });
            if (parseInt(parts.month, 10) === month && parseInt(parts.day, 10) === day) out.push(dt);
        }
        return out;
    }

    // US federal holidays on a weekend are observed on the Friday before / Monday after
    function usObserved(date) {
        const wd = date.getDay();
        return wd === 6 ? addDays(date, -1) : wd === 0 ? addDays(date, 1) : null;
    }

    function holidayList(code, y) {
        const E = easterSunday(y);
        const H = (date, name, local) => ({ date, name, local: local || '', obs: false });
        // observance: a special day, but not a day off
        const O = (date, name, local) => ({ date, name, local: local || '', obs: true });
        const lastSunday = (m) => lastWeekday(y, m, 0);
        switch (code) {
            case 'MOON': {
                const hm = (t) => `${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}`;
                return moonEvents(y).map((e) => {
                    const day = D(e.date.getFullYear(), e.date.getMonth() + 1, e.date.getDate());
                    if (e.type === 'full') return O(day, '🌕 Full moon', `at ${hm(e.date)}`);
                    if (e.type === 'new') return O(day, '🌑 New moon', `at ${hm(e.date)}`);
                    return O(day, `${e.solar ? '🌘' : '🌒'} ${e.kind}`, `greatest at ${hm(e.date)}, not visible everywhere`);
                });
            }
            case 'SKY': {
                const hm = (t) => `${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}`;
                const names = ['Spring equinox', 'Summer solstice (longest day)', 'Autumn equinox', 'Winter solstice (shortest day)'];
                const list = names.map((name, i) => {
                    const t = seasonMoment(y, i);
                    return O(D(t.getFullYear(), t.getMonth() + 1, t.getDate()), name, `at ${hm(t)}`);
                });
                clockChanges(y).forEach((c) => {
                    const amount = c.minutes === 60 ? '1 hour' : `${c.minutes} min`;
                    list.push(O(c.date, c.forward ? 'Summer time starts' : 'Winter time starts',
                        c.forward ? `clocks go forward ${amount}` : `clocks go back ${amount}`));
                });
                return list;
            }
            case 'INT': return [
                H(D(y, 1, 1), "New Year's Day"), H(D(y, 2, 14), "Valentine's Day"),
                H(D(y, 3, 8), "International Women's Day"), H(D(y, 4, 22), 'Earth Day'),
                H(D(y, 5, 1), "International Workers' Day"), H(D(y, 10, 31), 'Halloween'),
                H(D(y, 12, 24), 'Christmas Eve'), H(D(y, 12, 25), 'Christmas Day'), H(D(y, 12, 31), "New Year's Eve"),
                O(D(y, 3, 20), 'International Day of Happiness'), O(D(y, 4, 1), "April Fools' Day"),
                O(D(y, 4, 10), 'Siblings Day'), O(nthWeekday(y, 5, 0, 2), "Mother's Day"),
                O(D(y, 6, 1), "International Children's Day"), O(nthWeekday(y, 6, 0, 3), "Father's Day"),
                O(D(y, 7, 30), 'International Friendship Day'), O(D(y, 8, 1), 'Girlfriend Day'),
                O(D(y, 10, 3), 'Boyfriend Day'), O(D(y, 11, 19), "International Men's Day")
            ];
            case 'GB': {
                const list = [
                    H(D(y, 1, 1), "New Year's Day"), H(addDays(E, -2), 'Good Friday'), H(addDays(E, 1), 'Easter Monday'),
                    H(nthWeekday(y, 5, 1, 1), 'Early May bank holiday'), H(lastWeekday(y, 5, 1), 'Spring bank holiday'),
                    H(lastWeekday(y, 8, 1), 'Summer bank holiday'), H(D(y, 12, 25), 'Christmas Day'), H(D(y, 12, 26), 'Boxing Day'),
                    O(D(y, 2, 14), "Valentine's Day"), O(addDays(E, -21), 'Mothering Sunday'),
                    O(nthWeekday(y, 6, 0, 3), "Father's Day"), O(D(y, 10, 31), 'Halloween'),
                    O(D(y, 11, 5), 'Bonfire Night'), O(D(y, 3, 17), "St Patrick's Day")
                ];
                // bank holidays on a weekend move to the next weekday
                const ny = D(y, 1, 1);
                if (ny.getDay() === 6 || ny.getDay() === 0) list.push(H(weekdayFrom(ny, 1), "New Year's Day (substitute day)"));
                const xmas = D(y, 12, 25).getDay();
                if (xmas === 6) { list.push(H(D(y, 12, 27), 'Christmas Day (substitute day)'), H(D(y, 12, 28), 'Boxing Day (substitute day)')); }
                else if (xmas === 0) { list.push(H(D(y, 12, 27), 'Christmas Day (substitute day)')); }
                else if (xmas === 5) { list.push(H(D(y, 12, 28), 'Boxing Day (substitute day)')); }
                return list;
            }
            case 'NL': {
                const kingsDay = D(y, 4, 27).getDay() === 0 ? D(y, 4, 26) : D(y, 4, 27);
                return [
                    H(D(y, 1, 1), "New Year's Day", 'Nieuwjaarsdag'), H(addDays(E, -2), 'Good Friday', 'Goede Vrijdag'),
                    H(E, 'Easter Sunday', 'Eerste Paasdag'), H(addDays(E, 1), 'Easter Monday', 'Tweede Paasdag'),
                    H(kingsDay, "King's Day", 'Koningsdag'), H(D(y, 5, 5), 'Liberation Day', 'Bevrijdingsdag'),
                    H(addDays(E, 39), 'Ascension Day', 'Hemelvaartsdag'), H(addDays(E, 49), 'Whit Sunday', 'Eerste Pinksterdag'),
                    H(addDays(E, 50), 'Whit Monday', 'Tweede Pinksterdag'), H(D(y, 12, 25), 'Christmas Day', 'Eerste Kerstdag'),
                    H(D(y, 12, 26), 'Boxing Day', 'Tweede Kerstdag'),
                    O(D(y, 2, 14), "Valentine's Day", 'Valentijnsdag'), O(nthWeekday(y, 5, 0, 2), "Mother's Day", 'Moederdag'),
                    O(D(y, 5, 4), 'Remembrance of the Dead', 'Dodenherdenking'), O(nthWeekday(y, 6, 0, 3), "Father's Day", 'Vaderdag'),
                    O(nthWeekday(y, 9, 2, 3), "Prince's Day", 'Prinsjesdag'), O(D(y, 11, 11), "St Martin's Day", 'Sint-Maarten'),
                    O(D(y, 12, 5), 'Sinterklaas Eve', 'Sinterklaasavond / Pakjesavond'), O(D(y, 12, 31), "New Year's Eve", 'Oudejaarsavond')
                ];
            }
            case 'FR': return [
                H(D(y, 1, 1), "New Year's Day", "Jour de l'an"), H(addDays(E, 1), 'Easter Monday', 'Lundi de Pâques'),
                H(D(y, 5, 1), 'Labour Day', 'Fête du Travail'), H(D(y, 5, 8), 'Victory in Europe Day', 'Victoire 1945'),
                H(addDays(E, 39), 'Ascension Day', 'Ascension'), H(addDays(E, 50), 'Whit Monday', 'Lundi de Pentecôte'),
                H(D(y, 7, 14), 'Bastille Day', 'Fête nationale'), H(D(y, 8, 15), 'Assumption Day', 'Assomption'),
                H(D(y, 11, 1), "All Saints' Day", 'Toussaint'), H(D(y, 11, 11), 'Armistice Day', 'Armistice 1918'),
                H(D(y, 12, 25), 'Christmas Day', 'Noël'),
                O(D(y, 2, 14), "Valentine's Day", 'Saint-Valentin'),
                O(dayKey(lastSunday(5)) === dayKey(addDays(E, 49)) ? nthWeekday(y, 6, 0, 1) : lastSunday(5), "Mother's Day", 'Fête des Mères'),
                O(nthWeekday(y, 6, 0, 3), "Father's Day", 'Fête des Pères'), O(D(y, 6, 21), 'Music Day', 'Fête de la Musique')
            ];
            case 'SE': return [
                H(D(y, 1, 1), "New Year's Day", 'Nyårsdagen'), H(D(y, 1, 6), 'Epiphany', 'Trettondedag jul'),
                H(addDays(E, -2), 'Good Friday', 'Långfredagen'), H(E, 'Easter Sunday', 'Påskdagen'),
                H(addDays(E, 1), 'Easter Monday', 'Annandag påsk'), H(D(y, 5, 1), 'May Day', 'Första maj'),
                H(addDays(E, 39), 'Ascension Day', 'Kristi himmelsfärdsdag'), H(addDays(E, 49), 'Whit Sunday', 'Pingstdagen'),
                H(D(y, 6, 6), 'National Day of Sweden', 'Sveriges nationaldag'),
                H(weekdayFrom(D(y, 6, 19), 5), 'Midsummer Eve', 'Midsommarafton'),
                H(weekdayFrom(D(y, 6, 20), 6), 'Midsummer Day', 'Midsommardagen'),
                H(weekdayFrom(D(y, 10, 31), 6), "All Saints' Day", 'Alla helgons dag'),
                H(D(y, 12, 24), 'Christmas Eve', 'Julafton'), H(D(y, 12, 25), 'Christmas Day', 'Juldagen'),
                H(D(y, 12, 26), 'Boxing Day', 'Annandag jul'), H(D(y, 12, 31), "New Year's Eve", 'Nyårsafton'),
                O(D(y, 2, 14), "Valentine's Day", 'Alla hjärtans dag'), O(D(y, 4, 30), 'Walpurgis Night', 'Valborgsmässoafton'),
                O(lastSunday(5), "Mother's Day", 'Mors dag'), O(nthWeekday(y, 11, 0, 2), "Father's Day", 'Fars dag'),
                O(D(y, 12, 13), "St Lucia's Day", 'Luciadagen')
            ];
            case 'DE': return [
                H(D(y, 1, 1), "New Year's Day", 'Neujahr'), H(addDays(E, -2), 'Good Friday', 'Karfreitag'),
                H(addDays(E, 1), 'Easter Monday', 'Ostermontag'), H(D(y, 5, 1), 'Labour Day', 'Tag der Arbeit'),
                H(addDays(E, 39), 'Ascension Day', 'Christi Himmelfahrt'), H(addDays(E, 50), 'Whit Monday', 'Pfingstmontag'),
                H(D(y, 10, 3), 'German Unity Day', 'Tag der Deutschen Einheit'),
                H(D(y, 12, 25), 'Christmas Day', '1. Weihnachtstag'), H(D(y, 12, 26), 'Second Day of Christmas', '2. Weihnachtstag'),
                O(D(y, 2, 14), "Valentine's Day", 'Valentinstag'), O(nthWeekday(y, 5, 0, 2), "Mother's Day", 'Muttertag'),
                O(addDays(E, 39), "Father's Day", 'Vatertag'), O(D(y, 11, 11), "St Martin's Day", 'Martinstag'),
                O(D(y, 12, 6), "St Nicholas' Day", 'Nikolaustag'), O(D(y, 12, 24), 'Christmas Eve', 'Heiligabend'),
                O(D(y, 12, 31), "New Year's Eve", 'Silvester')
            ];
            case 'US': {
                const list = [
                    H(D(y, 1, 1), "New Year's Day"), H(nthWeekday(y, 1, 1, 3), 'Martin Luther King Jr. Day'),
                    H(nthWeekday(y, 2, 1, 3), "Washington's Birthday (Presidents' Day)"), H(lastWeekday(y, 5, 1), 'Memorial Day'),
                    H(D(y, 6, 19), 'Juneteenth'), H(D(y, 7, 4), 'Independence Day'),
                    H(nthWeekday(y, 9, 1, 1), 'Labor Day'), H(nthWeekday(y, 10, 1, 2), "Columbus Day / Indigenous Peoples' Day"),
                    H(D(y, 11, 11), 'Veterans Day'), H(nthWeekday(y, 11, 4, 4), 'Thanksgiving Day'),
                    H(D(y, 12, 25), 'Christmas Day'),
                    O(D(y, 2, 2), 'Groundhog Day'), O(D(y, 2, 14), "Valentine's Day"), O(D(y, 3, 17), "St. Patrick's Day"),
                    O(E, 'Easter Sunday'), O(nthWeekday(y, 5, 0, 2), "Mother's Day"), O(nthWeekday(y, 6, 0, 3), "Father's Day"),
                    O(D(y, 10, 31), 'Halloween'), O(addDays(nthWeekday(y, 11, 4, 4), 1), 'Black Friday'),
                    O(D(y, 12, 24), 'Christmas Eve'), O(D(y, 12, 31), "New Year's Eve")
                ];
                // weekend federal holidays: the day off moves to Friday / Monday
                [[D(y, 1, 1), "New Year's Day"], [D(y, 6, 19), 'Juneteenth'], [D(y, 7, 4), 'Independence Day'],
                    [D(y, 11, 11), 'Veterans Day'], [D(y, 12, 25), 'Christmas Day'], [D(y + 1, 1, 1), "New Year's Day"]].forEach(([d, n]) => {
                    const obs = usObserved(d);
                    if (obs && obs.getFullYear() === y) list.push(H(obs, `${n} (observed)`, 'day off'));
                });
                // Election Day: the Tuesday after the first Monday of November, in even years
                if (y % 2 === 0) list.push(O(addDays(nthWeekday(y, 11, 1, 1), 1), 'Election Day'));
                return list;
            }
            case 'CN': {
                const list = [
                    H(D(y, 1, 1), "New Year's Day", '元旦'), H(D(y, 5, 1), 'Labour Day', '劳动节'),
                    H(D(y, 10, 1), 'National Day', '国庆节')
                ];
                const add = (fn, dates, name, local) => dates.forEach((d) => list.push(fn(d, name, local)));
                const newYear = lunarDates(y, 1, 1);
                add(H, newYear, 'Chinese New Year (Spring Festival)', '春节');
                add(O, newYear.map((d) => addDays(d, -1)), "Chinese New Year's Eve", '除夕');
                add(O, lunarDates(y, 1, 15), 'Lantern Festival', '元宵节');
                const qingming = solarTermDay(y, 4, 3, 4, 15, 480);
                if (qingming) list.push(H(qingming, 'Qingming Festival (Tomb-Sweeping Day)', '清明节'));
                add(H, lunarDates(y, 5, 5), 'Dragon Boat Festival', '端午节');
                add(O, lunarDates(y, 7, 7), 'Qixi Festival', '七夕');
                add(O, lunarDates(y, 7, 15), 'Ghost Festival', '中元节');
                add(H, lunarDates(y, 8, 15), 'Mid-Autumn Festival', '中秋节');
                add(O, lunarDates(y, 9, 9), 'Double Ninth Festival', '重阳节');
                const dongzhi = solarTermDay(y, 12, 20, 4, 270, 480);
                if (dongzhi) list.push(O(dongzhi, 'Winter Solstice Festival', '冬至'));
                add(O, lunarDates(y, 12, 8), 'Laba Festival', '腊八节');
                return list;
            }
            case 'IN': {
                const hi = { 'Maha Shivaratri': 'महाशिवरात्रि', 'Holika Dahan': 'होलिका दहन', Holi: 'होली', 'Ram Navami': 'राम नवमी',
                    'Raksha Bandhan': 'रक्षा बन्धन', 'Krishna Janmashtami': 'जन्माष्टमी', 'Ganesh Chaturthi': 'गणेश चतुर्थी',
                    'Navratri begins': 'नवरात्रि', Dussehra: 'दशहरा', Diwali: 'दीपावली' };
                const days_off = new Set(['Holi', 'Dussehra', 'Diwali']);
                const list = [
                    H(D(y, 1, 26), 'Republic Day', 'गणतंत्र दिवस'), H(D(y, 8, 15), 'Independence Day', 'स्वतंत्रता दिवस'),
                    H(D(y, 10, 2), 'Gandhi Jayanti', 'गांधी जयंती'), H(addDays(E, -2), 'Good Friday'),
                    H(D(y, 12, 25), 'Christmas Day'), O(D(y, 4, 14), 'Ambedkar Jayanti', 'आंबेडकर जयंती')
                ];
                const makar = solarTermDay(y, 1, 12, 5, 270, IST, true);
                if (makar) list.push(O(makar, 'Makar Sankranti / Pongal', 'मकर संक्रान्ति'));
                const mesha = solarTermDay(y, 4, 12, 4, 0, IST, true);
                if (mesha) list.push(O(mesha, 'Vaisakhi (Baisakhi)', 'वैसाखी'));
                hinduFestivals(y).forEach(([d, name]) => list.push((days_off.has(name) ? H : O)(d, name, hi[name] || '')));
                islamicDates(y, 10, 1).forEach((d) => list.push(H(d, 'Eid al-Fitr', 'ईद उल-फ़ित्र · may be a day later in India')));
                islamicDates(y, 12, 10).forEach((d) => list.push(H(d, 'Eid al-Adha (Bakrid)', 'बकरीद · may be a day later in India')));
                return list;
            }
            case 'JP': {
                const t = y - 1980, leap = Math.floor(t / 4);
                const vernal = Math.floor(20.8431 + 0.242194 * t - leap);
                const autumnal = Math.floor(23.2488 + 0.242194 * t - leap);
                const list = [
                    H(D(y, 1, 1), "New Year's Day", '元日'), H(nthWeekday(y, 1, 1, 2), 'Coming of Age Day', '成人の日'),
                    H(D(y, 2, 11), 'National Foundation Day', '建国記念の日'), H(D(y, 2, 23), "Emperor's Birthday", '天皇誕生日'),
                    H(D(y, 3, vernal), 'Vernal Equinox Day', '春分の日'), H(D(y, 4, 29), 'Shōwa Day', '昭和の日'),
                    H(D(y, 5, 3), 'Constitution Memorial Day', '憲法記念日'), H(D(y, 5, 4), 'Greenery Day', 'みどりの日'),
                    H(D(y, 5, 5), "Children's Day", 'こどもの日'), H(nthWeekday(y, 7, 1, 3), 'Marine Day', '海の日'),
                    H(D(y, 8, 11), 'Mountain Day', '山の日'), H(nthWeekday(y, 9, 1, 3), 'Respect for the Aged Day', '敬老の日'),
                    H(D(y, 9, autumnal), 'Autumnal Equinox Day', '秋分の日'), H(nthWeekday(y, 10, 1, 2), 'Sports Day', 'スポーツの日'),
                    H(D(y, 11, 3), 'Culture Day', '文化の日'), H(D(y, 11, 23), 'Labour Thanksgiving Day', '勤労感謝の日')
                ];
                // special days that are not days off (added after the substitute-day rules below run on `list`)
                const extras = [
                    O(D(y, 2, 3), 'Setsubun', '節分'), O(D(y, 2, 14), "Valentine's Day", 'バレンタインデー'),
                    O(D(y, 3, 3), "Girls' Day (Hinamatsuri)", 'ひな祭り'), O(D(y, 3, 14), 'White Day', 'ホワイトデー'),
                    O(nthWeekday(y, 5, 0, 2), "Mother's Day", '母の日'), O(nthWeekday(y, 6, 0, 3), "Father's Day", '父の日'),
                    O(D(y, 7, 7), 'Tanabata', '七夕'), O(D(y, 11, 15), 'Shichi-Go-San', '七五三'),
                    O(D(y, 12, 24), 'Christmas Eve', 'クリスマスイブ'), O(D(y, 12, 31), "New Year's Eve", '大晦日')
                ];
                const taken = new Set(list.map((h) => dayKey(h.date)));
                // a holiday on a Sunday gives the next free weekday off
                list.slice().sort((a, b) => a.date - b.date).forEach((h) => {
                    if (h.date.getDay() !== 0) return;
                    let d = addDays(h.date, 1);
                    while (taken.has(dayKey(d))) d = addDays(d, 1);
                    taken.add(dayKey(d));
                    list.push(H(d, 'Substitute Holiday', '振替休日'));
                });
                // a single day between two holidays is also a holiday
                list.slice().forEach((h) => {
                    const mid = addDays(h.date, 1);
                    if (!taken.has(dayKey(mid)) && taken.has(dayKey(addDays(h.date, 2))) && mid.getDay() !== 0) {
                        taken.add(dayKey(mid));
                        list.push(H(mid, "Citizens' Holiday", '国民の休日'));
                    }
                });
                return list.concat(extras);
            }
            default: return [];
        }
    }

    const holidayCache = new Map(); // "NL-2026" -> Map(dayKey -> [holiday])
    function holidaysByDay(code, y) {
        const key = `${code}-${y}`;
        if (!holidayCache.has(key)) {
            const map = new Map();
            holidayList(code, y).forEach((h) => {
                const k = dayKey(h.date);
                if (!map.has(k)) map.set(k, []);
                map.get(k).push(h);
            });
            holidayCache.set(key, map);
        }
        return holidayCache.get(key);
    }

    function enabledHolidaySets() {
        const on = Array.isArray(state.prefs.holidays) ? state.prefs.holidays : [];
        return HOLIDAY_SETS.filter((s) => on.includes(s.code));
    }

    // Holidays on a day, the same holiday from several countries merged into one
    function holidaysOn(day) {
        const merged = new Map();
        enabledHolidaySets().forEach((set) => {
            (holidaysByDay(set.code, day.getFullYear()).get(dayKey(day)) || []).forEach((h) => {
                const item = merged.get(h.name) || { name: h.name, locals: [], sets: [], obs: true };
                if (!h.obs) item.obs = false; // a day off somewhere = holiday
                if (h.local && !item.locals.includes(h.local)) item.locals.push(h.local);
                item.sets.push(set.short);
                merged.set(h.name, item);
            });
        });
        return [...merged.values()];
    }

    function holidayTitle(h) {
        return `${h.name}${h.locals.length ? ` (${h.locals.join(' / ')})` : ''} · ${h.sets.join(', ')} ${h.obs ? 'observance' : 'holiday'}`;
    }

    /* ---------------- world clocks (Calendar tab → Clock) ---------------- */
    // id, name, country, IANA time zone, latitude, longitude
    const CLOCK_CITIES = [
        ['amsterdam', 'Amsterdam', 'Netherlands', 'Europe/Amsterdam', 52.37, 4.90],
        ['london', 'London', 'United Kingdom', 'Europe/London', 51.51, -0.13],
        ['dublin', 'Dublin', 'Ireland', 'Europe/Dublin', 53.35, -6.26],
        ['paris', 'Paris', 'France', 'Europe/Paris', 48.86, 2.35],
        ['brussels', 'Brussels', 'Belgium', 'Europe/Brussels', 50.85, 4.35],
        ['berlin', 'Berlin', 'Germany', 'Europe/Berlin', 52.52, 13.40],
        ['madrid', 'Madrid', 'Spain', 'Europe/Madrid', 40.42, -3.70],
        ['lisbon', 'Lisbon', 'Portugal', 'Europe/Lisbon', 38.72, -9.14],
        ['rome', 'Rome', 'Italy', 'Europe/Rome', 41.90, 12.50],
        ['vienna', 'Vienna', 'Austria', 'Europe/Vienna', 48.21, 16.37],
        ['copenhagen', 'Copenhagen', 'Denmark', 'Europe/Copenhagen', 55.68, 12.57],
        ['oslo', 'Oslo', 'Norway', 'Europe/Oslo', 59.91, 10.75],
        ['stockholm', 'Stockholm', 'Sweden', 'Europe/Stockholm', 59.33, 18.07],
        ['helsinki', 'Helsinki', 'Finland', 'Europe/Helsinki', 60.17, 24.94],
        ['reykjavik', 'Reykjavík', 'Iceland', 'Atlantic/Reykjavik', 64.15, -21.94],
        ['warsaw', 'Warsaw', 'Poland', 'Europe/Warsaw', 52.23, 21.01],
        ['athens', 'Athens', 'Greece', 'Europe/Athens', 37.98, 23.73],
        ['istanbul', 'Istanbul', 'Türkiye', 'Europe/Istanbul', 41.01, 28.98],
        ['moscow', 'Moscow', 'Russia', 'Europe/Moscow', 55.76, 37.62],
        ['cairo', 'Cairo', 'Egypt', 'Africa/Cairo', 30.04, 31.24],
        ['lagos', 'Lagos', 'Nigeria', 'Africa/Lagos', 6.52, 3.38],
        ['nairobi', 'Nairobi', 'Kenya', 'Africa/Nairobi', -1.29, 36.82],
        ['johannesburg', 'Johannesburg', 'South Africa', 'Africa/Johannesburg', -26.20, 28.05],
        ['riyadh', 'Riyadh', 'Saudi Arabia', 'Asia/Riyadh', 24.71, 46.68],
        ['dubai', 'Dubai', 'United Arab Emirates', 'Asia/Dubai', 25.20, 55.27],
        ['tehran', 'Tehran', 'Iran', 'Asia/Tehran', 35.69, 51.39],
        ['new-delhi', 'New Delhi', 'India', 'Asia/Kolkata', 28.61, 77.21],
        ['bangkok', 'Bangkok', 'Thailand', 'Asia/Bangkok', 13.76, 100.50],
        ['jakarta', 'Jakarta', 'Indonesia', 'Asia/Jakarta', -6.21, 106.85],
        ['singapore', 'Singapore', 'Singapore', 'Asia/Singapore', 1.35, 103.82],
        ['manila', 'Manila', 'Philippines', 'Asia/Manila', 14.60, 120.98],
        ['hong-kong', 'Hong Kong', 'China', 'Asia/Hong_Kong', 22.32, 114.17],
        ['beijing', 'Beijing', 'China', 'Asia/Shanghai', 39.90, 116.41],
        ['seoul', 'Seoul', 'South Korea', 'Asia/Seoul', 37.57, 126.98],
        ['tokyo', 'Tokyo', 'Japan', 'Asia/Tokyo', 35.68, 139.69],
        ['sydney', 'Sydney', 'Australia', 'Australia/Sydney', -33.87, 151.21],
        ['canberra', 'Canberra', 'Australia', 'Australia/Sydney', -35.28, 149.13],
        ['auckland', 'Auckland', 'New Zealand', 'Pacific/Auckland', -36.85, 174.76],
        ['honolulu', 'Honolulu', 'United States', 'Pacific/Honolulu', 21.31, -157.86],
        ['anchorage', 'Anchorage', 'United States', 'America/Anchorage', 61.22, -149.90],
        ['los-angeles', 'Los Angeles', 'United States', 'America/Los_Angeles', 34.05, -118.24],
        ['vancouver', 'Vancouver', 'Canada', 'America/Vancouver', 49.28, -123.12],
        ['denver', 'Denver', 'United States', 'America/Denver', 39.74, -104.99],
        ['mexico-city', 'Mexico City', 'Mexico', 'America/Mexico_City', 19.43, -99.13],
        ['chicago', 'Chicago', 'United States', 'America/Chicago', 41.88, -87.63],
        ['toronto', 'Toronto', 'Canada', 'America/Toronto', 43.65, -79.38],
        ['ottawa', 'Ottawa', 'Canada', 'America/Toronto', 45.42, -75.70],
        ['new-york', 'New York', 'United States', 'America/New_York', 40.71, -74.01],
        ['washington', 'Washington, D.C.', 'United States', 'America/New_York', 38.90, -77.04],
        ['bogota', 'Bogotá', 'Colombia', 'America/Bogota', 4.71, -74.07],
        ['lima', 'Lima', 'Peru', 'America/Lima', -12.05, -77.04],
        ['santiago', 'Santiago', 'Chile', 'America/Santiago', -33.45, -70.67],
        ['buenos-aires', 'Buenos Aires', 'Argentina', 'America/Argentina/Buenos_Aires', -34.60, -58.38],
        ['sao-paulo', 'São Paulo', 'Brazil', 'America/Sao_Paulo', -23.55, -46.63],
        ['brasilia', 'Brasília', 'Brazil', 'America/Sao_Paulo', -15.79, -47.88]
    ].map(([id, name, country, tz, lat, lon]) => ({ id, name, country, tz, lat, lon }));
    const cityById = new Map(CLOCK_CITIES.map((c) => [c.id, c]));
    const LOCAL_TZ = (() => { try { return Intl.DateTimeFormat().resolvedOptions().timeZone || ''; } catch (err) { return ''; } })();

    function clockInfo(id) {
        if (id === 'local') {
            // your own clock: use a known city in your time zone for sunrise / sunset
            const city = CLOCK_CITIES.find((c) => c.tz === LOCAL_TZ);
            return { id, name: 'Your time', country: city ? city.name : LOCAL_TZ.replace(/_/g, ' '), tz: LOCAL_TZ || undefined,
                lat: city ? city.lat : null, lon: city ? city.lon : null, local: true };
        }
        return cityById.get(id) || null;
    }

    const partsCache = new Map();
    function zoneParts(tz, date) {
        let fmt = partsCache.get(tz || '');
        if (!fmt) {
            fmt = new Intl.DateTimeFormat('en-GB', { timeZone: tz, hourCycle: 'h23', year: 'numeric', month: 'numeric',
                day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', weekday: 'short' });
            partsCache.set(tz || '', fmt);
        }
        const p = {};
        fmt.formatToParts(date).forEach(({ type, value }) => { p[type] = value; });
        return { y: +p.year, m: +p.month, d: +p.day, h: +p.hour % 24, min: +p.minute, s: +p.second, wd: p.weekday };
    }
    const pad2 = (n) => String(n).padStart(2, '0');
    // minutes ahead of UTC
    function zoneOffset(tz, date) {
        const p = zoneParts(tz, date);
        return Math.round((Date.UTC(p.y, p.m - 1, p.d, p.h, p.min, p.s) - date.getTime()) / 60000);
    }
    const zoneHM = (tz, date) => { const p = zoneParts(tz, date); return `${pad2(p.h)}:${pad2(p.min)}`; };

    const RING_R = 54.2; // middle of the 24-hour ring (inner edge just outside the 12-hour dial)

    // colour the 24-hour ring: light from sunrise to sunset, dark at night (like the clock face)
    function updateRing(card, c, now) {
        const p = zoneParts(c.tz, now);
        let start = 7 * 60, len = 12 * 60; // without a known place: 07:00–19:00
        if (c.lat !== null && c.lat !== undefined) {
            const sun = sunTimes(c.lat, c.lon, p.y, p.m, p.d);
            if (sun.polar) { start = 0; len = sun.polar === 'day' ? 1440 : 0; }
            else {
                const r = zoneParts(c.tz, sun.rise), st = zoneParts(c.tz, sun.set);
                start = r.h * 60 + r.min;
                len = ((st.h * 60 + st.min) - start + 1440) % 1440;
            }
        }
        const day = card.querySelector('.ring-day');
        day.setAttribute('stroke-dasharray', `${len} ${1440 - len + 0.001}`);
        // a circle starts at 3 o'clock: turn it so the arc starts at sunrise (24 at the top)
        day.setAttribute('transform', `rotate(${(start / 4 - 90).toFixed(2)} 60 60)`);
        card.querySelectorAll('.ring-ticks text').forEach((t) => {
            const m = Number(t.dataset.h) * 60;
            const isDay = len >= 1440 || ((m - start + 1440) % 1440) < len;
            t.classList.toggle('on-day', isDay);
        });
    }

    function clockFaceSvg() {
        let ticks = '';
        for (let i = 0; i < 60; i++) {
            const big = i % 5 === 0;
            ticks += `<line x1="50" y1="${big ? 6 : 7}" x2="50" y2="${big ? 13 : 10}" class="${big ? 'tick-big' : 'tick'}" transform="rotate(${i * 6} 50 50)"/>`;
        }
        let nums = '';
        for (let i = 1; i <= 12; i++) {
            const a = i * 30 * DEG;
            nums += `<text x="${(50 + 30 * Math.sin(a)).toFixed(2)}" y="${(50 - 30 * Math.cos(a) + 3.6).toFixed(2)}">${i}</text>`;
        }
        // 24-hour ring around the clock: 24 (midnight) at the top, 12 (noon) at the bottom
        let ring = '';
        for (let h = 0; h < 24; h++) {
            const a = h * 15 * DEG;
            ring += `<line x1="60" y1="0.9" x2="60" y2="3.4" class="ring-tick" transform="rotate(${h * 15 + 7.5} 60 60)"/>`;
            ring += `<text x="${(60 + RING_R * Math.sin(a)).toFixed(2)}" y="${(60 - RING_R * Math.cos(a) + 1.95).toFixed(2)}" data-h="${h}">${h || 24}</text>`;
        }
        return `<svg viewBox="0 0 120 120" class="clock-face" aria-hidden="true">
            <circle cx="60" cy="60" r="${RING_R}" class="ring-night"/>
            <circle cx="60" cy="60" r="${RING_R}" class="ring-day" pathLength="1440" stroke-dasharray="0 1440"/>
            <circle cx="60" cy="60" r="${RING_R + 5.2}" class="ring-edge"/>
            <g class="ring-ticks">${ring}</g>
            <g transform="translate(10 10)">
                <circle cx="50" cy="50" r="47" class="rim"/>
                ${ticks}<g class="nums">${nums}</g>
                <line x1="50" y1="55" x2="50" y2="27" class="hand hour"/>
                <line x1="50" y1="57" x2="50" y2="15" class="hand minute"/>
                <line x1="50" y1="60" x2="50" y2="11" class="hand second"/>
                <circle cx="50" cy="50" r="2.4" class="pin"/>
            </g>
            <g class="ring-mark" visibility="hidden"><line x1="60" y1="0.6" x2="60" y2="13.5"/></g>
        </svg>`;
    }

    function clockIds() {
        if (!Array.isArray(state.prefs.clocks)) state.prefs.clocks = ['local', 'tokyo', 'paris', 'new-york'];
        return state.prefs.clocks.filter((id) => clockInfo(id));
    }

    function renderClocks() {
        const now = new Date();
        $('clockGrid').innerHTML = clockIds().map((id) => {
            const c = clockInfo(id);
            return `<div class="clock-card" data-clock-id="${id}">
                ${c.local ? '' : `<button class="clock-remove icon-btn" type="button" title="Remove ${escapeHtml(c.name)}" aria-label="Remove ${escapeHtml(c.name)}">&times;</button>`}
                ${clockFaceSvg()}
                <div class="clock-name">${escapeHtml(c.name)}</div>
                <div class="clock-sub">${escapeHtml(c.country)}</div>
                <div class="clock-digital"></div>
                <div class="clock-date"></div>
                <div class="clock-diff"></div>
                <div class="clock-sun"></div>
                <div class="clock-mark" hidden></div>
            </div>`;
        }).join('') + `<button class="clock-card clock-add" id="clockAdd" type="button"><span class="clock-add-plus">+</span><span>Add clock</span></button>`;
        updateClocks(now, true);
        updateClockMarks();
    }

    /* Red marker: click (or drag) on a clock to mark a moment; every clock shows that same
     * moment in its own time. Dark red = night there, light red = day there. */
    let clockMark = null; // the marked moment (a Date), or null

    function isNightAt(c, when) {
        const p = zoneParts(c.tz, when);
        if (c.lat === null || c.lat === undefined) return p.h < 7 || p.h >= 19;
        const sun = sunTimes(c.lat, c.lon, p.y, p.m, p.d);
        if (sun.polar) return sun.polar === 'night';
        return when < sun.rise || when >= sun.set;
    }

    function updateClockMarks() {
        document.querySelectorAll('#clockGrid .clock-card[data-clock-id]').forEach((card) => {
            const c = clockInfo(card.dataset.clockId);
            const hand = card.querySelector('.ring-mark');
            const label = card.querySelector('.clock-mark');
            if (!c || !hand) return;
            if (!clockMark) {
                hand.setAttribute('visibility', 'hidden');
                label.hidden = true;
                return;
            }
            const p = zoneParts(c.tz, clockMark);
            const night = isNightAt(c, clockMark);
            hand.setAttribute('visibility', 'visible');
            hand.setAttribute('transform', `rotate(${((p.h * 60 + p.min) / 4).toFixed(2)} 60 60)`);
            hand.classList.toggle('night', night);
            const today = zoneParts(c.tz, new Date());
            const sameDay = today.y === p.y && today.m === p.m && today.d === p.d;
            label.hidden = false;
            label.classList.toggle('night', night);
            label.innerHTML = `<span class="clock-mark-dot"></span>${pad2(p.h)}:${pad2(p.min)}${sameDay ? '' : ` ${p.wd}`} · ${night ? 'night' : 'day'}`
                + '<button type="button" class="clock-mark-clear" title="Remove the red marker" aria-label="Remove the red marker">&times;</button>';
        });
    }

    // Where on the clock was clicked → that time of day on the 24-hour ring (the nearest such moment, ± 12 h from now)
    function markFromPointer(card, e) {
        const face = card.querySelector('.clock-face');
        const r = face.getBoundingClientRect();
        const dx = e.clientX - (r.left + r.width / 2), dy = e.clientY - (r.top + r.height / 2);
        let deg = Math.atan2(dx, -dy) * 180 / Math.PI;
        if (deg < 0) deg += 360;
        const dayMin = (Math.round(deg * 4 / 5) * 5) % 1440; // 24 hours = 360°, snap to 5 min
        const c = clockInfo(card.dataset.clockId);
        const now = new Date();
        const p = zoneParts(c.tz, now);
        let delta = dayMin - (p.h * 60 + p.min);
        if (delta > 720) delta -= 1440;
        if (delta <= -720) delta += 1440;
        clockMark = new Date(now.getTime() - p.s * 1000 - now.getMilliseconds() + delta * 60000);
        updateClockMarks();
    }

    let markDrag = null;
    $('clockGrid').addEventListener('pointerdown', (e) => {
        const face = e.target.closest('.clock-face');
        if (!face || e.button > 0) return;
        const card = face.closest('.clock-card');
        e.preventDefault();
        markDrag = { card, id: e.pointerId };
        try { face.setPointerCapture(e.pointerId); } catch (err) { /* ignore */ }
        markFromPointer(card, e);
    });
    $('clockGrid').addEventListener('pointermove', (e) => {
        if (markDrag && e.pointerId === markDrag.id) markFromPointer(markDrag.card, e);
    });
    const endMarkDrag = (e) => { if (markDrag && e.pointerId === markDrag.id) markDrag = null; };
    $('clockGrid').addEventListener('pointerup', endMarkDrag);
    $('clockGrid').addEventListener('pointercancel', endMarkDrag);

    let lastHourStamp = '';
    function updateClocks(now = new Date(), force = false) {
        const localOffset = -now.getTimezoneOffset();
        const hourStamp = `${now.getUTCFullYear()}-${now.getUTCMonth()}-${now.getUTCDate()}-${now.getUTCHours()}`;
        const newHour = force || hourStamp !== lastHourStamp; // time differences can change (summer/winter time)
        lastHourStamp = hourStamp;
        let dayChanged = false;
        document.querySelectorAll('#clockGrid .clock-card[data-clock-id]').forEach((card) => {
            const c = clockInfo(card.dataset.clockId);
            if (!c) return;
            const p = zoneParts(c.tz, now);
            const set = (cls, deg) => card.querySelector(cls).setAttribute('transform', `rotate(${deg.toFixed(2)} 50 50)`);
            set('.hand.hour', (p.h % 12) * 30 + p.min * 0.5 + p.s / 120);
            set('.hand.minute', p.min * 6 + p.s * 0.1);
            set('.hand.second', p.s * 6);
            card.querySelector('.clock-digital').textContent = `${pad2(p.h)}:${pad2(p.min)}:${pad2(p.s)}`;
            card.querySelector('.clock-date').textContent = `${p.wd} ${p.d} ${MONTHS[p.m - 1]} ${p.y}`;
            if (newHour) {
                const diff = zoneOffset(c.tz, now) - localOffset;
                const h = Math.trunc(diff / 60), m = Math.abs(diff % 60);
                card.querySelector('.clock-diff').textContent = c.local ? 'This device' : diff === 0 ? 'Same time as you'
                    : `${diff > 0 ? '+' : '−'}${Math.abs(h)}${m ? `:${pad2(m)}` : ''} h`;
            }
            // sunrise / sunset: worked out once per day there (new day there, or on first show)
            const dayKey = `${p.y}-${p.m}-${p.d}`;
            if (force || card._sunDay !== dayKey) {
                card._sunDay = dayKey;
                card._sun = c.lat !== null && c.lat !== undefined ? sunTimes(c.lat, c.lon, p.y, p.m, p.d) : null;
                const sun = card._sun;
                const sunText = !sun ? '' : sun.polar === 'day' ? '☀️ Sun up all day' : sun.polar === 'night' ? '🌙 Sun down all day'
                    : `🌅 ${zoneHM(c.tz, sun.rise)} · 🌇 ${zoneHM(c.tz, sun.set)}`;
                card.querySelector('.clock-sun').textContent = sunText;
                card.querySelector('.clock-sun').title = sunText ? 'Sunrise · sunset today (local time there)' : '';
                updateRing(card, c, now);
                dayChanged = true;
            }
            // day or night there: checked once a minute (when the second hand passes 12),
            // so the face turns dark / light within a minute of sunset / sunrise
            const minuteKey = `${dayKey} ${p.h}:${p.min}`;
            if (force || card._minuteKey !== minuteKey) {
                card._minuteKey = minuteKey;
                const sun = card._sun;
                const isDay = !sun ? p.h >= 7 && p.h < 19 : sun.polar ? sun.polar === 'day' : now >= sun.rise && now < sun.set;
                card.classList.toggle('night', !isDay);
            }
        });
        // the red marker's labels say "today" or the weekday: refresh them on a new day
        if (dayChanged && !force && clockMark) updateClockMarks();
    }

    let clockTimer = null;
    function setCalendarMode(mode) {
        state.prefs.calendarMode = mode === 'clock' ? 'clock' : 'calendar';
        savePrefs();
        applyCalendarMode();
    }
    function applyCalendarMode() {
        const clock = state.prefs.calendarMode === 'clock' && state.prefs.activeTab === 'calendar';
        const inCal = state.prefs.calendarMode === 'clock';
        $('calendarPane').hidden = inCal;
        $('clockPane').hidden = !inCal;
        $('calPrev').hidden = inCal;
        $('calNext').hidden = inCal;
        $('calendarMonth').textContent = inCal ? 'World clocks' : $('calendarMonth').textContent;
        $('clockToggle').innerHTML = inCal ? '📅 Calendar' : '🕐 Clock';
        $('clockToggle').title = inCal ? 'Back to the calendar' : 'World clocks';
        clearInterval(clockTimer);
        clockTimer = null;
        if (clock) {
            renderClocks();
            // tick right on the second
            setTimeout(() => {
                updateClocks();
                if (state.prefs.calendarMode === 'clock' && state.prefs.activeTab === 'calendar' && !clockTimer) {
                    clockTimer = setInterval(() => { if (!document.hidden) updateClocks(); }, 1000);
                }
            }, 1000 - (Date.now() % 1000));
        } else if (!inCal) {
            renderCalendar();
        }
    }
    $('clockToggle').addEventListener('click', () => setCalendarMode(state.prefs.calendarMode === 'clock' ? 'calendar' : 'clock'));

    $('clockGrid').addEventListener('click', (e) => {
        if (e.target.closest('#clockAdd')) { openClockPicker(); return; }
        if (e.target.closest('.clock-mark-clear')) { clockMark = null; updateClockMarks(); return; }
        const remove = e.target.closest('.clock-remove');
        if (remove) {
            const id = remove.closest('.clock-card').dataset.clockId;
            state.prefs.clocks = clockIds().filter((x) => x !== id);
            settingsChanged();
            renderClocks();
        }
    });

    function openClockPicker() {
        $('clockSearch').value = '';
        renderClockPicker();
        openModal('clockModal');
        if (!isMobile()) setTimeout(() => $('clockSearch').focus(), 30);
    }
    function renderClockPicker() {
        const q = $('clockSearch').value.trim().toLowerCase();
        const have = new Set(clockIds());
        const list = CLOCK_CITIES.filter((c) => !q || `${c.name} ${c.country}`.toLowerCase().includes(q));
        const now = new Date();
        $('clockCityList').innerHTML = list.length ? list.map((c) => `
            <button class="clock-city${have.has(c.id) ? ' added' : ''}" type="button" data-city="${c.id}" ${have.has(c.id) ? 'disabled' : ''}>
                <span><b>${escapeHtml(c.name)}</b> <span class="clock-city-country">${escapeHtml(c.country)}</span></span>
                <span class="clock-city-time">${have.has(c.id) ? 'Added' : zoneHM(c.tz, now)}</span>
            </button>`).join('') : '<div class="calendar-hint">No city found.</div>';
    }
    $('clockSearch').addEventListener('input', renderClockPicker);
    $('clockSearch').addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        const first = $('clockCityList').querySelector('.clock-city:not([disabled])');
        if (first) first.click();
    });
    $('clockCityList').addEventListener('click', (e) => {
        const btn = e.target.closest('.clock-city');
        if (!btn || btn.disabled) return;
        state.prefs.clocks = clockIds().concat(btn.dataset.city);
        settingsChanged();
        closeModals();
        renderClocks();
    });
    $('clockClose').addEventListener('click', closeModals);
    $('clockBackdrop').addEventListener('click', closeModals);

    /* settings window: which holidays to show */
    $('holidayOptions').innerHTML = HOLIDAY_SETS.map((s) =>
        `<label class="check-row holiday-option"><input type="checkbox" value="${s.code}"> ${s.label}</label>`
    ).join('');

    function syncHolidayCheckboxes() {
        const on = Array.isArray(state.prefs.holidays) ? state.prefs.holidays : [];
        $('holidayOptions').querySelectorAll('input').forEach((input) => { input.checked = on.includes(input.value); });
    }

    $('holidayOptions').addEventListener('change', () => {
        state.prefs.holidays = [...$('holidayOptions').querySelectorAll('input:checked')].map((el) => el.value);
        settingsChanged();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    });



    $('calendarGrid').addEventListener('click', (e) => {
        const chip = e.target.closest('.calendar-chip');
        // on a phone the labels fill the whole day, so a tap selects the day (open notes from the list below)
        if (chip && !isMobile()) {
            setTab('note');
            selectNote(chip.dataset.noteId);
            return;
        }
        const cell = e.target.closest('.calendar-day');
        if (!cell) return;
        const [y, m, d] = cell.dataset.date.split('-').map(Number);
        state.calendarDay = new Date(y, m - 1, d);
        renderCalendar();
    });

    // Dragging a note that is already on the calendar (from a day cell or the day panel)
    let draggedCalendarNote = null; // { noteId, fromKey }

    function startCalendarDrag(e, noteId, fromKey) {
        draggedCalendarNote = { noteId, fromKey };
        e.dataTransfer.setData('text/plain', noteId);
        e.dataTransfer.effectAllowed = 'move';
    }

    $('calendarGrid').addEventListener('dragstart', (e) => {
        const chip = e.target.closest('.calendar-chip');
        if (!chip) return;
        startCalendarDrag(e, chip.dataset.noteId, chip.closest('.calendar-day').dataset.date);
    });
    $('calendarDayNotes').addEventListener('dragstart', (e) => {
        const item = e.target.closest('.calendar-note-item');
        if (!item || !state.calendarDay) return;
        startCalendarDrag(e, item.dataset.noteId, dayKey(state.calendarDay));
    });
    document.addEventListener('dragend', () => {
        draggedCalendarNote = null;
        document.querySelectorAll('.calendar-day.drag-over').forEach((el) => el.classList.remove('drag-over'));
    });

    // Move a note from one day to another, keeping its time. For a repeating
    // note the whole series (start date and end date) shifts by the same number of days.
    function moveNoteToDay(noteId, fromKey, toKey) {
        const note = noteById(noteId);
        const base = note && parseDate(note.scheduledAt);
        if (!base || fromKey === toKey) return;
        const from = keyToDate(fromKey), to = keyToDate(toKey);
        const diff = Math.round((Date.UTC(to.getFullYear(), to.getMonth(), to.getDate()) -
            Date.UTC(from.getFullYear(), from.getMonth(), from.getDate())) / 86400000);
        const moved = new Date(base.getFullYear(), base.getMonth(), base.getDate() + diff,
            base.getHours(), base.getMinutes(), base.getSeconds());
        note.scheduledAt = toLocalIso(moved);
        if (note.repeatEndDate) {
            const end = keyToDate(note.repeatEndDate);
            note.repeatEndDate = dayKey(new Date(end.getFullYear(), end.getMonth(), end.getDate() + diff));
        }
        resetAlarmRuntime(note);
        touch(note);
        state.calendarDay = to;
        renderTree();
        renderCalendar();
        if (note.id === state.currentNoteId) updateScheduleUi(note);
        toast(`Moved to ${to.getDate()} ${MONTHS[to.getMonth()]}`);
    }

    $('calendarGrid').addEventListener('dragover', (e) => {
        const cell = e.target.closest('.calendar-day');
        if (!cell) return;
        if (!draggedTreeNoteId && !Array.from(e.dataTransfer.types || []).includes('text/plain')) return;
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        cell.classList.add('drag-over');
    });

    $('calendarGrid').addEventListener('dragleave', (e) => {
        const cell = e.target.closest('.calendar-day');
        if (!cell) return;
        const related = e.relatedTarget;
        if (related && cell.contains(related)) return;
        cell.classList.remove('drag-over');
    });

    $('calendarGrid').addEventListener('drop', (e) => {
        const cell = e.target.closest('.calendar-day');
        if (!cell) return;
        e.preventDefault();
        cell.classList.remove('drag-over');
        if (draggedCalendarNote) {
            const { noteId, fromKey } = draggedCalendarNote;
            draggedCalendarNote = null;
            moveNoteToDay(noteId, fromKey, cell.dataset.date);
            return;
        }
        const noteId = e.dataTransfer.getData('text/plain') || draggedTreeNoteId;
        if (!noteId) return;
        openPlanFromTreeModal(noteId, keyToDate(cell.dataset.date));
        draggedTreeNoteId = null;
    });

    // Take a note (and its alarm) off a date. The note itself is kept.
    // For a repeating note you choose: skip only this day, or remove the whole series.
    async function removeFromDate(noteId, day) {
        const note = noteById(noteId);
        if (!note || !day) return;
        const title = note.title.trim() || 'Untitled';
        const dateLabel = `${day.getDate()} ${MONTHS[day.getMonth()]}`;
        let choice;
        if (isRecurring(note)) {
            choice = await askDialog({
                title: 'Remove from date',
                message: `"${title}" repeats. Remove it only from ${dateLabel}, or remove the whole repeating schedule?`,
                okLabel: 'Only this day',
                altLabel: 'Whole series'
            });
            if (!choice) return;
        } else {
            choice = await askConfirm('Remove from date', `Remove "${title}" (and its alarm) from ${dateLabel}? The note itself is kept.`, 'Remove');
            if (!choice) return;
            choice = 'alt';
        }
        if (!noteById(noteId)) return;
        if (choice === 'alt') {
            note.scheduledAt = null;
            note.alarmEnabled = false;
            note.emailEnabled = false;
            note.repeatWeekdays = [];
            note.repeatMode = 'none';
            note.repeatEndDate = '';
            note.skipDates = [];
        } else {
            note.skipDates = [...new Set([...(note.skipDates || []), dayKey(day)])].sort();
        }
        resetAlarmRuntime(note);
        touch(note);
        renderTree();
        renderCalendar();
        if (note.id === state.currentNoteId) updateScheduleUi(note);
        toast(choice === 'alt' ? `Removed "${title}" from the calendar` : `Skipped "${title}" on ${dateLabel}`);
    }

    /* ---- pick an existing note for the selected day (same as dragging it onto the day) ---- */
    function openNotePicker() {
        const day = state.calendarDay;
        if (!day) return;
        $('notePickerTitle').textContent = `Add a note to ${day.getDate()} ${MONTHS[day.getMonth()]}`;
        $('notePickerSearch').value = '';
        renderNotePicker();
        openModal('notePickerModal');
        if (!isMobile()) setTimeout(() => $('notePickerSearch').focus(), 30);
    }

    function renderNotePicker() {
        const q = $('notePickerSearch').value.trim().toLowerCase();
        const match = (n) => !q || (n.title || '').toLowerCase().includes(q) || (!n.private && (n.content || '').toLowerCase().includes(q));
        const groups = [...folderTree().map(({ folder: f }) => ({ name: folderPath(f.id), notes: sortedNotes(state.notes.filter((n) => n.folderId === f.id)) })),
            { name: 'No folder', notes: sortedNotes(state.notes.filter((n) => !n.folderId)) }]
            .map((g) => ({ ...g, notes: g.notes.filter(match) }))
            .filter((g) => g.notes.length);
        const day = state.calendarDay;
        $('notePickerList').innerHTML = groups.length ? groups.map((g) => `
            <div class="picker-group">${escapeHtml(g.name)}</div>
            ${g.notes.map((n) => {
                const onDay = day && occurrenceOn(n, day);
                const when = n.scheduledAt
                    ? (isRecurring(n) ? `↻ ${repeatSummary(n)}` : `📅 ${formatDateTime(n.scheduledAt)}`)
                    : 'Not scheduled';
                return `<button type="button" class="picker-item${onDay ? ' on-day' : ''}" data-note-id="${n.id}">
                    <span class="picker-dot" style="${n.color ? `background:${n.color}` : ''}"></span>
                    <span class="picker-text">
                        <span class="picker-title">${escapeHtml(n.title.trim() || 'Untitled')}</span>
                        <span class="picker-meta">${escapeHtml(onDay ? 'Already on this day' : when)}</span>
                    </span>
                </button>`;
            }).join('')}`).join('')
            : `<div class="calendar-hint">${q ? 'No notes match your search.' : 'You have no notes yet.'}</div>`;
    }

    $('notePickerSearch').addEventListener('input', renderNotePicker);
    $('notePickerList').addEventListener('click', (e) => {
        const item = e.target.closest('.picker-item');
        if (!item || !state.calendarDay) return;
        $('notePickerModal').classList.remove('active');
        openPlanFromTreeModal(item.dataset.noteId, state.calendarDay);
    });
    $('notePickerClose').addEventListener('click', closeModals);
    $('notePickerBackdrop').addEventListener('click', closeModals);

    $('calendarDayNotes').addEventListener('click', (e) => {
        if (e.target.closest('#calendarPickNote')) { openNotePicker(); return; }
        if (e.target.closest('#calendarAddNote')) {
            const d = state.calendarDay;
            if (!d) return;
            newNote({ scheduledAt: toLocalIso(new Date(d.getFullYear(), d.getMonth(), d.getDate(), 9, 0, 0)) });
            toast(`New note on ${d.getDate()} ${MONTHS[d.getMonth()]} – give it a title`);
            return;
        }
        const calMore = e.target.closest('.cal-more');
        if (calMore) {
            const note = noteById(calMore.dataset.noteId);
            const day = state.calendarDay;
            if (!note) return;
            showActionSheet(note.title.trim() || 'Untitled', [
                { label: 'Open note', run: () => { setTab('note'); selectNote(note.id); } },
                { label: 'Change date / time…', run: () => openPlanFromTreeModal(note.id, day || new Date()) },
                { label: 'Remove from date', run: () => removeFromDate(note.id, day) },
                { label: 'Rename', run: () => renameNote(note.id) },
                { label: 'Delete', danger: true, run: () => deleteNote(note.id) }
            ]);
            return;
        }
        const unschedule = e.target.closest('.note-unschedule');
        if (unschedule) { removeFromDate(unschedule.dataset.noteId, state.calendarDay); return; }
        const rename = e.target.closest('.note-rename');
        if (rename) { renameNote(rename.dataset.noteId); return; }
        const del = e.target.closest('.note-delete');
        if (del) { deleteNote(del.dataset.noteId); return; }
        const item = e.target.closest('.calendar-note-item');
        if (!item) return;
        setTab('note');
        selectNote(item.dataset.noteId);
    });

    const planModal = $('planFromTreeModal');
    const planBackdrop = $('planFromTreeBackdrop');
    const planClose = $('planFromTreeClose');
    const planCancel = $('planFromTreeCancel');
    let pendingPlan = null;

    $('weekdayOptions').innerHTML = WEEKDAY_SHORT.map((letter, index) =>
        `<label class="weekday-chip" title="${WEEKDAY_FULL[index]}"><input type="checkbox" value="${index}" aria-label="${WEEKDAY_FULL[index]}"><span>${letter}</span></label>`
    ).join('');
    $('advancedSettingsToggle').addEventListener('click', () => {
        const panel = $('advancedSettings');
        panel.hidden = !panel.hidden;
        $('advancedSettingsToggle').setAttribute('aria-expanded', String(!panel.hidden));
    });

    // Fill the Repeat dropdown with options that match the chosen date
    function refreshRepeatOptions(selected) {
        const select = $('repeatMode');
        const value = selected || select.value || 'none';
        const date = $('planFromTreeDate').value;
        const base = date ? keyToDate(date) : new Date();
        const checked = [...$('weekdayOptions').querySelectorAll('input:checked')].map((el) => Number(el.value));
        const modes = ['none', 'weekly', 'monthlyDate', 'monthlyWeekday'];
        if (isLastWeekdayOfMonth(base)) modes.push('monthlyLastWeekday');
        modes.push('yearly');
        let pick = value;
        if (!modes.includes(pick)) pick = pick === 'monthlyLastWeekday' ? 'monthlyWeekday' : 'none';
        select.innerHTML = modes.map((m) =>
            `<option value="${m}">${escapeHtml(repeatLabel(m, base, checked))}</option>`).join('');
        select.value = pick;
        $('weeklyOptions').hidden = pick !== 'weekly';
        $('repeatEndRow').hidden = pick === 'none';
    }

    $('repeatMode').addEventListener('change', () => {
        if ($('repeatMode').value === 'weekly' && !$('weekdayOptions').querySelector('input:checked')) {
            // start with the weekday of the chosen date
            const date = $('planFromTreeDate').value;
            const idx = ((date ? keyToDate(date) : new Date()).getDay() + 6) % 7;
            const box = $('weekdayOptions').querySelector(`input[value="${idx}"]`);
            if (box) box.checked = true;
        }
        refreshRepeatOptions($('repeatMode').value);
    });
    $('planFromTreeDate').addEventListener('change', () => refreshRepeatOptions());
    $('weekdayOptions').addEventListener('change', () => refreshRepeatOptions());
    dragSelectDays($('weekdayOptions'));

    function openPlanFromTreeModal(noteId, day) {
        const note = noteById(noteId);
        if (!note) return;
        pendingPlan = { noteId, day: new Date(day.getFullYear(), day.getMonth(), day.getDate()) };
        $('planFromTreeNoteTitle').textContent = note.title.trim() || 'Untitled';
        const existing = parseDate(note.scheduledAt);
        let timeStr = '09:00:00';
        if (existing && sameDay(existing, day)) {
            timeStr = `${pad(existing.getHours())}:${pad(existing.getMinutes())}:${pad(existing.getSeconds())}`;
        } else if (existing) {
            timeStr = `${pad(existing.getHours())}:${pad(existing.getMinutes())}:${pad(existing.getSeconds())}`;
        }
        $('planFromTreeDate').value = `${day.getFullYear()}-${pad(day.getMonth() + 1)}-${pad(day.getDate())}`;
        $('planFromTreeTime').value = timeStr;
        $('planFromTreeAlarm').checked = note.alarmEnabled;
        $('planFromTreeSnooze').value = note.alarmSnoozeMinutes || 5;
        $('planFromTreeSnoozeLabel').textContent = $('planFromTreeSnooze').value;
        $('advancedSettings').hidden = !isRecurring(note);
        $('advancedSettingsToggle').setAttribute('aria-expanded', String(isRecurring(note)));
        $('weekdayOptions').querySelectorAll('input').forEach((input) => {
            input.checked = (note.repeatWeekdays || []).includes(Number(input.value));
        });
        $('repeatEndDate').value = note.repeatEndDate || '';
        refreshRepeatOptions(repeatModeOf(note));
        pendingPlan.color = note.color || '';
        pendingPlan.email = { enabled: !!note.emailEnabled, to: note.emailTo || '' };
        renderPlanExtras();
        planModal.classList.add('active');
    }

    function renderPlanExtras() {
        if (!pendingPlan) return;
        renderColorPicker($('planNoteColors'), pendingPlan.color, true, 'planColorCustom');
        $('planEmailEnabled').checked = pendingPlan.email.enabled;
        $('planEmailLabel').textContent = pendingPlan.email.enabled && pendingPlan.email.to ? `→ ${pendingPlan.email.to}` : '';
    }

    function planEmailTarget() {
        const note = pendingPlan && noteById(pendingPlan.noteId);
        if (!note) return null;
        return {
            note,
            isPlan: true,
            address: () => pendingPlan.email.to,
            enabled: () => pendingPlan.email.enabled,
            save(address) { pendingPlan.email = { enabled: true, to: address }; },
            off() { if (pendingPlan) pendingPlan.email.enabled = false; },
            sync() { renderPlanExtras(); }
        };
    }

    $('planNoteColors').addEventListener('click', (e) => {
        const btn = e.target.closest('button.note-color');
        if (!btn || !pendingPlan) return;
        pendingPlan.color = btn.dataset.color;
        renderPlanExtras();
    });
    $('planNoteColors').addEventListener('change', (e) => {
        if (e.target.id !== 'planColorCustom' || !pendingPlan) return;
        pendingPlan.color = e.target.value.toLowerCase();
        renderPlanExtras();
    });
    $('planEmailEnabled').addEventListener('change', () => {
        if (!pendingPlan) return;
        if ($('planEmailEnabled').checked) {
            const address = pendingPlan.email.to || defaultEmail();
            if (!address) { openEmailModal(planEmailTarget()); return; }
            pendingPlan.email = { enabled: true, to: address };
        } else {
            pendingPlan.email.enabled = false;
        }
        renderPlanExtras();
    });
    $('planEmailLink').addEventListener('click', () => openEmailModal(planEmailTarget()));

    function closePlanFromTreeModal() {
        planModal.classList.remove('active');
        pendingPlan = null;
    }

    planBackdrop.addEventListener('click', closePlanFromTreeModal);
    planClose.addEventListener('click', closePlanFromTreeModal);
    planCancel.addEventListener('click', closePlanFromTreeModal);

    $('planFromTreeSnooze').addEventListener('input', () => {
        $('planFromTreeSnoozeLabel').textContent = $('planFromTreeSnooze').value;
    });

    $('planFromTreeSave').addEventListener('click', () => {
        if (!pendingPlan) return;
        const note = noteById(pendingPlan.noteId);
        if (!note) return;
        const date = $('planFromTreeDate').value;
        const time = $('planFromTreeTime').value || '00:00:00';
        const endDate = $('repeatEndDate').value;
        if (endDate && date && endDate < date) {
            toast('End date is before the start date');
            return;
        }
        if (date) {
            const parts = time.split(':');
            const dt = new Date(`${date}T${pad(parts[0] || 0)}:${pad(parts[1] || 0)}:${pad(parts[2] || 0)}`);
            note.scheduledAt = isNaN(dt.getTime()) ? null : toLocalIso(dt);
        } else {
            note.scheduledAt = null;
        }
        note.alarmEnabled = $('planFromTreeAlarm').checked;
        note.alarmSnoozeMinutes = Number($('planFromTreeSnooze').value);
        const mode = $('repeatMode').value;
        const weekdays = [...$('weekdayOptions').querySelectorAll('input:checked')].map((el) => Number(el.value));
        if (mode === 'weekly' && !weekdays.length) {
            toast('Pick at least one weekday');
            return;
        }
        note.repeatMode = REPEAT_MODES.includes(mode) ? mode : 'none';
        note.repeatWeekdays = mode === 'weekly' ? weekdays : [];
        note.repeatEndDate = mode === 'none' ? '' : $('repeatEndDate').value;
        note.color = /^#[0-9a-f]{6}$/i.test(pendingPlan.color || '') ? pendingPlan.color : '';
        note.emailEnabled = !!(pendingPlan.email.enabled && pendingPlan.email.to);
        note.emailTo = pendingPlan.email.to || note.emailTo || '';
        resetAlarmRuntime(note);
        touch(note);
        renderTree();
        if (state.prefs.activeTab === 'calendar') {
            state.calendarDay = new Date(pendingPlan.day.getFullYear(), pendingPlan.day.getMonth(), pendingPlan.day.getDate());
            renderCalendar();
        }
        closePlanFromTreeModal();
        if (note.id === state.currentNoteId) updateScheduleUi(note);
        toast(isRecurring(note) ? `Scheduled · ${repeatSummary(note)}` : 'Note scheduled');
    });

    $('calPrev').addEventListener('click', () => {
        state.calendarCursor = new Date(state.calendarCursor.getFullYear(), state.calendarCursor.getMonth() - 1, 1);
        renderCalendar();
    });

    $('calNext').addEventListener('click', () => {
        state.calendarCursor = new Date(state.calendarCursor.getFullYear(), state.calendarCursor.getMonth() + 1, 1);
        renderCalendar();
    });

    /* ---------------- zoom (pinch on phone, Ctrl + scroll on computer) ---------------- */

    // Two fingers = zoom/move, one finger = draw. `content` (the canvas) is
    // scaled with a CSS transform inside `viewport`.
    // hold the right mouse button + scroll = zoom, like Ctrl + scroll (Collage and Draw)
    let rightHeld = false, rightZoomed = false;
    window.addEventListener('pointerdown', (e) => { if (e.pointerType === 'mouse' && e.button === 2) { rightHeld = true; rightZoomed = false; } }, true);
    window.addEventListener('pointerup', (e) => { if (e.pointerType === 'mouse' && e.button === 2) rightHeld = false; }, true);
    window.addEventListener('pointermove', (e) => { if (e.pointerType === 'mouse' && !(e.buttons & 2)) rightHeld = false; }, true);
    window.addEventListener('blur', () => { rightHeld = false; });
    // letting go of the right button after zooming: no right-click menu
    window.addEventListener('contextmenu', (e) => { if (rightZoomed) { rightZoomed = false; e.preventDefault(); e.stopPropagation(); } }, true);
    function rightZoomWheel(e) {
        if (e.ctrlKey || e.metaKey || !(rightHeld || (e.buttons & 2))) return false;
        if (!rightZoomed) { rightZoomed = true; closeMenu(); }
        return true;
    }

    function createZoom(viewport, content, onPinchStart, onChange) {
        const z = { scale: 1, tx: 0, ty: 0 };
        const MAX = 8;
        const touches = new Map();
        let pinch = null;
        let lock = false; // stays on until all fingers are lifted, so a pinch never draws
        const resetBtn = viewport.querySelector('.zoom-reset');

        function apply() {
            const w = viewport.clientWidth;
            const h = content.offsetHeight;
            z.scale = Math.min(MAX, Math.max(1, z.scale));
            if (z.scale <= 1.001) { z.scale = 1; z.tx = 0; z.ty = 0; }
            z.tx = Math.min(0, Math.max(w - w * z.scale, z.tx));
            z.ty = Math.min(0, Math.max(h - h * z.scale, z.ty));
            content.style.transform = z.scale === 1 ? '' : `translate(${z.tx}px, ${z.ty}px) scale(${z.scale})`;
            if (resetBtn) resetBtn.hidden = z.scale === 1;
            if (onChange) onChange(z);
        }

        function local(e) {
            const r = viewport.getBoundingClientRect();
            return { x: e.clientX - r.left - viewport.clientLeft, y: e.clientY - r.top - viewport.clientTop };
        }

        function pinchInfo() {
            const [a, b] = [...touches.values()];
            return { mid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }, dist: Math.hypot(a.x - b.x, a.y - b.y) || 1 };
        }

        function startPinch() {
            const { mid, dist } = pinchInfo();
            pinch = { s0: z.scale, tx0: z.tx, ty0: z.ty, mid0: mid, d0: dist };
        }

        // Middle mouse button: drag to move around while zoomed in (never draws)
        let pan = null;
        viewport.addEventListener('pointerdown', (e) => {
            if (e.pointerType !== 'mouse' || e.button !== 1) return;
            e.stopPropagation();
            e.preventDefault();
            if (z.scale <= 1) return;
            pan = { id: e.pointerId, x: e.clientX, y: e.clientY, tx: z.tx, ty: z.ty };
            try { viewport.setPointerCapture(e.pointerId); } catch (err) { /* ignore */ }
            viewport.classList.add('panning');
        }, true);
        viewport.addEventListener('pointermove', (e) => {
            if (!pan || e.pointerId !== pan.id) return;
            e.stopPropagation();
            e.preventDefault();
            z.tx = pan.tx + (e.clientX - pan.x);
            z.ty = pan.ty + (e.clientY - pan.y);
            apply();
        }, true);
        const endPan = (e) => {
            if (!pan || e.pointerId !== pan.id) return;
            e.stopPropagation();
            pan = null;
            viewport.classList.remove('panning');
        };
        viewport.addEventListener('pointerup', endPan, true);
        viewport.addEventListener('pointercancel', endPan, true);
        // no auto-scroll circle / paste-on-middle-click in the drawing area
        viewport.addEventListener('mousedown', (e) => { if (e.button === 1) e.preventDefault(); });
        viewport.addEventListener('auxclick', (e) => { if (e.button === 1) e.preventDefault(); });

        // capture phase: runs before the canvas' own drawing handlers
        viewport.addEventListener('pointerdown', (e) => {
            if (e.pointerType !== 'touch') return;
            touches.set(e.pointerId, local(e));
            if (touches.size >= 2) {
                e.stopPropagation();
                e.preventDefault();
                if (!lock && onPinchStart) onPinchStart();
                lock = true;
                startPinch();
            } else if (lock) {
                e.stopPropagation();
            }
        }, true);

        viewport.addEventListener('pointermove', (e) => {
            if (!touches.has(e.pointerId)) return;
            touches.set(e.pointerId, local(e));
            if (!lock) return;
            e.stopPropagation();
            e.preventDefault();
            if (pinch && touches.size >= 2) {
                const { mid, dist } = pinchInfo();
                const s = Math.min(MAX, Math.max(1, pinch.s0 * dist / pinch.d0));
                const px = (pinch.mid0.x - pinch.tx0) / pinch.s0;
                const py = (pinch.mid0.y - pinch.ty0) / pinch.s0;
                z.scale = s;
                z.tx = mid.x - px * s;
                z.ty = mid.y - py * s;
                apply();
            }
        }, true);

        const up = (e) => {
            if (!touches.has(e.pointerId)) return;
            touches.delete(e.pointerId);
            if (lock) e.stopPropagation();
            if (touches.size >= 2) startPinch();
            else pinch = null;
            if (touches.size === 0) lock = false;
        };
        viewport.addEventListener('pointerup', up, true);
        viewport.addEventListener('pointercancel', up, true);

        viewport.addEventListener('wheel', (e) => {
            const rz = rightZoomWheel(e);
            if (e.ctrlKey || e.metaKey || rz) {
                // Ctrl + scroll (or right button + scroll): zoom in small steps
                e.preventDefault();
                if (rz && onPinchStart) onPinchStart(); // no erasing while zooming with the right button
                const p = local(e);
                const s0 = z.scale;
                const s = Math.min(MAX, Math.max(1, s0 * wheelZoomFactor(e)));
                z.tx = p.x - (p.x - z.tx) * (s / s0);
                z.ty = p.y - (p.y - z.ty) * (s / s0);
                z.scale = s;
                apply();
            } else if (e.shiftKey) {
                // Shift + scroll: move the canvas sideways
                e.preventDefault();
                z.tx -= wheelSideways(e);
                apply();
            }
            // plain scrolling is left alone: it moves the page up and down
        }, { passive: false });

        if (resetBtn) resetBtn.addEventListener('click', () => { z.scale = 1; apply(); });

        return { apply, reset() { z.scale = 1; apply(); }, isPinching: () => lock };
    }

    // Sharper canvases (and still sharp-ish when zoomed in)
    function backingScale(cssWidth) {
        const k = Math.min(3, Math.max(1.5, (window.devicePixelRatio || 1) * 1.5));
        return Math.min(k, 3200 / Math.max(1, cssWidth));
    }

    /* ---------------- tekenen ---------------- */

    const canvas = $('drawCanvas');
    const ctx = canvas.getContext('2d');
    const drawContainer = $('drawContainer');

    function fitCanvas() {
        if (!drawContainer || drawContainer.style.display === 'none') return;
        const cssW = Math.max(200, drawContainer.clientWidth);
        const width = Math.round(cssW * backingScale(cssW));
        const height = Math.round(width * 0.75);
        if (canvas.width !== width || canvas.height !== height) {
            canvas.width = width;
            canvas.height = height;
            renderStrokes();
        }
        drawZoom.apply();
    }

    // A pinch started while the first finger was drawing: drop that stroke
    function cancelStroke() {
        if (rightErase) {
            // right button held to zoom: undo what its first click erased
            rightErase = false;
            if (eraseSaved && drawUndo.length) { restoreDrawSnapshot(drawUndo.pop()); eraseSaved = false; flushEditor(); }
        }
        if (state.draw.drawing && state.draw.current) {
            const i = state.draw.strokes.indexOf(state.draw.current);
            if (i >= 0) state.draw.strokes.splice(i, 1);
        }
        state.draw.drawing = false;
        state.draw.current = null;
        renderStrokes();
    }
    const drawZoom = createZoom(drawContainer, canvas, cancelStroke);

    function renderStrokes() {
        const note = noteById(state.currentNoteId);
        ctx.save();
        ctx.setTransform(1, 0, 0, 1, 0, 0);
        ctx.fillStyle = note ? (note.canvasBackground || DEFAULT_CANVAS_BG) : DEFAULT_CANVAS_BG;
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.restore();
        state.draw.strokes.forEach((item) => drawStroke(item));
        drawSelectionFrame();
        drawPickFrame();
        animateDrawGifs();
        updateDrawCtx();
    }

    // draws one item; by default on the drawing itself, or on another canvas (g, size W×H, shifted by ox/oy)
    function drawStroke(stroke, g = ctx, W = canvas.width, H = canvas.height, ox = 0, oy = 0) {
        if (stroke.image && stroke.video) {
            // a video: its current frame (it plays muted, over and over); the first frame until it is ready
            const v = drawVideoEl(stroke.video);
            if (v.readyState >= 2 && v.videoWidth) { g.drawImage(v, ox + stroke.x * W, oy + stroke.y * H, stroke.w * W, stroke.h * H); return; }
        }
        if (stroke.image) {
            const frame = gifFrame(stroke.image);
            if (frame) { g.drawImage(frame, ox + stroke.x * W, oy + stroke.y * H, stroke.w * W, stroke.h * H); return; }
            const img = drawImageEl(stroke.image);
            if (img instanceof HTMLCanvasElement || (img.complete && img.naturalWidth)) {
                g.drawImage(img, ox + stroke.x * W, oy + stroke.y * H, stroke.w * W, stroke.h * H);
            }
            return;
        }
        if (!stroke.points.length) return;
        g.strokeStyle = stroke.color || '#000000';
        g.lineWidth = Math.max(1, (stroke.thicknessRatio || 0.006) * W);
        g.lineCap = 'round';
        g.lineJoin = 'round';
        g.beginPath();
        const first = stroke.points[0];
        g.moveTo(ox + first.x * W, oy + first.y * H);
        for (let i = 1; i < stroke.points.length; i++) {
            const p = stroke.points[i];
            g.lineTo(ox + p.x * W, oy + p.y * H);
        }
        if (stroke.points.length === 1) {
            g.lineTo(ox + first.x * W + 0.01, oy + first.y * H);
        }
        g.stroke();
    }

    /* ---------------- bucket fill (like in Paint) ----------------
     * Fills the closed area you click on with the current colour. The filled area becomes
     * its own item (a picture with see-through edges), so it can be selected, moved and undone. */
    function bucketFill(pos) {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const W = canvas.width, H = canvas.height;
        // a clean picture of the drawing (no selection frames)
        const off = document.createElement('canvas');
        off.width = W; off.height = H;
        const g = off.getContext('2d', { willReadFrequently: true });
        g.fillStyle = note.canvasBackground || DEFAULT_CANVAS_BG;
        g.fillRect(0, 0, W, H);
        state.draw.strokes.forEach((s) => drawStroke(s, g, W, H));
        const data = g.getImageData(0, 0, W, H).data;
        const sx = Math.min(W - 1, Math.max(0, Math.floor(pos.x * W)));
        const sy = Math.min(H - 1, Math.max(0, Math.floor(pos.y * H)));
        const i0 = (sy * W + sx) * 4;
        const r0 = data[i0], g0 = data[i0 + 1], b0 = data[i0 + 2];
        const fill = parseInt(state.draw.color.slice(1), 16);
        const fr = (fill >> 16) & 255, fg = (fill >> 8) & 255, fb = fill & 255;
        if (Math.abs(fr - r0) + Math.abs(fg - g0) + Math.abs(fb - b0) < 6) return; // already that colour
        const TOL = 70; // how different a pixel may be and still count as "the same area"
        const same = (p) => { const k = p * 4; return Math.abs(data[k] - r0) + Math.abs(data[k + 1] - g0) + Math.abs(data[k + 2] - b0) <= TOL; };
        const mask = new Uint8Array(W * H);
        const stack = [sy * W + sx];
        let x0 = sx, x1 = sx, y0 = sy, y1 = sy, count = 0;
        // scan-line flood fill
        while (stack.length) {
            const p = stack.pop();
            const y = (p / W) | 0;
            let x = p - y * W;
            while (x > 0 && !mask[y * W + x - 1] && same(y * W + x - 1)) x--;
            let up = false, down = false;
            for (; x < W && !mask[y * W + x] && same(y * W + x); x++) {
                const q = y * W + x;
                mask[q] = 1; count++;
                if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y;
                if (y > 0) { const a = q - W; if (!mask[a] && same(a)) { if (!up) { stack.push(a); up = true; } } else up = false; }
                if (y < H - 1) { const b = q + W; if (!mask[b] && same(b)) { if (!down) { stack.push(b); down = true; } } else down = false; }
            }
        }
        if (!count) return;
        // grow the area 2 px so it slides under the soft edges of the lines (no light seams)
        const pad = 2;
        x0 = Math.max(0, x0 - pad); y0 = Math.max(0, y0 - pad); x1 = Math.min(W - 1, x1 + pad); y1 = Math.min(H - 1, y1 + pad);
        const bw = x1 - x0 + 1, bh = y1 - y0 + 1;
        const out = document.createElement('canvas');
        out.width = bw; out.height = bh;
        const og = out.getContext('2d');
        const img = og.createImageData(bw, bh);
        const px = img.data;
        for (let y = y0; y <= y1; y++) {
            for (let x = x0; x <= x1; x++) {
                let on = mask[y * W + x];
                for (let d = 1; !on && d <= pad; d++) {
                    on = (x >= d && mask[y * W + x - d]) || (x + d < W && mask[y * W + x + d])
                        || (y >= d && mask[(y - d) * W + x]) || (y + d < H && mask[(y + d) * W + x]);
                }
                if (!on) continue;
                const k = ((y - y0) * bw + (x - x0)) * 4;
                px[k] = fr; px[k + 1] = fg; px[k + 2] = fb; px[k + 3] = 255;
            }
        }
        og.putImageData(img, 0, 0);
        const item = { image: out.toDataURL('image/png'), x: x0 / W, y: y0 / H, w: bw / W, h: bh / H, points: [], fill: true };
        drawImageCache.set(item.image, out); // draw it right away (no waiting for the picture to load)
        pushDrawUndo();
        // fills go above earlier fills but below the lines drawn after them
        let at = 0;
        state.draw.strokes.forEach((s, i) => { if (s.fill) at = i + 1; });
        state.draw.strokes.splice(at, 0, item);
        renderStrokes();
        flushEditor();
    }

    /* ---------------- select tool: pick parts of the drawing, move / copy / delete them ---------------- */
    let drawPick = [];     // the selected items
    let marquee = null;    // { a, b } while dragging a selection box
    let pickDrag = null;   // while moving the selection
    let drawClipboard = null;
    const COPY_MARK = '[Remmin drawing selection]';

    function cloneItem(s) {
        if (s.image) return { ...s };
        return { color: s.color, thicknessRatio: s.thicknessRatio, points: s.points.map((p) => ({ x: p.x, y: p.y })) };
    }
    function itemBox(s) {
        if (s.image) return { x0: s.x, y0: s.y, x1: s.x + s.w, y1: s.y + s.h };
        const r = (s.thicknessRatio || 0) / 2;
        const ry = r * canvas.width / (canvas.height || 1);
        let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
        s.points.forEach((p) => { x0 = Math.min(x0, p.x); y0 = Math.min(y0, p.y); x1 = Math.max(x1, p.x); y1 = Math.max(y1, p.y); });
        return { x0: x0 - r, y0: y0 - ry, x1: x1 + r, y1: y1 + ry };
    }
    function pickBox() {
        if (!drawPick.length) return null;
        return drawPick.map(itemBox).reduce((a, b) => ({
            x0: Math.min(a.x0, b.x0), y0: Math.min(a.y0, b.y0), x1: Math.max(a.x1, b.x1), y1: Math.max(a.y1, b.y1)
        }));
    }
    // the top-most item under a point (pen lines count when you are close to them)
    function itemAt(pos) {
        const tol = 10 / (canvas.clientWidth || 1);
        for (let i = state.draw.strokes.length - 1; i >= 0; i--) {
            const s = state.draw.strokes[i];
            if (s.image) {
                if (pos.x >= s.x && pos.x <= s.x + s.w && pos.y >= s.y && pos.y <= s.y + s.h) return s;
            } else {
                const r = (s.thicknessRatio || 0) / 2 + tol;
                if (s.points.some((p) => Math.hypot(p.x - pos.x, (p.y - pos.y) * canvas.height / canvas.width) <= r)) return s;
            }
        }
        return null;
    }

    function updatePickBar() {
        const show = state.draw.tool === 'select' && drawPick.length > 0;
        $('pickActions').hidden = !show;
        if (show) $('pickCount').textContent = drawPick.length === 1 ? '1 selected' : `${drawPick.length} selected`;
        updateDrawCtx();
    }

    function drawPickFrame() {
        drawPick = drawPick.filter((s) => state.draw.strokes.includes(s));
        const W = canvas.width, H = canvas.height;
        const px = W / (canvas.clientWidth || W);
        ctx.save();
        ctx.lineWidth = 1.5 * px;
        ctx.setLineDash([6 * px, 4 * px]);
        const box = pickBox();
        if (box) {
            const pad = 6 * px;
            ctx.strokeStyle = '#7c6ff0';
            ctx.fillStyle = 'rgba(124,111,240,.08)';
            ctx.fillRect(box.x0 * W - pad, box.y0 * H - pad, (box.x1 - box.x0) * W + 2 * pad, (box.y1 - box.y0) * H + 2 * pad);
            ctx.strokeRect(box.x0 * W - pad, box.y0 * H - pad, (box.x1 - box.x0) * W + 2 * pad, (box.y1 - box.y0) * H + 2 * pad);
            // handle at the bottom-right corner: drag it to make the selection bigger / smaller
            if (!marquee) {
                const hs = 14 * px, cx = box.x1 * W + pad, cy = box.y1 * H + pad;
                ctx.setLineDash([]);
                ctx.fillStyle = '#7c6ff0';
                ctx.fillRect(cx - hs / 2, cy - hs / 2, hs, hs);
                ctx.fillStyle = '#ffffff';
                ctx.fillRect(cx - hs / 4, cy - hs / 4, hs / 2, hs / 2);
            }
        }
        if (marquee) {
            ctx.strokeStyle = '#a5b4fc';
            ctx.fillStyle = 'rgba(165,180,252,.12)';
            const x = Math.min(marquee.a.x, marquee.b.x) * W, y = Math.min(marquee.a.y, marquee.b.y) * H;
            const w = Math.abs(marquee.b.x - marquee.a.x) * W, h = Math.abs(marquee.b.y - marquee.a.y) * H;
            ctx.fillRect(x, y, w, h);
            ctx.strokeRect(x, y, w, h);
        }
        ctx.restore();
        updatePickBar();
    }

    // is the pointer on the resize handle (bottom-right corner of the selection)?
    function onScaleHandle(pos, box) {
        if (!box) return false;
        const cw = canvas.clientWidth || 1, ch = canvas.clientHeight || 1;
        const cx = box.x1 + 6 / cw, cy = box.y1 + 6 / ch;
        return Math.abs(pos.x - cx) * cw <= 12 && Math.abs(pos.y - cy) * ch <= 12;
    }

    function startPick(e) {
        const pos = pointerPos(e, true);
        try { canvas.setPointerCapture?.(e.pointerId); } catch (err) { /* ignore */ }
        const box = pickBox();
        if (onScaleHandle(pos, box)) {
            // resize: change copies, so Ctrl+Z gets the old size back
            pushDrawUndo();
            const copies = new Map(drawPick.map((s) => [s, cloneItem(s)]));
            state.draw.strokes = state.draw.strokes.map((s) => copies.get(s) || s);
            drawPick = [...copies.values()];
            pickDrag = { scale: true, start: pos, box, orig: drawPick.map(cloneItem), moved: false };
            canvas.style.cursor = 'nwse-resize';
            return;
        }
        if (e.shiftKey) {
            // Shift + click: add or remove one thing · Shift + drag: add everything in the box
            const hit = itemAt(pos);
            if (hit) {
                drawPick = drawPick.includes(hit) ? drawPick.filter((s) => s !== hit) : [...drawPick, hit];
                renderStrokes();
                return;
            }
            marquee = { a: pos, b: pos, base: drawPick.slice() };
            renderStrokes();
            return;
        }
        const pad = 6 / (canvas.clientWidth || 1);
        const inBox = box && pos.x >= box.x0 - pad && pos.x <= box.x1 + pad && pos.y >= box.y0 - pad && pos.y <= box.y1 + pad;
        const hit = inBox ? null : itemAt(pos);
        if (!inBox && hit) drawPick = [hit]; // press on an item: select it and move it right away
        if (inBox || hit) {
            // move: change copies, so Ctrl+Z gets the old place back
            pushDrawUndo();
            const copies = new Map(drawPick.map((s) => [s, cloneItem(s)]));
            state.draw.strokes = state.draw.strokes.map((s) => copies.get(s) || s);
            drawPick = [...copies.values()];
            pickDrag = { start: pos, orig: drawPick.map(cloneItem), moved: false };
            canvas.style.cursor = 'move';
            renderStrokes();
            return;
        }
        drawPick = [];
        marquee = { a: pos, b: pos };
        renderStrokes();
    }

    function movePick(e) {
        const pos = pointerPos(e, true);
        if (pickDrag && pickDrag.scale) {
            e.preventDefault();
            const { box, start, orig } = pickDrag;
            const bw = Math.max(box.x1 - box.x0, 0.001), bh = Math.max(box.y1 - box.y0, 0.001);
            // same factor for width and height (keeps the shape); never smaller than 3% of the canvas
            let f = Math.max((bw + pos.x - start.x) / bw, (bh + pos.y - start.y) / bh);
            f = Math.max(f, 0.03 / Math.max(bw, bh));
            if (Math.abs(f - 1) > 0.002) pickDrag.moved = true;
            const ax = box.x0, ay = box.y0; // the top-left corner stays put
            drawPick.forEach((s, i) => {
                const o = orig[i];
                if (s.image) {
                    s.x = ax + (o.x - ax) * f; s.y = ay + (o.y - ay) * f; s.w = o.w * f; s.h = o.h * f;
                } else {
                    s.thicknessRatio = o.thicknessRatio * f;
                    s.points.forEach((p, j) => { p.x = ax + (o.points[j].x - ax) * f; p.y = ay + (o.points[j].y - ay) * f; });
                }
            });
            renderStrokes();
            return;
        }
        if (pickDrag) {
            e.preventDefault();
            const dx = pos.x - pickDrag.start.x, dy = pos.y - pickDrag.start.y;
            if (Math.abs(dx) + Math.abs(dy) > 0.001) pickDrag.moved = true;
            drawPick.forEach((s, i) => {
                const o = pickDrag.orig[i];
                if (s.image) { s.x = o.x + dx; s.y = o.y + dy; }
                else s.points.forEach((p, j) => { p.x = o.points[j].x + dx; p.y = o.points[j].y + dy; });
            });
            renderStrokes();
            return;
        }
        if (marquee) {
            e.preventDefault();
            marquee.b = pos;
            renderStrokes();
            return;
        }
        // hover: show where you can grab
        const box = pickBox();
        if (onScaleHandle(pos, box)) { canvas.style.cursor = 'nwse-resize'; return; }
        const inBox = box && pos.x >= box.x0 && pos.x <= box.x1 && pos.y >= box.y0 && pos.y <= box.y1;
        canvas.style.cursor = inBox || itemAt(pos) ? 'move' : 'default';
    }

    function endPick() {
        if (!pickDrag && !marquee) return; // e.g. after a Shift + click
        if (pickDrag) {
            const moved = pickDrag.moved;
            pickDrag = null;
            if (!moved) drawUndo.pop(); // just a click: nothing changed
            renderStrokes();
            if (moved) flushEditor();
            return;
        }
        const x0 = Math.min(marquee.a.x, marquee.b.x), x1 = Math.max(marquee.a.x, marquee.b.x);
        const y0 = Math.min(marquee.a.y, marquee.b.y), y1 = Math.max(marquee.a.y, marquee.b.y);
        const base = marquee.base || [];
        marquee = null;
        if (x1 - x0 < 0.004 && y1 - y0 < 0.004) { drawPick = base; renderStrokes(); return; }
        // everything that touches the box (plus what was already selected, with Shift)
        const inside = state.draw.strokes.filter((s) => {
            if (s.image) return s.x < x1 && s.x + s.w > x0 && s.y < y1 && s.y + s.h > y0;
            return s.points.some((p) => p.x >= x0 && p.x <= x1 && p.y >= y0 && p.y <= y1);
        });
        drawPick = [...new Set([...base, ...inside])];
        renderStrokes();
    }

    function deletePick() {
        if (!drawPick.length) return false;
        pushDrawUndo();
        const gone = new Set(drawPick);
        state.draw.strokes = state.draw.strokes.filter((s) => !gone.has(s));
        drawPick = [];
        renderStrokes();
        flushEditor();
        return true;
    }
    // put copies of the items in the drawing, a little to the right and down, and select them
    function placeCopies(items, offset = 0.03) {
        if (!items || !items.length) return;
        pushDrawUndo();
        const copies = items.map((s) => {
            const c = cloneItem(s);
            if (c.image) { c.x += offset; c.y += offset; }
            else c.points.forEach((p) => { p.x += offset; p.y += offset; });
            return c;
        });
        state.draw.strokes = state.draw.strokes.concat(copies);
        drawPick = copies;
        if (state.draw.tool !== 'select') setTool('select');
        renderStrokes();
        flushEditor();
    }
    function duplicatePick() { placeCopies(drawPick); }

    // Ctrl+C / Ctrl+X: keep a copy of the selection (and mark the clipboard so Ctrl+V pastes it)
    function drawPickActive() {
        if (state.prefs.mode !== 'draw' || state.prefs.activeTab === 'calendar' || !state.currentNoteId) return false;
        if (document.querySelector('.modal.active')) return false;
        const t = document.activeElement && document.activeElement.closest && document.activeElement.closest('input, textarea, [contenteditable="true"]');
        return !(t && t.offsetParent !== null);
    }
    // the selected part of the drawing as a picture (PNG), for pasting into the text
    function drawPickPng() {
        const box = pickBox();
        if (!box) return null;
        const note = noteById(state.currentNoteId);
        const W = canvas.width, H = canvas.height;
        const pad = 8;
        let x0 = Math.floor(box.x0 * W) - pad, y0 = Math.floor(box.y0 * H) - pad;
        let bw = Math.ceil((box.x1 - box.x0) * W) + 2 * pad, bh = Math.ceil((box.y1 - box.y0) * H) + 2 * pad;
        const scale = Math.min(1, 1600 / Math.max(bw, bh));
        const out = document.createElement('canvas');
        out.width = Math.max(1, Math.round(bw * scale)); out.height = Math.max(1, Math.round(bh * scale));
        const g = out.getContext('2d');
        g.fillStyle = (note && note.canvasBackground) || DEFAULT_CANVAS_BG;
        g.fillRect(0, 0, out.width, out.height);
        drawPick.forEach((s) => drawStroke(s, g, W * scale, H * scale, -x0 * scale, -y0 * scale));
        return out.toDataURL('image/png');
    }
    const copyImgHtml = (src) => (src ? `<img data-remmin-copy="1" src="${src}">` : '');

    // the picture / video that is being placed (no select tool): Ctrl+C copies it
    ['copy', 'cut'].forEach((type) => document.addEventListener(type, (e) => {
        if (!drawPickActive() || (state.draw.tool === 'select' && drawPick.length) || !drawSel || !state.draw.strokes.includes(drawSel) || !drawSel.image) return;
        e.preventDefault();
        writeClipItems(e, [drawSel.video ? { kind: 'video', src: drawSel.video, loop: drawSel.loop !== false } : { kind: 'image', src: drawSel.image }]);
        if (!drawSel.video) { const src = drawSel.image; setTimeout(() => copyPictureSrc(src, true), 0); }
        if (type === 'cut') { pushDrawUndo(); state.draw.strokes = state.draw.strokes.filter((st) => st !== drawSel); drawSel = null; renderStrokes(); flushEditor(); }
        toast(`${drawSel && drawSel.video ? 'Video' : 'Picture'} ${type === 'cut' ? 'cut' : 'copied'}: paste with Ctrl+V in Text, Collage or Draw`);
    }));
    ['copy', 'cut'].forEach((type) => document.addEventListener(type, (e) => {
        if (!drawPickActive() || state.draw.tool !== 'select' || !drawPick.length) return;
        e.preventDefault();
        drawClipboard = drawPick.map(cloneItem);
        if (e.clipboardData) {
            e.clipboardData.setData('text/plain', COPY_MARK);
            const png = drawPickPng();
            e.clipboardData.setData('text/html', copyImgHtml(png)); // pastes as a picture in the text
            // only pictures / videos picked: they go to Text and Collage as they are (videos keep playing)
            const onlyMedia = drawPick.every((st) => st.image);
            const media = drawPick.filter((st) => st.image).map((st) => (st.video ? { kind: 'video', src: st.video, loop: st.loop !== false } : { kind: 'image', src: st.image }));
            const items = onlyMedia ? media : [{ kind: 'image', src: png }, ...media.filter((m) => m.kind === 'video')];
            if (items.length) {
                e.clipboardData.setData(ITEMS_TYPE, JSON.stringify(items.map(cleanClipItem).filter(Boolean)));
            }
        }
        if (type === 'cut') deletePick();
        else toast('Copied — Ctrl+V pastes it here, or as a picture in the text');
    }));

    function pointerPos(e, unclamped) {
        const rect = canvas.getBoundingClientRect();
        const x = (e.clientX - rect.left) / rect.width;
        const y = (e.clientY - rect.top) / rect.height;
        if (unclamped) return { x, y };
        return { x: Math.min(1, Math.max(0, x)), y: Math.min(1, Math.max(0, y)) };
    }

    /* ---------------- animated GIFs in the drawing ----------------
     * A canvas only shows the first picture of a GIF, so GIFs in Draw are decoded here into
     * their frames, and the drawing is redrawn while one is on screen. */
    function decodeGif(bytes) {
        let p = 0;
        const u8 = () => bytes[p++];
        const u16 = () => { const v = bytes[p] | (bytes[p + 1] << 8); p += 2; return v; };
        if (String.fromCharCode(...bytes.subarray(0, 3)) !== 'GIF') return null;
        p = 6;
        const W = u16(), H = u16(), flags = u8();
        p += 2; // background colour, aspect
        if (!W || !H || W * H > 4e6) return null;
        const readTable = (n) => { const t = bytes.subarray(p, p + n * 3); p += n * 3; return t; };
        const gct = flags & 0x80 ? readTable(1 << ((flags & 7) + 1)) : null;
        const frames = [];
        const screen = new Uint8ClampedArray(W * H * 4);
        let gce = { disposal: 0, delay: 100, trans: -1 };
        let budget = 160e6; // bytes of frames we are willing to keep
        const subBlocks = () => {
            const parts = [];
            let size = 0, n;
            while ((n = u8())) { parts.push(bytes.subarray(p, p + n)); size += n; p += n; }
            const out = new Uint8Array(size);
            let o = 0;
            parts.forEach((b) => { out.set(b, o); o += b.length; });
            return out;
        };
        while (p < bytes.length) {
            const block = u8();
            if (block === 0x3b) break; // end
            if (block === 0x21) {
                const label = u8();
                if (label === 0xf9) {
                    u8(); // size 4
                    const f = u8();
                    const delay = u16();
                    const ti = u8();
                    u8(); // terminator
                    gce = { disposal: (f >> 2) & 7, delay: Math.max(20, delay * 10 || 100), trans: f & 1 ? ti : -1 };
                } else subBlocks();
                continue;
            }
            if (block !== 0x2c) break; // unknown: stop here
            const fx = u16(), fy = u16(), fw = u16(), fh = u16(), ff = u8();
            const table = ff & 0x80 ? readTable(1 << ((ff & 7) + 1)) : gct;
            const interlaced = !!(ff & 0x40);
            const minCode = u8();
            const data = subBlocks();
            if (!table) { gce = { disposal: 0, delay: 100, trans: -1 }; continue; }
            // LZW
            const pixels = new Uint8Array(fw * fh);
            const clear = 1 << minCode, eoi = clear + 1;
            let size = minCode + 1, mask = (1 << size) - 1, next = eoi + 1;
            const prefix = new Uint16Array(4096), suffix = new Uint8Array(4096), stack = new Uint8Array(4097);
            for (let i = 0; i < clear; i++) suffix[i] = i;
            let bits = 0, acc = 0, di = 0, out = 0, prev = -1, first = 0;
            while (out < pixels.length) {
                while (bits < size && di < data.length) { acc |= data[di++] << bits; bits += 8; }
                if (bits < size) break;
                let code = acc & mask;
                acc >>>= size; bits -= size;
                if (code === clear) { size = minCode + 1; mask = (1 << size) - 1; next = eoi + 1; prev = -1; continue; }
                if (code === eoi) break;
                let sp = 0, c = code;
                if (prev === -1) { pixels[out++] = suffix[code]; prev = code; first = code; continue; }
                if (code >= next) { stack[sp++] = first; c = prev; }
                while (c >= clear) { stack[sp++] = suffix[c]; c = prefix[c]; }
                first = suffix[c];
                stack[sp++] = first;
                while (sp && out < pixels.length) pixels[out++] = stack[--sp];
                if (next < 4096) {
                    prefix[next] = prev; suffix[next] = first; next++;
                    if (next > mask && size < 12) { size++; mask = (1 << size) - 1; }
                }
                prev = code;
            }
            // put the frame on the screen
            const before = gce.disposal === 3 ? screen.slice() : null;
            const rowOf = (r) => {
                if (!interlaced) return r;
                const passes = [[0, 8], [4, 8], [2, 4], [1, 2]];
                let n = r;
                for (const [start, step] of passes) {
                    const count = Math.ceil((fh - start) / step);
                    if (n < count) return start + n * step;
                    n -= count;
                }
                return r;
            };
            for (let r = 0; r < fh; r++) {
                const y = fy + rowOf(r);
                if (y >= H) continue;
                for (let x = 0; x < fw; x++) {
                    const idx = pixels[r * fw + x];
                    if (idx === gce.trans || fx + x >= W) continue;
                    const o = (y * W + fx + x) * 4, t = idx * 3;
                    screen[o] = table[t]; screen[o + 1] = table[t + 1]; screen[o + 2] = table[t + 2]; screen[o + 3] = 255;
                }
            }
            const c = document.createElement('canvas');
            c.width = W; c.height = H;
            c.getContext('2d').putImageData(new ImageData(screen.slice(), W, H), 0, 0);
            frames.push({ canvas: c, delay: gce.delay });
            budget -= W * H * 4;
            if (budget < 0 || frames.length >= 400) break;
            if (gce.disposal === 2) {
                for (let r = fy; r < Math.min(H, fy + fh); r++) screen.fill(0, (r * W + fx) * 4, (r * W + Math.min(W, fx + fw)) * 4);
            } else if (before) screen.set(before);
            gce = { disposal: 0, delay: 100, trans: -1 };
        }
        if (frames.length < 2) return null; // not animated: the normal picture is fine
        const total = frames.reduce((a, f) => a + f.delay, 0);
        return { frames, total, width: W, height: H };
    }
    const gifAnims = new Map(); // src → decoded frames (or null when it is not an animation)
    function gifFrame(src) {
        if (!/^data:image\/gif;base64,/.test(src)) return null;
        if (!gifAnims.has(src)) {
            let anim = null;
            try {
                const bin = atob(src.slice(src.indexOf(',') + 1));
                const bytes = new Uint8Array(bin.length);
                for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
                anim = decodeGif(bytes);
            } catch (err) { anim = null; }
            gifAnims.set(src, anim);
        }
        const anim = gifAnims.get(src);
        if (!anim) return null;
        let t = performance.now() % anim.total;
        for (const f of anim.frames) { if (t < f.delay) return f.canvas; t -= f.delay; }
        return anim.frames[0].canvas;
    }
    // keep redrawing while an animated GIF is on the drawing
    let gifTimer = null;
    function animateDrawGifs() {
        clearTimeout(gifTimer);
        if (typeof dvcUpdate === 'function' && (state.prefs.mode !== 'draw' || state.prefs.activeTab === 'calendar')) dvcUpdate();
        const showing = state.prefs.mode === 'draw' && state.prefs.activeTab !== 'calendar' && !document.hidden;
        // videos only play while they are on the shown drawing
        const onDrawing = showing ? state.draw.strokes.filter((s) => s.video) : [];
        const playing = new Set(onDrawing.map((s) => s.video).filter((r) => !drawVideoPaused.has(r)));
        drawVideoCache.forEach((v, src) => {
            if (!playing.has(src)) { if (!v.paused && !v.classList.contains('draw-video-full')) v.pause(); return; }
            const items = onDrawing.filter((s) => s.video === src);
            const sound = items.some((s) => !s.muted);
            v.loop = items.some((s) => s.loop !== false);
            const vol = Math.max(0, Math.min(1, typeof items[0].volume === 'number' ? items[0].volume : 1));
            if (Math.abs(v.volume - vol) > 0.001) v.volume = vol;
            if ((v.paused && !v.ended) || (sound && v.muted && !drawNeedsUnmute) || (!sound && !v.muted)) playDrawVideo(v, sound);
        });
        dvcUpdate();
        if (!showing) return;
        if (!onDrawing.length && !state.draw.strokes.some((s) => s.image && gifAnims.get(s.image))) return;
        gifTimer = setTimeout(() => { if (!state.draw.drawing) renderStrokes(); else animateDrawGifs(); }, playing.size ? 33 : 60);
    }
    document.addEventListener('visibilitychange', () => { if (!document.hidden) animateDrawGifs(); });

    /* ---------------- pictures in the drawing ---------------- */
    const drawImageCache = new Map();
    let drawSel = null;      // the picture that is being placed (move / resize)
    let drawSelDrag = null;

    function drawImageEl(src) {
        let img = drawImageCache.get(src);
        if (!img) {
            img = new Image();
            img.onload = () => renderStrokes();
            img.src = src;
            drawImageCache.set(src, img);
        }
        return img;
    }

    // size of the corner handle, as part of the canvas width / height
    const handleSize = () => ({ x: 18 / (canvas.clientWidth || 1), y: 18 / (canvas.clientHeight || 1) });

    function drawSelectionFrame() {
        if (!drawSel || !state.draw.strokes.includes(drawSel)) { drawSel = null; return; }
        const W = canvas.width, H = canvas.height;
        const px = W / (canvas.clientWidth || W);
        const x = drawSel.x * W, y = drawSel.y * H, w = drawSel.w * W, h = drawSel.h * H;
        ctx.save();
        ctx.lineWidth = 2 * px;
        ctx.setLineDash([6 * px, 4 * px]);
        ctx.strokeStyle = '#7c6ff0';
        ctx.strokeRect(x, y, w, h);
        ctx.setLineDash([]);
        const hs = handleSize();
        ctx.fillStyle = '#7c6ff0';
        ctx.fillRect(x + w - hs.x * W / 2, y + h - hs.y * H / 2, hs.x * W, hs.y * H);
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(x + w - hs.x * W / 4, y + h - hs.y * H / 4, hs.x * W / 2, hs.y * H / 2);
        ctx.restore();
    }

    function selectionHit(pos) {
        if (!drawSel) return null;
        const hs = handleSize();
        const cx = drawSel.x + drawSel.w, cy = drawSel.y + drawSel.h;
        if (Math.abs(pos.x - cx) <= hs.x * 0.9 && Math.abs(pos.y - cy) <= hs.y * 0.9) return 'resize';
        if (pos.x >= drawSel.x && pos.x <= cx && pos.y >= drawSel.y && pos.y <= cy) return 'move';
        return null;
    }

    function dragSelection(e) {
        e.preventDefault();
        const pos = pointerPos(e, true);
        const { mode, start, orig } = drawSelDrag;
        const dx = pos.x - start.x, dy = pos.y - start.y;
        if (mode === 'move') {
            // keep at least a bit of the picture on the canvas
            drawSel.x = Math.min(0.95, Math.max(0.05 - orig.w, orig.x + dx));
            drawSel.y = Math.min(0.95, Math.max(0.05 - orig.h, orig.y + dy));
        } else {
            // resize from the corner, keeping the picture's shape
            const ratio = orig.h / orig.w;
            const grow = Math.max(dx, dy / ratio);
            drawSel.w = Math.max(0.04, orig.w + grow);
            drawSel.h = drawSel.w * ratio;
        }
        renderStrokes();
    }

    // the top-most picture under a point
    function pictureAt(pos) {
        for (let i = state.draw.strokes.length - 1; i >= 0; i--) {
            const s = state.draw.strokes[i];
            if (s.image && pos.x >= s.x && pos.x <= s.x + s.w && pos.y >= s.y && pos.y <= s.y + s.h) return s;
        }
        return null;
    }

    /* ---------------- videos in the drawing ----------------
     * A video item is a picture (its first frame, so moving / resizing / grids all work)
     * with the video itself in item.video; while the drawing is shown it plays muted, on repeat. */
    const drawVideoCache = new Map(); // "media:<id>" → <video>
    const drawVideoPaused = new Set();  // videos paused with the menu
    let drawNeedsUnmute = false;
    function drawVideoEl(ref) {
        let v = drawVideoCache.get(ref);
        if (!v) {
            v = document.createElement('video');
            v.loop = true; v.playsInline = true; v.preload = 'auto';
            v.addEventListener('loadeddata', () => { renderStrokes(); }, { once: true });
            mediaUrl(ref).then((url) => { v.src = url; }).catch(() => {});
            drawVideoCache.set(ref, v);
        }
        return v;
    }
    function dropDrawVideo(ref) {
        const v = drawVideoCache.get(ref);
        if (v) { v.pause(); v.removeAttribute('src'); drawVideoCache.delete(ref); }
        if (state.prefs.mode === 'draw') renderStrokes();
    }
    // with sound: the browser only allows that after a click on the page; until then it plays muted
    function playDrawVideo(v, wantSound) {
        v.muted = !wantSound;
        v.play().catch(() => {
            if (!wantSound) return;
            v.muted = true;
            drawNeedsUnmute = true;
            v.play().catch(() => {});
        });
    }
    document.addEventListener('pointerdown', () => {
        if (!drawNeedsUnmute) return;
        drawNeedsUnmute = false;
        animateDrawGifs();
    }, true);
    // the first frame of a video as a picture (and its size)
    function videoPoster(url) {
        return new Promise((resolve, reject) => {
            const v = document.createElement('video');
            v.muted = true; v.playsInline = true; v.preload = 'auto';
            const fail = () => reject(new Error('no video'));
            v.addEventListener('error', fail, { once: true });
            v.addEventListener('loadeddata', () => { v.currentTime = Math.min(0.1, (v.duration || 1) / 2); }, { once: true });
            v.addEventListener('seeked', () => {
                const c = document.createElement('canvas');
                const scale = Math.min(1, 960 / (v.videoWidth || 1));
                c.width = Math.max(1, Math.round(v.videoWidth * scale)); c.height = Math.max(1, Math.round(v.videoHeight * scale));
                c.getContext('2d').drawImage(v, 0, 0, c.width, c.height);
                resolve({ image: c.toDataURL('image/jpeg', 0.8), width: v.videoWidth, height: v.videoHeight });
            }, { once: true });
            v.src = url;
        });
    }
    async function addDrawVideos(files, at) {
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        addDrawVideoRefs(await readVideoFiles(files), at);
    }
    async function addDrawVideoRefs(refs, at) {
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        let added = null;
        for (const [i, ref] of refs.entries()) {
            if (!isMediaRef(ref)) { toast('YouTube / Vimeo videos cannot go in a drawing, only video files'); continue; }
            let poster;
            try { poster = await videoPoster(await mediaUrl(ref)); } catch (err) { toast('This video cannot be played in the browser'); continue; }
            const W = canvas.clientWidth || canvas.width, H = canvas.clientHeight || canvas.height;
            let w = Math.min(0.6, poster.width / W);
            let h = w * W * (poster.height / poster.width) / H;
            if (h > 0.8) { w *= 0.8 / h; h = 0.8; }
            const cx = at ? at.x : 0.5, cy = at ? at.y : 0.5;
            const item = {
                image: poster.image, video: ref,
                x: Math.min(1 - w, Math.max(0, cx - w / 2)) + i * 0.03,
                y: Math.min(1 - h, Math.max(0, cy - h / 2)) + i * 0.03,
                w, h, points: []
            };
            if (!added) pushDrawUndo();
            state.draw.strokes.push(item);
            added = item;
        }
        if (!added) return;
        drawSel = added;
        renderStrokes();
        flushEditor();
        toast('Video added: it plays on repeat. Right-click it for sound, pause and more');
    }
    /* ---- the controls of a video in the drawing (shown when the mouse is on it) ---- */
    const dvc = document.createElement('div');
    dvc.className = 'draw-video-controls';
    dvc.hidden = true;
    dvc.innerHTML = '<button type="button" data-a="play" title="Play / pause">⏸</button>' +
        '<input type="range" class="dvc-seek" min="0" max="1000" value="0" title="Go to">' +
        '<span class="dvc-time">0:00 / 0:00</span>' +
        '<button type="button" data-a="loop" title="Loop: play again and again">🔁</button>' +
        '<button type="button" data-a="mute" title="Sound on / off">🔊</button>' +
        '<input type="range" class="dvc-vol" min="0" max="100" value="100" title="Volume">' +
        '<button type="button" data-a="full" title="Full screen">⛶</button>';
    document.body.appendChild(dvc);
    let dvcItem = null, dvcHideTimer = null, dvcSeeking = false;
    state.dvcReady = true;
    function dvcShow(item) {
        clearTimeout(dvcHideTimer);
        dvcItem = item;
        dvc.hidden = false;
        dvcUpdate();
    }
    function dvcHideSoon() {
        clearTimeout(dvcHideTimer);
        dvcHideTimer = setTimeout(() => { if (!dvc.matches(':hover') && !dvcSeeking) { dvc.hidden = true; dvcItem = null; } }, 1200);
    }
    function dvcUpdate() {
        if (!state.dvcReady) return;
        if (!dvcItem) { dvc.hidden = true; return; }
        if (state.prefs.mode !== 'draw' || state.prefs.activeTab === 'calendar' || !state.draw.strokes.includes(dvcItem) || drawContainer.offsetParent === null) {
            dvc.hidden = true; dvcItem = null; return;
        }
        const r = canvas.getBoundingClientRect();
        const w = Math.max(300, dvcItem.w * r.width);
        const left = Math.max(4, Math.min(window.innerWidth - w - 4, r.left + dvcItem.x * r.width));
        const top = Math.min(window.innerHeight - 44, r.top + (dvcItem.y + dvcItem.h) * r.height - 40);
        dvc.style.left = `${left}px`;
        dvc.style.top = `${Math.max(4, top)}px`;
        dvc.style.width = `${w}px`;
        const v = drawVideoEl(dvcItem.video);
        const paused = drawVideoPaused.has(dvcItem.video) || (v.ended && !v.loop);
        dvc.querySelector('[data-a="play"]').textContent = paused ? '▶' : '⏸';
        if (!dvcSeeking && v.duration) dvc.querySelector('.dvc-seek').value = String(Math.round(v.currentTime / v.duration * 1000));
        dvc.querySelector('.dvc-time').textContent = `${formatSeconds(v.currentTime || 0)} / ${formatSeconds(v.duration || 0)}`;
        dvc.querySelector('[data-a="loop"]').classList.toggle('on', dvcItem.loop !== false);
        dvc.querySelector('[data-a="mute"]').textContent = dvcItem.muted ? '🔇' : '🔊';
        const vol = dvc.querySelector('.dvc-vol');
        if (document.activeElement !== vol) vol.value = String(Math.round((typeof dvcItem.volume === 'number' ? dvcItem.volume : 1) * 100));
    }
    drawContainer.addEventListener('pointermove', (e) => {
        if (state.prefs.mode !== 'draw' || state.draw.drawing) return;
        const pic = pictureAt(pointerPos(e, true));
        if (pic && pic.video) dvcShow(pic); else if (dvcItem) dvcHideSoon();
    });
    drawContainer.addEventListener('pointerdown', (e) => {
        if (e.pointerType === 'mouse' || state.prefs.mode !== 'draw') return;
        const pic = pictureAt(pointerPos(e, true));
        if (pic && pic.video) dvcShow(pic); // touch: tap the video to see its controls
    });
    drawContainer.addEventListener('pointerleave', () => { if (dvcItem) dvcHideSoon(); });
    dvc.addEventListener('pointerenter', () => clearTimeout(dvcHideTimer));
    dvc.addEventListener('pointerleave', () => dvcHideSoon());
    dvc.addEventListener('click', (e) => {
        const b = e.target.closest('button[data-a]');
        if (!b || !dvcItem) return;
        const item = dvcItem, ref = item.video, v = drawVideoEl(ref);
        if (b.dataset.a === 'play') {
            if (drawVideoPaused.has(ref) || v.ended) {
                drawVideoPaused.delete(ref);
                if (v.ended) v.currentTime = 0;
                playDrawVideo(v, !item.muted);
            } else { drawVideoPaused.add(ref); v.pause(); }
        } else if (b.dataset.a === 'loop') {
            item.loop = item.loop === false;
            if (item.loop === true) delete item.loop;
            flushEditor();
        } else if (b.dataset.a === 'mute') {
            item.muted = !item.muted;
            if (!item.muted) delete item.muted;
            flushEditor();
        } else if (b.dataset.a === 'full') {
            v.classList.add('draw-video-full');
            v.controls = true;
            document.body.appendChild(v);
            const back = () => {
                if (document.fullscreenElement === v) return;
                document.removeEventListener('fullscreenchange', back);
                v.controls = false;
                v.classList.remove('draw-video-full');
                v.remove();
            };
            document.addEventListener('fullscreenchange', back);
            (v.requestFullscreen ? v.requestFullscreen() : Promise.reject()).catch(() => { back(); toast('Full screen is not possible here'); });
            if (v.paused) playDrawVideo(v, !item.muted);
        }
        animateDrawGifs();
        dvcUpdate();
    });
    const dvcSeek = dvc.querySelector('.dvc-seek');
    dvcSeek.addEventListener('pointerdown', () => { dvcSeeking = true; });
    dvcSeek.addEventListener('input', () => {
        if (!dvcItem) return;
        const v = drawVideoEl(dvcItem.video);
        if (v.duration) v.currentTime = Number(dvcSeek.value) / 1000 * v.duration;
        renderStrokes();
    });
    dvcSeek.addEventListener('change', () => { dvcSeeking = false; });
    document.addEventListener('pointerup', () => { dvcSeeking = false; });
    const dvcVol = dvc.querySelector('.dvc-vol');
    dvcVol.addEventListener('input', () => {
        if (!dvcItem) return;
        const vol = Number(dvcVol.value) / 100;
        if (vol >= 1) delete dvcItem.volume; else dvcItem.volume = vol;
        drawVideoEl(dvcItem.video).volume = vol;
        if (vol > 0 && dvcItem.muted) delete dvcItem.muted;
    });
    dvcVol.addEventListener('change', () => { flushEditor(); animateDrawGifs(); });

    // right-click on a video in the drawing: its options
    function drawPicActions(pic, x, y) {
        return pictureMenuActions(pic.image, {
            x, y,
            place: (src) => { if (!state.draw.strokes.includes(pic)) return; pushDrawUndo(); pic.image = src; renderStrokes(); flushEditor(); },
            remove: () => { pushDrawUndo(); state.draw.strokes = state.draw.strokes.filter((st) => st !== pic); if (drawSel === pic) drawSel = null; drawPick = drawPick.filter((st) => st !== pic); updatePickBar(); renderStrokes(); flushEditor(); }
        });
    }
    function drawVideoMenu(item, x, y) { menuAt(x, y, 'Video', drawVideoActions(item)); }
    function drawVideoActions(item) {
        const v = drawVideoEl(item.video);
        return videoMenuActions(item.video, {
            playing: { on: !drawVideoPaused.has(item.video), toggle: () => { if (drawVideoPaused.has(item.video)) drawVideoPaused.delete(item.video); else { drawVideoPaused.add(item.video); v.pause(); } animateDrawGifs(); } },
            sound: { on: !item.muted, toggle: () => { pushDrawUndo(); item.muted = !item.muted; flushEditor(); animateDrawGifs(); } },
            loop: { on: item.loop !== false, toggle: () => { pushDrawUndo(); if (item.loop === false) delete item.loop; else item.loop = false; flushEditor(); animateDrawGifs(); } },
            remove: () => {
                pushDrawUndo();
                state.draw.strokes = state.draw.strokes.filter((s) => s !== item);
                if (drawSel === item) drawSel = null;
                drawPick = drawPick.filter((s) => s !== item);
                updatePickBar();
                renderStrokes();
                flushEditor();
            }
        });
    }
    // the options of the selected picture / video (or the selection) in the drawing, right above it
    var drawCtx = null;
    function drawRectOf(x0, y0, x1, y1) {
        const r = canvas.getBoundingClientRect();
        if (!r.width) return null;
        const left = r.left + x0 * r.width, top = r.top + y0 * r.height, right = r.left + x1 * r.width, bottom = r.top + y1 * r.height;
        return { left, top, right, bottom, width: right - left, height: bottom - top };
    }
    function updateDrawCtx() {
        if (!drawCtx) return;
        if (state.prefs.mode !== 'draw') { drawCtx.hide(); return; }
        const mediaActions = (s) => (s.video ? drawVideoActions(s) : drawPicActions(s, null, null));
        if (drawSel && state.draw.strokes.includes(drawSel) && drawSel.image) {
            const s = drawSel;
            drawCtx.show({ key: s, anchor: () => drawRectOf(s.x, s.y, s.x + s.w, s.y + s.h), busy: () => !!drawSelDrag, after: updateDrawCtx, actions: () => mediaActions(s) });
        } else if (state.draw.tool === 'select' && drawPick.length) {
            const one = drawPick.length === 1 && drawPick[0].image ? drawPick[0] : null;
            drawCtx.show({
                key: one || `pick:${drawPick.length}`,
                anchor: () => { const b = pickBox(); return b && drawRectOf(b.x0, b.y0, b.x1, b.y1); },
                busy: () => !!pickDrag || !!marquee,
                groups: [$('pickActions')],
                after: updateDrawCtx,
                actions: () => (one ? mediaActions(one).filter((a) => !a.danger) : [])
            });
        } else drawCtx.hide();
    }

    async function addDrawImages(files, at) {
        const note = noteById(state.currentNoteId);
        if (!note || !files.length || isPrivateLocked(note)) return;
        let added = null;
        for (const [i, file] of files.entries()) {
            let src;
            try { src = await pictureFileToStorable(file); } catch (err) { toast('Could not read that picture'); continue; }
            if (!src) continue;
            const img = drawImageEl(src);
            if (!img.complete || !img.naturalWidth) {
                await new Promise((resolve) => { img.addEventListener('load', resolve, { once: true }); img.addEventListener('error', resolve, { once: true }); });
            }
            if (!img.naturalWidth) { toast('Could not read that picture'); continue; }
            const W = canvas.clientWidth || canvas.width, H = canvas.clientHeight || canvas.height;
            // at most 60% of the width and 80% of the height, never bigger than the picture itself
            let w = Math.min(0.6, img.naturalWidth / W);
            let h = w * W * (img.naturalHeight / img.naturalWidth) / H;
            if (h > 0.8) { w *= 0.8 / h; h = 0.8; }
            const cx = at ? at.x : 0.5, cy = at ? at.y : 0.5;
            const item = {
                image: src,
                x: Math.min(1 - w, Math.max(0, cx - w / 2)) + i * 0.03,
                y: Math.min(1 - h, Math.max(0, cy - h / 2)) + i * 0.03,
                w, h, points: []
            };
            if (!added) pushDrawUndo();
            state.draw.strokes.push(item);
            added = item;
        }
        if (!added) return;
        drawSel = added;
        renderStrokes();
        flushEditor();
        toast('Picture added: drag it to move, drag the corner to resize, click outside when done');
    }

    function deleteSelectedPicture() {
        if (!drawSel) return false;
        pushDrawUndo();
        state.draw.strokes = state.draw.strokes.filter((s) => s !== drawSel);
        drawSel = null;
        renderStrokes();
        flushEditor();
        return true;
    }

    // Double-click a picture to move / resize / delete it again
    canvas.addEventListener('dblclick', (e) => {
        if (state.prefs.mode !== 'draw') return;
        const pic = pictureAt(pointerPos(e, true));
        if (!pic) return;
        // the double-click also drew a dot: take that away again
        const last = state.draw.strokes[state.draw.strokes.length - 1];
        if (last && !last.image && last.points.length <= 2) state.draw.strokes.pop();
        drawSel = pic;
        renderStrokes();
        flushEditor();
    });

    // Ctrl+V a picture while the drawing is open
    document.addEventListener('paste', (e) => {
        if (state.prefs.mode !== 'draw' || state.prefs.activeTab === 'calendar' || !state.currentNoteId) return;
        if (document.querySelector('.modal.active')) return;
        const typing = e.target.closest && e.target.closest('input, textarea, [contenteditable="true"], [contenteditable=""]');
        if (typing && typing.offsetParent !== null) return;
        // pictures / videos copied with Ctrl+C in Text or Collage (a copy made in the drawing itself is pasted as is)
        const plainMark = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
        if (!(plainMark === COPY_MARK && drawClipboard)) {
            const clipItems = readClipItems(e, true);
            if (clipItems.length && !isPrivateLocked(noteById(state.currentNoteId))) {
                e.preventDefault();
                const pics = clipItems.filter((it) => it.kind === 'image').map((it) => it.src);
                const vids = clipItems.filter((it) => it.kind === 'video').map((it) => it.src);
                if (clipItems.some((it) => it.kind === 'audio')) toast('Audio can go in Text and Collage (not in a drawing)');
                (async () => { if (pics.length) await addDrawImages(pics); if (vids.length) await addDrawVideoRefs(vids); })();
                return;
            }
        }
        const copiedVid = pastedVideoRef(e);
        if (copiedVid) { e.preventDefault(); addDrawVideoRefs([copiedVid]); return; }
        if (pastedAudio(e)) { e.preventDefault(); toast('Audio can go in Text and Collage (not in a drawing)'); return; }
        const ownPics = ownPictureCopy(e);
        if (ownPics.length && !isPrivateLocked(noteById(state.currentNoteId))) { e.preventDefault(); addDrawImages(ownPics); return; }
        const files = [...((e.clipboardData && e.clipboardData.files) || [])].filter((f) => f.type.startsWith('image/'));
        if (!files.length) {
            // pieces copied with the select tool (they also carry a picture: use the real lines instead)
            const mark = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
            if (drawClipboard && mark === COPY_MARK && !isPrivateLocked(noteById(state.currentNoteId))) {
                e.preventDefault();
                placeCopies(drawClipboard);
                return;
            }
            // a picture copied from the text part of a note, from the grid, or from a web page
            const html = e.clipboardData ? e.clipboardData.getData('text/html') : '';
            const pics = (mark === PIC_MARK || mark === CB_MARK) ? pastedPictureSrcs(html) : [];
            if (pics.length && !isPrivateLocked(noteById(state.currentNoteId))) { e.preventDefault(); addDrawImages(pics); return; }
            const m = html && /<img[^>]+src="(data:image\/[^"]+)"/i.exec(html);
            if (m && !isPrivateLocked(noteById(state.currentNoteId))) {
                e.preventDefault();
                fetch(m[1].replace(/&quot;/g, '"')).then((r) => r.blob()).then((blob) => {
                    const type = blob.type || 'image/png';
                    addDrawImages([new File([blob], `picture.${type.split('/')[1] || 'png'}`, { type })]);
                }).catch(() => toast('Could not paste that picture'));
                return;
            }
            // pieces copied with the select tool
            const text = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
            if (drawClipboard && text === COPY_MARK && !isPrivateLocked(noteById(state.currentNoteId))) {
                e.preventDefault();
                placeCopies(drawClipboard);
            }
            return;
        }
        e.preventDefault();
        addDrawImages(files);
    });

    // Drop picture files on the drawing
    drawContainer.addEventListener('dragover', (e) => {
        if (Array.from(e.dataTransfer.types || []).includes('Files')) e.preventDefault();
    });
    drawContainer.addEventListener('drop', (e) => {
        const files = [...(e.dataTransfer.files || [])].filter((f) => f.type.startsWith('image/'));
        const videos = [...(e.dataTransfer.files || [])].filter(isVideoFile);
        if (!files.length && !videos.length) return;
        e.preventDefault();
        if (videos.length) addDrawVideos(videos, pointerPos(e));
        if (files.length) addDrawImages(files, pointerPos(e));
    });

    // the right mouse button (and the eraser end of a pen) always erases, whatever tool is chosen
    const isEraseButton = (e) => (e.pointerType === 'mouse' && e.button === 2) || (e.pointerType === 'pen' && e.button === 5);
    let rightErase = false;
    const drawToolNow = () => (rightErase ? 'eraser' : state.draw.tool);

    function startStroke(e) {
        if (state.prefs.mode !== 'draw' || drawZoom.isPinching()) return;
        e.preventDefault();
        const note = noteById(state.currentNoteId);
        if (!note) return;
        if (e.pointerType === 'mouse' && e.button === 2) {
            const pic = pictureAt(pointerPos(e, true));
            if (pic && pic.video) { drawVideoMenu(pic, e.clientX, e.clientY); return; }
            if (pic && pic.image) { menuAt(e.clientX, e.clientY, 'Picture', drawPicActions(pic, e.clientX, e.clientY)); return; }
        }
        if (isEraseButton(e)) {
            if (state.draw.drawing || drawSelDrag || pickDrag || marquee) return;
            rightErase = true;
            try { canvas.setPointerCapture?.(e.pointerId); } catch (err) { /* ignore */ }
            state.draw.drawing = true;
            eraseSaved = false;
            eraseAt(pointerPos(e));
            return;
        }
        if (e.button > 0) return;

        // A selected picture: drag it to move, drag the corner to resize, click elsewhere when done
        if (drawSel) {
            const pos = pointerPos(e, true);
            const hit = selectionHit(pos);
            if (hit) {
                try { canvas.setPointerCapture?.(e.pointerId); } catch (err) { /* ignore */ }
                // keep the old version for Ctrl+Z: change a copy of the picture
                pushDrawUndo();
                const copy = { ...drawSel };
                state.draw.strokes = state.draw.strokes.map((s) => (s === drawSel ? copy : s));
                drawSel = copy;
                drawSelDrag = { mode: hit, start: pos, orig: { x: drawSel.x, y: drawSel.y, w: drawSel.w, h: drawSel.h } };
                return;
            }
            drawSel = null;
            renderStrokes();
            return;
        }

        if (state.draw.tool === 'select') {
            startPick(e);
            return;
        }

        if (state.draw.tool === 'fill') {
            bucketFill(pointerPos(e));
            return;
        }

        const pos = pointerPos(e);
        if (state.draw.tool === 'eraser') {
            state.draw.drawing = true;
            eraseSaved = false;
            eraseAt(pos);
            return;
        }

        try {
            canvas.setPointerCapture?.(e.pointerId);
        } catch (err) {
            /* niet elke browser staat capture toe voor synthetische events */
        }
        state.draw.current = {
            color: state.draw.color,
            thicknessRatio: state.draw.size / (canvas.clientWidth || canvas.width),
            points: [pos]
        };
        pushDrawUndo();
        state.draw.strokes.push(state.draw.current);
        state.draw.drawing = true;
        renderStrokes();
    }

    function moveStroke(e) {
        if (drawSelDrag) { dragSelection(e); return; }
        if (drawToolNow() === 'select') { movePick(e); return; }
        if (drawSel && !state.draw.drawing) {
            const hit = selectionHit(pointerPos(e, true));
            canvas.style.cursor = hit === 'resize' ? 'nwse-resize' : hit === 'move' ? 'move' : '';
        }
        if (!state.draw.drawing) return;
        e.preventDefault();
        const pos = pointerPos(e);
        if (drawToolNow() === 'eraser') {
            eraseAt(pos);
            return;
        }
        const current = state.draw.current;
        if (!current) return;
        const last = current.points[current.points.length - 1];
        if (last && Math.hypot(pos.x - last.x, pos.y - last.y) < 0.0015) return;
        current.points.push(pos);
        // only draw the new piece instead of repainting everything
        ctx.strokeStyle = current.color;
        ctx.lineWidth = Math.max(1, current.thicknessRatio * canvas.width);
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.beginPath();
        ctx.moveTo(last.x * canvas.width, last.y * canvas.height);
        ctx.lineTo(pos.x * canvas.width, pos.y * canvas.height);
        ctx.stroke();
    }

    function endStroke() {
        if (rightErase) { rightErase = false; state.draw.drawing = false; flushEditor(); return; }
        if (drawSelDrag) { drawSelDrag = null; flushEditor(); return; }
        if (pickDrag || marquee) { endPick(); return; }
        if (!state.draw.drawing) return;
        const cur = state.draw.current;
        state.draw.drawing = false;
        state.draw.current = null;
        // a click (no drag) on a picture or video: select it, so its options show above it
        if (cur && cur.points && state.draw.tool === 'pen' && cur.points.length <= 2) {
            const p0 = cur.points[0], p1 = cur.points[cur.points.length - 1];
            const pic = Math.hypot(p1.x - p0.x, p1.y - p0.y) < 0.004 ? pictureAt(p0) : null;
            if (pic) {
                state.draw.strokes = state.draw.strokes.filter((st) => st !== cur);
                if (drawUndo.length) drawUndo.pop();
                drawSel = pic;
                renderStrokes();
                return;
            }
        }
        flushEditor();
    }

    let eraseSaved = false;
    function eraseAt(pos) {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const radius = Math.max(0.006, (state.draw.size / 2) / (canvas.clientWidth || canvas.width));
        const before = state.draw.strokes;
        const after = before.filter((stroke) =>
            !stroke.points.some((p) => Math.hypot(p.x - pos.x, p.y - pos.y) <= radius)
        );
        if (after.length !== before.length) {
            if (!eraseSaved) { pushDrawUndo(); eraseSaved = true; }
            state.draw.strokes = after;
            renderStrokes();
            flushEditor();
        }
    }

    canvas.addEventListener('pointerdown', startStroke);
    canvas.addEventListener('contextmenu', (e) => e.preventDefault()); // right button = eraser
    canvas.addEventListener('pointermove', moveStroke);
    canvas.addEventListener('pointerup', endStroke);
    canvas.addEventListener('pointercancel', endStroke);
    window.addEventListener('resize', fitCanvas);

    const BUCKET_CURSOR = `url("data:image/svg+xml,${encodeURIComponent('<svg xmlns=\'http://www.w3.org/2000/svg\' width=\'24\' height=\'24\'><path d=\'M5 11l7-7 7 7-7 7z\' fill=\'white\' stroke=\'black\' stroke-width=\'1.5\'/><path d=\'M19 14c1.5 2 2 3 2 4a2 2 0 0 1-4 0c0-1 .5-2 2-4z\' fill=\'%23e11d1d\' stroke=\'black\'/></svg>').replace(/%2523/g, '%23')}") 2 20, copy`;
    function setTool(tool) {
        state.draw.tool = tool;
        if (tool !== 'select' && drawPick.length) { drawPick = []; renderStrokes(); }
        updatePickBar();
        $('toolSelect').classList.toggle('active', tool === 'select');
        $('toolPen').classList.toggle('active', tool === 'pen');
        $('toolEraser').classList.toggle('active', tool === 'eraser');
        $('toolFill').classList.toggle('active', tool === 'fill');
        canvas.style.cursor = tool === 'eraser' ? 'cell' : tool === 'fill' ? BUCKET_CURSOR : tool === 'select' ? 'default' : 'crosshair';
    }

    $('toolImage').addEventListener('click', () => $('drawImageFile').click());
    $('drawImageFile').addEventListener('change', (e) => {
        const files = [...e.target.files].filter((f) => f.type.startsWith('image/'));
        e.target.value = '';
        addDrawImages(files);
    });
    $('toolPen').addEventListener('click', () => setTool('pen'));
    $('toolSelect').addEventListener('click', () => setTool('select'));
    $('pickCopy').addEventListener('click', () => duplicatePick());
    drawCtx = makeCtxBar($('drawToolbar'), [$('pickActions')]);
    addMenu($('drawAddBtn'), [['Picture', '🖼', 'toolImage'], ['GIF', '🎞', 'toolGif']]);
    $('pickDelete').addEventListener('click', () => deletePick());
    $('toolEraser').addEventListener('click', () => setTool('eraser'));
    // Bucket: click an area of the drawing to fill it with the current colour
    $('toolFill').addEventListener('click', () => setTool('fill'));

    $('toolUndo').addEventListener('click', () => {
        let prev = drawUndo.pop();
        // nothing remembered (e.g. after a reload): take away the last item like before
        if (!prev) {
            if (!state.draw.strokes.length) return;
            prev = state.draw.strokes.slice(0, -1);
        }
        drawRedo.push(drawSnapshot());
        restoreDrawSnapshot(prev);
        drawSel = null;
        renderStrokes();
        flushEditor();
    });

    // Redo (Ctrl+Y): bring back what was undone last
    function redoDraw() {
        const next = drawRedo.pop();
        if (!next) return;
        drawUndo.push(drawSnapshot());
        restoreDrawSnapshot(next);
        drawSel = null;
        renderStrokes();
        flushEditor();
    }

    $('toolClear').addEventListener('click', async () => {
        if (!state.draw.strokes.length) return;
        if (!(await askConfirm('Clear drawing', 'Clear the entire drawing?', 'Clear'))) return;
        pushDrawUndo();
        state.draw.strokes = [];
        drawPick = [];
        renderStrokes();
        flushEditor();
    });

    $('brushSize').addEventListener('input', () => {
        state.draw.size = Number($('brushSize').value);
        $('brushSizeLabel').textContent = state.draw.size;
    });

    /* ---- the windows of a note (Text, Collage, Draw, Project): a new note has none, + adds them ---- */
    const ALL_WINDOWS = ['text', 'collage', 'draw', 'project'];
    const WINDOW_INFO = { text: ['Text', '📝'], collage: ['Collage', '🧩'], draw: ['Draw', '✏️'], project: ['Project', '📋'] };
    // older notes (from before windows existed) have all four
    const noteWindows = (note) => (note && Array.isArray(note.windows) ? ALL_WINDOWS.filter((w) => note.windows.includes(w)) : ALL_WINDOWS.slice());
    function updateWindowTabs(note) {
        const wins = noteWindows(note);
        const show = (id, w) => { $(id).hidden = !wins.includes(w); };
        show('modeText', 'text'); show('modeCollage', 'collage'); show('modeDraw', 'draw'); show('modeProject', 'project');
        $('addWindowBtn').hidden = !note || wins.length === ALL_WINDOWS.length;
        $('addWindowBtn').classList.toggle('big', !!note && !wins.length);
    }
    function addWindow(w) {
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        const wins = noteWindows(note);
        if (wins.includes(w)) { setMode(w); return; }
        note.windows = ALL_WINDOWS.filter((x) => x === w || wins.includes(x));
        touch(note);
        saveNotes();
        updateWindowTabs(note);
        setMode(w);
    }
    function removeWindow(w) {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        note.windows = noteWindows(note).filter((x) => x !== w);
        touch(note);
        saveNotes();
        updateWindowTabs(note);
        setMode(note.windows[0] || w);
        toast(`${WINDOW_INFO[w][0]} window removed (what was in it stays saved: add the window again to see it)`);
    }
    $('addWindowBtn').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const wins = noteWindows(note);
        showMenu($('addWindowBtn'), 'Add a window', ALL_WINDOWS.filter((w) => !wins.includes(w))
            .map((w) => ({ label: WINDOW_INFO[w][0], icon: WINDOW_INFO[w][1], run: () => addWindow(w) })));
    });
    // right-click a window tab: remove it from this note
    ['modeText', 'modeCollage', 'modeDraw', 'modeProject'].forEach((id) => $(id).addEventListener('contextmenu', (e) => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        e.preventDefault();
        const w = id.slice(4).toLowerCase();
        menuAt(e.clientX, e.clientY, WINDOW_INFO[w][0], [{ label: `Remove the ${WINDOW_INFO[w][0]} window from this note`, icon: '✕', danger: true, run: () => removeWindow(w) }]);
    }));

    function setMode(mode) {
        if (!['text', 'collage', 'project', 'draw', 'grid'].includes(mode)) mode = 'text';
        // only the windows this note has (Grid is part of Draw)
        const winNote = noteById(state.currentNoteId);
        const wins = noteWindows(winNote);
        updateWindowTabs(winNote);
        const noWindows = !!winNote && !wins.length;
        if (noWindows) {
            ['collageBoard', 'collageToolbar', 'projectBoard', 'textToolbar', 'drawToolbar', 'gridToolbar', 'zoomHint'].forEach((id) => { $(id).style.display = 'none'; });
            editorEl.style.display = 'none'; drawContainer.style.display = 'none'; gridContainer.style.display = 'none';
            ['modeText', 'modeCollage', 'modeDraw', 'modeProject'].forEach((id) => $(id).classList.remove('active'));
            return;
        }
        if (winNote && !wins.includes(mode === 'grid' ? 'draw' : mode)) mode = wins.includes(state.prefs.mode === 'grid' ? 'draw' : state.prefs.mode) ? state.prefs.mode : wins[0];
        if (typeof placeImgHandle === 'function') requestAnimationFrame(placeImgHandle);
        if (mode !== 'collage' && typeof cbEndEdit === 'function') cbEndEdit(true);
        state.prefs.mode = mode;
        savePrefs();
        $('modeText').classList.toggle('active', mode === 'text');
        $('modeCollage').classList.toggle('active', mode === 'collage');
        $('collageBoard').style.display = mode === 'collage' ? 'block' : 'none';
        $('collageToolbar').style.display = mode === 'collage' ? 'block' : 'none';
        $('modeProject').classList.toggle('active', mode === 'project');
        $('projectBoard').style.display = mode === 'project' ? 'block' : 'none';
        $('modeDraw').classList.toggle('active', mode === 'draw' || mode === 'grid'); // Grid lives inside Draw
        editorEl.style.display = mode === 'text' ? 'block' : 'none';
        $('textToolbar').style.display = mode === 'text' ? 'block' : 'none';
        drawContainer.style.display = mode === 'draw' ? 'block' : 'none';
        $('drawToolbar').style.display = mode === 'draw' ? 'block' : 'none';
        gridContainer.style.display = mode === 'grid' ? 'block' : 'none';
        $('gridToolbar').style.display = mode === 'grid' ? 'block' : 'none';
        $('zoomHint').style.display = mode === 'text' || mode === 'project' ? 'none' : 'block';
        $('zoomHint').textContent = mode === 'collage'
            ? 'Ctrl + scroll to zoom · Shift + scroll to move sideways · drag with the middle mouse button (or hold Space) to move · double-click to add a sticky note · pinch with two fingers on a phone'
            : 'Pinch with two fingers to zoom and move · on a computer: Ctrl + scroll to zoom, Shift + scroll to move sideways, middle mouse button to move';
        if (mode === 'collage') requestAnimationFrame(cbShow);
        if (mode === 'draw') {
            requestAnimationFrame(() => {
                fitCanvas();
                renderStrokes();
            });
        }
        if (mode === 'grid') requestAnimationFrame(fitGrid);
    }

    $('modeText').addEventListener('click', () => setMode('text'));
    $('modeCollage').addEventListener('click', () => setMode('collage'));
    $('modeDraw').addEventListener('click', () => setMode('draw'));
    $('modeGrid').addEventListener('click', () => setMode('grid'));
    $('gridBack').addEventListener('click', () => setMode('draw'));
    $('modeProject').addEventListener('click', () => setMode('project'));

    /* ---------------- Project mode: a board like GitHub Projects ----------------
     * note.project = { columns: [{ id, name, cards: [{ id, text, people: [ids], depts: [ids], fields: { dropdownId: optionId } }] }],
     *                  boxes: [{ id, kind: 'dept' | 'person' | 'select', name, items }] }
     *   dept items: { id, name, color } · person items: { id, name } · select items (dropdowns): { id, name, options: [{ id, name }] }
     *   (older boards had only people + departments: they become the boxes "Departments" and "People")
     * New boards start with To do / In progress / Review / Done (only saved once you change something). */
    const pjBoard = $('projectBoard');
    const pjCols = $('pjColumns');
    const pj = { columns: [], boxes: [], noteId: null, undo: [], redo: [], editing: null, sel: new Set(), openFields: new Set() };
    const pjDefaultBoxes = (depts = [], people = []) => [
        { id: uid(), kind: 'dept', name: 'Departments', items: depts },
        { id: uid(), kind: 'person', name: 'People', items: people }
    ];
    const pjBoxesOf = (kind) => pj.boxes.filter((b) => b.kind === kind);
    const pjAllOf = (kind) => pjBoxesOf(kind).flatMap((b) => b.items);
    const pjBoxOfItem = (id) => pj.boxes.find((b) => b.items.some((x) => x.id === id));
    const PJ_DEFAULT = ['To do', 'In progress', 'Review', 'Done'];
    const pjDefault = () => PJ_DEFAULT.map((name) => ({ id: uid(), name, cards: [] }));
    const pjActive = () => state.prefs.mode === 'project' && state.prefs.activeTab !== 'calendar' && !!pj.noteId &&
        !document.querySelector('.modal.active') && !isPrivateLocked(noteById(pj.noteId));

    function loadProject(note) {
        if (pj.editing && pj.editFinish && pj.noteId) pj.editFinish(true);
        pj.editing = null; pj.editFinish = null;
        pj.noteId = note ? note.id : null;
        const saved = note && note.project ? JSON.parse(JSON.stringify(note.project)) : {};
        pj.columns = Array.isArray(saved.columns) ? saved.columns : pjDefault();
        pj.boxes = Array.isArray(saved.boxes) ? saved.boxes
            : pjDefaultBoxes(Array.isArray(saved.departments) ? saved.departments : [], Array.isArray(saved.people) ? saved.people : []);
        // the open eyes: only the items with these departments / people are shown (none open: everything)
        pj.filter = saved.filter && typeof saved.filter === 'object' ? { depts: saved.filter.depts || [], people: saved.filter.people || [] } : { depts: [], people: [] };
        pj.undo = []; pj.redo = []; pj.editing = null; pj.sel = new Set();
        renderProject();
    }
    function pjSave() {
        const note = noteById(pj.noteId);
        if (!note || isPrivateLocked(note)) return;
        note.project = JSON.parse(JSON.stringify({ columns: pj.columns, boxes: pj.boxes, filter: pj.filter }));
        touch(note);
    }
    const pjState = () => JSON.stringify({ columns: pj.columns, boxes: pj.boxes, filter: pj.filter });
    function pjRestore(text) {
        const v = JSON.parse(text);
        pj.columns = v.columns; pj.boxes = v.boxes || pjDefaultBoxes();
        pj.filter = v.filter || { depts: [], people: [] };
    }
    function pjRecord() {
        pj.undo.push(pjState());
        if (pj.undo.length > 100) pj.undo.shift();
        pj.redo = [];
    }
    function pjUndoRedo(back) {
        if (pj.editing && pj.editFinish) pj.editFinish(false);
        const from = back ? pj.undo : pj.redo, to = back ? pj.redo : pj.undo;
        if (!from.length) return;
        to.push(pjState());
        pjRestore(from.pop());
        pj.editing = null;
        renderProject();
        pjSave();
    }
    const pjCol = (id) => pj.columns.find((c) => c.id === id);
    function pjFindCard(id) {
        for (const col of pj.columns) {
            const i = col.cards.findIndex((c) => c.id === id);
            if (i >= 0) return { col, i, card: col.cards[i] };
        }
        return null;
    }

    // new departments take these Draw / Grid colours in turn
    const DEPT_COLORS = ['#e11d1d', '#3b82f6', '#22c55e', '#f97316', '#8b5cf6', '#facc15', '#14b8a6', '#f472b6', '#22d3ee', '#d946ef'];
    const pjDept = (id) => pjAllOf('dept').find((d) => d.id === id);
    const pjPerson = (id) => pjAllOf('person').find((p) => p.id === id);
    const pjField = (id) => pjAllOf('select').find((f) => f.id === id);
    const initials = (name) => name.trim().split(/\s+/).slice(0, 2).map((w) => w[0]).join('').toUpperCase() || '?';
    function pjCardHtml(card) {
        const depts = (card.depts || []).map(pjDept).filter(Boolean);
        const people = (card.people || []).map(pjPerson).filter(Boolean);
        const fields = Object.keys(card.fields || {}).map(pjField).filter(Boolean);
        const fieldsHtml = fields.length ? `<div class="pj-card-fields">${fields.map((f) => `
                            <span class="pj-field" data-field="${f.id}">
                                <span class="pj-field-name">${escapeHtml(f.name)}</span>
                                <select class="pj-field-select" aria-label="${escapeHtml(f.name)}"><option value="">—</option>${(f.options || []).map((o) =>
                                    `<option value="${o.id}"${card.fields[f.id] === o.id ? ' selected' : ''}>${escapeHtml(o.name)}</option>`).join('')}</select>
                                <button type="button" class="pj-field-del" title="Take ${escapeHtml(f.name)} off this item" aria-label="Remove">✕</button>
                            </span>`).join('')}</div>` : '';
        // one coloured stripe per department along the left edge
        const step = 100 / (depts.length || 1);
        const edge = depts.length ? `<span class="pj-edge" style="background:linear-gradient(to bottom, ${depts.map((d, i) =>
            `${escapeHtml(d.color)} ${(i * step).toFixed(2)}% ${((i + 1) * step).toFixed(2)}%`).join(', ')})"></span>` : '';
        return `
                    <div class="pj-card${depts.length ? ' has-edge' : ''}${pj.sel.has(card.id) ? ' selected' : ''}" data-card="${card.id}">${edge}
                        <span class="pj-grip" title="Drag to move" aria-hidden="true">⠿</span>
                        <div class="pj-card-text">${card.text ? escapeHtml(card.text) : '<span class="pj-untitled">Untitled</span>'}</div>${fieldsHtml}
                        <div class="pj-card-foot">
                            ${depts.map((d) => `<span class="pj-chip" style="background:${escapeHtml(d.color)};color:${textOn(d.color)}">${escapeHtml(d.name)}</span>`).join('')}
                            ${people.map((p) => `<span class="pj-avatar" title="${escapeHtml(p.name)}">${escapeHtml(initials(p.name))}</span>`).join('')}
                            <span class="pj-assign">
                                <button type="button" class="pj-assign-dept" title="Department" aria-label="Department"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"><path d="M3 12V4a1 1 0 0 1 1-1h8l9 9-9 9-9-9z"/><circle cx="7.5" cy="7.5" r="1.6" fill="currentColor"/></svg></button>
                                <button type="button" class="pj-assign-person" title="Assign a person" aria-label="Assign a person">👤</button>
                                <button type="button" class="pj-assign-field" title="Add a dropdown" aria-label="Add a dropdown">▾</button>
                                <button type="button" class="pj-done" title="Save this item (Enter)">Done</button>
                            </span>
                        </div>
                    </div>`;
    }

    // the eye: hide / show all items with this department or person
    const EYE = '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z"/><circle cx="12" cy="12" r="3"/></svg>';
    const EYE_OFF = '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z"/><circle cx="12" cy="12" r="3"/><path d="M3 3l18 18"/></svg>';
    // an open eye = a filter: only the items with that department / person (several open: items with any of them)
    const pjFilterDept = (id) => (pj.filter.depts || []).includes(id);
    const pjFilterPerson = (id) => (pj.filter.people || []).includes(id);
    const pjFiltering = () => !!((pj.filter.depts || []).length || (pj.filter.people || []).length);
    const pjShown = (card) => !pjFiltering() || (card.depts || []).some(pjFilterDept) || (card.people || []).some(pjFilterPerson);
    const eyeBtn = (open, name) => `<button type="button" class="pj-eye${open ? ' on' : ''}" title="${open ? 'Stop showing only' : 'Show only'} the items of ${escapeHtml(name)}" aria-pressed="${open}">${open ? EYE : EYE_OFF}</button>`;

    // the boxes on the right: departments (with a colour), people and dropdowns; add your own, rename them
    const PJ_BOX_KINDS = { dept: ['Tags with a colour (like Departments)', '🏷'], person: ['People', '👤'], select: ['Dropdowns', '▾'] };
    function pjItemRow(box, x) {
        if (box.kind === 'dept') return `
            <div class="pj-side-item" data-dept="${x.id}">
                <button type="button" class="pj-dot" style="background:${escapeHtml(x.color)}" title="Change colour"></button>
                <span class="pj-side-name" title="Double-click to rename · drag onto an item">${escapeHtml(x.name)}</span>
                ${eyeBtn(pjFilterDept(x.id), x.name)}
                <button type="button" class="pj-side-del" title="Remove">✕</button>
            </div>`;
        if (box.kind === 'person') return `
            <div class="pj-side-item" data-person="${x.id}">
                <span class="pj-avatar">${escapeHtml(initials(x.name))}</span>
                <span class="pj-side-name" title="Double-click to rename · drag onto an item">${escapeHtml(x.name)}</span>
                ${eyeBtn(pjFilterPerson(x.id), x.name)}
                <button type="button" class="pj-side-del" title="Remove">✕</button>
            </div>`;
        const open = pj.openFields.has(x.id);
        return `
            <div class="pj-side-item pj-field-row" data-field="${x.id}">
                <button type="button" class="pj-field-toggle" title="${open ? 'Hide' : 'Show'} the options" aria-expanded="${open}">${open ? '▾' : '▸'}</button>
                <span class="pj-side-name" title="Double-click to rename · drag onto an item">${escapeHtml(x.name)}</span>
                <span class="pj-field-count">${(x.options || []).length}</span>
                <button type="button" class="pj-field-rename" title="Rename" aria-label="Rename">✎</button>
                <button type="button" class="pj-side-del" title="Remove">✕</button>
            </div>${open ? `
            <div class="pj-options" data-options-of="${x.id}">
                ${(x.options || []).map((o) => `<div class="pj-option" data-option="${o.id}"><span class="pj-option-name" title="Double-click to rename">${escapeHtml(o.name)}</span><button type="button" class="pj-option-del" title="Remove">✕</button></div>`).join('')}
                <form class="pj-side-form pj-option-form" data-field="${x.id}"><input type="text" maxlength="50" placeholder="+ Add option" autocomplete="off"></form>
            </div>` : ''}`;
    }
    function renderProjectSide() {
        const empty = { dept: 'e.g. Art department', person: 'Add the people who work on this', select: 'e.g. Status (with the options To do, Busy, Ready)' };
        const add = { dept: '+ Add', person: '+ Add person', select: '+ Add dropdown' };
        // keep what is typed in the "+ Add" boxes
        const typed = {};
        $('pjSide').querySelectorAll('form[data-box] input, form.pj-option-form input').forEach((i) => { if (i.value) typed[i.closest('form').dataset.box || `o${i.closest('form').dataset.field}`] = i.value; });
        const focused = document.activeElement && $('pjSide').contains(document.activeElement) && document.activeElement.closest('form');
        const focusKey = focused ? focused.dataset.box || `o${focused.dataset.field}` : null;
        $('pjSide').innerHTML = pj.boxes.map((b) => `
            <div class="pj-box" data-box="${b.id}">
                <div class="pj-box-head">
                    <span class="pj-box-title" title="Double-click to rename">${escapeHtml(b.name)}</span>
                    <button type="button" class="pj-box-menu" title="Box options" aria-label="Box options">⋯</button>
                </div>
                <div class="pj-side-list">${b.items.length ? b.items.map((x) => pjItemRow(b, x)).join('') : `<div class="pj-side-empty">${empty[b.kind]}</div>`}</div>
                <form class="pj-side-form" data-box="${b.id}"><input type="text" maxlength="50" placeholder="${add[b.kind]}" autocomplete="off"></form>
            </div>`).join('') + '<button type="button" class="pj-add-box" title="Add a box (tags, people or dropdowns)">+ Add box</button>';
        $('pjSide').querySelectorAll('form').forEach((f) => {
            const key = f.dataset.box || `o${f.dataset.field}`;
            const input = f.querySelector('input');
            if (typed[key]) input.value = typed[key];
            if (key === focusKey) input.focus();
        });
    }

    // assign a department / person to an item (click again to take it off)
    function pjAssignMenu(anchor, cardId, kind) {
        const f = pjFindCard(cardId);
        if (!f) return;
        const list = pjAllOf(kind);
        const field = kind === 'dept' ? 'depts' : 'people';
        if (!list.length) {
            toast(kind === 'dept' ? 'Add a department in a box on the right first' : 'Add a person in a box on the right first');
            const box = pjBoxesOf(kind)[0];
            const input = box && $('pjSide').querySelector(`form[data-box="${box.id}"] input`);
            if (input) input.focus();
            return;
        }
        const have = new Set(f.card[field] || []);
        showMenu(anchor, kind === 'dept' ? 'Department' : 'Assign', list.map((x) => ({
            label: `${kind === 'dept' && have.has(x.id) ? '✓ ' : ''}${x.name}`,
            icon: kind === 'dept' ? `<span class="pj-dot small" style="background:${escapeHtml(x.color)}"></span>` : (have.has(x.id) ? '✓' : ''),
            run: () => {
                const g = pjFindCard(cardId);
                if (!g) return;
                const editing = pj.editing && pj.editing.kind === 'card' && pj.editing.id === cardId;
                if (!editing) pjRecord();
                const add = !(g.card[field] || []).includes(x.id);
                // with several items selected: give (or take) it to all of them
                const ids = !editing && pj.sel.has(cardId) && pj.sel.size > 1 ? [...pj.sel] : [cardId];
                ids.forEach((cid) => {
                    const h = pjFindCard(cid);
                    if (!h) return;
                    const now = new Set(h.card[field] || []);
                    if (add) now.add(x.id); else now.delete(x.id);
                    h.card[field] = [...now];
                });
                renderProject();           // an item being typed in stays open (and keeps its text)
                if (!editing) pjSave();
                // the little window stays open, so you can pick several (click outside or Done to close)
                setTimeout(() => {
                    const cardEl = $('pjColumns').querySelector(`.pj-card[data-card="${cardId}"]`);
                    const btn = cardEl && cardEl.querySelector(kind === 'dept' ? '.pj-assign-dept' : '.pj-assign-person');
                    if (btn) { cardEl.classList.add('pj-menu-open'); pjAssignMenu(btn, cardId, kind); }
                }, 0);
            }
        })).concat([{ label: 'Done', icon: '✓', run: () => {
            const cardEl = $('pjColumns').querySelector(`.pj-card[data-card="${cardId}"]`);
            if (cardEl) cardEl.classList.remove('pj-menu-open');
        } }]));
    }
    function pjAddSide(boxId, input) {
        const box = pj.boxes.find((b) => b.id === boxId);
        const name = input.value.trim().slice(0, 50);
        if (!box || !name) return;
        pjRecord();
        if (box.kind === 'dept') box.items.push({ id: uid(), name, color: DEPT_COLORS[pjAllOf('dept').length % DEPT_COLORS.length] });
        else if (box.kind === 'select') { const f = { id: uid(), name, options: [] }; box.items.push(f); pj.openFields.add(f.id); }
        else box.items.push({ id: uid(), name });
        input.value = '';
        renderProject();
        pjSave();
        // a new dropdown: type its options right away
        if (box.kind === 'select') { const i = $('pjSide').querySelector(`.pj-option-form[data-field="${box.items[box.items.length - 1].id}"] input`); if (i) i.focus(); }
        else { const i = $('pjSide').querySelector(`form[data-box="${box.id}"] input`); if (i) i.focus(); }
    }
    function pjAddOption(fieldId, input) {
        const f = pjField(fieldId);
        const name = input.value.trim().slice(0, 50);
        if (!f || !name) return;
        pjRecord();
        (f.options = f.options || []).push({ id: uid(), name });
        input.value = '';
        renderProject();
        pjSave();
        const i = $('pjSide').querySelector(`.pj-option-form[data-field="${fieldId}"] input`);
        if (i) i.focus();
    }
    const PJ_KIND_WORD = { dept: 'tag', person: 'person', select: 'dropdown' };
    async function pjRenameSide(kind, id) {
        const item = kind === 'dept' ? pjDept(id) : kind === 'person' ? pjPerson(id) : pjField(id);
        if (!item) return;
        const name = await askDialog({ title: `Rename ${PJ_KIND_WORD[kind]}`, input: item.name, okLabel: 'Save' });
        if (!name || !name.trim() || name.trim() === item.name) return;
        pjRecord();
        item.name = name.trim().slice(0, 50);
        renderProject();
        pjSave();
    }
    async function pjRenameBox(boxId) {
        const box = pj.boxes.find((b) => b.id === boxId);
        if (!box) return;
        const name = await askDialog({ title: 'Rename box', input: box.name, okLabel: 'Save' });
        if (!name || !name.trim() || name.trim() === box.name) return;
        pjRecord();
        box.name = name.trim().slice(0, 40);
        renderProject();
        pjSave();
    }
    async function pjRenameOption(fieldId, optionId) {
        const f = pjField(fieldId);
        const o = f && (f.options || []).find((x) => x.id === optionId);
        if (!o) return;
        const name = await askDialog({ title: `Rename option of ${f.name}`, input: o.name, okLabel: 'Save' });
        if (!name || !name.trim() || name.trim() === o.name) return;
        pjRecord();
        o.name = name.trim().slice(0, 50);
        renderProject();
        pjSave();
    }
    // take these tags / people / dropdowns off every item
    function pjStripFromCards(ids) {
        const gone = new Set(ids);
        pj.columns.forEach((c) => c.cards.forEach((card) => {
            ['depts', 'people'].forEach((k) => { if (card[k]) card[k] = card[k].filter((x) => !gone.has(x)); });
            if (card.fields) { gone.forEach((id) => delete card.fields[id]); if (!Object.keys(card.fields).length) delete card.fields; }
        }));
        pj.filter = { depts: (pj.filter.depts || []).filter((x) => !gone.has(x)), people: (pj.filter.people || []).filter((x) => !gone.has(x)) };
    }
    function pjRemoveSide(kind, id) {
        const box = pjBoxOfItem(id);
        if (!box) return;
        pjRecord();
        box.items = box.items.filter((x) => x.id !== id);
        pjStripFromCards([id]);
        renderProject();
        pjSave();
        toast('Removed (↶ to undo)');
    }
    function pjRemoveOption(fieldId, optionId) {
        const f = pjField(fieldId);
        if (!f) return;
        pjRecord();
        f.options = (f.options || []).filter((o) => o.id !== optionId);
        pj.columns.forEach((c) => c.cards.forEach((card) => { if (card.fields && card.fields[fieldId] === optionId) card.fields[fieldId] = ''; }));
        renderProject();
        pjSave();
    }
    async function pjAddBox() {
        showMenu($('pjSide').querySelector('.pj-add-box'), 'Add a box with', Object.entries(PJ_BOX_KINDS).map(([kind, [label, icon]]) => ({
            label, icon,
            run: async () => {
                const name = await askDialog({ title: 'New box', message: kind === 'select'
                    ? 'A box with dropdowns: e.g. "Status" with the options To do / Busy / Ready. Give a dropdown to an item and choose its option there.'
                    : 'Give the box a name.', input: kind === 'dept' ? 'Tags' : kind === 'person' ? 'People' : 'Dropdowns', okLabel: 'Add' });
                if (!name || !name.trim()) return;
                pjRecord();
                pj.boxes.push({ id: uid(), kind, name: name.trim().slice(0, 40), items: [] });
                renderProject();
                pjSave();
                const forms = $('pjSide').querySelectorAll('form[data-box] input');
                if (forms.length) forms[forms.length - 1].focus();
            }
        })));
    }
    function pjBoxMenu(btn, boxId) {
        const i = pj.boxes.findIndex((b) => b.id === boxId);
        const box = pj.boxes[i];
        if (!box) return;
        const move = (d) => { pjRecord(); const [b] = pj.boxes.splice(i, 1); pj.boxes.splice(i + d, 0, b); renderProject(); pjSave(); };
        const actions = [{ label: 'Rename', icon: '✎', run: () => pjRenameBox(boxId) }];
        if (i > 0) actions.push({ label: 'Move up', icon: '↑', run: () => move(-1) });
        if (i < pj.boxes.length - 1) actions.push({ label: 'Move down', icon: '↓', run: () => move(1) });
        actions.push({ label: 'Delete box', icon: '🗑', danger: true, run: () => {
            pjRecord();
            pj.boxes.splice(i, 1);
            pjStripFromCards(box.items.map((x) => x.id));
            renderProject();
            pjSave();
            toast(`"${box.name}" deleted (↶ to undo)`);
        } });
        showMenu(btn, box.name, actions);
    }
    $('pjSide').addEventListener('submit', (e) => {
        e.preventDefault();
        if (!pjActive()) return;
        const form = e.target.closest('form');
        const input = form && form.querySelector('input');
        if (!input) return;
        if (form.classList.contains('pj-option-form')) pjAddOption(form.dataset.field, input);
        else pjAddSide(form.dataset.box, input);
    });
    $('pjSide').addEventListener('click', (e) => {
        if (!pjActive() || Date.now() - pjTagDroppedAt < 400) return;
        if (e.target.closest('.pj-add-box')) { pjAddBox(); return; }
        const boxMenu = e.target.closest('.pj-box-menu');
        if (boxMenu) { pjBoxMenu(boxMenu, boxMenu.closest('.pj-box').dataset.box); return; }
        const opt = e.target.closest('.pj-option');
        if (opt && e.target.closest('.pj-option-del')) { pjRemoveOption(opt.closest('.pj-options').dataset.optionsOf, opt.dataset.option); return; }
        const row = e.target.closest('.pj-side-item');
        if (!row) return;
        const kind = row.dataset.dept ? 'dept' : row.dataset.person ? 'person' : 'select';
        const id = row.dataset.dept || row.dataset.person || row.dataset.field;
        if (kind === 'select' && e.target.closest('.pj-field-rename')) { pjRenameSide('select', id); return; }
        // the name: double-click renames it (a single click doesn't fold it open / closed)
        if (kind === 'select' && e.target.closest('.pj-side-name')) return;
        if (kind === 'select' && !e.target.closest('.pj-side-del')) {
            if (pj.openFields.has(id)) pj.openFields.delete(id); else pj.openFields.add(id);
            renderProjectSide();
            return;
        }
        if (e.target.closest('.pj-side-del')) { pjRemoveSide(kind, id); return; }
        if (e.target.closest('.pj-eye')) {
            if (pj.editing && pj.editFinish) pj.editFinish(true);
            const key = kind === 'dept' ? 'depts' : 'people';
            const list = new Set(pj.filter[key] || []);
            if (list.has(id)) list.delete(id); else list.add(id);
            pj.filter = { ...pj.filter, [key]: [...list] };
            pj.sel = new Set();
            renderProject();
            pjSave();
            return;
        }
        if (e.target.closest('.pj-dot')) {
            showMenu(e.target.closest('.pj-dot'), 'Colour', PALETTE_NAMED.map(([c, name]) => ({
                label: name,
                icon: `<span class="pj-dot small" style="background:${c}"></span>`,
                run: () => { const d = pjDept(id); if (!d) return; pjRecord(); d.color = c; renderProject(); pjSave(); }
            })));
        }
    });
    /* drag a department / person from the lists on the right onto an item (onto a selected item: all selected ones get it) */
    let pjTagDrag = null;
    let pjTagDroppedAt = 0;
    $('pjSide').addEventListener('pointerdown', (e) => {
        if (e.button !== 0) return;
        const opt = e.target.closest('.pj-option');
        if (opt && !e.target.closest('button, input')) {
            pjTagDrag = { kind: 'option', id: opt.dataset.option, fieldId: opt.closest('.pj-options').dataset.optionsOf, row: opt, x0: e.clientX, y0: e.clientY, ghost: null, over: null, sort: null, pointerId: e.pointerId };
            return;
        }
        const row = e.target.closest('.pj-side-item');
        if (!row || e.target.closest('button, input')) return;
        const kind = row.dataset.dept ? 'dept' : row.dataset.person ? 'person' : 'select';
        const id = row.dataset.dept || row.dataset.person || row.dataset.field;
        pjTagDrag = { kind, id, row, x0: e.clientX, y0: e.clientY, ghost: null, over: null, pointerId: e.pointerId };
    });
    document.addEventListener('pointermove', (e) => {
        const d = pjTagDrag;
        if (!d || e.pointerId !== d.pointerId) return;
        if (!d.ghost) {
            if (Math.hypot(e.clientX - d.x0, e.clientY - d.y0) < 6) return;
            const item = d.kind === 'dept' ? pjDept(d.id) : d.kind === 'person' ? pjPerson(d.id) : d.kind === 'option'
                ? ((pjField(d.fieldId) || {}).options || []).find((o) => o.id === d.id) : pjField(d.id);
            if (!item) { pjTagDrag = null; return; }
            d.ghost = document.createElement('div');
            d.ghost.className = 'pj-tag-ghost';
            d.ghost.innerHTML = d.kind === 'dept'
                ? `<span class="pj-dot small" style="background:${escapeHtml(item.color)}"></span>${escapeHtml(item.name)}`
                : d.kind === 'person' ? `<span class="pj-avatar">${escapeHtml(initials(item.name))}</span>${escapeHtml(item.name)}`
                    : d.kind === 'option' ? escapeHtml(item.name) : `▾ ${escapeHtml(item.name)}`;
            document.body.appendChild(d.ghost);
            document.body.classList.add('pj-tag-dragging');
            window.getSelection()?.removeAllRanges();
        }
        e.preventDefault();
        d.ghost.style.left = `${e.clientX + 12}px`;
        d.ghost.style.top = `${e.clientY + 10}px`;
        const el = document.elementFromPoint(e.clientX, e.clientY);
        // over another row of the same box (or the same dropdown): change the order
        const sideRow = el && el.closest && (d.kind === 'option' ? el.closest('.pj-option') : el.closest('#pjSide .pj-side-item'));
        const same = sideRow && sideRow !== d.row && (d.kind === 'option' ? sideRow.closest('.pj-options') === d.row.closest('.pj-options')
            : sideRow.closest('.pj-box') === d.row.closest('.pj-box'));
        if (d.sort) d.sort.row.classList.remove('pj-sort-before', 'pj-sort-after');
        d.sort = null;
        if (same) {
            const r = sideRow.getBoundingClientRect();
            d.sort = { row: sideRow, after: e.clientY > r.top + r.height / 2 };
            sideRow.classList.add(d.sort.after ? 'pj-sort-after' : 'pj-sort-before');
        }
        const card = d.kind === 'option' ? null : el && el.closest && el.closest('#pjColumns .pj-card');
        if (d.over !== card) {
            if (d.over) d.over.classList.remove('pj-tag-target');
            d.over = card || null;
            if (card) card.classList.add('pj-tag-target');
        }
    }, { passive: false });
    // move a row of a box (or an option of a dropdown) before / after another one
    function pjSortSide(d) {
        const list = d.kind === 'option' ? (pjField(d.fieldId) || {}).options : (pjBoxOfItem(d.id) || {}).items;
        const targetId = d.kind === 'option' ? d.sort.row.dataset.option : d.sort.row.dataset.dept || d.sort.row.dataset.person || d.sort.row.dataset.field;
        if (!list) return;
        const from = list.findIndex((x) => x.id === d.id);
        if (from < 0 || !list.some((x) => x.id === targetId)) return;
        pjRecord();
        const [moved] = list.splice(from, 1);
        const to = list.findIndex((x) => x.id === targetId) + (d.sort.after ? 1 : 0);
        list.splice(to, 0, moved);
        renderProject();
        pjSave();
    }
    const pjTagDrop = (e) => {
        const d = pjTagDrag;
        if (!d || (e && e.pointerId !== d.pointerId)) return;
        pjTagDrag = null;
        if (!d.ghost) return; // a plain click
        pjTagDroppedAt = Date.now();
        d.ghost.remove();
        document.body.classList.remove('pj-tag-dragging');
        const card = d.over;
        if (card) card.classList.remove('pj-tag-target');
        if (d.sort) d.sort.row.classList.remove('pj-sort-before', 'pj-sort-after');
        if (d.sort && e && e.type !== 'pointercancel') { pjSortSide(d); return; }
        if (!card || !e || e.type === 'pointercancel') return;
        const cardId = card.dataset.card;
        const editing = pj.editing && pj.editing.kind === 'card' && pj.editing.id === cardId;
        const ids = !editing && pj.sel.has(cardId) && pj.sel.size > 1 ? [...pj.sel] : [cardId];
        if (d.kind === 'select') { pjGiveField(ids, d.id, true, editing); return; }
        const field = d.kind === 'dept' ? 'depts' : 'people';
        const missing = ids.filter((cid) => { const h = pjFindCard(cid); return h && !(h.card[field] || []).includes(d.id); });
        if (!missing.length) { toast(ids.length > 1 ? 'These items already have it' : 'This item already has it'); return; }
        if (!editing) pjRecord();
        missing.forEach((cid) => { const h = pjFindCard(cid); h.card[field] = [...(h.card[field] || []), d.id]; });
        renderProject();
        if (!editing) pjSave();
    };
    document.addEventListener('pointerup', pjTagDrop);
    document.addEventListener('pointercancel', pjTagDrop);
    $('pjSide').addEventListener('dblclick', (e) => {
        if (!pjActive()) return;
        const title = e.target.closest('.pj-box-title');
        if (title) { pjRenameBox(title.closest('.pj-box').dataset.box); return; }
        const opt = e.target.closest('.pj-option-name');
        if (opt) { const o = opt.closest('.pj-option'); pjRenameOption(o.closest('.pj-options').dataset.optionsOf, o.dataset.option); return; }
        const row = e.target.closest('.pj-side-item');
        if (row && e.target.closest('.pj-side-name')) pjRenameSide(row.dataset.dept ? 'dept' : row.dataset.person ? 'person' : 'select', row.dataset.dept || row.dataset.person || row.dataset.field);
    });
    // give a dropdown to items (or take it off); its option is chosen on the item itself
    function pjGiveField(ids, fieldId, add, editing) {
        const todo = ids.filter((cid) => { const h = pjFindCard(cid); return h && (add ? !(h.card.fields && fieldId in h.card.fields) : h.card.fields && fieldId in h.card.fields); });
        if (!todo.length) { if (add) toast(ids.length > 1 ? 'These items already have it' : 'This item already has it'); return; }
        if (!editing) pjRecord();
        todo.forEach((cid) => {
            const h = pjFindCard(cid);
            if (add) h.card.fields = { ...(h.card.fields || {}), [fieldId]: '' };
            else { delete h.card.fields[fieldId]; if (!Object.keys(h.card.fields).length) delete h.card.fields; }
        });
        renderProject();
        if (!editing) pjSave();
    }
    function pjFieldMenu(anchor, cardId) {
        const f = pjFindCard(cardId);
        if (!f) return;
        const list = pjAllOf('select');
        if (!list.length) {
            toast('Make a dropdown first: "+ Add box" on the right → Dropdowns');
            return;
        }
        const editing = pj.editing && pj.editing.kind === 'card' && pj.editing.id === cardId;
        const ids = !editing && pj.sel.has(cardId) && pj.sel.size > 1 ? [...pj.sel] : [cardId];
        showMenu(anchor, 'Dropdown', list.map((x) => {
            const has = !!(f.card.fields && x.id in f.card.fields);
            return { label: `${has ? '✓ ' : ''}${x.name}`, icon: '▾', run: () => pjGiveField(ids, x.id, !has, editing) };
        }));
    }
    // choosing an option on an item
    pjBoard.addEventListener('change', (e) => {
        const sel = e.target.closest('.pj-field-select');
        if (!sel || !pjActive()) return;
        const cardId = sel.closest('.pj-card').dataset.card;
        const fieldId = sel.closest('.pj-field').dataset.field;
        const h = pjFindCard(cardId);
        if (!h) return;
        const editing = pj.editing && pj.editing.kind === 'card' && pj.editing.id === cardId;
        if (!editing) pjRecord();
        h.card.fields = { ...(h.card.fields || {}), [fieldId]: sel.value };
        if (!editing) { renderProject(); pjSave(); }
    });
    // keep the text box focused when clicking the assign buttons of the item being edited
    pjBoard.addEventListener('pointerdown', (e) => { if (e.target.closest('.pj-assign, .pj-field-del')) e.preventDefault(); });

    function renderProject() {
        pj.sel = new Set([...pj.sel].filter((id) => pjFindCard(id)));
        $('pjSelCount').textContent = pj.sel.size > 1 ? `${pj.sel.size} items selected · Delete to remove · drag to move them together` : '';
        requestAnimationFrame(updatePjCtx);
        renderProjectSide();
        $('pjUndo').disabled = !pj.undo.length;
        $('pjRedo').disabled = !pj.redo.length;
        pjCols.innerHTML = pj.columns.map((col) => `
            <div class="pj-col" data-col="${col.id}">
                <div class="pj-col-head">
                    <span class="pj-col-name" title="Double-click to rename">${escapeHtml(col.name)}</span>
                    <span class="pj-count" title="${col.cards.filter(pjShown).length === col.cards.length ? '' : `${col.cards.length - col.cards.filter(pjShown).length} hidden`}">${col.cards.filter(pjShown).length === col.cards.length ? col.cards.length : `${col.cards.filter(pjShown).length}/${col.cards.length}`}</span>
                    <button type="button" class="pj-col-menu" title="Column options" aria-label="Column options">⋯</button>
                </div>
                <div class="pj-cards">${col.cards.filter(pjShown).map(pjCardHtml).join('')}</div>
                <button type="button" class="pj-add-card">+ Add item</button>
            </div>`).join('') +
            '<button type="button" class="pj-add-col" id="pjAddCol">+ Add column</button>';
        if (pj.editing) {
            const ed = pj.editing;
            if (ed.kind === 'card') pjEditCard(ed.id, ed); else { pj.editing = null; pjEditColumn(ed.id); }
        }
    }

    // typing in a card (Enter or Done = save, Shift+Enter = new line, Esc = cancel).
    // Nothing is saved before Enter / Done: also not a department or person you give it meanwhile.
    function pjEditCard(id, ed = null) {
        if (!ed && pj.editing && pj.editing.kind === 'card' && pj.editing.id !== id && pj.editFinish) pj.editFinish(true);
        const found = pjFindCard(id);
        const el = pjCols.querySelector(`[data-card="${id}"]`);
        if (!found || !el) return;
        if (!ed) {
            ed = { kind: 'card', id, isNew: false, orig: JSON.stringify(found.card), before: pjState(), draft: null };
        }
        pj.editing = ed;
        const box = document.createElement('textarea');
        box.className = 'pj-card-input';
        box.value = ed.draft !== null ? ed.draft : found.card.text;
        box.rows = 2;
        box.placeholder = 'What needs to be done?';
        el.querySelector('.pj-card-text').replaceWith(box);
        el.classList.add('editing');
        const grow = () => { box.style.height = 'auto'; box.style.height = `${box.scrollHeight}px`; };
        grow();
        box.addEventListener('input', () => { ed.draft = box.value; grow(); });
        box.focus();
        box.setSelectionRange(box.value.length, box.value.length);
        const finish = (keep) => {
            if (pj.editing !== ed) return;
            pj.editing = null;
            pj.editFinish = null;
            const f = pjFindCard(id);
            if (f) {
                if (keep) {
                    f.card.text = (ed.draft !== null ? ed.draft : f.card.text).trim();
                    const empty = !f.card.text && !(f.card.people || []).length && !(f.card.depts || []).length;
                    if (empty) f.col.cards.splice(f.i, 1);
                    const changed = ed.isNew ? !empty : JSON.stringify(empty ? null : f.card) !== ed.orig;
                    if (changed) { pj.undo.push(ed.before); pj.redo = []; pjSave(); }
                } else if (ed.isNew) {
                    f.col.cards.splice(f.i, 1);          // a new item that was cancelled
                } else {
                    Object.assign(f.card, JSON.parse(ed.orig)); // back to how it was
                    if (!JSON.parse(ed.orig).people) delete f.card.people;
                    if (!JSON.parse(ed.orig).depts) delete f.card.depts;
                }
            }
            renderProject();
        };
        pj.editFinish = finish;
        updatePjCtx();
        box.addEventListener('keydown', (e) => {
            e.stopPropagation();
            if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); finish(true); }
            else if (e.key === 'Escape') { e.preventDefault(); finish(false); }
        });
    }
    function pjEditColumn(id) {
        const col = pjCol(id);
        const el = pjCols.querySelector(`[data-col="${id}"] .pj-col-name`);
        if (!col || !el) return;
        pj.editing = { kind: 'col', id };
        const input = document.createElement('input');
        input.className = 'pj-col-input';
        input.value = col.name;
        input.maxLength = 60;
        el.replaceWith(input);
        input.focus();
        input.select();
        let done = false;
        const finish = (keep) => {
            if (done) return;
            done = true;
            pj.editing = null;
            const name = input.value.trim();
            const c = pjCol(id);
            if (c && keep && name && name !== c.name) { pjRecord(); c.name = name; pjSave(); }
            renderProject();
        };
        input.addEventListener('keydown', (e) => {
            e.stopPropagation();
            if (e.key === 'Enter') { e.preventDefault(); finish(true); }
            else if (e.key === 'Escape') { e.preventDefault(); finish(false); }
        });
        input.addEventListener('blur', () => finish(true));
    }
    function pjAddCard(colId) {
        const col = pjCol(colId);
        if (!col) return;
        if (pj.editing && pj.editFinish) pj.editFinish(true); // save the item you were typing in first
        const before = pjState();
        const card = { id: uid(), text: '' };
        pj.sel = new Set();
        col.cards.push(card);
        pj.editing = { kind: 'card', id: card.id, isNew: true, orig: null, before, draft: null };
        renderProject();
        const el = pjCols.querySelector(`[data-card="${card.id}"]`);
        if (el) el.scrollIntoView({ block: 'nearest' });
    }
    async function pjDeleteColumn(id) {
        const col = pjCol(id);
        if (!col) return;
        if (col.cards.length) {
            const ok = await askDialog({ title: `Delete column "${col.name}"?`, message: `Its ${col.cards.length} item(s) are deleted too. You can undo this with ↶.`, okLabel: 'Delete', danger: true });
            if (!ok) return;
        }
        pjRecord();
        pj.columns = pj.columns.filter((c) => c.id !== id);
        renderProject();
        pjSave();
    }
    function pjMoveColumn(id, dir) {
        const i = pj.columns.findIndex((c) => c.id === id);
        const j = i + dir;
        if (i < 0 || j < 0 || j >= pj.columns.length) return;
        pjRecord();
        [pj.columns[i], pj.columns[j]] = [pj.columns[j], pj.columns[i]];
        renderProject();
        pjSave();
    }

    pjBoard.addEventListener('click', (e) => {
        if (!pjActive() || pjDrag) return;
        const t = e.target;
        if (t.closest('#pjUndo')) { pjUndoRedo(true); return; }
        if (t.closest('#pjRedo')) { pjUndoRedo(false); return; }
        if (t.closest('#pjPaste')) { pjPasteButton(); return; }
        const inCol = t.closest('.pj-col');
        if (inCol) pj.lastCol = inCol.dataset.col;
        if (t.closest('#pjAddCol')) {
            if (pj.editing && pj.editFinish) pj.editFinish(true);
            pjRecord();
            const col = { id: uid(), name: 'New column', cards: [] };
            pj.columns.push(col);
            pj.editing = { kind: 'col', id: col.id };
            renderProject();
            pjSave();
            pjCols.scrollLeft = pjCols.scrollWidth;
            return;
        }
        const colEl = t.closest('.pj-col');
        if (!colEl) {
            // a click next to the items: no selection any more
            if (pj.sel.size && !t.closest('button, input, textarea, select, .ctx-bar, .pj-side')) { pj.sel = new Set(); renderProject(); }
            return;
        }
        const colId = colEl.dataset.col;
        if (t.closest('.pj-add-card')) { pjAddCard(colId); return; }
        if (t.closest('.pj-col-menu')) {
            const i = pj.columns.findIndex((c) => c.id === colId);
            const actions = [
                { label: 'Rename', icon: '✎', run: () => pjEditColumn(colId) },
                { label: 'Add item', icon: '+', run: () => pjAddCard(colId) }
            ];
            if (pjClip) actions.push({ label: `Paste ${pjClip.cards.length === 1 ? 'item' : `${pjClip.cards.length} items`} here`, icon: '📋', run: () => pjPasteCards(pjClip.cards, { colId }) });
            if (i > 0) actions.push({ label: 'Move left', icon: '←', run: () => pjMoveColumn(colId, -1) });
            if (i < pj.columns.length - 1) actions.push({ label: 'Move right', icon: '→', run: () => pjMoveColumn(colId, 1) });
            actions.push({ label: 'Delete column', icon: '🗑', danger: true, run: () => pjDeleteColumn(colId) });
            showMenu(t.closest('.pj-col-menu'), pjCol(colId).name, actions);
            return;
        }
        const del = t.closest('.pj-card-del');
        if (del) {
            const f = pjFindCard(del.closest('.pj-card').dataset.card);
            if (!f) return;
            pjRecord();
            f.col.cards.splice(f.i, 1);
            renderProject();
            pjSave();
            toast('Item deleted (↶ to undo)');
            return;
        }
        if (t.closest('.pj-done')) { if (pj.editFinish) pj.editFinish(true); return; }
        const fieldDel = t.closest('.pj-field-del');
        if (fieldDel) {
            const cardId = fieldDel.closest('.pj-card').dataset.card;
            pjGiveField([cardId], fieldDel.closest('.pj-field').dataset.field, false, pj.editing && pj.editing.kind === 'card' && pj.editing.id === cardId);
            return;
        }
        if (t.closest('.pj-field')) return; // choosing an option
        const fieldBtn = t.closest('.pj-assign-field');
        if (fieldBtn) { pjFieldMenu(fieldBtn, fieldBtn.closest('.pj-card').dataset.card); return; }
        const assign = t.closest('.pj-assign-dept, .pj-assign-person');
        if (assign) {
            const id = assign.closest('.pj-card').dataset.card;
            pjAssignMenu(assign, id, assign.classList.contains('pj-assign-dept') ? 'dept' : 'person');
            return;
        }
        const card = t.closest('.pj-card');
        if (card && e.shiftKey && !card.classList.contains('editing')) {
            // Shift + click: select several items
            const id = card.dataset.card;
            if (pj.sel.has(id)) pj.sel.delete(id); else pj.sel.add(id);
            renderProject();
            return;
        }
        if (card && !card.classList.contains('editing') && !t.closest('.pj-card-foot')) {
            if (pj.sel.size) { pj.sel = new Set(); }
            pjEditCard(card.dataset.card);
            return;
        }
        if (!card && pj.sel.size && !t.closest('button, input, .ctx-bar')) { pj.sel = new Set(); renderProject(); }
    });
    pjBoard.addEventListener('dblclick', (e) => {
        const name = e.target.closest('.pj-col-name');
        if (name && pjActive()) pjEditColumn(name.closest('.pj-col').dataset.col);
    });

    /* ---- dragging items between columns (mouse: drag the item · touch: drag its ⠿ grip) ---- */
    let pjDrag = null;      // { id, ghost, holder, dx, dy } while dragging
    let pjPress = null;     // a press that may become a drag
    function pjStartDrag(p) {
        const el = pjCols.querySelector(`[data-card="${p.id}"]`);
        if (!el) return;
        const r = el.getBoundingClientRect();
        const ghost = el.cloneNode(true);
        ghost.classList.add('pj-ghost');
        ghost.style.width = `${r.width}px`;
        document.body.appendChild(ghost);
        const holder = document.createElement('div');
        holder.className = 'pj-holder';
        holder.style.height = `${r.height}px`;
        el.replaceWith(holder);
        // several selected items move together
        const group = pj.sel.has(p.id) && pj.sel.size > 1 ? [...pj.sel] : [p.id];
        group.forEach((id) => { if (id !== p.id) { const o = pjCols.querySelector(`[data-card="${id}"]`); if (o) o.classList.add('pj-hidden'); } });
        if (group.length > 1) { const badge = document.createElement('span'); badge.className = 'pj-ghost-count'; badge.textContent = group.length; ghost.appendChild(badge); }
        pjDrag = { id: p.id, group, ghost, holder, dx: p.x - r.left, dy: p.y - r.top, pointerId: p.pointerId };
        pjBoard.classList.add('dragging');
        pjMoveDrag(p.x, p.y);
    }
    function pjMoveDrag(x, y) {
        const d = pjDrag;
        d.ghost.style.transform = `translate(${x - d.dx}px, ${y - d.dy}px) rotate(1.5deg)`;
        const under = document.elementFromPoint(x, y);
        const colEl = under && under.closest('.pj-col');
        if (!colEl) return;
        const list = colEl.querySelector('.pj-cards');
        const cards = [...list.querySelectorAll('.pj-card:not(.pj-hidden)')];
        const before = cards.find((c) => { const r = c.getBoundingClientRect(); return y < r.top + r.height / 2; });
        if (before) { if (before.previousSibling !== d.holder) list.insertBefore(d.holder, before); }
        else if (list.lastChild !== d.holder) list.appendChild(d.holder);
        // scroll the board sideways near its edges
        const br = pjCols.getBoundingClientRect();
        if (x < br.left + 40) pjCols.scrollLeft -= 12; else if (x > br.right - 40) pjCols.scrollLeft += 12;
    }
    function pjEndDrag(drop) {
        const d = pjDrag;
        pjDrag = null;
        pjBoard.classList.remove('dragging');
        d.ghost.remove();
        if (drop) {
            const list = d.holder.closest('.pj-cards');
            const colId = list && list.closest('.pj-col').dataset.col;
            const index = list ? [...list.children].filter((c) => c === d.holder || (c.classList.contains('pj-card') && !c.classList.contains('pj-hidden'))).indexOf(d.holder) : -1;
            // the item shown right after the drop place (hidden items are not on screen)
            let nextEl = d.holder.nextElementSibling;
            while (nextEl && (!nextEl.classList.contains('pj-card') || nextEl.classList.contains('pj-hidden'))) nextEl = nextEl.nextElementSibling;
            const nextId = nextEl ? nextEl.dataset.card : null;
            const f = pjFindCard(d.id);
            const moved = d.group.length > 1 || (f && !(f.col.id === colId && f.i === index));
            if (f && colId && index >= 0 && moved) {
                pjRecord();
                // take the items out (in board order), then put them in at the drop place
                const order = [];
                pj.columns.forEach((c) => c.cards.forEach((k) => { if (d.group.includes(k.id)) order.push(k); }));
                pj.columns.forEach((c) => { c.cards = c.cards.filter((k) => !d.group.includes(k.id)); });
                const target = pjCol(colId).cards;
                const at = nextId ? target.findIndex((k) => k.id === nextId) : -1;
                target.splice(at >= 0 ? at : target.length, 0, ...order);
                pjSave();
            }
            if (d.group.length > 1) pj.sel = new Set(); // moved together: done with this selection
        }
        renderProject();
    }
    pjBoard.addEventListener('pointerdown', (e) => {
        if (!pjActive() || e.button !== 0) return;
        const card = e.target.closest('.pj-card');
        if (!card || card.classList.contains('editing') || e.target.closest('.pj-card-del, .pj-assign, .pj-field')) return;
        const grip = e.target.closest('.pj-grip');
        if (e.shiftKey && !grip) return; // Shift + click selects
        if (e.pointerType !== 'mouse' && !grip) return; // on a touch screen the card itself scrolls / opens
        pjPress = { id: card.dataset.card, x: e.clientX, y: e.clientY, pointerId: e.pointerId };
        if (grip) { e.preventDefault(); const pr = pjPress; pjPress = null; pjStartDrag(pr); }
    });
    document.addEventListener('pointermove', (e) => {
        if (pjDrag) { if (e.pointerId === pjDrag.pointerId) { e.preventDefault(); pjMoveDrag(e.clientX, e.clientY); } return; }
        if (!pjPress || e.pointerId !== pjPress.pointerId) return;
        const far = Math.hypot(e.clientX - pjPress.x, e.clientY - pjPress.y) > 6;
        if (!far) return;
        const p = pjPress;
        pjPress = null;
        p.x = e.clientX; p.y = e.clientY;
        pjStartDrag(p);
    });
    const pjUp = (e) => {
        pjPress = null;
        if (pjDrag && e.pointerId === pjDrag.pointerId) {
            pjEndDrag(e.type === 'pointerup');
            // don't treat the end of a drag as a click on the card
            const stop = (ev) => { ev.stopPropagation(); ev.preventDefault(); };
            window.addEventListener('click', stop, { capture: true, once: true });
            setTimeout(() => window.removeEventListener('click', stop, { capture: true }), 50);
        }
    };
    document.addEventListener('pointerup', pjUp);
    document.addEventListener('pointercancel', pjUp);
    // while dragging on a phone, the page must not scroll
    document.addEventListener('touchmove', (e) => { if (pjDrag) e.preventDefault(); }, { passive: false });
    pjBoard.addEventListener('contextmenu', (e) => {
        const card = e.target.closest('.pj-card');
        if (!card) return;
        if (e.pointerType && e.pointerType !== 'mouse') { e.preventDefault(); return; }
        if (!pjActive() || card.classList.contains('editing') || e.target.closest('select')) return;
        // right-click: copy / cut / paste / delete (the selected items, or just this one)
        e.preventDefault();
        const id = card.dataset.card;
        const ids = pj.sel.has(id) ? [...pj.sel] : [id];
        const n = ids.length > 1 ? `${ids.length} items` : 'item';
        const actions = [
            { label: `Copy ${n}`, icon: '⧉', run: () => pjCopyCards(ids, false, true) },
            { label: `Cut ${n}`, icon: '✂', run: () => pjCopyCards(ids, true, true) }
        ];
        if (pjClip) actions.push({ label: `Paste ${pjClip.cards.length === 1 ? 'item' : `${pjClip.cards.length} items`} below`, icon: '📋', run: () => pjPasteCards(pjClip.cards, { afterId: id }) });
        actions.push({ label: `Delete ${n}`, icon: '🗑', danger: true, run: () => {
            pjRecord();
            const gone = new Set(ids);
            pj.columns.forEach((c) => { c.cards = c.cards.filter((k) => !gone.has(k.id)); });
            pj.sel = new Set();
            renderProject();
            pjSave();
            toast(`${ids.length === 1 ? 'Item' : `${ids.length} items`} deleted (↶ to undo)`);
        } });
        menuAt(e.clientX, e.clientY, ids.length > 1 ? `${ids.length} items` : 'Item', actions);
    });

    /* ---- select items by dragging a box around them (mouse), like in Collage and Draw ---- */
    let pjMarq = null;
    pjCtx = makeCtxBar($('projectBoard').querySelector('.pj-bar'));
    pjBoard.addEventListener('pointerdown', (e) => {
        if (!pjActive() || e.button !== 0 || e.pointerType !== 'mouse') return;
        if (!e.target.closest('#pjColumns') || e.target.closest('.pj-card, button, input, textarea, select, .pj-col-name, .pj-col-head')) return;
        e.preventDefault();
        pjMarq = { x0: e.clientX, y0: e.clientY, base: e.shiftKey ? new Set(pj.sel) : new Set(), el: null, pointerId: e.pointerId };
    });
    document.addEventListener('pointermove', (e) => {
        const m = pjMarq;
        if (!m || e.pointerId !== m.pointerId) return;
        if (!m.el) {
            if (Math.hypot(e.clientX - m.x0, e.clientY - m.y0) < 5) return;
            m.el = document.createElement('div');
            m.el.className = 'pj-marquee';
            document.body.appendChild(m.el);
        }
        const x1 = Math.min(m.x0, e.clientX), y1 = Math.min(m.y0, e.clientY), x2 = Math.max(m.x0, e.clientX), y2 = Math.max(m.y0, e.clientY);
        Object.assign(m.el.style, { left: `${x1}px`, top: `${y1}px`, width: `${x2 - x1}px`, height: `${y2 - y1}px` });
        const sel = new Set(m.base);
        pjCols.querySelectorAll('.pj-card').forEach((c) => {
            const r = c.getBoundingClientRect();
            const hit = r.right > x1 && r.left < x2 && r.bottom > y1 && r.top < y2;
            if (hit) sel.add(c.dataset.card);
            c.classList.toggle('selected', sel.has(c.dataset.card));
        });
        pj.sel = sel;
        updatePjCtx();
    });
    const pjMarqEnd = (e) => {
        const m = pjMarq;
        if (!m || e.pointerId !== m.pointerId) return;
        pjMarq = null;
        if (!m.el) return; // a plain click: handled as a click
        m.el.remove();
        renderProject();
        // the click that ends the box must not clear the selection
        const stop = (ev) => { ev.stopPropagation(); ev.preventDefault(); };
        window.addEventListener('click', stop, { capture: true, once: true });
        setTimeout(() => window.removeEventListener('click', stop, { capture: true }), 50);
    };
    document.addEventListener('pointerup', pjMarqEnd);
    document.addEventListener('pointercancel', pjMarqEnd);

    // Copy / Cut / Delete right above the selected items
    var pjCtx;
    function pjDeleteCards(ids) {
        pjRecord();
        const gone = new Set(ids);
        pj.columns.forEach((c) => { c.cards = c.cards.filter((k) => !gone.has(k.id)); });
        pj.sel = new Set();
        renderProject();
        pjSave();
        toast(`${ids.length === 1 ? 'Item' : `${ids.length} items`} deleted (↶ to undo)`);
    }
    function updatePjCtx() {
        if (!pjCtx) return;
        // the item being edited: Copy / Cut / Delete above it too
        if (pjActive() && pj.editing && pj.editing.kind === 'card' && pjFindCard(pj.editing.id)) {
            const id = pj.editing.id;
            const done = () => { if (pj.editing && pj.editFinish) pj.editFinish(true); };
            pjCtx.show({
                key: `edit:${id}`,
                anchor: () => rectOf([pjCols.querySelector(`[data-card="${id}"]`)]),
                busy: () => !!pjDrag,
                after: updatePjCtx,
                actions: () => [
                    { label: 'Copy', text: 'Copy', icon: '⧉', run: () => { done(); pjCopyCards([id], false, true); } },
                    { label: 'Cut', text: 'Cut', icon: '✂', run: () => { done(); pjCopyCards([id], true, true); } },
                    { label: 'Delete', text: 'Delete', icon: '🗑', danger: true, run: () => { done(); if (pjFindCard(id)) pjDeleteCards([id]); } }
                ]
            });
            return;
        }
        if (!pjActive() || !pj.sel.size || pj.editing) { pjCtx.hide(); return; }
        const ids = [...pj.sel];
        pjCtx.show({
            key: `${ids.join(',')}|${pjClip ? pjClip.cards.length : 0}`,
            anchor: () => rectOf(ids.map((id) => pjCols.querySelector(`[data-card="${id}"]`))),
            busy: () => !!pjDrag || !!(pjMarq && pjMarq.el),
            after: updatePjCtx,
            actions: () => {
                const n = ids.length > 1 ? ` ${ids.length} items` : '';
                const list = [
                    { label: `Copy${n}`, icon: '⧉', run: () => pjCopyCards(ids, false, true) },
                    { label: `Cut${n}`, icon: '✂', run: () => pjCopyCards(ids, true, true) }
                ];
                if (pjClip) list.push({ label: `Paste ${pjClip.cards.length === 1 ? 'item' : `${pjClip.cards.length} items`} below`, icon: '📋', run: () => pjPasteCards(pjClip.cards) });
                list.push({ label: `Delete${n}`, icon: '🗑', danger: true, run: () => pjDeleteCards(ids) });
                return list.map((a) => ({ ...a, text: a.label.split(' ')[0] }));
            }
        });
    }

    /* ---- copy / paste in the boxes on the right: dropdown options, dropdowns, tags, people ---- */
    let pjSideClip = null; // { kind: 'option' | 'dept' | 'person' | 'select', items }
    function pjSideCopy(kind, items) {
        pjSideClip = { kind, items: JSON.parse(JSON.stringify(items)) };
        const plain = items.map((x) => x.name).join('\n');
        pjSideClip.plain = plain;
        if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(plain).catch(() => {});
        const what = kind === 'option' ? 'option' : PJ_KIND_WORD[kind];
        toast(`${items.length === 1 ? `"${items[0].name}"` : `${items.length} ${what}s`} copied`);
    }
    // new copies (new ids), so the same thing can be pasted more than once
    const pjFreshCopy = (kind, x) => (kind === 'select'
        ? { id: uid(), name: x.name, options: (x.options || []).map((o) => ({ id: uid(), name: o.name })) }
        : kind === 'dept' ? { id: uid(), name: x.name, color: x.color || DEPT_COLORS[0] } : { id: uid(), name: x.name });
    function pjSidePaste(where) {
        const c = pjSideClip;
        if (!c) return;
        pjRecord();
        if (where.fieldId) {
            const f = pjField(where.fieldId);
            if (!f) return;
            const names = c.kind === 'option' ? c.items.map((x) => x.name) : c.kind === 'select' ? c.items.flatMap((x) => (x.options || []).map((o) => o.name)) : c.items.map((x) => x.name);
            const at = where.afterOption ? (f.options || []).findIndex((o) => o.id === where.afterOption) + 1 : (f.options || []).length;
            (f.options = f.options || []).splice(at, 0, ...names.map((name) => ({ id: uid(), name })));
            pj.openFields.add(f.id);
        } else {
            const box = pj.boxes.find((b) => b.id === where.boxId);
            if (!box) return;
            const at = where.afterId ? box.items.findIndex((x) => x.id === where.afterId) + 1 : box.items.length;
            const kind = box.kind;
            box.items.splice(at, 0, ...c.items.map((x) => pjFreshCopy(kind, c.kind === kind ? x : { name: x.name })));
        }
        renderProject();
        pjSave();
    }
    const pjClipLabel = () => (pjSideClip ? (pjSideClip.items.length === 1 ? `"${pjSideClip.items[0].name}"` : `${pjSideClip.items.length} ${pjSideClip.kind === 'option' ? 'options' : `${PJ_KIND_WORD[pjSideClip.kind]}s`}`) : '');
    $('pjSide').addEventListener('contextmenu', (e) => {
        if (!pjActive() || e.target.closest('input')) return;
        const opt = e.target.closest('.pj-option');
        const row = e.target.closest('.pj-side-item');
        const boxEl = e.target.closest('.pj-box');
        if (!opt && !row && !boxEl) return;
        e.preventDefault();
        const x = e.clientX, y = e.clientY;
        if (opt) {
            const fieldId = opt.closest('.pj-options').dataset.optionsOf;
            const f = pjField(fieldId);
            const o = f && (f.options || []).find((v) => v.id === opt.dataset.option);
            if (!o) return;
            const actions = [
                { label: `Copy "${o.name}"`, icon: '⧉', run: () => pjSideCopy('option', [o]) },
                { label: `Copy all options of ${f.name}`, icon: '⧉', run: () => pjSideCopy('option', f.options || []) }
            ];
            if (pjSideClip) actions.push({ label: `Paste ${pjClipLabel()} below`, icon: '📋', run: () => pjSidePaste({ fieldId, afterOption: o.id }) });
            actions.push({ label: 'Rename', icon: '✎', run: () => pjRenameOption(fieldId, o.id) });
            actions.push({ label: 'Delete', icon: '🗑', danger: true, run: () => pjRemoveOption(fieldId, o.id) });
            menuAt(x, y, o.name, actions);
            return;
        }
        if (row) {
            const kind = row.dataset.dept ? 'dept' : row.dataset.person ? 'person' : 'select';
            const id = row.dataset.dept || row.dataset.person || row.dataset.field;
            const box = pjBoxOfItem(id);
            const item = box && box.items.find((v) => v.id === id);
            if (!item) return;
            const actions = [{ label: `Copy "${item.name}"${kind === 'select' ? ' (with its options)' : ''}`, icon: '⧉', run: () => pjSideCopy(kind, [item]) }];
            if (kind === 'select') actions.push({ label: 'Copy its options', icon: '⧉', run: () => pjSideCopy('option', item.options || []) });
            if (pjSideClip && kind === 'select' && pjSideClip.kind !== 'select') actions.push({ label: `Paste ${pjClipLabel()} into ${item.name}`, icon: '📋', run: () => pjSidePaste({ fieldId: id }) });
            if (pjSideClip && (pjSideClip.kind === kind || (kind !== 'select' && pjSideClip.kind !== 'select'))) actions.push({ label: `Paste ${pjClipLabel()} below`, icon: '📋', run: () => pjSidePaste({ boxId: box.id, afterId: id }) });
            actions.push({ label: 'Rename', icon: '✎', run: () => pjRenameSide(kind, id) });
            actions.push({ label: 'Delete', icon: '🗑', danger: true, run: () => pjRemoveSide(kind, id) });
            menuAt(x, y, item.name, actions);
            return;
        }
        const box = pj.boxes.find((b) => b.id === boxEl.dataset.box);
        if (!box) return;
        const actions = [{ label: `Copy everything in ${box.name}`, icon: '⧉', run: () => box.items.length && pjSideCopy(box.kind, box.items) }];
        if (pjSideClip) actions.push({ label: `Paste ${pjClipLabel()} here`, icon: '📋', run: () => pjSidePaste({ boxId: box.id }) });
        menuAt(x, y, box.name, actions);
    });
    // pasting several lines in a "+ Add" box adds one per line
    $('pjSide').addEventListener('paste', (e) => {
        const input = e.target.closest('form input');
        if (!input || !pjActive()) return;
        const text = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
        const form = input.closest('form');
        const fromClip = pjSideClip && text.trim() === pjSideClip.plain.trim();
        if (!fromClip && !/\n/.test(text.trim())) return; // one line: normal paste
        e.preventDefault();
        if (fromClip) {
            if (form.classList.contains('pj-option-form')) pjSidePaste({ fieldId: form.dataset.field });
            else pjSidePaste({ boxId: form.dataset.box });
        } else {
            const names = text.split(/\r?\n/).map((l) => l.trim().slice(0, 50)).filter(Boolean).slice(0, 200);
            const keep = pjSideClip;
            pjSideClip = { kind: 'option', items: names.map((name) => ({ name })), plain: '' };
            if (form.classList.contains('pj-option-form')) pjSidePaste({ fieldId: form.dataset.field });
            else pjSidePaste({ boxId: form.dataset.box });
            pjSideClip = keep;
        }
        const again = $('pjSide').querySelector(form.classList.contains('pj-option-form') ? `.pj-option-form[data-field="${form.dataset.field}"] input` : `form[data-box="${form.dataset.box}"] input`);
        if (again) again.focus();
    });

    /* ---- copy / cut / paste items ---- */
    let pjClip = null; // { plain, cards } — the items copied last (with their tags, people and dropdowns)
    const pjCardsText = (cards) => cards.map((c) => (c.text || '').replace(/\n+/g, ' ').trim() || 'Untitled').join('\n');
    function pjCopyCards(ids, cut = false, toClipboard = false) {
        const cards = [];
        pj.columns.forEach((c) => c.cards.forEach((k) => { if (ids.includes(k.id)) cards.push(JSON.parse(JSON.stringify(k))); }));
        if (!cards.length) { toast('Select items first (drag a box around them, or Shift + click), then copy'); return null; }
        // their tags, people and dropdowns go along (so they also work in the project of another note)
        const defs = [];
        const addDef = (id, kind) => {
            if (defs.some((d) => d.item.id === id)) return;
            const box = pjBoxOfItem(id);
            const item = box && box.items.find((x) => x.id === id);
            if (item) defs.push({ kind, boxName: box.name, item: JSON.parse(JSON.stringify(item)) });
        };
        cards.forEach((c) => {
            (c.depts || []).forEach((id) => addDef(id, 'dept'));
            (c.people || []).forEach((id) => addDef(id, 'person'));
            Object.keys(c.fields || {}).forEach((id) => addDef(id, 'select'));
        });
        pjClip = { plain: pjCardsText(cards), cards, defs };
        if (toClipboard && navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(pjClip.plain).catch(() => {});
        if (cut) {
            pjRecord();
            const gone = new Set(ids);
            pj.columns.forEach((c) => { c.cards = c.cards.filter((k) => !gone.has(k.id)); });
            pj.sel = new Set();
            renderProject();
            pjSave();
        }
        toast(`${cards.length === 1 ? 'Item' : `${cards.length} items`} ${cut ? 'cut' : 'copied'}: paste with Ctrl+V or 📋 Paste`);
        return pjClip;
    }
    // where pasted items go: below the (last) selected item, else in the column you clicked last, else the first column
    function pjPasteSpot() {
        const sel = [...pj.sel].map(pjFindCard).filter(Boolean);
        if (sel.length) { const last = sel[sel.length - 1]; return { col: last.col, index: last.i + 1 }; }
        const col = pjCol(pj.lastCol) || pj.columns[0];
        return col ? { col, index: col.cards.length } : null;
    }
    function pjPasteCards(cards, where = {}) {
        if (!cards || !cards.length) return;
        if (pj.editing && pj.editFinish) pj.editFinish(true);
        let spot = null;
        if (where.afterId) { const f = pjFindCard(where.afterId); if (f) spot = { col: f.col, index: f.i + 1 }; }
        else if (where.colId) { const col = pjCol(where.colId); if (col) spot = { col, index: col.cards.length }; }
        if (!spot) spot = pjPasteSpot();
        if (!spot) return;
        pjRecord();
        // the copied tags / people / dropdowns this board doesn't have yet are added to it
        const defs = pjClip && cards === pjClip.cards ? pjClip.defs || [] : [];
        defs.forEach((d) => {
            const have = d.kind === 'dept' ? pjDept(d.item.id) : d.kind === 'person' ? pjPerson(d.item.id) : pjField(d.item.id);
            if (have) {
                if (d.kind === 'select') (d.item.options || []).forEach((o) => { if (!(have.options || []).some((x) => x.id === o.id)) (have.options = have.options || []).push({ ...o }); });
                return;
            }
            let box = pj.boxes.find((b) => b.kind === d.kind && b.name === d.boxName) || pjBoxesOf(d.kind)[0];
            if (!box) { box = { id: uid(), kind: d.kind, name: d.boxName || PJ_BOX_KINDS[d.kind][0], items: [] }; pj.boxes.push(box); }
            box.items.push(JSON.parse(JSON.stringify(d.item)));
        });
        const keep = (ids, has) => (ids || []).filter((x) => has(x));
        const fresh = cards.map((c) => {
            const k = { id: uid(), text: c.text || '' };
            const depts = keep(c.depts, pjDept), people = keep(c.people, pjPerson);
            if (depts.length) k.depts = depts;
            if (people.length) k.people = people;
            const fields = {};
            Object.entries(c.fields || {}).forEach(([fid, v]) => { const f = pjField(fid); if (f) fields[fid] = (f.options || []).some((o) => o.id === v) ? v : ''; });
            if (Object.keys(fields).length) k.fields = fields;
            return k;
        });
        spot.col.cards.splice(spot.index, 0, ...fresh);
        pj.sel = new Set(fresh.map((k) => k.id));
        renderProject();
        pjSave();
        const el = pjCols.querySelector(`[data-card="${fresh[fresh.length - 1].id}"]`);
        if (el) el.scrollIntoView({ block: 'nearest' });
        toast(`${fresh.length === 1 ? 'Item' : `${fresh.length} items`} pasted`);
    }
    // text from somewhere else: one item per line
    function pjPasteText(text) {
        if (pjClip && text.trim() === pjClip.plain.trim()) { pjPasteCards(pjClip.cards); return; }
        const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean).slice(0, 200);
        if (lines.length) pjPasteCards(lines.map((l) => ({ text: l.slice(0, 2000) })));
    }
    async function pjPasteButton() {
        let text = '';
        try { text = navigator.clipboard && navigator.clipboard.readText ? await navigator.clipboard.readText() : ''; } catch (_) { text = ''; }
        if (text) pjPasteText(text);
        else if (pjClip) pjPasteCards(pjClip.cards);
        else toast('Nothing to paste: copy items first (Shift + click to select, then Ctrl+C)');
    }
    const pjTyping = (e) => {
        const t = e.target.closest && e.target.closest('input, textarea, select, [contenteditable="true"], [contenteditable=""]');
        return !!(t && t.offsetParent !== null);
    };
    ['copy', 'cut'].forEach((type) => document.addEventListener(type, (e) => {
        if (!pjActive() || pj.editing || pjTyping(e) || !pj.sel.size) return;
        const sel = window.getSelection();
        if (sel && !sel.isCollapsed && String(sel).trim()) return; // copying selected text
        const clip = pjCopyCards([...pj.sel], type === 'cut');
        if (!clip) return;
        e.preventDefault();
        e.clipboardData.setData('text/plain', clip.plain);
    }));
    document.addEventListener('paste', (e) => {
        if (!pjActive() || pj.editing || pjTyping(e)) return;
        const text = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
        if (!text && !pjClip) return;
        e.preventDefault();
        if (text) pjPasteText(text); else pjPasteCards(pjClip.cards);
    });

    document.addEventListener('keydown', (e) => {
        if (!pjActive() || pj.editing) return;
        const t = e.target.closest && e.target.closest('input, textarea, select, [contenteditable="true"], [contenteditable=""]');
        if (t && t.offsetParent !== null) return;
        const key = e.key.toLowerCase();
        if ((e.ctrlKey || e.metaKey) && !e.altKey && key === 'z') { e.preventDefault(); pjUndoRedo(!e.shiftKey); }
        else if ((e.ctrlKey || e.metaKey) && !e.altKey && key === 'y') { e.preventDefault(); pjUndoRedo(false); }
        else if (e.key === 'Escape' && pjDrag) pjEndDrag(false);
        else if (e.key === 'Escape' && pj.sel.size) { pj.sel = new Set(); renderProject(); }
        else if ((e.key === 'Delete' || e.key === 'Backspace') && pj.sel.size) {
            e.preventDefault();
            pjRecord();
            pj.columns.forEach((c) => { c.cards = c.cards.filter((k) => !pj.sel.has(k.id)); });
            const n = pj.sel.size;
            pj.sel = new Set();
            renderProject();
            pjSave();
            toast(`${n} item${n === 1 ? '' : 's'} deleted (↶ to undo)`);
        }
    });


    // Ctrl + scroll zooms in steps: about 13.5% per notch of a mouse wheel
    // (a touchpad pinch sends many small steps and follows your fingers)
    function wheelZoomFactor(e) {
        const d = e.deltaMode === 1 ? e.deltaY * 33 : e.deltaY;
        if (Math.abs(d) >= 40) return d < 0 ? 1.135 : 1 / 1.135;
        return Math.exp(-d * 0.015);
    }
    // Shift + scroll: how far to move sideways (browsers report it as deltaX or deltaY)
    const wheelSideways = (e) => (e.deltaX || e.deltaY) * (e.deltaMode === 1 ? 33 : 1);

    /* ---------------- collage mode: a big board (like Miro) ----------------
     * note.collage = { items: [...] } with items:
     *   sticky { x, y, w, h, color, html }   text { x, y, w, html, color }
     *   frame  { x, y, w, h, color, title }  image { x, y, w, h, src }
     *   arrow  { from, to, color }  (an end is { id } of a thing, or a free point { x, y })
     * All positions are board units; the view (pan + zoom) is kept per note while the app is open. */
    const cbBoard = $('collageBoard');
    const cbWorld = $('cbWorld');
    const cbFramesEl = $('cbFrames');
    const cbItemsEl = $('cbItems');
    const cbSvg = $('cbArrows');
    const cbMarquee = $('cbMarquee');
    const CB_COLORS = [
        ['#fff9b1', 'Light yellow'], ['#f5d128', 'Yellow'], ['#ff9d48', 'Orange'], ['#f24726', 'Red'],
        ['#ea94bb', 'Pink'], ['#be88c7', 'Violet'], ['#a6ccf5', 'Light blue'], ['#2d9bf0', 'Blue'],
        ['#67c6c0', 'Teal'], ['#8fd14f', 'Green'], ['#ffffff', 'White'], ['#bfbfbf', 'Light grey'],
        ['#808080', 'Grey'], ['#404040', 'Dark grey'], ['#1a1a1a', 'Black']
    ];
    const CB_DEFAULT = { sticky: '#fff9b1', frame: '#ffffff', arrow: '#cbd5e1' };
    const cb = { items: [], view: { x: 40, y: 40, s: 1 }, tool: 'select', sel: new Set(), editing: null,
        editStart: null, undo: [], redo: [], noteId: null, needFit: true };
    const cbViews = new Map();
    const cbEls = new Map();
    let cbClipboard = null;
    let cbSpace = false;
    const CB_MARK = 'remmin-collage-copy';

    const cbById = (id) => cb.items.find((i) => i.id === id) || null;
    const cbActive = () => state.prefs.mode === 'collage' && state.prefs.activeTab !== 'calendar' && !!cb.noteId &&
        !document.querySelector('.modal.active') && !isPrivateLocked(noteById(cb.noteId));
    const cbClone = (v) => JSON.parse(JSON.stringify(v));
    function textOn(hex) {
        const m = /^#?([0-9a-f]{6})$/i.exec(hex || '');
        if (!m) return '#1f2937';
        const n = parseInt(m[1], 16);
        const lum = (0.299 * (n >> 16) + 0.587 * ((n >> 8) & 255) + 0.114 * (n & 255)) / 255;
        return lum > 0.55 ? '#1f2937' : '#f9fafb';
    }
    const cbColorPref = (type) => (state.prefs.collageColors && state.prefs.collageColors[type]) || CB_DEFAULT[type];

    function loadCollage(note) {
        cbEndEdit(true);
        cb.noteId = note ? note.id : null;
        cb.items = note && note.collage && Array.isArray(note.collage.items) ? cbClone(note.collage.items) : [];
        cb.sel = new Set();
        cb.undo = []; cb.redo = [];
        cbEls.forEach((el) => el.remove());
        cbEls.clear();
        const v = note && cbViews.get(note.id);
        if (v) { cb.view = { ...v }; cb.needFit = false; } else cb.needFit = true;
        if (state.prefs.mode === 'collage') cbShow();
        else cbUpdateToolbar();
    }
    function cbShow() {
        if (cb.needFit) cbFit(); else { cbApplyView(); cbRender(); }
    }
    function cbSave() {
        const note = noteById(cb.noteId);
        if (!note || isPrivateLocked(note)) return;
        note.collage = { items: cbClone(cb.items) };
        touch(note);
    }
    // undo versions keep a short reference instead of the (big) video data
    const cbSnap = () => JSON.stringify(cb.items, (k, v) => (k === 'src' && isLocalVideo(v) ? videoRef(v) : v));
    const cbUnsnap = (str) => JSON.parse(str, (k, v) => (k === 'src' ? resolveVideo(v) : v));
    function cbRecord() {
        cb.undo.push(cbSnap());
        if (cb.undo.length > 150) cb.undo.shift();
        cb.redo = [];
    }
    function cbUndoRedo(back) {
        cbEndEdit(true);
        const from = back ? cb.undo : cb.redo, to = back ? cb.redo : cb.undo;
        if (!from.length) return;
        to.push(cbSnap());
        cb.items = cbUnsnap(from.pop());
        cb.sel = new Set([...cb.sel].filter((id) => cbById(id)));
        cbRender();
        cbSave();
    }

    /* ---- view (pan + zoom) ---- */
    function cbApplyView() {
        const { x, y, s } = cb.view;
        cbWorld.style.transform = `translate(${x}px, ${y}px) scale(${s})`;
        cbWorld.style.setProperty('--cbs', s);
        // axonometric dots (turned 90°): columns √3/2 apart, every other column shifted half a step (two layers)
        const step = 42 * s, col = step * Math.sqrt(3); // dots 42 px apart (175% of 24)
        cbBoard.style.backgroundSize = `${col}px ${step}px`;
        cbBoard.style.backgroundPosition = `${x}px ${y}px, ${x + col / 2}px ${y + step / 2}px`;
        $('cbZoom').textContent = `${Math.round(s * 100)}%`;
        if (cb.noteId) cbViews.set(cb.noteId, { ...cb.view });
    }
    function cbZoomAt(mx, my, k) {
        const s = cb.view.s, s2 = Math.min(4, Math.max(0.1, s * k));
        cb.view.x = mx - (mx - cb.view.x) * s2 / s;
        cb.view.y = my - (my - cb.view.y) * s2 / s;
        cb.view.s = s2;
        cbApplyView();
        cbRenderArrows(); // the end handles keep their screen size
    }
    function cbBounds(items) {
        let x1 = Infinity, y1 = Infinity, x2 = -Infinity, y2 = -Infinity;
        items.forEach((it) => {
            if (it.type === 'arrow') {
                const [a, b] = cbArrowPoints(it);
                [a, b].forEach((p) => { x1 = Math.min(x1, p.x); y1 = Math.min(y1, p.y); x2 = Math.max(x2, p.x); y2 = Math.max(y2, p.y); });
                return;
            }
            const r = cbRect(it);
            x1 = Math.min(x1, r.x - (it.type === 'frame' ? 0 : 0)); y1 = Math.min(y1, r.y - (it.type === 'frame' ? 24 : 0));
            x2 = Math.max(x2, r.x + r.w); y2 = Math.max(y2, r.y + r.h);
        });
        return x1 === Infinity ? null : { x: x1, y: y1, w: x2 - x1, h: y2 - y1 };
    }
    function cbFit() {
        cb.needFit = false;
        if (cbBoard.style.display === 'none' || !cbBoard.clientWidth) { cbRender(); return; }
        cbRender();
        const W = cbBoard.clientWidth, H = cbBoard.clientHeight;
        const b = cbBounds(cb.items);
        if (!b) { cb.view = { x: W / 2, y: H / 2, s: 1 }; }
        else {
            const s = Math.min(1, Math.max(0.1, Math.min((W - 80) / Math.max(1, b.w), (H - 80) / Math.max(1, b.h))));
            cb.view = { s, x: W / 2 - (b.x + b.w / 2) * s, y: H / 2 - (b.y + b.h / 2) * s };
        }
        cbApplyView();
        cbRenderArrows();
    }

    /* ---- drawing the board ---- */
    function cbRect(it) {
        if (it.type === 'text') {
            const el = cbEls.get(it.id);
            return { x: it.x, y: it.y, w: el && el.offsetWidth ? el.offsetWidth : it.w, h: el && el.offsetHeight ? el.offsetHeight : (it.h || 32) };
        }
        return { x: it.x, y: it.y, w: it.w, h: it.h };
    }
    function cbEndCenter(end) {
        const it = end && end.id ? cbById(end.id) : null;
        if (it) { const r = cbRect(it); return { x: r.x + r.w / 2, y: r.y + r.h / 2 }; }
        return { x: (end && end.x) || 0, y: (end && end.y) || 0 };
    }
    function cbAnchor(end, toward) {
        const it = end && end.id ? cbById(end.id) : null;
        if (!it) return cbEndCenter(end);
        const r = cbRect(it);
        const cx = r.x + r.w / 2, cy = r.y + r.h / 2, dx = toward.x - cx, dy = toward.y - cy;
        if (!dx && !dy) return { x: cx, y: cy };
        const gap = 6;
        const t = Math.min(dx ? (r.w / 2 + gap) / Math.abs(dx) : Infinity, dy ? (r.h / 2 + gap) / Math.abs(dy) : Infinity, 1);
        return { x: cx + dx * t, y: cy + dy * t };
    }
    function cbArrowPoints(a) {
        const c1 = cbEndCenter(a.from), c2 = cbEndCenter(a.to);
        return [cbAnchor(a.from, c2), cbAnchor(a.to, c1)];
    }
    function cbRenderArrows() {
        const s = cb.view.s;
        let out = '';
        cb.items.forEach((a) => {
            if (a.type !== 'arrow') return;
            const [p1, p2] = cbArrowPoints(a);
            const dx = p2.x - p1.x, dy = p2.y - p1.y, len = Math.hypot(dx, dy) || 1;
            const ux = dx / len, uy = dy / len;
            const hl = Math.min(16, len * 0.6), hw = hl * 0.55;
            const bx = p2.x - ux * hl, by = p2.y - uy * hl;
            const color = escapeHtml(a.color || CB_DEFAULT.arrow);
            const sel = cb.sel.has(a.id);
            out += `<g data-arrow="${a.id}" class="${sel ? 'cb-sel' : ''}">` +
                `<path class="cb-arrow-hit" d="M${p1.x} ${p1.y}L${p2.x} ${p2.y}"/>` +
                `<path class="cb-arrow-line" stroke="${color}" d="M${p1.x} ${p1.y}L${bx} ${by}"/>` +
                `<polygon fill="${color}" points="${p2.x},${p2.y} ${bx - uy * hw},${by + ux * hw} ${bx + uy * hw},${by - ux * hw}"/>` +
                (sel && cb.sel.size === 1 ? `<circle class="cb-arrow-end" data-end="from" cx="${p1.x}" cy="${p1.y}" r="${7 / s}" stroke-width="${2 / s}"/>` +
                    `<circle class="cb-arrow-end" data-end="to" cx="${p2.x}" cy="${p2.y}" r="${7 / s}" stroke-width="${2 / s}"/>` : '') +
                '</g>';
        });
        cbSvg.innerHTML = out;
    }
    function cbRender() {
        const seen = new Set();
        const idx = { frame: 0, other: 0 };
        const single = cb.sel.size === 1 ? [...cb.sel][0] : null;
        cb.items.forEach((it) => {
            if (it.type === 'arrow') return;
            seen.add(it.id);
            let el = cbEls.get(it.id);
            if (!el) {
                el = document.createElement('div');
                el.className = `cb-item cb-${it.type === 'text' ? 'textbox' : it.type}`;
                el.dataset.id = it.id;
                el.innerHTML = it.type === 'frame' ? '<div class="cb-frame-title" spellcheck="false"></div>'
                    : it.type === 'image' ? '<img draggable="false" alt="">'
                    : it.type === 'audio' ? '<span class="voice-chip cb-chip"><button class="voice-back" type="button" title="15 seconds back">⏪</button>' +
                        '<button class="voice-play" type="button" title="Play">▶</button><button class="voice-fwd" type="button" title="15 seconds forward">⏩</button>' +
                        '<input class="voice-seek" type="range" min="0" max="1000" value="0" title="Click or drag to go to a moment">' +
                        '<button class="voice-loop" type="button" title="Loop: play again and again">🔁</button>' +
                        '<span class="voice-vol-icon" title="Volume">🔊</span><input class="voice-vol" type="range" min="0" max="100" value="100" title="Volume"><span class="voice-name"></span><span class="voice-time"></span></span>'
                    : it.type === 'video' ? `<div class="cb-video-bar" title="Drag here to move">▶ Video</div>${isVideoSrc(it.src) ? videoFrameHtml(it.src) : ''}`
                    : '<div class="cb-text"></div>';
                cbEls.set(it.id, el);
            }
            const layer = it.type === 'frame' ? cbFramesEl : cbItemsEl;
            const key = it.type === 'frame' ? 'frame' : 'other';
            if (layer.children[idx[key]] !== el) layer.insertBefore(el, layer.children[idx[key]] || null);
            idx[key]++;
            el.style.left = `${it.x}px`;
            el.style.top = `${it.y}px`;
            if (it.type === 'text') el.style.maxWidth = `${it.w}px`;
            else if (it.type === 'audio') {
                el.style.width = `${it.w}px`; el.style.height = '';
                const chip = el.firstChild;
                if (el._src !== it.src) { chip.setAttribute('data-voice', it.src || ''); el._src = it.src; }
                chip.setAttribute('data-name', it.name || 'Voice note');
                chip.setAttribute('data-duration', String(Math.round(it.duration || 0)));
                if (it.loop) chip.setAttribute('data-loop', '1'); else chip.removeAttribute('data-loop');
                const vol = Number.isFinite(it.volume) ? it.volume : 100;
                if (vol < 100) chip.setAttribute('data-vol', String(vol)); else chip.removeAttribute('data-vol');
                const slider = chip.querySelector('.voice-vol');
                if (document.activeElement !== slider) slider.value = String(vol);
                chip.querySelector('.voice-loop').classList.toggle('on', !!it.loop);
                const nm = `🎤 ${it.name || 'Voice note'}`;
                if (chip.querySelector('.voice-name').textContent !== nm) chip.querySelector('.voice-name').textContent = nm;
                if (voicePlayingChip !== chip) chip.querySelector('.voice-time').textContent = formatSeconds(it.duration || 0);
            }
            else { el.style.width = `${it.w}px`; el.style.height = `${it.h}px`; }
            if (it.type === 'sticky') { el.style.background = it.color || CB_DEFAULT.sticky; el.style.color = textOn(it.color || CB_DEFAULT.sticky); }
            if (it.type === 'frame') {
                el.style.background = it.color || CB_DEFAULT.frame;
                const t = el.firstChild;
                if (cb.editing !== it.id && t.textContent !== (it.title || 'Frame')) t.textContent = it.title || 'Frame';
            }
            if (it.type === 'text') el.style.color = it.color || '';
            if (it.type === 'image') { const img = el.firstChild; if (img.getAttribute('src') !== it.src) img.src = it.src; }
            if (it.type === 'video') { const pl = el.querySelector('video'); if (pl) pl.loop = !!it.loop; }
            const t = el.querySelector('.cb-text');
            if (t && cb.editing !== it.id && el._html !== it.html) { t.innerHTML = sanitizeHtml(it.html || ''); el._html = it.html; }
            el.classList.toggle('selected', cb.sel.has(it.id));
            let handle = el.querySelector(':scope > .cb-handle');
            if (single === it.id && cb.editing !== it.id) {
                if (!handle) { handle = document.createElement('div'); handle.className = 'cb-handle'; el.appendChild(handle); }
            } else if (handle) handle.remove();
        });
        cbEls.forEach((el, id) => { if (!seen.has(id)) { el.remove(); cbEls.delete(id); } });
        cbRenderArrows();
        $('cbEmpty').hidden = cb.items.length > 0;
        // several things selected: a box around them with the same corner handle, to scale them together
        let group = $('cbGroupBox');
        const b = cb.sel.size > 1 && !cb.editing ? cbBounds(cb.items.filter((it) => cb.sel.has(it.id))) : null;
        if (b) {
            if (!group) {
                group = document.createElement('div');
                group.id = 'cbGroupBox';
                group.className = 'cb-group-box';
                group.innerHTML = '<div class="cb-group-handle" title="Drag to make the selection bigger / smaller"></div>';
                cbWorld.appendChild(group);
            }
            Object.assign(group.style, { left: `${b.x}px`, top: `${b.y}px`, width: `${b.w}px`, height: `${b.h}px`, display: 'block' });
        } else if (group) group.style.display = 'none';
        cbUpdateToolbar();
    }
    function cbUpdateToolbar() {
        document.querySelectorAll('[data-cb-tool]').forEach((b) => b.classList.toggle('active', b.dataset.cbTool === cb.tool));
        cbBoard.dataset.tool = cb.tool;
        $('cbUndo').disabled = !cb.undo.length;
        $('cbRedo').disabled = !cb.redo.length;
        const none = !cb.sel.size;
        ['cbFront', 'cbBack', 'cbDelete'].forEach((id) => { $(id).disabled = none; });
        updateCbCtx();
    }
    function cbSetTool(tool) {
        cbEndEdit(true);
        cb.tool = tool;
        cbUpdateToolbar();
    }

    /* ---- adding things ---- */
    function cbAdd(item, select = true) {
        item.id = uid();
        cb.items.push(item);
        if (select) cb.sel = new Set([item.id]);
        return item;
    }
    function cbViewCenter() {
        return { x: (cbBoard.clientWidth / 2 - cb.view.x) / cb.view.s, y: (cbBoard.clientHeight / 2 - cb.view.y) / cb.view.s };
    }
    function cbNewSticky(p) {
        cbRecord();
        const it = cbAdd({ type: 'sticky', x: p.x - 100, y: p.y - 100, w: 200, h: 200, color: cbColorPref('sticky'), html: '' });
        cbRender();
        cbSave();
        cbStartEdit(it.id, true);
    }
    function cbNewText(p) {
        cbRecord();
        const it = cbAdd({ type: 'text', x: p.x, y: p.y - 16, w: 280, html: '' });
        cbRender();
        cbStartEdit(it.id, true);
    }
    function cbNewFrame(x, y, w, h) {
        const n = cb.items.filter((i) => i.type === 'frame').length + 1;
        return cbAdd({ type: 'frame', x, y, w, h, color: cbColorPref('frame'), title: `Frame ${n}` });
    }
    function cbAddAudio(a, at) {
        const note = noteById(cb.noteId);
        if (!note || isPrivateLocked(note) || !a || !isVoiceSrc(a.src || '')) return;
        if (a.src.length > VOICE_MAX_CHARS) { toast('This recording is too large to store. Keep voice notes under a few minutes.'); return; }
        a = { ...a, src: audioRef(a.src) };
        const p = at || cbViewCenter();
        cbRecord();
        cbAdd({ type: 'audio', x: p.x - 220, y: p.y - 20, w: 440, h: 40, src: a.src, name: a.name || 'Voice note', duration: Math.round(a.duration || 0), loop: !!a.loop, ...(a.vol < 100 ? { volume: a.vol } : {}) });
        cbRender();
        cbSave();
    }
    async function cbAddAudioFiles(files, at) {
        for (const [i, file] of [...files].filter((f) => f.type.startsWith('audio/')).entries()) {
            try {
                const src = await audioFileToStorable(file);
                if (!src) continue;
                if (!isVoiceSrc(src)) throw new Error('unsupported');
                const p = at || cbViewCenter();
                cbAddAudio({ src, name: file.name.replace(/\.[^.]+$/, ''), duration: await audioDuration(src) }, { x: p.x + i * 20, y: p.y + i * 50 });
            } catch (_) { toast(`Could not add audio: ${file.name}`); }
        }
    }
    cbBoard.addEventListener('input', (e) => {
        if (e.target.classList && e.target.classList.contains('voice-seek')) {
            const chip = e.target.closest('.voice-chip');
            if (chip) voiceSeekTo(chip, Number(e.target.value) / 1000);
            return;
        }
        if (!e.target.classList || !e.target.classList.contains('voice-vol')) return;
        const el = e.target.closest('.cb-item');
        const it = el && cbById(el.dataset.id);
        if (it) chipVolumeInput(e.target, (v) => { if (v >= 100) delete it.volume; else it.volume = v; });
    });
    cbBoard.addEventListener('change', (e) => {
        if (e.target.classList && e.target.classList.contains('voice-vol')) cbSave();
    });
    function cbPicActions(it, x, y) {
        return pictureMenuActions(it.src, {
            x, y,
            place: (src) => { if (!cb.items.includes(it)) return; cbRecord(); it.src = src; cbRender(); cbSave(); },
            remove: () => { cb.sel = new Set([it.id]); cbDelete(); }
        });
    }
    function cbVidActions(it) {
        return videoMenuActions(it.src, {
            loop: { on: !!it.loop, toggle: () => { cbRecord(); it.loop = !it.loop; if (!it.loop) delete it.loop; cbRender(); cbSave(); toast(it.loop ? 'Loop on: the video plays again and again' : 'Loop off'); } },
            remove: () => { cb.sel = new Set([it.id]); cbDelete(); }
        });
    }
    // the options of what is selected on the board (colours, text style, media options…), right above it
    var cbCtx = null;
    function updateCbCtx() {
        if (!cbCtx) return;
        if (!cbActive()) { cbCtx.hide(); return; }
        const ids = (cb.editing ? [cb.editing] : [...cb.sel]).filter((id) => cbById(id));
        if (!ids.length) { cbCtx.hide(); return; }
        const items = ids.map(cbById);
        const isMedia = (it) => it.type === 'image' || it.type === 'video' || it.type === 'audio';
        const groups = [];
        if (items.some((it) => !isMedia(it))) groups.push($('cbGroupColors'));
        if (cb.editing || items.some((it) => it.type === 'sticky' || it.type === 'text')) groups.push($('cbGroupFormat'));
        if (!cb.editing) groups.push($('cbGroupArrange'));
        const one = items.length === 1 && isMedia(items[0]) ? items[0] : null;
        cbCtx.show({
            key: `${ids.join(',')}|${cb.editing || ''}|${one ? `${!!one.loop}|${(one.src || '').length}|${one.name || ''}` : ''}`,
            anchor: () => rectOf(ids.flatMap((id) => { const el = cbEls.get(id); return el ? [el, el.querySelector(':scope > .cb-frame-title')] : [cbBoard.querySelector(`[data-arrow="${id}"]`)]; })),
            busy: () => !!cbAct && cbPointers.size > 0,
            groups,
            after: updateCbCtx,
            actions: () => (!one ? [] : one.type === 'image' ? cbPicActions(one, null, null) : one.type === 'video' ? cbVidActions(one)
                : cbAudioActions(one, cbEls.get(one.id) && cbEls.get(one.id).querySelector('.cb-chip'), null, null).filter((a) => !['▶', '⏸', '🔁'].includes(a.icon)))
                .filter((a) => !a.danger) // 🗑 is already in the bar
        });
    }
    function cbAudioActions(it, chip, x, y) {
        return audioMenuActions(chip, {
            x, y,
            loop: !!it.loop,
            setLoop: () => { cbRecord(); it.loop = !it.loop; if (voicePlayingChip === chip && voiceAudio) voiceAudio.loop = it.loop; cbRender(); cbSave(); },
            rename: async () => {
                const name = await askText('Rename audio', it.name || 'Voice note', 'Rename');
                if (name === null || !name.trim()) return;
                cbRecord(); it.name = name.trim(); cbRender(); cbSave();
            },
            remove: () => { if (voicePlayingChip === chip) stopVoicePlayback(); cb.sel = new Set([it.id]); cbDelete(); }
        });
    }
    async function cbPasteItems(items) {
        const c = cbViewCenter();
        let k = 0;
        const next = () => { const o = (k++) * 36; return { x: c.x + o, y: c.y + o }; };
        for (const it of items) {
            if (it.kind === 'image') await cbAddImages([it.src], next());
            else if (it.kind === 'video') {
                cbAddVideo(it.src, next());
                if (it.loop) { const last = cb.items[cb.items.length - 1]; if (last && last.type === 'video') { last.loop = true; cbRender(); cbSave(); } }
            } else cbAddAudio({ src: it.src, name: it.name, duration: it.duration, loop: it.loop, vol: it.vol }, next());
        }
    }
    function cbAddVideo(src, at) {
        const note = noteById(cb.noteId);
        if (!note || isPrivateLocked(note)) return;
        const p = at || cbViewCenter();
        cbRecord();
        cbAdd({ type: 'video', x: p.x - 240, y: p.y - 146, w: 480, h: 292, src });
        cbRender();
        cbSave();
    }
    async function cbAddImages(files, at) {
        const note = noteById(cb.noteId);
        if (!note || isPrivateLocked(note)) return;
        // files, or the web address of a picture (a KLIPY GIF that could not be downloaded)
        const images = [...files].filter((f) => typeof f === 'string' || (f.type && f.type.startsWith('image/')));
        if (!images.length) return;
        const p = at || cbViewCenter();
        let recorded = false, offset = 0;
        const added = [];
        for (const file of images) {
            try {
                const src = await pictureFileToStorable(file);
                if (!src) continue;
                const img = new Image();
                img.src = src;
                await (img.decode ? img.decode().catch(() => {}) : new Promise((r) => { img.onload = img.onerror = r; }));
                const nw = img.naturalWidth || 300, nh = img.naturalHeight || 200;
                const w = Math.min(400, nw), h = w * nh / nw;
                if (!recorded) { cbRecord(); recorded = true; }
                added.push(cbAdd({ type: 'image', x: p.x - w / 2 + offset, y: p.y - h / 2 + offset, w, h, src }, false).id);
                offset += 30;
            } catch (err) {
                toast(`Could not load image: ${file.name}`);
            }
        }
        if (added.length) { cb.sel = new Set(added); cbRender(); cbSave(); }
    }

    /* ---- editing text in a sticky note, a text or a frame title ---- */
    function cbStartEdit(id, fresh = false) {
        const it = cbById(id), el = cbEls.get(id);
        if (!it || !el || !['sticky', 'text', 'frame'].includes(it.type)) return;
        cbEndEdit(true);
        cb.editing = id;
        cb.editStart = fresh ? null : cbSnap();
        if (!fresh) cbRecord();
        cb.sel = new Set([id]);
        const t = it.type === 'frame' ? el.firstChild : el.querySelector('.cb-text');
        el.classList.add('cb-editing');
        t.contentEditable = 'true';
        cbRender();
        t.focus({ preventScroll: true });
        const r = document.createRange();
        r.selectNodeContents(t);
        if (it.type !== 'frame') r.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(r);
    }
    function cbEndEdit(save = true) {
        if (!cb.editing) return;
        const id = cb.editing;
        const it = cbById(id), el = cbEls.get(id);
        cb.editing = null;
        if (!it || !el) return;
        const t = it.type === 'frame' ? el.firstChild : el.querySelector('.cb-text');
        t.contentEditable = 'false';
        el.classList.remove('cb-editing');
        if (it.type === 'frame') it.title = t.textContent.trim() || 'Frame';
        else {
            cbStripPlainColor(t, el);
            it.html = sanitizeHtml(t.innerHTML);
            el._html = null;
            if (it.type === 'text' && !htmlToText(it.html).trim() && !/<img/i.test(it.html)) {
                cb.items = cb.items.filter((i) => i !== it); // an empty text disappears
                cb.sel.delete(id);
            }
        }
        if (cb.editStart !== null && cb.editStart === cbSnap()) cb.undo.pop(); // nothing changed
        cb.editStart = null;
        if (window.getSelection().rangeCount && t.contains(window.getSelection().anchorNode)) window.getSelection().removeAllRanges();
        if (save) cbSave();
        cbRender();
    }
    // focus left the text (and not to the toolbar): stop editing
    cbBoard.addEventListener('focusout', (e) => {
        if (!cb.editing) return;
        if (e.relatedTarget && $('collageToolbar').contains(e.relatedTarget)) return;
        const id = cb.editing;
        setTimeout(() => {
            const el = cbEls.get(id);
            if (cb.editing === id && !(el && el.contains(document.activeElement))) cbEndEdit(true);
        }, 0);
    });
    cbBoard.addEventListener('keydown', (e) => {
        if (!cb.editing) return;
        const editEl = cbEls.get(cb.editing);
        const textEl = editEl && editEl.querySelector('.cb-text');
        if (textEl) keepHeadingOnEnter(e, textEl, updateCbHeadingBtn);
        const it = cbById(cb.editing);
        if (e.key === 'Escape' || (it && it.type === 'frame' && e.key === 'Enter')) { e.preventDefault(); cbEndEdit(true); }
        e.stopPropagation(); // typing never triggers the board shortcuts
    });

    /* ---- formatting (same options as in Text) ---- */
    function cbFormat(fn) {
        if (cb.editing) {
            const el = cbEls.get(cb.editing);
            const t = el && (el.querySelector('.cb-text'));
            if (t) { t.focus({ preventScroll: true }); fn(t); }
            return;
        }
        // selected sticky notes / texts: format all of their text
        const ids = [...cb.sel].filter((id) => { const it = cbById(id); return it && (it.type === 'sticky' || it.type === 'text'); });
        if (!ids.length) { toast('Select a sticky note or text first (double-click to type in it)'); return; }
        cbRecord();
        const sel = window.getSelection();
        ids.forEach((id) => {
            const el = cbEls.get(id), t = el.querySelector('.cb-text');
            t.contentEditable = 'true';
            t.focus({ preventScroll: true });
            const r = document.createRange();
            r.selectNodeContents(t);
            sel.removeAllRanges();
            sel.addRange(r);
            fn(t);
            cbStripPlainColor(t, el);
            const it = cbById(id);
            it.html = sanitizeHtml(t.innerHTML);
            el._html = null;
            t.contentEditable = 'false';
        });
        sel.removeAllRanges();
        cbRender();
        cbSave();
    }
    // colour tags with the normal text colour of this sticky note are not needed
    function cbStripPlainColor(t, el) {
        const plain = cssColorToHex(getComputedStyle(el).color);
        t.querySelectorAll('font[color], span[style*="color"]').forEach((c) => {
            const v = cssColorToHex(c.tagName === 'FONT' ? c.getAttribute('color') : c.style.color);
            if (v && (v === plain || v === COLOR_MARK)) c.replaceWith(...c.childNodes);
        });
    }
    function cbTextColor(t, color) {
        try { document.execCommand('styleWithCSS', false, false); } catch (_) {}
        if (color) { document.execCommand('foreColor', false, color); return; }
        const sel = window.getSelection();
        if (!sel.rangeCount || sel.isCollapsed) {
            // nothing selected: type on in the normal colour of this sticky note / text
            document.execCommand('foreColor', false, cssColorToHex(getComputedStyle(t.parentElement).color) || '#ffffff');
            return;
        }
        const MARK = COLOR_MARK;
        document.execCommand('foreColor', false, MARK);
        t.querySelectorAll('font[color], span[style*="color"]').forEach((el) => {
            const c = el.tagName === 'FONT' ? el.getAttribute('color') : cssColorToHex(el.style.color);
            if (cssColorToHex(c) !== MARK) return;
            el.querySelectorAll('font[color], span[style*="color"]').forEach((inner) => inner.replaceWith(...inner.childNodes));
            el.replaceWith(...el.childNodes);
        });
    }
    $('cbTextColors').innerHTML = TEXT_COLORS.map(([c, name]) =>
        `<button type="button" class="text-color${c ? '' : ' reset'}" data-cb-text-color="${c}" title="${name}" style="${c ? `background:${c}` : ''}">${c ? '' : 'A'}</button>`).join('');
    $('cbColors').innerHTML = CB_COLORS.map(([c, name]) =>
        `<button type="button" class="text-color" data-cb-color="${c}" title="${name}" style="background:${c}"></button>`).join('');

    // toolbar buttons keep the text cursor where it is
    $('collageToolbar').addEventListener('pointerdown', (e) => {
        if (e.target.closest('button')) e.preventDefault();
    });
    $('collageToolbar').addEventListener('click', (e) => {
        if (!noteById(cb.noteId) || isPrivateLocked(noteById(cb.noteId))) return;
        const tool = e.target.closest('[data-cb-tool]');
        if (tool) { cbSetTool(tool.dataset.cbTool); return; }
        const cmd = e.target.closest('[data-cb-cmd]');
        if (cmd) { cbFormat(() => document.execCommand(cmd.dataset.cbCmd, false, null)); return; }
        const tc = e.target.closest('[data-cb-text-color]');
        if (tc) { cbFormat((t) => cbTextColor(t, tc.dataset.cbTextColor)); return; }
        const col = e.target.closest('[data-cb-color]');
        if (col) { cbColor(col.dataset.cbColor); return; }
        const btn = e.target.closest('button');
        if (!btn) return;
        switch (btn.id) {
            case 'cbHeading':
                cbFormat((t) => {
                    const s = window.getSelection();
                    const n = s.rangeCount ? s.anchorNode : null;
                    const inH = n && (n.nodeType === 1 ? n : n.parentElement).closest('h1, h2, h3');
                    document.execCommand('formatBlock', false, inH && t.contains(inH) ? '<div>' : '<h2>');
                });
                updateCbHeadingBtn();
                break;
            case 'cbImageBtn': $('cbImageFile').click(); break;
            case 'cbGifBtn': {
                const at = cbViewCenter();
                openGifPicker(async (url) => { const file = await fetchGifFile(url); cbAddImages([file || url], at); }, (files) => cbAddImages(files, at));
                break;
            }
            case 'cbVideoBtn': {
                const at = cbViewCenter();
                askVideoUrl().then((src) => {
                    if (src && src.files) readVideoFiles(src.files).then((srcs) => srcs.forEach((s, i) => cbAddVideo(s, { x: at.x + i * 30, y: at.y + i * 30 })));
                    else if (src) cbAddVideo(src);
                });
                break;
            }
            case 'cbAudioBtn': openVoiceModal('collage'); break;
            case 'cbUndo': cbUndoRedo(true); break;
            case 'cbRedo': cbUndoRedo(false); break;
            case 'cbDup': cbDuplicate(); break;
            case 'cbFront': cbOrder(true); break;
            case 'cbBack': cbOrder(false); break;
            case 'cbDelete': cbDelete(); break;
            case 'cbZoom': cbZoomAt(cbBoard.clientWidth / 2, cbBoard.clientHeight / 2, 1 / cb.view.s); break;
            case 'cbFit': cbFit(); break;
        }
    });
    $('cbImageFile').addEventListener('change', (e) => {
        cbAddImages(e.target.files);
        e.target.value = '';
    });
    cbCtx = makeCtxBar($('collageToolbar'), [$('cbGroupColors'), $('cbGroupFormat'), $('cbGroupArrange')]);
    addMenu($('cbAddBtn'), [['Picture', '🖼', 'cbImageBtn'], ['GIF', '🎞', 'cbGifBtn'], ['Video file / link', '▶', 'cbVideoBtn'], ['Audio / voice note', '🎤', 'cbAudioBtn']]);

    // colour of the selected sticky notes / frames / arrows / texts (also the colour for new ones)
    function cbColor(color) {
        const ids = [...cb.sel].filter((id) => cbById(id) && cbById(id).type !== 'image' && cbById(id).type !== 'video');
        const prefs = state.prefs.collageColors = state.prefs.collageColors || {};
        if (!ids.length) {
            // nothing selected: the colour for the next sticky note
            prefs.sticky = color;
            settingsChanged();
            toast('New sticky notes get this colour');
            return;
        }
        if (cb.editing) cbEndEdit(true);
        cbRecord();
        ids.forEach((id) => {
            const it = cbById(id);
            it.color = color;
            if (it.type !== 'text') prefs[it.type] = color;
        });
        settingsChanged();
        cbRender();
        cbSave();
    }

    /* ---- selection actions ---- */
    const cbInside = (r, f) => r.x >= f.x && r.y >= f.y && r.x + r.w <= f.x + f.w && r.y + r.h <= f.y + f.h;
    // the selection plus everything inside selected frames
    function cbExpanded(ids) {
        const out = new Set(ids);
        cb.items.forEach((f) => {
            if (f.type !== 'frame' || !ids.has(f.id)) return;
            const fr = cbRect(f);
            cb.items.forEach((it) => {
                if (it === f || out.has(it.id)) return;
                if (it.type === 'arrow') {
                    const [a, b] = cbArrowPoints(it);
                    if (cbInside({ x: a.x, y: a.y, w: 0, h: 0 }, fr) && cbInside({ x: b.x, y: b.y, w: 0, h: 0 }, fr)) out.add(it.id);
                } else if (cbInside(cbRect(it), fr)) out.add(it.id);
            });
        });
        return out;
    }
    function cbCopies(list, dx, dy) {
        const map = new Map();
        const copies = list.filter((it) => it.type !== 'arrow').map((it) => {
            const c = cbClone(it);
            c.id = uid();
            map.set(it.id, c.id);
            c.x += dx; c.y += dy;
            return c;
        });
        list.filter((it) => it.type === 'arrow').forEach((a) => {
            const c = cbClone(a);
            c.id = uid();
            ['from', 'to'].forEach((k) => {
                if (c[k].id) {
                    if (map.has(c[k].id)) c[k] = { id: map.get(c[k].id) };
                    else { const p = cbEndCenter(a[k]); c[k] = { x: p.x + dx, y: p.y + dy }; }
                } else { c[k] = { x: c[k].x + dx, y: c[k].y + dy }; }
            });
            copies.push(c);
        });
        return copies;
    }
    function cbDuplicate() {
        if (!cb.sel.size) return;
        cbEndEdit(true);
        cbRecord();
        const ids = cbExpanded(cb.sel);
        const copies = cbCopies(cb.items.filter((it) => ids.has(it.id)), 24, 24);
        cb.items.push(...copies);
        cb.sel = new Set(copies.map((c) => c.id));
        cbRender();
        cbSave();
    }
    function cbDelete() {
        if (!cb.sel.size) return;
        cbEndEdit(true);
        cbRecord();
        const gone = new Set(cb.sel);
        // arrows attached to something that is deleted keep their end where it was
        cb.items.forEach((a) => {
            if (a.type !== 'arrow' || gone.has(a.id)) return;
            const [p1, p2] = cbArrowPoints(a);
            if (a.from.id && gone.has(a.from.id)) a.from = { x: p1.x, y: p1.y };
            if (a.to.id && gone.has(a.to.id)) a.to = { x: p2.x, y: p2.y };
        });
        cb.items = cb.items.filter((it) => !gone.has(it.id));
        cb.sel = new Set();
        cbRender();
        cbSave();
    }
    function cbOrder(front) {
        if (!cb.sel.size) return;
        cbRecord();
        const picked = cb.items.filter((it) => cb.sel.has(it.id));
        const rest = cb.items.filter((it) => !cb.sel.has(it.id));
        cb.items = front ? [...rest, ...picked] : [...picked, ...rest];
        cbRender();
        cbSave();
    }
    function cbPaste(list, at) {
        const b = cbBounds(list);
        if (!b) return;
        const p = at || cbViewCenter();
        cbRecord();
        const copies = cbCopies(list, p.x - (b.x + b.w / 2), p.y - (b.y + b.h / 2));
        cb.items.push(...copies);
        cb.sel = new Set(copies.map((c) => c.id));
        cbRender();
        cbSave();
    }

    /* ---- mouse / touch on the board ---- */
    const cbPointers = new Map();
    let cbAct = null;
    let cbLastTap = null;
    let cbDownTarget = null;
    let cbMiddleAt = 0; // Linux pastes with the middle mouse button: not on the board
    function cbWorldPt(e) {
        const r = cbBoard.getBoundingClientRect();
        return { x: (e.clientX - r.left - cbBoard.clientLeft - cb.view.x) / cb.view.s, y: (e.clientY - r.top - cbBoard.clientTop - cb.view.y) / cb.view.s };
    }
    function cbLocal(e) {
        const r = cbBoard.getBoundingClientRect();
        return { x: e.clientX - r.left - cbBoard.clientLeft, y: e.clientY - r.top - cbBoard.clientTop };
    }
    // the sticky note / text / image / frame under a screen point (for the ends of an arrow)
    function cbItemAt(clientX, clientY, not) {
        const els = document.elementsFromPoint(clientX, clientY);
        for (const el of els) {
            const item = el.closest && el.closest('.cb-item');
            if (item && cbBoard.contains(item) && item.dataset.id !== not) return cbById(item.dataset.id);
        }
        return null;
    }
    function cbStartPan(e) {
        cbAct = { type: 'pan', sx: e.clientX, sy: e.clientY, vx: cb.view.x, vy: cb.view.y };
        cbBoard.classList.add('panning');
    }

    cbBoard.addEventListener('pointerdown', (e) => {
        if (!cbActive()) return;
        // the volume slider of an audio item: just the slider (no moving of the item)
        if (e.target.closest && e.target.closest('.cb-chip .voice-vol, .cb-chip .voice-seek')) { e.stopPropagation(); return; }
        // the play / loop buttons of an audio item
        const chipBtn = e.button === 0 && e.target.closest && e.target.closest('.cb-chip .voice-play, .cb-chip .voice-loop, .cb-chip .voice-back, .cb-chip .voice-fwd');
        if (chipBtn) {
            const el = chipBtn.closest('.cb-item');
            const it = el && cbById(el.dataset.id);
            if (it) {
                e.preventDefault();
                e.stopPropagation();
                const chip = el.querySelector('.cb-chip');
                if (chipBtn.classList.contains('voice-play')) toggleVoicePlayback(chip);
                else if (chipBtn.classList.contains('voice-back')) voiceSkip(chip, -15);
                else if (chipBtn.classList.contains('voice-fwd')) voiceSkip(chip, 15);
                else { cbRecord(); it.loop = !it.loop; if (voicePlayingChip === chip && voiceAudio) voiceAudio.loop = it.loop; cbRender(); cbSave(); }
                return;
            }
        }
        // right-click on a picture: compress / undo, download, remove
        const imgEl = e.button === 2 && e.target.closest && e.target.closest('.cb-image');
        if (imgEl) {
            const it = cbById(imgEl.dataset.id);
            if (it) {
                e.preventDefault();
                cb.sel = new Set([it.id]);
                cbRender();
                menuAt(e.clientX, e.clientY, 'Picture', cbPicActions(it, e.clientX, e.clientY));
                return;
            }
        }
        // right-click on an audio item: its options
        const audEl = e.button === 2 && e.target.closest && e.target.closest('.cb-audio');
        if (audEl) {
            const it = cbById(audEl.dataset.id);
            if (it) {
                e.preventDefault();
                cb.sel = new Set([it.id]);
                cbRender();
                menuAt(e.clientX, e.clientY, 'Audio', cbAudioActions(it, audEl.querySelector('.cb-chip'), e.clientX, e.clientY));
                return;
            }
        }
        // right-click on a video: its options
        const vidEl = e.button === 2 && e.target.closest && e.target.closest('.cb-video');
        if (vidEl) {
            const it = cbById(vidEl.dataset.id);
            if (it) {
                e.preventDefault();
                cb.sel = new Set([it.id]);
                cbRender();
                menuAt(e.clientX, e.clientY, 'Video', cbVidActions(it));
                return;
            }
        }
        if (cb.editing && e.target.closest('.cb-editing') && e.button === 0) return; // place the text cursor
        cbPointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
        try { cbBoard.setPointerCapture(e.pointerId); } catch (_) { /* ignore */ }
        if (cbPointers.size === 2) {
            // two fingers: zoom and move
            if (cbAct && cbAct.recorded && cbAct.type !== 'pan') { cb.items = cbUnsnap(cb.undo.pop()); cbRender(); }
            const [a, b] = [...cbPointers.values()];
            const r = cbBoard.getBoundingClientRect();
            cbAct = { type: 'pinch', d0: Math.hypot(a.x - b.x, a.y - b.y) || 1, cx: (a.x + b.x) / 2 - r.left, cy: (a.y + b.y) / 2 - r.top, v0: { ...cb.view } };
            cbMarquee.hidden = true;
            return;
        }
        if (cbPointers.size > 2) return;
        cbDownTarget = e.target; // (the pointer is captured: pointerup is always on the board)
        e.preventDefault(); // no mouse focus change: a new text keeps its cursor
        const ae = document.activeElement;
        if (ae && ae !== document.body && !cbBoard.contains(ae) && ae.blur) ae.blur();
        if (cb.editing) cbEndEdit(true);
        const p = cbWorldPt(e);
        if (e.button === 1) cbMiddleAt = Date.now();
        if (e.button === 1 || (e.button === 0 && (cb.tool === 'hand' || cbSpace))) { e.preventDefault(); cbStartPan(e); return; }
        if (e.button !== 0) return;
        const tool = cb.tool;
        if (tool === 'sticky') { cbSetTool('select'); cbNewSticky(p); return; }
        if (tool === 'text') { cbSetTool('select'); cbNewText(p); return; }
        if (tool === 'frame') { cbAct = { type: 'newframe', start: p, id: null }; return; }
        if (tool === 'arrow') {
            const hit = cbItemAt(e.clientX, e.clientY);
            cbAct = { type: 'newarrow', start: p, from: hit ? { id: hit.id } : { x: p.x, y: p.y }, fromId: hit ? hit.id : null, id: null };
            return;
        }
        // select tool
        const end = e.target.closest('.cb-arrow-end');
        if (end) {
            const id = end.parentNode.dataset.arrow;
            cbAct = { type: 'arrowend', id, which: end.dataset.end, recorded: false };
            return;
        }
        if (e.target.closest('.cb-group-handle')) {
            const ids = cbExpanded(cb.sel);
            const b = cbBounds(cb.items.filter((it) => cb.sel.has(it.id)));
            if (!b) return;
            const orig = new Map();
            cb.items.forEach((it) => {
                if (!ids.has(it.id)) return;
                orig.set(it.id, it.type === 'arrow' ? { from: { ...it.from }, to: { ...it.to } } : { x: it.x, y: it.y, w: it.w, h: it.h });
            });
            cbAct = { type: 'groupscale', start: p, b, orig, recorded: false };
            return;
        }
        if (e.target.closest('.cb-handle')) {
            const it = cbById([...cb.sel][0]);
            if (!it) return;
            const r = cbRect(it);
            cbAct = { type: 'resize', id: it.id, start: p, w: it.type === 'text' ? r.w : r.w, h: r.h, ratio: r.h / Math.max(1, r.w), recorded: false };
            return;
        }
        const arrowG = e.target.closest('[data-arrow]');
        const itemEl = e.target.closest('.cb-item');
        const onFrameBody = itemEl && itemEl.classList.contains('cb-frame') && !e.target.closest('.cb-frame-title');
        const id = arrowG ? arrowG.dataset.arrow : (itemEl && !onFrameBody ? itemEl.dataset.id : null);
        if (id) {
            if (e.shiftKey) {
                if (cb.sel.has(id)) cb.sel.delete(id); else cb.sel.add(id);
                cbRender();
                if (!cb.sel.has(id)) return;
            } else if (!cb.sel.has(id)) cb.sel = new Set([id]);
            let ids = cbExpanded(cb.sel);
            const startItems = cbSnap();
            if (e.altKey) {
                // Alt + drag: drag a copy
                cbRecord();
                const copies = cbCopies(cb.items.filter((it) => ids.has(it.id)), 0, 0);
                cb.items.push(...copies);
                cb.sel = new Set(copies.map((c) => c.id));
                ids = new Set(copies.map((c) => c.id));
            }
            const orig = new Map();
            cb.items.forEach((it) => {
                if (!ids.has(it.id)) return;
                if (it.type === 'arrow') orig.set(it.id, { from: { ...it.from }, to: { ...it.to } });
                else orig.set(it.id, { x: it.x, y: it.y });
            });
            cbRender();
            cbAct = { type: 'move', start: p, orig, moved: false, recorded: !!e.altKey, startItems, clickId: id };
            return;
        }
        // empty board (or inside a frame)
        if (e.pointerType === 'touch') { cbStartPan(e); cbAct.tapFrame = onFrameBody ? itemEl.dataset.id : null; return; }
        cbAct = { type: 'marquee', start: p, base: e.shiftKey ? new Set(cb.sel) : new Set(), frame: onFrameBody ? itemEl.dataset.id : null, moved: false };
        if (!e.shiftKey && cb.sel.size) { cb.sel = new Set(); cbRender(); }
    });

    cbBoard.addEventListener('pointermove', (e) => {
        if (cbPointers.has(e.pointerId)) cbPointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
        if (!cbAct) return;
        if (cbAct.type === 'pinch') {
            if (cbPointers.size < 2) return;
            const [a, b] = [...cbPointers.values()];
            const r = cbBoard.getBoundingClientRect();
            const d = Math.hypot(a.x - b.x, a.y - b.y) || 1;
            const cx = (a.x + b.x) / 2 - r.left, cy = (a.y + b.y) / 2 - r.top;
            const v0 = cbAct.v0;
            const s = Math.min(4, Math.max(0.1, v0.s * d / cbAct.d0));
            const wx = (cbAct.cx - v0.x) / v0.s, wy = (cbAct.cy - v0.y) / v0.s;
            cb.view = { s, x: cx - wx * s, y: cy - wy * s };
            cbApplyView();
            cbRenderArrows();
            return;
        }
        if (cbAct.type === 'pan') {
            cb.view.x = cbAct.vx + e.clientX - cbAct.sx;
            cb.view.y = cbAct.vy + e.clientY - cbAct.sy;
            if (Math.hypot(e.clientX - cbAct.sx, e.clientY - cbAct.sy) > 4) cbAct.moved = true;
            cbApplyView();
            return;
        }
        const p = cbWorldPt(e);
        const dx = p.x - (cbAct.start ? cbAct.start.x : p.x), dy = p.y - (cbAct.start ? cbAct.start.y : p.y);
        const far = Math.hypot(dx, dy) * cb.view.s > 3;
        if (cbAct.type === 'move') {
            if (!cbAct.moved && !far) return;
            if (!cbAct.recorded) { cb.undo.push(cbAct.startItems); cb.redo = []; cbAct.recorded = true; }
            cbAct.moved = true;
            cbAct.orig.forEach((o, id) => {
                const it = cbById(id);
                if (!it) return;
                if (it.type === 'arrow') {
                    if (!o.from.id) it.from = { x: o.from.x + dx, y: o.from.y + dy };
                    if (!o.to.id) it.to = { x: o.to.x + dx, y: o.to.y + dy };
                } else { it.x = o.x + dx; it.y = o.y + dy; }
            });
            cbRender();
        } else if (cbAct.type === 'groupscale') {
            const { b, orig } = cbAct;
            // same factor for width and height; the top-left corner stays put
            const f = Math.max(0.1, Math.max((b.w + dx) / Math.max(1, b.w), (b.h + dy) / Math.max(1, b.h)));
            if (!cbAct.recorded) { if (Math.abs(f - 1) < 0.01) return; cbRecord(); cbAct.recorded = true; }
            const sx = (v) => b.x + (v - b.x) * f, sy = (v) => b.y + (v - b.y) * f;
            orig.forEach((o, id) => {
                const it = cbById(id);
                if (!it) return;
                if (it.type === 'arrow') {
                    if (!o.from.id) it.from = { x: sx(o.from.x), y: sy(o.from.y) };
                    if (!o.to.id) it.to = { x: sx(o.to.x), y: sy(o.to.y) };
                    return;
                }
                it.x = sx(o.x); it.y = sy(o.y);
                it.w = Math.max(20, o.w * f);
                if (o.h != null) it.h = Math.max(20, o.h * f);
            });
            cbRender();
        } else if (cbAct.type === 'resize') {
            const it = cbById(cbAct.id);
            if (!it) return;
            if (!cbAct.recorded) { cbRecord(); cbAct.recorded = true; }
            it.w = Math.max(40, cbAct.w + dx);
            if (it.type === 'image' || it.type === 'video' || e.shiftKey) it.h = Math.max(20, it.w * cbAct.ratio);
            else if (it.type !== 'text') it.h = Math.max(it.type === 'frame' ? 60 : 40, cbAct.h + dy);
            cbRender();
        } else if (cbAct.type === 'newframe') {
            if (!cbAct.id && !far) return;
            if (!cbAct.id) { cbRecord(); cbAct.id = cbNewFrame(p.x, p.y, 1, 1).id; }
            const it = cbById(cbAct.id);
            it.x = Math.min(cbAct.start.x, p.x); it.y = Math.min(cbAct.start.y, p.y);
            it.w = Math.max(40, Math.abs(dx)); it.h = Math.max(40, Math.abs(dy));
            cbRender();
        } else if (cbAct.type === 'newarrow' || cbAct.type === 'arrowend') {
            if (cbAct.type === 'newarrow' && !cbAct.id) {
                if (!far) return;
                cbRecord();
                cbAct.id = cbAdd({ type: 'arrow', from: cbAct.from, to: { x: p.x, y: p.y }, color: cbColorPref('arrow') }).id;
            }
            if (cbAct.type === 'arrowend' && !cbAct.recorded) { cbRecord(); cbAct.recorded = true; }
            const a = cbById(cbAct.id);
            if (!a) return;
            const which = cbAct.type === 'newarrow' ? 'to' : cbAct.which;
            const other = which === 'to' ? a.from : a.to;
            const hit = cbItemAt(e.clientX, e.clientY, other.id);
            a[which] = hit ? { id: hit.id } : { x: p.x, y: p.y };
            cbRender();
        } else if (cbAct.type === 'marquee') {
            if (!cbAct.moved && !far) return;
            cbAct.moved = true;
            const x1 = Math.min(cbAct.start.x, p.x), y1 = Math.min(cbAct.start.y, p.y);
            const r = { x: x1, y: y1, w: Math.abs(dx), h: Math.abs(dy) };
            const s = cb.view.s;
            cbMarquee.hidden = false;
            Object.assign(cbMarquee.style, { left: `${x1 * s + cb.view.x}px`, top: `${y1 * s + cb.view.y}px`, width: `${r.w * s}px`, height: `${r.h * s}px` });
            const sel = new Set(cbAct.base);
            cb.items.forEach((it) => {
                if (it.type === 'arrow') {
                    const [a, b] = cbArrowPoints(it);
                    if (cbInside({ x: a.x, y: a.y, w: 0, h: 0 }, r) && cbInside({ x: b.x, y: b.y, w: 0, h: 0 }, r)) sel.add(it.id);
                    return;
                }
                const ir = cbRect(it);
                const hit = it.type === 'frame' ? cbInside(ir, r)
                    : !(ir.x > r.x + r.w || ir.x + ir.w < r.x || ir.y > r.y + r.h || ir.y + ir.h < r.y);
                if (hit) sel.add(it.id);
            });
            cb.sel = sel;
            cbRender();
        }
    });

    function cbPointerEnd(e) {
        const wasPinch = cbAct && cbAct.type === 'pinch';
        cbPointers.delete(e.pointerId);
        if (wasPinch) { if (!cbPointers.size) cbAct = null; return; }
        if (!cbAct) return;
        const act = cbAct;
        cbAct = null;
        cbBoard.classList.remove('panning');
        cbMarquee.hidden = true;
        const p = cbWorldPt(e);
        let tapped = false;
        if (act.type === 'move') {
            if (act.moved) cbSave(); else tapped = true;
            if (act.recorded && !act.moved) cbSave(); // an Alt-click copy
        } else if (act.type === 'resize' || act.type === 'arrowend' || act.type === 'groupscale') {
            if (act.recorded) cbSave();
        } else if (act.type === 'newframe') {
            if (!act.id) { cbRecord(); cbNewFrame(p.x - 320, p.y - 210, 640, 420); }
            cbSetTool('select');
            cbRender();
            cbSave();
        } else if (act.type === 'newarrow') {
            if (act.id) { cbSetTool('select'); cbSave(); }
        } else if (act.type === 'marquee') {
            if (!act.moved) {
                tapped = true;
                if (act.frame) { cb.sel = new Set([act.frame]); cbRender(); }
            }
        } else if (act.type === 'pan') {
            if (!act.moved) {
                tapped = true;
                if (act.tapFrame) { cb.sel = new Set([act.tapFrame]); cbRender(); }
                else if (cb.sel.size) { cb.sel = new Set(); cbRender(); }
            }
        }
        // a double click / double tap
        if (tapped && e.button === 0 && e.type === 'pointerup') {
            const now = Date.now();
            if (cbLastTap && now - cbLastTap.t < 400 && Math.hypot(e.clientX - cbLastTap.x, e.clientY - cbLastTap.y) < 12) {
                cbLastTap = null;
                cbDouble(e, p);
            } else cbLastTap = { t: now, x: e.clientX, y: e.clientY };
        } else cbLastTap = null;
    }
    cbBoard.addEventListener('pointerup', cbPointerEnd);
    cbBoard.addEventListener('pointercancel', cbPointerEnd);

    function cbDouble(e, p) {
        const target = cbDownTarget && cbDownTarget.isConnected ? cbDownTarget : e.target;
        const itemEl = target.closest && target.closest('.cb-item');
        if (itemEl) {
            const it = cbById(itemEl.dataset.id);
            if (it && (it.type === 'sticky' || it.type === 'text')) { cbStartEdit(it.id); return; }
            if (it && it.type === 'frame' && target.closest('.cb-frame-title')) { cbStartEdit(it.id); return; }
            if (it && (it.type === 'image' || it.type === 'video')) return;
        }
        if (target.closest && target.closest('[data-arrow]')) return;
        cbNewSticky(p); // double-click on the board: a new sticky note
    }

    cbBoard.addEventListener('wheel', (e) => {
        if (!cbActive()) return;
        if (e.ctrlKey || e.metaKey || rightZoomWheel(e)) {
            // Ctrl + scroll (or right button + scroll): zoom in small steps
            e.preventDefault();
            const l = cbLocal(e);
            cbZoomAt(l.x, l.y, wheelZoomFactor(e));
        } else if (e.shiftKey) {
            // Shift + scroll: move the board sideways
            e.preventDefault();
            cb.view.x -= wheelSideways(e);
            cbApplyView();
        }
        // plain scrolling is left alone: it moves the page up and down
    }, { passive: false });
    cbBoard.addEventListener('auxclick', (e) => { if (e.button === 1) e.preventDefault(); });
    cbBoard.addEventListener('contextmenu', (e) => { if (cbActive() && !cb.editing) e.preventDefault(); });

    // drop pictures on the board
    cbBoard.addEventListener('dragover', (e) => {
        if (Array.from(e.dataTransfer.types || []).includes('Files')) e.preventDefault();
    });
    cbBoard.addEventListener('drop', (e) => {
        const files = [...(e.dataTransfer.files || [])].filter((f) => f.type.startsWith('image/'));
        const videos = [...(e.dataTransfer.files || [])].filter(isVideoFile);
        const audios = [...(e.dataTransfer.files || [])].filter((f) => f.type.startsWith('audio/'));
        if (!files.length && !videos.length && !audios.length) return;
        e.preventDefault();
        if (audios.length) cbAddAudioFiles(audios, cbWorldPt(e));
        if (videos.length) {
            const at = cbWorldPt(e);
            readVideoFiles(videos).then((srcs) => srcs.forEach((src, i) => cbAddVideo(src, { x: at.x + i * 30, y: at.y + i * 30 })));
        }
        if (files.length) cbAddImages(files, cbWorldPt(e));
    });

    /* ---- keyboard, copy and paste ---- */
    const cbTypingElsewhere = (e) => {
        const t = e.target.closest && e.target.closest('input, textarea, select, [contenteditable="true"], [contenteditable=""]');
        return !!(t && t.offsetParent !== null);
    };
    document.addEventListener('keydown', (e) => {
        if (e.key === ' ' && !e.repeat && cbActive() && !cb.editing && !cbTypingElsewhere(e)) { cbSpace = true; e.preventDefault(); return; }
        if (!cbActive() || cb.editing || cbTypingElsewhere(e)) return;
        const k = e.key.toLowerCase();
        const mod = e.ctrlKey || e.metaKey;
        if (e.key === 'Delete' || e.key === 'Backspace') { if (cb.sel.size) { e.preventDefault(); cbDelete(); } return; }
        if (e.key === 'Escape') { cb.sel = new Set(); cbSetTool('select'); cbRender(); return; }
        if (e.key === 'Enter' && cb.sel.size === 1) { e.preventDefault(); cbStartEdit([...cb.sel][0]); return; }
        if (mod && !e.altKey && k === 'z') { e.preventDefault(); cbUndoRedo(!e.shiftKey); return; }
        if (mod && !e.altKey && k === 'y') { e.preventDefault(); cbUndoRedo(false); return; }
        if (mod && k === 'd') { e.preventDefault(); cbDuplicate(); return; }
        if (mod && k === 'a') { e.preventDefault(); cb.sel = new Set(cb.items.map((i) => i.id)); cbRender(); return; }
        if (mod && (e.key === '=' || e.key === '+')) { e.preventDefault(); cbZoomAt(cbBoard.clientWidth / 2, cbBoard.clientHeight / 2, 1.25); return; }
        if (mod && e.key === '-') { e.preventDefault(); cbZoomAt(cbBoard.clientWidth / 2, cbBoard.clientHeight / 2, 0.8); return; }
        if (mod || e.altKey) return;
        const tools = { v: 'select', h: 'hand', n: 'sticky', t: 'text', f: 'frame', a: 'arrow', l: 'arrow' };
        if (tools[k]) { e.preventDefault(); cbSetTool(tools[k]); }
    });
    document.addEventListener('keyup', (e) => { if (e.key === ' ') cbSpace = false; });
    window.addEventListener('blur', () => { cbSpace = false; });

    function cbCopySelection(e, cut) {
        if (!cbActive() || cb.editing || cbTypingElsewhere(e) || !cb.sel.size) return;
        const ids = cbExpanded(cb.sel);
        cbClipboard = cbClone(cb.items.filter((it) => ids.has(it.id)));
        e.clipboardData.setData('text/plain', CB_MARK);
        // pictures, videos and audio also for Text and Draw
        const media = cbClipboard.filter((it) => it.type === 'image' || it.type === 'video' || it.type === 'audio')
            .map((it) => (it.type === 'image' ? { kind: 'image', src: it.src } : it.type === 'video' ? { kind: 'video', src: it.src, loop: !!it.loop }
                : { kind: 'audio', src: it.src, name: it.name, duration: it.duration, loop: !!it.loop, vol: Number.isFinite(it.volume) ? it.volume : 100 }));
        if (media.length) writeClipItems(e, media, CB_MARK);
        e.preventDefault();
        if (cbClipboard.length === 1 && cbClipboard[0].type === 'image') { const src = cbClipboard[0].src; setTimeout(() => copyPictureSrc(src, true), 0); }
        if (cut) cbDelete();
    }
    document.addEventListener('copy', (e) => cbCopySelection(e, false));
    document.addEventListener('cut', (e) => cbCopySelection(e, true));
    document.addEventListener('paste', (e) => {
        if (!cbActive() || !e.clipboardData) return;
        if (Date.now() - cbMiddleAt < 1500) { e.preventDefault(); return; }
        // pictures, videos and audio from Text / Draw (a copy made on the board itself is pasted as board items)
        if (!cb.editing && !cbTypingElsewhere(e) && !(e.clipboardData.getData('text/plain') === CB_MARK && cbClipboard)) {
            const clipItems = readClipItems(e, true);
            if (clipItems.length) { e.preventDefault(); cbPasteItems(clipItems); return; }
        }
        const copiedVid = !cb.editing && !cbTypingElsewhere(e) && pastedVideoRef(e);
        if (copiedVid) { e.preventDefault(); cbAddVideo(copiedVid); return; }
        const copiedAud = !cb.editing && !cbTypingElsewhere(e) && pastedAudio(e);
        if (copiedAud) { e.preventDefault(); cbAddAudio(copiedAud); return; }
        const ownPics = !cbTypingElsewhere(e) ? ownPictureCopy(e) : [];
        if (ownPics.length) {
            e.preventDefault();
            // copied on the board itself: paste it like the board does (same size)
            if (ownPics.length === 1 && cbClipboard && cbClipboard.length === 1 && cbClipboard[0].type === 'image' && cbClipboard[0].src === ownPics[0]) cbPaste(cbClipboard);
            else cbAddImages(ownPics);
            return;
        }
        const audFiles = !cb.editing ? [...(e.clipboardData.files || [])].filter((f) => f.type.startsWith('audio/')) : [];
        if (audFiles.length) { e.preventDefault(); cbAddAudioFiles(audFiles); return; }
        const files = [...(e.clipboardData.files || [])].filter((f) => f.type.startsWith('image/'));
        if (cb.editing) {
            // pictures pasted while typing become pictures on the board; text goes in the note
            if (files.length) { e.preventDefault(); cbAddImages(files); }
            return;
        }
        if (cbTypingElsewhere(e)) return;
        e.preventDefault();
        if (files.length) { cbAddImages(files); return; }
        const text = e.clipboardData.getData('text/plain');
        if (text === CB_MARK && cbClipboard) { cbPaste(cbClipboard); return; }
        const html = e.clipboardData.getData('text/html');
        const pics = pastedPictureSrcs(html);
        if (pics.length > 1 || (pics.length && text === PIC_MARK)) { cbAddImages(pics); return; }
        const m = html && /<img[^>]+src="(data:image\/[^"]+)"/i.exec(html);
        if (m) {
            fetch(m[1].replace(/&quot;/g, '"')).then((r) => r.blob())
                .then((blob) => cbAddImages([new File([blob], 'pasted', { type: blob.type || 'image/png' })]))
                .catch(() => toast('Could not paste that picture'));
            return;
        }
        const vid = text && !/\s/.test(text.trim()) && videoEmbedUrl(text);
        if (vid) { cbAddVideo(vid); return; }
        if (text && text.trim()) {
            const c = cbViewCenter();
            cbRecord();
            cbAdd({ type: 'text', x: c.x - 140, y: c.y - 16, w: 280, html: sanitizeHtml(html || escapeHtml(text).replace(/\n/g, '<br>')) });
            cbRender();
            cbSave();
        }
    });
    window.addEventListener('resize', () => { if (state.prefs.mode === 'collage') cbRenderArrows(); });
    // the Heading button is lit while you type in a heading line
    function updateCbHeadingBtn() {
        const el = cb.editing && cbEls.get(cb.editing);
        const t = el && el.querySelector('.cb-text');
        $('cbHeading').classList.toggle('active', !!(t && headingAt(t)));
    }
    document.addEventListener('selectionchange', () => { if (state.prefs.mode === 'collage') updateCbHeadingBtn(); });
    // Ctrl (+ scroll) over a video must zoom the board, not the page: the video stops catching the mouse
    const setCtrlHeld = (on) => document.body.classList.toggle('ctrl-held', on);
    document.addEventListener('keydown', (e) => { if (e.key === 'Control' || e.key === 'Meta') setCtrlHeld(true); });
    document.addEventListener('keyup', (e) => { if (e.key === 'Control' || e.key === 'Meta') setCtrlHeld(false); });
    window.addEventListener('blur', () => setCtrlHeld(false));
    document.addEventListener('pointermove', (e) => { if (!e.ctrlKey && !e.metaKey && document.body.classList.contains('ctrl-held')) setCtrlHeld(false); }, { passive: true });


    /* ---------------- grid mode (fill cells by drawing over them) ---------------- */

    const gridCanvas = $('gridCanvas');
    const gctx = gridCanvas.getContext('2d');
    const gridContainer = $('gridContainer');
    const GRID_TYPES = ['triangle', 'square', 'hexagon', 'octagon'];
    const GRID_CELL_AREA = 1 / 2500; // ≈ 50 × 50 cells, same cell size for every shape
    const gridCache = {};
    let gridTool = 'pen';
    let gridStroke = null;   // { changes: Map(id -> previous color), last: {x, y} }
    let drawRedo = [];       // drawing versions that Ctrl+Y can bring back
    let drawUndo = [];       // earlier versions of the drawing (lists of items) for Ctrl+Z
    // Call before every change to the drawing: remembers the drawing as it is now
    function drawSnapshot() {
        const snap = state.draw.strokes.slice();
        const note = noteById(state.currentNoteId);
        snap.bg = note ? note.canvasBackground : undefined; // the canvas colour is part of the version too
        return snap;
    }
    function pushDrawUndo() {
        drawUndo.push(drawSnapshot());
        if (drawUndo.length > 100) drawUndo.shift();
        drawRedo = [];
    }
    function restoreDrawSnapshot(snap) {
        state.draw.strokes = snap.slice();
        const note = noteById(state.currentNoteId);
        if (note && 'bg' in snap && (note.canvasBackground || undefined) !== (snap.bg || undefined)) {
            if (snap.bg) note.canvasBackground = snap.bg; else delete note.canvasBackground;
            touch(note);
        }
    }
    let gridHistory = [];    // undo steps for the open note
    let gridRedo = [];       // undone steps that Ctrl+Y can redo

    // Cell size: L = the original size, M = half, S = a quarter (side length)
    const GRID_SIZES = { L: 1, M: 0.5, S: 0.25 };
    // Turning a grid by this many degrees gives the same grid again
    const GRID_PERIOD = { square: 90, octagon: 90, triangle: 180, hexagon: 180 };
    const GRID_STEP = { square: 45, octagon: 45, triangle: 90, hexagon: 90 };

    function gridSettings(note) {
        const type = currentGridType(note);
        const size = GRID_SIZES[note && note.gridSize] ? note.gridSize : 'L';
        let rot = ((((note && +note.gridRotation) || 0) % GRID_PERIOD[type]) + GRID_PERIOD[type]) % GRID_PERIOD[type];
        rot -= rot % GRID_STEP[type]; // triangles and hexagons only turn in steps of 90°
        return { type, size, rot };
    }
    // Where the filled cells are stored: the original L / 0° grids keep their old key
    // every grid (type / size / turn) has its own background colour; Draw has note.canvasBackground
    const gridBgOf = (note) => (note && note.gridBgs && note.gridBgs[gridKey(note)]) || DEFAULT_CANVAS_BG;
    // the background colour of what is on screen now (Draw, or the grid that is open)
    function setCanvasBg(note, color) {
        if (state.prefs.mode === 'grid') {
            note.gridBgs = note.gridBgs || {};
            note.gridBgs[gridKey(note)] = color;
        } else note.canvasBackground = color;
    }
    function gridKey(note) {
        const { type, size, rot } = gridSettings(note);
        return size === 'L' && rot === 0 ? type : `${type}@${size}@${rot}`;
    }
    const gridOf = (note) => { const g = gridSettings(note); return buildGrid(g.type, g.size, g.rot); };

    // All cell polygons for a grid, in coordinates 0..1 (the canvas is square)
    function buildGrid(type, size = 'L', rot = 0) {
        const cacheKey = `${type}@${size}@${rot}`;
        if (gridCache[cacheKey]) return gridCache[cacheKey];
        const raw = [];
        const add = (id, pts) => raw.push({ id, pts });
        // A turned grid must also cover the corners: build it on a larger area around the canvas
        const lo = rot ? 0.5 - Math.SQRT1_2 - 0.02 : 0;
        const span = rot ? 2 * (Math.SQRT1_2 + 0.02) : 1;
        const k = GRID_SIZES[size] || 1;
        const A = GRID_CELL_AREA * k * k / (span * span); // cell area on the 0..1 building area

        if (type === 'square') {
            const s = Math.sqrt(A);
            const n = Math.round(1 / s);
            for (let r = 0; r < n; r++) for (let c = 0; c < n; c++) {
                add(`${c}.${r}`, [[c * s, r * s], [(c + 1) * s, r * s], [(c + 1) * s, (r + 1) * s], [c * s, (r + 1) * s]]);
            }
        } else if (type === 'triangle') {
            const a = 0.75 * Math.sqrt(4 * A / Math.sqrt(3)); // triangles are a bit smaller (25%)
            const h = a * Math.sqrt(3) / 2;
            const rows = Math.ceil(1 / h);
            const per = Math.ceil(2 / a) + 2;
            for (let r = 0; r < rows; r++) {
                const y = r * h;
                for (let j = 0; j <= per; j++) {
                    const x = (j - 1) * a / 2;
                    const up = (j + r) % 2 === 0;
                    add(`${j}.${r}`, up
                        ? [[x, y + h], [x + a, y + h], [x + a / 2, y]]
                        : [[x, y], [x + a, y], [x + a / 2, y + h]]);
                }
            }
        } else if (type === 'hexagon') {
            const R = Math.sqrt(2 * A / (3 * Math.sqrt(3)));
            const w = Math.sqrt(3) * R;
            const step = 1.5 * R;
            const rows = Math.ceil(1 / step) + 1;
            const cols = Math.ceil(1 / w) + 1;
            for (let r = 0; r <= rows; r++) for (let c = -1; c <= cols; c++) {
                const cx = c * w + (r % 2 ? w / 2 : 0);
                const cy = r * step;
                const pts = [];
                for (let q = 0; q < 6; q++) {
                    const t = (Math.PI / 180) * (60 * q - 90);
                    pts.push([cx + R * Math.cos(t), cy + R * Math.sin(t)]);
                }
                add(`${c}.${r}`, pts);
            }
        } else if (type === 'octagon') {
            // octagons with small diamonds in between (truncated square tiling)
            const s = Math.sqrt(A);
            const n = Math.round(1 / s);
            const t = s / (2 + Math.SQRT2);
            const q = s / 2 - t;
            for (let j = 0; j < n; j++) for (let i = 0; i < n; i++) {
                const cx = (i + 0.5) * s, cy = (j + 0.5) * s;
                add(`o${i}.${j}`, [[cx - s / 2, cy - q], [cx - q, cy - s / 2], [cx + q, cy - s / 2], [cx + s / 2, cy - q],
                    [cx + s / 2, cy + q], [cx + q, cy + s / 2], [cx - q, cy + s / 2], [cx - s / 2, cy + q]]);
            }
            for (let j = 0; j <= n; j++) for (let i = 0; i <= n; i++) {
                const x = i * s, y = j * s;
                add(`s${i}.${j}`, [[x, y - t], [x + t, y], [x, y + t], [x - t, y]]);
            }
        }

        // place on the canvas (scale + turn around the middle) and keep the cells that are visible
        const cos = Math.cos(rot * Math.PI / 180), sin = Math.sin(rot * Math.PI / 180);
        const cells = [];
        raw.forEach(({ id, pts }) => {
            let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
            const out = pts.map(([px, py]) => {
                let x = lo + px * span, y = lo + py * span;
                if (rot) {
                    const dx = x - 0.5, dy = y - 0.5;
                    x = 0.5 + dx * cos - dy * sin;
                    y = 0.5 + dx * sin + dy * cos;
                }
                x0 = Math.min(x0, x); y0 = Math.min(y0, y); x1 = Math.max(x1, x); y1 = Math.max(y1, y);
                return [x, y];
            });
            if (x1 <= 0 || y1 <= 0 || x0 >= 1 || y0 >= 1) return;
            cells.push({ id, pts: out, x0, y0, x1, y1 });
        });

        // bucket index for fast "which cell is under the pen?"
        const B = Math.round(40 / k);
        const buckets = Array.from({ length: B * B }, () => []);
        const clampB = (v) => Math.min(B - 1, Math.max(0, Math.floor(v * B)));
        cells.forEach((c) => {
            for (let by = clampB(c.y0); by <= clampB(c.y1); by++) {
                for (let bx = clampB(c.x0); bx <= clampB(c.x1); bx++) buckets[by * B + bx].push(c);
            }
        });
        const byId = new Map(cells.map((c) => [c.id, c]));
        const cellSide = Math.sqrt(GRID_CELL_AREA) * k;
        gridCache[cacheKey] = { cells, buckets, B, clampB, byId, cellSide };
        return gridCache[cacheKey];
    }

    function pointInPoly(x, y, pts) {
        let inside = false;
        for (let i = 0, j = pts.length - 1; i < pts.length; j = i++) {
            const [xi, yi] = pts[i], [xj, yj] = pts[j];
            if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
        }
        return inside;
    }

    function gridHit(grid, x, y) {
        const list = grid.buckets[grid.clampB(y) * grid.B + grid.clampB(x)];
        for (const c of list) {
            if (x < c.x0 || x > c.x1 || y < c.y0 || y > c.y1) continue;
            if (pointInPoly(x, y, c.pts)) return c;
        }
        return null;
    }

    function currentGridType(note) {
        return GRID_TYPES.includes(note && note.gridType) ? note.gridType : 'square';
    }

    function gridCellsOf(note) {
        const type = gridKey(note);
        if (!note.grids || typeof note.grids !== 'object') note.grids = {};
        if (!note.grids[type] || typeof note.grids[type] !== 'object') note.grids[type] = {};
        return note.grids[type];
    }

    function gridLineColor(bg) {
        const m = /^#?([0-9a-f]{6})$/i.exec(bg || '');
        const v = m ? parseInt(m[1], 16) : 0;
        const lum = (0.299 * ((v >> 16) & 255) + 0.587 * ((v >> 8) & 255) + 0.114 * (v & 255)) / 255;
        return lum > 0.55 ? 'rgba(0,0,0,0.22)' : 'rgba(255,255,255,0.16)';
    }

    // A drawing target: canvas context + how 0..1 grid coordinates map to its pixels
    const mainGridView = () => ({ g: gctx, sx: gridCanvas.width, sy: gridCanvas.height, ox: 0, oy: 0 });

    function tracePoly(c, v) {
        const { g, sx, sy, ox, oy } = v;
        g.moveTo(ox + c.pts[0][0] * sx, oy + c.pts[0][1] * sy);
        for (let i = 1; i < c.pts.length; i++) g.lineTo(ox + c.pts[i][0] * sx, oy + c.pts[i][1] * sy);
        g.closePath();
    }

    function drawGridOn(v, note, bg) {
        const { g } = v;
        g.setTransform(1, 0, 0, 1, 0, 0);
        g.fillStyle = bg;
        g.fillRect(0, 0, g.canvas.width, g.canvas.height);
        if (!note) return;
        const grid = gridOf(note);
        const cells = gridCellsOf(note);
        // only the cells that are visible on this target
        const x0 = -v.ox / v.sx, y0 = -v.oy / v.sy;
        const x1 = (g.canvas.width - v.ox) / v.sx, y1 = (g.canvas.height - v.oy) / v.sy;
        const list = (v.ox || v.oy || x1 < 1 || y1 < 1)
            ? grid.cells.filter((c) => c.x1 >= x0 && c.x0 <= x1 && c.y1 >= y0 && c.y0 <= y1)
            : grid.cells;
        list.forEach((c) => {
            const color = cells[c.id];
            if (!color) return;
            g.beginPath();
            tracePoly(c, v);
            g.fillStyle = color;
            g.fill();
        });
        g.strokeStyle = gridLineColor(bg);
        g.lineWidth = Math.max(1, v.sx / 1600);
        // draw the lines in small batches: one huge path is very slow for the small (S) grids
        for (let i = 0; i < list.length; i += 100) {
            g.beginPath();
            for (let j = i; j < Math.min(i + 100, list.length); j++) tracePoly(list[j], v);
            g.stroke();
        }
        drawGridPickOverlay(v);
    }

    function renderGrid() {
        const note = noteById(state.currentNoteId);
        const bg = gridBgOf(note);
        drawGridOn(mainGridView(), note, bg);
        renderGridDetail();
    }

    function paintGridCell(c, color, bg) {
        const views = [mainGridView()];
        if (gridDetailView) views.push(gridDetailView);
        views.forEach((v) => {
            v.g.beginPath();
            tracePoly(c, v);
            v.g.fillStyle = color || bg;
            v.g.fill();
            v.g.strokeStyle = gridLineColor(bg);
            v.g.lineWidth = Math.max(1, v.sx / 1600);
            v.g.stroke();
        });
    }

    /* Sharp zoom: when zoomed in, an extra canvas on top shows only the visible part,
     * drawn at the screen's full resolution (the big canvas underneath only gets stretched). */
    const gridDetail = document.createElement('canvas');
    gridDetail.className = 'grid-detail';
    gridDetail.hidden = true;
    gridContainer.appendChild(gridDetail);
    const gridDetailCtx = gridDetail.getContext('2d');
    let gridDetailView = null;
    let gridDetailTimer = null;
    let gridZoomState = { scale: 1, tx: 0, ty: 0 };

    function onGridZoom(z) {
        gridZoomState = { scale: z.scale, tx: z.tx, ty: z.ty };
        // while zooming/moving show the stretched picture, sharpen it as soon as you stop
        gridDetail.hidden = true;
        gridDetailView = null;
        clearTimeout(gridDetailTimer);
        if (z.scale > 1) gridDetailTimer = setTimeout(renderGridDetail, 120);
    }

    function renderGridDetail() {
        const { scale, tx, ty } = gridZoomState;
        if (scale <= 1 || gridContainer.style.display === 'none') {
            gridDetail.hidden = true;
            gridDetailView = null;
            return;
        }
        const note = noteById(state.currentNoteId);
        const dpr = Math.min(3, window.devicePixelRatio || 1);
        const w = gridContainer.clientWidth, h = gridCanvas.offsetHeight;
        gridDetail.style.width = `${w}px`;
        gridDetail.style.height = `${h}px`;
        gridDetail.width = Math.round(w * dpr);
        gridDetail.height = Math.round(h * dpr);
        gridDetailView = {
            g: gridDetailCtx,
            sx: gridCanvas.offsetWidth * scale * dpr,
            sy: gridCanvas.offsetHeight * scale * dpr,
            ox: tx * dpr,
            oy: ty * dpr
        };
        drawGridOn(gridDetailView, note, gridBgOf(note));
        gridDetail.hidden = false;
    }

    function fitGrid() {
        if (!gridContainer || gridContainer.style.display === 'none') return;
        const cssW = Math.max(200, gridContainer.clientWidth);
        const W = Math.round(cssW * backingScale(cssW));
        if (gridCanvas.width !== W || gridCanvas.height !== W) {
            gridCanvas.width = W;
            gridCanvas.height = W;
        }
        renderGrid();
        gridZoom.apply();
    }

    function gridPos(e) {
        const rect = gridCanvas.getBoundingClientRect();
        return { x: (e.clientX - rect.left) / rect.width, y: (e.clientY - rect.top) / rect.height };
    }

    function gridPaintAt(note, pos) {
        const grid = gridOf(note);
        const c = gridHit(grid, pos.x, pos.y);
        if (!c) return;
        const cells = gridCellsOf(note);
        const prev = cells[c.id];
        const next = gridTool === 'eraser' || (gridStroke && gridStroke.erase) ? undefined : state.draw.color;
        if (prev === next) return;
        if (!gridStroke.changes.has(c.id)) gridStroke.changes.set(c.id, prev);
        if (next) cells[c.id] = next; else delete cells[c.id];
        paintGridCell(c, next, gridBgOf(note));
    }

    gridCanvas.addEventListener('pointerdown', (e) => {
        if (state.prefs.mode !== 'grid' || gridZoom.isPinching()) return;
        const note = noteById(state.currentNoteId);
        if (!note) return;
        e.preventDefault();
        try { gridCanvas.setPointerCapture(e.pointerId); } catch (_) { /* ignore */ }
        const pos = gridPos(e);
        // the right mouse button always erases (whatever tool is chosen)
        const erase = isEraseButton(e);
        if (!erase && e.button > 0) return;
        if (erase && (gridStroke || gridPickDrag || gridMarquee)) return;
        if (gridTool === 'select' && !erase) { startGridPick(note, pos, e.altKey, e.shiftKey); return; }
        gridStroke = { changes: new Map(), last: pos, type: gridKey(note), erase };
        gridPaintAt(note, pos);
    });
    gridCanvas.addEventListener('contextmenu', (e) => e.preventDefault()); // right button = eraser

    gridCanvas.addEventListener('pointermove', (e) => {
        if (gridTool === 'select' && !gridStroke) { moveGridPick(e); return; }
        if (!gridStroke) return;
        e.preventDefault();
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const pos = gridPos(e);
        const { last } = gridStroke;
        const dist = Math.hypot(pos.x - last.x, pos.y - last.y);
        const steps = Math.max(1, Math.ceil(dist / (gridOf(note).cellSide * 0.25)));
        for (let i = 1; i <= steps; i++) {
            gridPaintAt(note, { x: last.x + (pos.x - last.x) * (i / steps), y: last.y + (pos.y - last.y) * (i / steps) });
        }
        gridStroke.last = pos;
    });

    function endGridStroke() {
        if (gridPickDrag || gridMarquee || gridScaleDrag) { endGridPick(); return; }
        if (!gridStroke) return;
        const stroke = gridStroke;
        gridStroke = null;
        const note = noteById(state.currentNoteId);
        if (!note || !stroke.changes.size) return;
        gridHistory.push(stroke);
        gridRedo = [];
        if (gridHistory.length > 100) gridHistory.shift();
        touch(note);
        renderTree();
    }

    function revertGridChanges(note, stroke) {
        const cells = note.grids[stroke.type] || (note.grids[stroke.type] = {});
        stroke.after = new Map();   // remember the painted colors so the step can be redone
        stroke.changes.forEach((prev, id) => {
            stroke.after.set(id, cells[id]);
            if (prev) cells[id] = prev; else delete cells[id];
        });
    }

    // A pinch started while the first finger was already painting: undo that paint
    function cancelGridStroke() {
        const note = noteById(state.currentNoteId);
        if (gridStroke && note) revertGridChanges(note, gridStroke);
        gridStroke = null;
        renderGrid();
    }

    gridCanvas.addEventListener('pointerup', endGridStroke);
    gridCanvas.addEventListener('pointercancel', endGridStroke);

    const gridZoom = createZoom(gridContainer, gridCanvas, cancelGridStroke, onGridZoom);

    function setGridTool(tool) {
        gridTool = tool;
        if (tool !== 'select' && (gridPick.length || gridCopyMode)) { gridPick = []; gridCopyMode = false; renderGrid(); }
        $('gridPen').classList.toggle('active', tool === 'pen');
        $('gridEraser').classList.toggle('active', tool === 'eraser');
        $('gridSelect').classList.toggle('active', tool === 'select');
        gridCanvas.style.cursor = tool === 'eraser' ? 'cell' : tool === 'select' ? 'default' : 'crosshair';
        updateGridPickBar();
    }

    /* ---------- select tool in the grid: pick filled cells, move / copy / delete them ---------- */
    let gridPick = [];        // ids of the selected cells
    let gridMarquee = null;   // { a, b } while dragging a selection box
    let gridPickDrag = null;  // while moving the selection
    let gridScaleDrag = null; // while scaling the selection with its corner handle
    function gridPickBox(grid) {
        let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
        gridPick.forEach((id) => {
            const c = grid.byId.get(id);
            if (!c) return;
            x0 = Math.min(x0, c.x0); y0 = Math.min(y0, c.y0); x1 = Math.max(x1, c.x1); y1 = Math.max(y1, c.y1);
        });
        return x0 === Infinity ? null : { x0, y0, x1, y1 };
    }
    function onGridScaleHandle(pos, box) {
        if (!box) return false;
        const r = gridCanvas.getBoundingClientRect();
        return Math.abs(pos.x - box.x1) * r.width <= 12 && Math.abs(pos.y - box.y1) * r.height <= 12;
    }
    // the selected colours drawn again, f times as big, from the top-left corner of the selection
    function scaleGridCells(grid, colors, box, f) {
        const colorOf = new Map(colors);
        const back = (q) => ({ x: box.x0 + (q.x - box.x0) / f, y: box.y0 + (q.y - box.y0) / f });
        const nx1 = box.x0 + (box.x1 - box.x0) * f, ny1 = box.y0 + (box.y1 - box.y0) * f;
        const out = [];
        grid.cells.forEach((t) => {
            if (t.x1 < box.x0 || t.x0 > nx1 || t.y1 < box.y0 || t.y0 > ny1) return;
            const m = cellCenter(t);
            const spots = [m, ...[0, 1, 2, 3].map((k) => {
                const v = t.pts[Math.floor(k * t.pts.length / 4)];
                return { x: m.x + (v[0] - m.x) * 0.5, y: m.y + (v[1] - m.y) * 0.5 };
            })];
            const votes = new Map();
            let hits = 0;
            spots.forEach((q) => {
                const c = gridHit(grid, ...Object.values(back(q)));
                const col = c && colorOf.get(c.id);
                if (!col) return;
                hits++;
                votes.set(col, (votes.get(col) || 0) + 1);
            });
            if (hits < (f < 1 ? 1 : 2)) return;
            let best = null, n = 0;
            votes.forEach((v, col) => { if (v > n) { n = v; best = col; } });
            out.push([t.id, best]);
        });
        return out;
    }
    let gridCopyMode = false; // the next move leaves the originals where they are (a copy)
    let gridClipboard = null; // { key, cells: [[id, colour]] }
    const GRID_COPY_MARK = '[Remmin grid selection]';
    const cellCenter = (c) => {
        let x = 0, y = 0;
        c.pts.forEach(([px, py]) => { x += px; y += py; });
        return { x: x / c.pts.length, y: y / c.pts.length };
    };
    // same kind of cell (triangles up/down, octagon vs small square), so a move keeps the pattern
    const cellKind = (c) => `${c.pts.length}:${Math.round(Math.atan2(c.pts[1][1] - c.pts[0][1], c.pts[1][0] - c.pts[0][0]) * 100)}`;

    function updateGridPickBar() {
        const show = gridTool === 'select' && gridPick.length > 0;
        $('gridPickActions').hidden = !show;
        if (show) $('gridPickCount').textContent = `${gridPick.length} cell${gridPick.length === 1 ? '' : 's'}${gridCopyMode ? ' · copy' : ''}`;
    }

    // outline of the selected cells and the selection box (drawn on top of the grid)
    function drawGridPickOverlay(v) {
        if (!gridPick.length && !gridMarquee) return;
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const grid = gridOf(note);
        const { g } = v;
        const px = Math.max(1, v.sx / 800);
        g.save();
        g.setLineDash([5 * px, 3 * px]);
        g.lineWidth = 2 * px;
        g.strokeStyle = '#a78bfa';
        g.fillStyle = 'rgba(167,139,250,.18)';
        gridPick.forEach((id) => {
            const c = grid.byId.get(id);
            if (!c) return;
            g.beginPath();
            tracePoly(c, v);
            g.fill();
            g.stroke();
        });
        // the corner handle: drag it to make the selection bigger / smaller
        const pb = gridPick.length && !gridMarquee && !gridPickDrag ? gridPickBox(grid) : null;
        if (pb) {
            const hs = 13 * px, cx = v.ox + pb.x1 * v.sx, cy = v.oy + pb.y1 * v.sy;
            g.setLineDash([]);
            g.fillStyle = '#7c6ff0';
            g.fillRect(cx - hs / 2, cy - hs / 2, hs, hs);
            g.fillStyle = '#ffffff';
            g.fillRect(cx - hs / 4, cy - hs / 4, hs / 2, hs / 2);
        }
        if (gridMarquee) {
            const { a, b } = gridMarquee;
            g.strokeStyle = '#c4b5fd';
            g.fillStyle = 'rgba(196,181,253,.12)';
            const x = v.ox + Math.min(a.x, b.x) * v.sx, y = v.oy + Math.min(a.y, b.y) * v.sy;
            g.fillRect(x, y, Math.abs(b.x - a.x) * v.sx, Math.abs(b.y - a.y) * v.sy);
            g.strokeRect(x, y, Math.abs(b.x - a.x) * v.sx, Math.abs(b.y - a.y) * v.sy);
        }
        g.restore();
    }

    function startGridPick(note, pos, alt, shift = false) {
        const grid = gridOf(note);
        const cells = gridCellsOf(note);
        const hit = gridHit(grid, pos.x, pos.y);
        const pbox = gridPick.length && !shift ? gridPickBox(grid) : null;
        if (pbox && onGridScaleHandle(pos, pbox)) {
            gridScaleDrag = { key: gridKey(note), base: { ...cells }, box: pbox, start: pos, f: 1,
                colors: gridPick.map((id) => [id, cells[id]]).filter(([, c]) => c), orig: gridPick.slice() };
            gridCanvas.style.cursor = 'nwse-resize';
            return;
        }
        if (shift) {
            // Shift + click: add or remove a filled cell · Shift + drag: add the cells in the box
            if (hit && cells[hit.id]) {
                gridPick = gridPick.includes(hit.id) ? gridPick.filter((id) => id !== hit.id) : [...gridPick, hit.id];
                gridCopyMode = false;
                renderGrid();
                updateGridPickBar();
                return;
            }
            gridMarquee = { a: pos, b: pos, base: gridPick.slice() };
            renderGrid();
            return;
        }
        if (hit && gridPick.includes(hit.id)) {
            // drag the selection (with Alt, or after "Copy": leave the originals)
            gridPickDrag = {
                key: gridKey(note), anchor: hit, base: { ...cells }, moved: false, copy: gridCopyMode || alt,
                colors: gridPick.map((id) => [id, cells[id]]), targets: gridPick.slice()
            };
            gridCanvas.style.cursor = 'move';
            return;
        }
        if (hit && cells[hit.id]) {
            // press on a filled cell: select it and move it right away
            gridPick = [hit.id];
            gridCopyMode = false;
            gridPickDrag = { key: gridKey(note), anchor: hit, base: { ...cells }, moved: false, copy: alt,
                colors: [[hit.id, cells[hit.id]]], targets: [hit.id] };
            renderGrid();
            updateGridPickBar();
            return;
        }
        gridPick = [];
        gridCopyMode = false;
        gridMarquee = { a: pos, b: pos };
        renderGrid();
        updateGridPickBar();
    }

    function moveGridPick(e) {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const pos = gridPos(e);
        const grid = gridOf(note);
        if (gridScaleDrag) {
            e.preventDefault();
            const d = gridScaleDrag, b = d.box;
            const bw = Math.max(b.x1 - b.x0, 1e-4), bh = Math.max(b.y1 - b.y0, 1e-4);
            const f = Math.min(10, Math.max(0.15, Math.max((bw + pos.x - d.start.x) / bw, (bh + pos.y - d.start.y) / bh)));
            if (Math.abs(f - d.f) < 0.02) return;
            d.f = f;
            const cells = { ...d.base };
            d.colors.forEach(([id]) => { delete cells[id]; });
            const scaled = Math.abs(f - 1) < 0.02 ? d.colors : scaleGridCells(grid, d.colors, b, f);
            scaled.forEach(([id, c]) => { cells[id] = c; });
            note.grids[d.key] = cells;
            gridPick = scaled.map(([id]) => id);
            renderGrid();
            return;
        }
        if (gridPickDrag) {
            e.preventDefault();
            const d = gridPickDrag;
            const over = gridHit(grid, pos.x, pos.y);
            if (!over || cellKind(over) !== cellKind(d.anchor)) return; // wait for a matching cell
            const a = cellCenter(d.anchor), o = cellCenter(over);
            const dx = o.x - a.x, dy = o.y - a.y;
            const targets = [];
            const cells = { ...d.base };
            if (!d.copy) d.colors.forEach(([id]) => { delete cells[id]; });
            for (const [id, color] of d.colors) {
                const c = grid.byId.get(id);
                if (!c) continue;
                const cc = cellCenter(c);
                const t = gridHit(grid, cc.x + dx, cc.y + dy);
                if (!t) continue; // moved off the grid
                cells[t.id] = color;
                targets.push(t.id);
            }
            d.moved = over.id !== d.anchor.id;
            d.targets = targets;
            note.grids[d.key] = cells;
            gridPick = targets;
            renderGrid();
            return;
        }
        if (gridMarquee) {
            e.preventDefault();
            gridMarquee.b = pos;
            renderGrid();
            return;
        }
        const hit = gridHit(grid, pos.x, pos.y);
        if (gridPick.length && onGridScaleHandle(pos, gridPickBox(grid))) { gridCanvas.style.cursor = 'nwse-resize'; return; }
        gridCanvas.style.cursor = hit && (gridPick.includes(hit.id) || gridCellsOf(note)[hit.id]) ? 'move' : 'default';
    }

    // record the change between two versions of the cells as one undo step
    function commitGridChange(note, key, before) {
        const after = note.grids[key] || {};
        const changes = new Map();
        new Set([...Object.keys(before), ...Object.keys(after)]).forEach((id) => {
            if (before[id] !== after[id]) changes.set(id, before[id]);
        });
        if (!changes.size) return false;
        gridHistory.push({ changes, type: key });
        gridRedo = [];
        if (gridHistory.length > 100) gridHistory.shift();
        touch(note);
        return true;
    }

    function endGridPick() {
        if (gridScaleDrag) {
            const d = gridScaleDrag;
            gridScaleDrag = null;
            const note = noteById(state.currentNoteId);
            if (note && Math.abs(d.f - 1) >= 0.02) commitGridChange(note, d.key, d.base);
            else if (note) { note.grids[d.key] = d.base; gridPick = d.orig; }
            renderGrid();
            updateGridPickBar();
            return;
        }
        if (!gridPickDrag && !gridMarquee) return; // e.g. after a Shift + click
        const note = noteById(state.currentNoteId);
        if (gridPickDrag) {
            const d = gridPickDrag;
            gridPickDrag = null;
            if (note && d.moved) {
                commitGridChange(note, d.key, d.base);
                gridCopyMode = false;
            } else if (note) {
                note.grids[d.key] = d.base;
                gridPick = d.colors.map(([id]) => id);
            }
            renderGrid();
            updateGridPickBar();
            return;
        }
        const { a, b } = gridMarquee;
        const base = gridMarquee.base || [];
        gridMarquee = null;
        if (note) {
            const x0 = Math.min(a.x, b.x), x1 = Math.max(a.x, b.x), y0 = Math.min(a.y, b.y), y1 = Math.max(a.y, b.y);
            const grid = gridOf(note);
            const cells = gridCellsOf(note);
            // every filled cell whose middle is inside the box
            const inside = grid.cells.filter((c) => {
                if (!cells[c.id]) return false;
                const m = cellCenter(c);
                return m.x >= x0 && m.x <= x1 && m.y >= y0 && m.y <= y1;
            }).map((c) => c.id);
            gridPick = [...new Set([...base, ...inside])];
        }
        renderGrid();
        updateGridPickBar();
    }

    function deleteGridPick() {
        const note = noteById(state.currentNoteId);
        if (!note || !gridPick.length) return false;
        const key = gridKey(note);
        const before = { ...gridCellsOf(note) };
        const cells = { ...before };
        gridPick.forEach((id) => { delete cells[id]; });
        note.grids[key] = cells;
        commitGridChange(note, key, before);
        gridPick = [];
        gridCopyMode = false;
        renderGrid();
        updateGridPickBar();
        return true;
    }
    function copyGridPick() {
        if (!gridPick.length) return;
        gridCopyMode = true;
        updateGridPickBar();
        toast('Copy made — drag it to where you want it');
    }
    function gridPickActive() {
        if (state.prefs.mode !== 'grid' || state.prefs.activeTab === 'calendar' || !state.currentNoteId) return false;
        if (document.querySelector('.modal.active')) return false;
        const t = document.activeElement && document.activeElement.closest && document.activeElement.closest('input, textarea, select, [contenteditable="true"]');
        return !(t && t.offsetParent !== null);
    }
    // the selected cells as a picture (PNG), for pasting into the text
    function gridPickPng(note) {
        const grid = gridOf(note);
        const cells = gridCellsOf(note);
        const list = gridPick.map((id) => grid.byId.get(id)).filter(Boolean);
        if (!list.length) return null;
        let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
        list.forEach((c) => { x0 = Math.min(x0, c.x0); y0 = Math.min(y0, c.y0); x1 = Math.max(x1, c.x1); y1 = Math.max(y1, c.y1); });
        const side = Math.min(gridCanvas.width, 1600 / Math.max(x1 - x0, y1 - y0, 0.001));
        const pad = 6;
        const out = document.createElement('canvas');
        out.width = Math.max(1, Math.round((x1 - x0) * side) + 2 * pad);
        out.height = Math.max(1, Math.round((y1 - y0) * side) + 2 * pad);
        const g = out.getContext('2d');
        const bg = gridBgOf(note);
        g.fillStyle = bg;
        g.fillRect(0, 0, out.width, out.height);
        const v = { g, sx: side, sy: side, ox: pad - x0 * side, oy: pad - y0 * side };
        g.strokeStyle = gridLineColor(bg);
        g.lineWidth = Math.max(1, side / 1600);
        list.forEach((c) => {
            g.beginPath();
            tracePoly(c, v);
            g.fillStyle = cells[c.id] || bg;
            g.fill();
            g.stroke();
        });
        return out.toDataURL('image/png');
    }

    // Copied cells from one grid → the cells of another grid (other size, shape or angle).
    // Each target cell looks at 5 spots (its middle + 4 spots towards its corners) in the copied
    // part; if at least 2 land on copied cells, it gets the colour that came up most.
    function resampleGridCells(clip, target) {
        const src = buildGrid(clip.settings.type, clip.settings.size, clip.settings.rot);
        const colorOf = new Map(clip.cells);
        let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
        clip.cells.forEach(([id]) => {
            const c = src.byId.get(id);
            if (!c) return;
            x0 = Math.min(x0, c.x0); y0 = Math.min(y0, c.y0); x1 = Math.max(x1, c.x1); y1 = Math.max(y1, c.y1);
        });
        if (x0 === Infinity) return [];
        const out = [];
        target.cells.forEach((t) => {
            if (t.x1 < x0 || t.x0 > x1 || t.y1 < y0 || t.y0 > y1) return;
            const m = cellCenter(t);
            const spots = [m, ...[0, 1, 2, 3].map((k) => {
                const v = t.pts[Math.floor(k * t.pts.length / 4)];
                return { x: m.x + (v[0] - m.x) * 0.5, y: m.y + (v[1] - m.y) * 0.5 };
            })];
            const votes = new Map();
            let hits = 0;
            spots.forEach((q) => {
                const c = gridHit(src, q.x, q.y);
                const col = c && colorOf.get(c.id);
                if (!col) return;
                hits++;
                votes.set(col, (votes.get(col) || 0) + 1);
            });
            if (hits < 2) return;
            let best = null, n = 0;
            votes.forEach((v, col) => { if (v > n) { n = v; best = col; } });
            out.push([t.id, best]);
        });
        return out;
    }

    ['copy', 'cut'].forEach((type) => document.addEventListener(type, (e) => {
        if (!gridPickActive() || gridTool !== 'select' || !gridPick.length) return;
        const note = noteById(state.currentNoteId);
        e.preventDefault();
        const cells = gridCellsOf(note);
        gridClipboard = { key: gridKey(note), settings: gridSettings(note), cells: gridPick.map((id) => [id, cells[id]]).filter(([, c]) => c) };
        if (e.clipboardData) {
            e.clipboardData.setData('text/plain', GRID_COPY_MARK);
            e.clipboardData.setData('text/html', copyImgHtml(gridPickPng(note))); // pastes as a picture in the text
        }
        if (type === 'cut') deleteGridPick();
        else toast('Copied — Ctrl+V pastes it here (then drag it), or as a picture in the text');
    }));
    document.addEventListener('paste', (e) => {
        if (!gridPickActive() || !gridClipboard) return;
        const text = e.clipboardData ? e.clipboardData.getData('text/plain') : '';
        if (text !== GRID_COPY_MARK) return;
        const note = noteById(state.currentNoteId);
        if (!note || isPrivateLocked(note)) return;
        e.preventDefault();
        // put the copied colours on the cells, selected and ready to be dragged away;
        // copied from another size / shape / angle: draw it again on this grid's cells
        const key = gridKey(note);
        const pasted = gridClipboard.key === key ? gridClipboard.cells : resampleGridCells(gridClipboard, gridOf(note));
        if (!pasted.length) { toast('Nothing to paste: the copied part is too small for this grid'); return; }
        const before = { ...gridCellsOf(note) };
        const cells = { ...before };
        pasted.forEach(([id, c]) => { cells[id] = c; });
        note.grids[key] = cells;
        commitGridChange(note, key, before);
        gridPick = pasted.map(([id]) => id);
        gridCopyMode = true;
        if (gridTool !== 'select') setGridTool('select');
        renderGrid();
        updateGridPickBar();
        toast('Pasted — drag it to where you want it');
    });

    $('gridPen').addEventListener('click', () => setGridTool('pen'));
    $('gridSelect').addEventListener('click', () => setGridTool('select'));
    $('gridPickCopy').addEventListener('click', copyGridPick);
    $('gridPickDelete').addEventListener('click', deleteGridPick);
    $('gridEraser').addEventListener('click', () => setGridTool('eraser'));
    $('gridColorSwatch').addEventListener('click', () => openModal('colorModal'));
    $('gridBg').addEventListener('click', () => $('toolCanvasBg').click());

    $('gridType').addEventListener('change', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        note.gridType = $('gridType').value;
        gridPick = []; gridCopyMode = false;
        gridHistory = [];
        gridRedo = [];
        updateGridControls(note);
        touch(note);
        renderGrid();
    });

    function updateGridControls(note) {
        const g = gridSettings(note);
        $('gridSize').value = g.size;
        $('gridRotate').textContent = `⟳ ${g.rot}°`;
        $('gridRotate').title = `Turn the grid ${GRID_STEP[g.type]}° (now ${g.rot}°)`;
    }

    $('gridSize').addEventListener('change', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        note.gridSize = $('gridSize').value;
        gridPick = []; gridCopyMode = false;
        gridHistory = [];
        gridRedo = [];
        updateGridControls(note);
        touch(note);
        renderGrid();
    });

    $('gridRotate').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        const g = gridSettings(note);
        note.gridRotation = (g.rot + GRID_STEP[g.type]) % GRID_PERIOD[g.type];
        gridPick = []; gridCopyMode = false;
        gridHistory = [];
        gridRedo = [];
        updateGridControls(note);
        touch(note);
        renderGrid();
    });

    $('gridUndo').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        const stroke = gridHistory.pop();
        if (!note || !stroke) return;
        revertGridChanges(note, stroke);
        gridRedo.push(stroke);
        gridPick = []; gridCopyMode = false; updateGridPickBar();
        touch(note);
        renderGrid();
    });

    function redoGrid() {
        const note = noteById(state.currentNoteId);
        const stroke = gridRedo.pop();
        if (!note || !stroke || !stroke.after) return;
        const cells = note.grids[stroke.type] || (note.grids[stroke.type] = {});
        stroke.after.forEach((value, id) => {
            if (value) cells[id] = value; else delete cells[id];
        });
        gridHistory.push(stroke);
        touch(note);
        renderGrid();
    }

    $('gridClear').addEventListener('click', async () => {
        const note = noteById(state.currentNoteId);
        if (!note || !Object.keys(gridCellsOf(note)).length) return;
        if (!(await askConfirm('Clear grid', 'Clear all filled cells of this grid?', 'Clear'))) return;
        note.grids[gridKey(note)] = {};
        gridHistory = [];
        gridRedo = [];
        touch(note);
        renderGrid();
        renderTree();
    });

    window.addEventListener('resize', fitGrid);


    /* ---------------- kleuren ---------------- */

    function buildPalette(host, onPick) {
        const persistentToast = host.querySelector('.toast[data-persistent="true"]');
        if (!persistentToast) host.innerHTML = '';
        PALETTE.forEach((color) => {
            const b = document.createElement('button');
            b.className = 'palette-color';
            b.style.background = color;
            b.title = (PALETTE_NAMED.find(([c]) => c === color) || [, color])[1];
            b.addEventListener('click', () => {
                onPick(color);
                closeModals();
            });
            host.appendChild(b);
        });
    }

    function openModal(id) {
        $(id).classList.add('active');
    }

    function closeModals() {
        if (dialogResolve) { finishDialog(false); return; }
        if ($('voiceModal').classList.contains('active')) resetVoiceModal();
        if ($('emailHelpModal').classList.contains('active')) { $('emailHelpModal').classList.remove('active'); return; }
        if ($('emailModal').classList.contains('active')) { closeEmailModal(); return; }
        document.querySelectorAll('.modal.active').forEach((m) => m.classList.remove('active'));
    }

    /* In-app dialogs. Browser prompt()/confirm() can be silently blocked
     * (e.g. after ticking "prevent this page from creating dialogs"). */
    let dialogResolve = null;

    function askDialog({ title, message = '', input = null, okLabel = 'OK', danger = false, altLabel = null, password = false }) {
        return new Promise((resolve) => {
            if (dialogResolve) finishDialog(false);
            dialogResolve = resolve;
            $('dialogAlt').hidden = !altLabel;
            $('dialogAlt').textContent = altLabel || '';
            $('dialogTitle').textContent = title;
            $('dialogMessage').textContent = message;
            $('dialogMessage').hidden = !message;
            const field = $('dialogInput');
            field.hidden = input === null;
            field.value = input === null ? '' : input;
            field.type = password ? 'password' : 'text';
            field.autocomplete = password ? 'off' : '';
            field.placeholder = password ? 'Private Note Pass' : '';
            const ok = $('dialogOk');
            ok.textContent = okLabel;
            ok.classList.toggle('btn-danger', danger);
            ok.classList.toggle('btn-primary', !danger);
            $('dialogModal').classList.add('active');
            setTimeout(() => {
                if (input !== null) { field.focus(); field.select(); } else ok.focus();
            }, 30);
        });
    }

    function finishDialog(ok) {
        if (!dialogResolve) return;
        const resolve = dialogResolve;
        dialogResolve = null;
        $('dialogModal').classList.remove('active');
        if (ok === 'alt') { resolve('alt'); return; }
        if ($('dialogInput').hidden) resolve(!!ok);
        else resolve(ok ? $('dialogInput').value : null);
    }

    const askText = (title, value, okLabel = 'OK') => askDialog({ title, input: value || '', okLabel });
    const askConfirm = (title, message, okLabel = 'OK') =>
        askDialog({ title, message, okLabel, danger: /delete|clear|replace/i.test(okLabel) });

    $('dialogOk').addEventListener('click', () => finishDialog(true));
    $('dialogCancel').addEventListener('click', () => finishDialog(false));
    $('dialogAlt').addEventListener('click', () => finishDialog('alt'));
    $('dialogClose').addEventListener('click', () => finishDialog(false));
    $('dialogBackdrop').addEventListener('click', () => finishDialog(false));
    $('dialogInput').addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); finishDialog(true); }
    });

    buildPalette($('palette'), (color) => {
        state.draw.color = color;
        $('colorSwatch').style.background = color;
        $('gridColorSwatch').style.background = color;
        $('colorCustom').value = color;
        setTool('pen');
        setGridTool('pen');
    });

    buildPalette($('canvasPalette'), (color) => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        setCanvasBg(note, color);
        $('canvasBgCustom').value = color;
        touch(note);
        renderStrokes();
        renderGrid();
    });

    $('colorSwatch').addEventListener('click', () => openModal('colorModal'));
    $('colorClose').addEventListener('click', closeModals);
    $('colorBackdrop').addEventListener('click', closeModals);

    $('colorCustom').addEventListener('input', (e) => {
        state.draw.color = e.target.value;
        $('colorSwatch').style.background = e.target.value;
        $('gridColorSwatch').style.background = e.target.value;
        setTool('pen');
        setGridTool('pen');
    });

    $('toolCanvasBg').addEventListener('click', () => {
        const note = noteById(state.currentNoteId);
        if (note) $('canvasBgCustom').value = state.prefs.mode === 'grid' ? gridBgOf(note) : (note.canvasBackground || DEFAULT_CANVAS_BG);
        openModal('canvasBgModal');
    });

    $('canvasBgClose').addEventListener('click', closeModals);
    $('canvasBgBackdrop').addEventListener('click', closeModals);
    $('canvasBgCustom').addEventListener('input', (e) => {
        const note = noteById(state.currentNoteId);
        if (!note) return;
        setCanvasBg(note, e.target.value);
        touch(note);
        renderStrokes();
        renderGrid();
    });

    // the Text / Draw / Grid bar sticks to the top; the toolbars stick just below it
    function measureModeBar() {
        const bar = document.querySelector('#viewNote .editor-toolbar');
        if (bar && bar.offsetHeight) document.documentElement.style.setProperty('--modebar-h', `${bar.offsetHeight}px`);
    }
    window.addEventListener('resize', measureModeBar);
    setTimeout(measureModeBar, 0);

    /* ---------------- zijbalk-layout ---------------- */

    const app = $('app');

    function applyLayout() {
        app.style.setProperty('--sidebar-width', `${state.prefs.sidebarWidth}px`);
        app.classList.toggle('sidebar-right', state.prefs.sidebarSide === 'right');
        const collapsed = state.prefs.sidebarCollapsed && !isMobile();
        app.classList.toggle('sidebar-collapsed', collapsed);
        $('toggleSidebarSide').title = isMobile() ? 'Close sidebar' : 'Move sidebar left or right';
        $('expandSidebar').style.display = collapsed ? 'block' : 'none';
        $('expandSidebar').style.left = state.prefs.sidebarSide === 'right' ? 'auto' : '8px';
        $('expandSidebar').style.right = state.prefs.sidebarSide === 'right' ? '8px' : 'auto';
        $('expandSidebar').textContent = String.fromCharCode(state.prefs.sidebarSide === 'right' ? 8249 : 8250);
        $('collapseSidebar').textContent = String.fromCharCode(state.prefs.sidebarSide === 'right' ? 8250 : 8249);
    }

    function toggleSidebarSide() {
        if (isMobile()) {
            app.classList.toggle('mobile-open');
            return;
        }
        state.prefs.sidebarSide = state.prefs.sidebarSide === 'left' ? 'right' : 'left';
        savePrefs();
        applyLayout();
        if (typeof applyHubs === 'function') applyHubs();
    }

    $('toggleSidebarSide').addEventListener('click', toggleSidebarSide);
    // Hamburger button: opens the settings window
    $('toggleSidebarMain').addEventListener('click', () => openSettings());
    function openSettings(tab) {
        syncHolidayCheckboxes();
        showSettingsTab(tab || state.settingsTab || 'holidays');
        $('settingsModal').classList.add('active');
    }
    function showSettingsTab(tab) {
        state.settingsTab = tab;
        document.querySelectorAll('.settings-tab').forEach((b) => {
            const on = b.dataset.tab === tab;
            b.classList.toggle('active', on);
            b.setAttribute('aria-selected', on ? 'true' : 'false');
        });
        document.querySelectorAll('[data-settings-panel]').forEach((p) => { p.hidden = p.dataset.settingsPanel !== tab; });
        if (tab === 'pass') updatePassPanel();
        if (tab === 'admin') $('broadcastInput').value = broadcastText();
    }

    /* ---------------- moving lines on the site background ----------------
     * An axonometric grid of dots (every other column half a step lower) that only shows near
     * thin lines travelling along the grid. A line fades out strongly towards its tail and a
     * little towards its head. Drawn on one canvas behind everything. */
    (function backgroundLines() {
        const canvas = document.createElement('canvas');
        canvas.className = 'bg-lines';
        canvas.setAttribute('aria-hidden', 'true');
        document.body.prepend(canvas);
        const g = canvas.getContext('2d');
        const STEP = 24;                       // distance between neighbouring dots
        const COL = STEP * Math.sqrt(3) / 2;   // distance between columns
        const TRAIL = 9;                       // a line is this many steps long
        const REACH = 54;                      // dots show up to this far from a line
        const SPEED = 46;                      // px per second
        const reduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        let W = 0, H = 0, dpr = 1, lines = [], last = 0, running = false;
        // 0 = normal (white), 1 = gold: while a private note is open the lines turn gold (fading over)
        let gold = 0;
        const GOLD = [232, 190, 72];
        const privateOpen = () => {
            const n = state.prefs.activeTab !== 'calendar' && noteById(state.currentNoteId);
            return !!(n && n.private);
        };
        const tint = () => GOLD.map((c) => Math.round(255 + (c - 255) * gold)).join(', ');

        const nodeXY = (i, j) => ({ x: i * COL, y: j * STEP + (i & 1 ? STEP / 2 : 0) });
        // the six neighbours: 0 up, 1 right-up, 2 right-down, 3 down, 4 left-down, 5 left-up
        function neighbour(i, j, d) {
            const odd = i & 1;
            switch (d) {
                case 0: return [i, j - 1];
                case 1: return [i + 1, j + (odd ? 0 : -1)];
                case 2: return [i + 1, j + (odd ? 1 : 0)];
                case 3: return [i, j + 1];
                case 4: return [i - 1, j + (odd ? 1 : 0)];
                default: return [i - 1, j + (odd ? 0 : -1)];
            }
        }
        function newLine() {
            const i = Math.floor(Math.random() * (W / COL + 2)) - 1;
            const j = Math.floor(Math.random() * (H / STEP + 2)) - 1;
            return { nodes: [[i, j]], dir: Math.floor(Math.random() * 6), t: 0, age: 0, life: 9 + Math.random() * 10 };
        }
        function resize() {
            dpr = Math.min(2, window.devicePixelRatio || 1);
            W = window.innerWidth; H = window.innerHeight;
            canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
            g.setTransform(dpr, 0, 0, dpr, 0, 0);
            const want = Math.max(4, Math.min(22, Math.round((W * H) / 95000)));
            while (lines.length < want) { const l = newLine(); l.age = Math.random() * l.life * 0.6; lines.push(l); }
            lines.length = want;
        }
        // brightness along a line: u = 0 at the tail .. 1 at the head
        const shade = (u) => (u < 0.78 ? Math.pow(u / 0.78, 1.6) : 1 - ((u - 0.78) / 0.22) * 0.5);

        function step(l, dt) {
            l.age += dt;
            l.t += (SPEED * dt) / STEP;
            while (l.t >= 1) {
                l.t -= 1;
                const [i, j] = l.nodes[l.nodes.length - 1];
                l.nodes.push(neighbour(i, j, l.dir));
                if (l.nodes.length > TRAIL + 1) l.nodes.shift();
                const r = Math.random();
                if (r < 0.14) l.dir = (l.dir + 1) % 6; else if (r < 0.28) l.dir = (l.dir + 5) % 6;
            }
        }
        // the points of a line from tail to head, with how bright each one is
        function points(l) {
            const n = l.nodes;
            const head = neighbour(n[n.length - 1][0], n[n.length - 1][1], l.dir);
            const all = [...n, head].map(([i, j]) => nodeXY(i, j));
            const full = n.length > TRAIL; // the tail moves along too once the line is full length
            const pts = [];
            for (let k = 0; k < all.length - 1; k++) {
                const a = all[k], b = all[k + 1];
                const from = k === 0 && full ? l.t : 0;
                const to = k === all.length - 2 ? l.t : 1;
                for (let s = 0; s <= 4; s++) {
                    const f = from + (to - from) * (s / 4);
                    if (s === 0 && k > 0) continue;
                    pts.push({ x: a.x + (b.x - a.x) * f, y: a.y + (b.y - a.y) * f });
                }
            }
            const len = pts.length - 1 || 1;
            pts.forEach((p, k) => { p.a = shade(k / len); });
            return pts;
        }

        function frame(now) {
            if (!running) return;
            const dt = Math.min(0.05, (now - (last || now)) / 1000) * (reduced ? 0.3 : 1);
            last = now;
            gold += ((privateOpen() ? 1 : 0) - gold) * Math.min(1, dt * 3);
            const rgb = tint();
            g.clearRect(0, 0, W, H);
            const dots = new Map();
            lines.forEach((l, idx) => {
                step(l, dt);
                const fade = Math.min(1, l.age / 1.2, (l.life - l.age) / 1.2);
                const pts = points(l);
                const headPt = pts[pts.length - 1];
                if (fade <= 0 || headPt.x < -200 || headPt.y < -200 || headPt.x > W + 200 || headPt.y > H + 200) {
                    lines[idx] = newLine();
                    return;
                }
                // the line: short pieces, each with its own brightness
                g.lineWidth = 1.4;
                g.lineCap = 'round';
                for (let k = 1; k < pts.length; k++) {
                    const a = ((pts[k - 1].a + pts[k].a) / 2) * fade;
                    if (a < 0.01) continue;
                    // almost the colour of the background: you barely see the line itself
                    g.strokeStyle = `rgba(${rgb}, ${(a * (0.04 + 0.14 * gold)).toFixed(3)})`;
                    g.beginPath();
                    g.moveTo(pts[k - 1].x, pts[k - 1].y);
                    g.lineTo(pts[k].x, pts[k].y);
                    g.stroke();
                }
                // the dots near the line
                pts.forEach((p, k) => {
                    if (k % 2 || p.a * fade < 0.03) return;
                    const i0 = Math.floor((p.x - REACH) / COL), i1 = Math.ceil((p.x + REACH) / COL);
                    for (let i = i0; i <= i1; i++) {
                        const off = i & 1 ? STEP / 2 : 0;
                        const j0 = Math.floor((p.y - REACH - off) / STEP), j1 = Math.ceil((p.y + REACH - off) / STEP);
                        for (let j = j0; j <= j1; j++) {
                            const x = i * COL, y = j * STEP + off;
                            const d = Math.hypot(x - p.x, y - p.y);
                            if (d >= REACH) continue;
                            const v = p.a * fade * Math.pow(1 - d / REACH, 1.4);
                            const key = `${i},${j}`;
                            if (!(dots.get(key) >= v)) dots.set(key, v);
                        }
                    }
                });
            });
            dots.forEach((v, key) => {
                const [i, j] = key.split(',').map(Number);
                const { x, y } = nodeXY(i, j);
                g.fillStyle = `rgba(0, 0, 0, ${Math.min(1, v * 1.5).toFixed(3)})`; // darker than the background
                g.beginPath();
                g.arc(x, y, 2, 0, Math.PI * 2);
                g.fill();
                // a faint light rim so the dark dot stands out a little on the dark background
                g.strokeStyle = `rgba(${rgb}, ${(v * (0.12 + 0.28 * gold)).toFixed(3)})`;
                g.lineWidth = 0.8;
                g.stroke();
            });
            requestAnimationFrame(frame);
        }
        function start() {
            if (running || document.hidden) return;
            running = true; last = 0;
            requestAnimationFrame(frame);
        }
        resize();
        window.addEventListener('resize', resize);
        document.addEventListener('visibilitychange', () => { if (document.hidden) running = false; else start(); });
        start();
    })();

    /* ---------------- a soft trail behind the mouse ----------------
     * In the background colour, gone within about a third of a second. Mouse only (not touch),
     * drawn on a canvas that never catches clicks. */
    (function mouseTrail() {
        if (window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
        const canvas = document.createElement('canvas');
        canvas.className = 'mouse-trail';
        canvas.setAttribute('aria-hidden', 'true');
        document.body.appendChild(canvas);
        const g = canvas.getContext('2d');
        const LIFE = 320; // ms a point stays visible
        let pts = [], W = 0, H = 0, dpr = 1, running = false;
        function resize() {
            dpr = Math.min(2, window.devicePixelRatio || 1);
            W = window.innerWidth; H = window.innerHeight;
            canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
            g.setTransform(dpr, 0, 0, dpr, 0, 0);
        }
        function frame() {
            const now = performance.now();
            pts = pts.filter((p) => now - p.t < LIFE);
            g.clearRect(0, 0, W, H);
            g.lineCap = 'round';
            for (let i = 1; i < pts.length; i++) {
                const a = pts[i - 1], b = pts[i];
                const k = 1 - (now - b.t) / LIFE;        // 1 = just now, 0 = gone
                if (k <= 0) continue;
                g.strokeStyle = `rgba(18, 18, 26, ${(k * k * 0.175).toFixed(3)})`; // the background colour (--bg)
                g.lineWidth = 2.4 + k * 9.6;             // thinner as it fades
                g.beginPath();
                g.moveTo(a.x, a.y);
                g.lineTo(b.x, b.y);
                g.stroke();
            }
            if (pts.length) requestAnimationFrame(frame);
            else running = false;
        }
        document.addEventListener('pointermove', (e) => {
            if (e.pointerType !== 'mouse') return;
            pts.push({ x: e.clientX, y: e.clientY, t: performance.now() });
            if (pts.length > 80) pts.shift();
            if (!running) { running = true; requestAnimationFrame(frame); }
        }, { passive: true });
        document.addEventListener('mouseleave', () => { pts = []; });
        resize();
        window.addEventListener('resize', resize);
    })();

    /* ---------------- broadcast text in the top bar ---------------- */
    // a very short summary of the latest patch; "XXX" in the broadcast text is replaced with it
    // newest first: the first line is the latest patch (shown in the broadcast), the rest is shown in its popup
    // which version of Remmin wrote the sync file (an older version must not overwrite a newer one)
    const APP_BUILD = '2026-11-23.1';
    const PATCH_HISTORY = [
        'GitHub sync: fewer "error 500": a hiccup at GitHub is tried again, and only media files that really exist are removed; Project: Copy / Cut / Delete also above the item you edit (no more ✕ on items), clicking next to the items clears the selection',
        'Project: drag a box with the mouse to select items, Copy / Cut / Delete show right above them; copied items take their tags, people and dropdowns along to another note; copy and paste dropdown options, tags and people (right-click, or paste several lines in a "+ Add" box)',
        'Project: copy, cut and paste items (Ctrl+C / Ctrl+X / Ctrl+V, the Copy / Paste buttons or right-click); pasting text from elsewhere makes one item per line',
        'Project: drag things in the boxes on the right (and the options of a dropdown) to change their order; Repeat on: hold the mouse button and slide over the days to select them',
        'Explorer: the ☰ button of a note is centred vertically',
        'Project: rename a dropdown (double-click its name or ✎)',
        'Project: add and rename boxes (departments, people) and Dropdown boxes (each dropdown has its own options; give it to an item and pick an option there); the Explorer only shows note titles',
        'Folder / note background colours: 12 colours plus black, grey and white',
        'Folders can be hidden in the Explorer too (folder ☰ menu → Hide in Explorer); the 📆 button shows them again',
        'New Remmin logo (tab icon and top of the Explorer) and new Hub pictures; Hub logos fade in 0.33 s',
        'Paste button in Text, Collage and Draw (handy on a phone); the Duplicate buttons are gone (Ctrl+D still works)',
        'Copy picture also copies the real picture (paste it in Paint, Discord…); Upload video file / Upload GIF file buttons; the option bar no longer covers a frame name',
        'Simpler note toolbars: click a picture, video, audio, sticky note or frame and its options (and colours) show right above it; Image / GIF / Video / Audio are under one "+ Add" button; Save is a 💾 button',
        'Collage and Draw: hold the right mouse button and scroll to zoom (like Ctrl + scroll); a note without windows only shows the small + button',
        'Phone: the sync text at the bottom of the Explorer is bigger and no longer hidden behind the browser bar',
        'GitHub sync: pictures are stored as separate files too, so remmin.json stays small and the 100 MB limit is gone (GitHub meter now shows the whole repo, up to 5 GB)',
        'Hub logos fade smoothly between normal and hover (0.25 s)',
        'Hubs: your own logos; Community Hub instead of the Authoritarian and Anarchy hubs',
        'Collage: bigger standard text (about 120%)',
        'Authoritarian Hub logo: a podium with press microphones',
        'New logos for the hubs (garden, microphone, anarchy A, two profiles)',
        'Project: the Department / Assign windows stay open, so you can pick several',
        'Project: drag a department or person onto an item to give it to that item',
        'Project: an open eye now shows ONLY the items of that department / person',
        'New notes start empty: add Text, Collage, Draw or Project windows with the + button',
        'Videos play on phones (Convert to MP4 for WebM); Hubs bar can collapse and shows on phones; hubs renamed',
        'GitHub sync: videos / audio as separate files (no more "file is too large"), only new media is uploaded',
        'Sync fix: no more endless fetching / pushing (stale GitHub reads, too-big files, older Remmin on another device)',
        'The note you had open last opens again when you come back',
        'Hubs bar (Personal stuff, Blue Hub, Orange Hub, Friends Hub) on the other side of the Explorer',
        'Ctrl+C / Ctrl+V of pictures, videos and audio between Text, Collage and Draw (several at once)',
        'Compress audio (right-click) + undo; "Keep this size" for videos, audio and pictures',
        'Audio: timeline to click / drag, and 15 s back / forward buttons',
        'Audio stored once (like videos), much faster audio compression, choose the MB size for too-big files',
        'Volume slider on audio; copy / paste pictures between Text, Collage and Draw; files over 25 MB can be compressed first',
        'Compress pictures (right-click) in Text, Collage and Draw; Pictures in the storage overview',
        'Click the storage bars: see which notes use the most space (after your Private Note Pass)',
        'Undo the compression of a video (right-click, for 7 days on this device)',
        'Compress a video to a size you choose (… MB)',
        'Loop option for videos (right-click) in Text, Collage and Draw',
        'Audio in Collage (record, drop, copy / paste); full video controls in Draw; deleted videos free their space',
        'No hidden-note stripes on the pass screen of a private note',
        'Videos: move + resize in Text, same right-click menu everywhere, copy / paste, compress, sound in Draw; 🔁 loop for audio',
        'Drag video files from your disk into Text, Collage and Draw',
        'Holiday stripes: new green colour, twice as wide',
        'Background lines turn gold while a private note is open',
        'Stripes of hidden notes in the Explorer line up perfectly',
        'Hidden-note stripes now cover the whole right side, top bar included',
        'Stripes now also behind the Text / Collage / Draw / Project buttons, not in the writing area',
        'Hidden notes get the striped background in the editor too; private notes show a 🔒 in the title',
        'Hidden-notes button sends out purple waves while on; hidden notes darker',
        'Explorer 📆 button shows ONLY the hidden notes, on a dark striped background',
        'Click the broadcast to see it in full, with the recent updates',
        'Project: eye buttons hide / show the items of a department or person',
        '📆 button next to search shows the notes hidden in the Explorer',
        'Scheduled block starts folded for every note',
        'Heading works as an on / off switch in Text and Collage',
        'Scale handle for multi-selected pictures and items',
        'Draw and every grid keep their own background colour',
        'GIFs, YouTube / Vimeo videos and KLIPY GIFs in Text and Collage',
        'Picture sizes 1/8, 1/4, 1/2 and Full, Shift-click to select several',
        'Explorer search bar',
    ];
    const PATCH_NOTE = PATCH_HISTORY[0];
    const DEFAULT_BROADCAST = 'Broadcast: Remmin 1.1: XXX';
    const broadcastText = () => (typeof state.prefs.broadcast === 'string' ? state.prefs.broadcast : DEFAULT_BROADCAST);
    function renderBroadcast() {
        const text = broadcastText().replace(/XXX/g, PATCH_NOTE).trim();
        const box = $('broadcast'), track = $('broadcastTrack');
        box.hidden = !text;
        if (!text) { track.innerHTML = ''; return; }
        // the text repeated: two equal halves, so moving one half to the left loops without a jump
        const one = document.createElement('span');
        one.textContent = text;
        track.innerHTML = '';
        track.appendChild(one);
        const copies = Math.max(1, Math.ceil((box.clientWidth || 200) / Math.max(1, one.offsetWidth)));
        for (let i = 1; i < copies; i++) track.appendChild(one.cloneNode(true));
        const half = [...track.children].map((n) => n.cloneNode(true));
        half.forEach((n) => track.appendChild(n));
        const halfWidth = track.scrollWidth / 2 || 200;
        track.style.setProperty('--broadcast-time', `${Math.max(6, halfWidth / 40)}s`); // about 40 px per second
    }
    // click the broadcast: a popup with the full text and the recent updates
    function openBroadcastInfo() {
        const text = broadcastText().replace(/XXX/g, PATCH_NOTE).trim();
        const version = (text.match(/Remmin\s*v?(\d+(?:\.\d+)*)/i) || [])[1];
        $('broadcastInfoText').textContent = text;
        $('broadcastInfoDetails').innerHTML = `
            <div class="bi-row"><span>Version</span><b>${escapeHtml(version ? `Remmin ${version}` : 'Remmin')}</b></div>
            <div class="bi-row"><span>Latest update</span><b>${escapeHtml(PATCH_NOTE)}</b></div>
            <div class="bi-row"><span>Synced to</span><b>${escapeHtml([githubConfigured() ? 'GitHub' : '', serverConfigured() ? 'Server' : ''].filter(Boolean).join(' + ') || 'only this device')}</b></div>
            <div class="bi-head">Recent updates</div>
            <ol class="bi-list">${PATCH_HISTORY.map((p, i) => `<li${i === 0 ? ' class="new"' : ''}>${escapeHtml(p)}</li>`).join('')}</ol>`;
        openModal('broadcastInfoModal');
    }
    $('broadcast').addEventListener('click', openBroadcastInfo);
    $('broadcast').addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openBroadcastInfo(); } });
    $('broadcastInfoClose').addEventListener('click', closeModals);
    $('broadcastInfoBackdrop').addEventListener('click', closeModals);
    $('broadcastInfoEdit').addEventListener('click', () => { closeModals(); openSettings('admin'); });
    $('broadcastSave').addEventListener('click', () => {
        state.prefs.broadcast = $('broadcastInput').value.trim();
        settingsChanged();
        renderBroadcast();
        toast(state.prefs.broadcast ? 'Broadcast text saved' : 'Broadcast text hidden');
    });
    $('broadcastInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); $('broadcastSave').click(); } });
    let broadcastResize = null;
    window.addEventListener('resize', () => { clearTimeout(broadcastResize); broadcastResize = setTimeout(renderBroadcast, 200); });
    document.querySelectorAll('.settings-tab').forEach((b) => b.addEventListener('click', () => showSettingsTab(b.dataset.tab)));
    $('settingsClose').addEventListener('click', closeModals);
    $('settingsBackdrop').addEventListener('click', closeModals);

    $('collapseSidebar').addEventListener('click', () => {
        if (isMobile()) {
            app.classList.remove('mobile-open');
            return;
        }
        state.prefs.sidebarCollapsed = true;
        savePrefs();
        applyLayout();
    });

    $('expandSidebar').addEventListener('click', () => {
        if (isMobile()) {
            app.classList.add('mobile-open');
            return;
        }
        state.prefs.sidebarCollapsed = false;
        savePrefs();
        applyLayout();
    });

    let dragging = false;
    $('sidebarSplitter').addEventListener('pointerdown', (e) => {
        if (isMobile()) return;
        dragging = true;
        e.preventDefault();
        $('sidebarSplitter').classList.add('dragging');
        document.body.style.cursor = 'col-resize';
    });

    window.addEventListener('pointermove', (e) => {
        if (!dragging) return;
        const width = state.prefs.sidebarSide === 'right' ? window.innerWidth - e.clientX : e.clientX;
        state.prefs.sidebarWidth = Math.min(480, Math.max(180, width));
        applyLayout();
        fitCanvas();
    });

    window.addEventListener('pointerup', () => {
        if (!dragging) return;
        dragging = false;
        $('sidebarSplitter').classList.remove('dragging');
        document.body.style.cursor = '';
        savePrefs();
    });

    window.matchMedia('(max-width: 760px)').addEventListener('change', () => {
        app.classList.remove('mobile-open');
        applyLayout();
        fitCanvas();
    });

    /* ---------------- GitHub-sync ---------------- */

    let githubConfig = readJSON(STORAGE.github, {});
    // The data file used to be called remindate.json (and before that notes.json): switch to
    // remmin.json. The first sync reads the old file once and saves everything as remmin.json
    // (the old file stays untouched, as a backup).
    if (githubConfig.path === 'notes.json' || githubConfig.path === 'remindate.json') {
        githubConfig.legacyPath = githubConfig.path;
        githubConfig.path = 'remmin.json';
        githubConfig.sha = null;
        writeJSON(STORAGE.github, githubConfig);
    }
    let githubTimer = null;
    let githubBusy = false;

    const githubConfigured = () => !!(githubConfig.owner && githubConfig.repo && githubConfig.path && githubConfig.token);
    let githubPending = false;

    function shortTime(iso) {
        const d = parseDate(iso);
        if (!d) return '–';
        return sameDay(d, new Date()) ? formatTime(toLocalIso(d)) : formatDateTime(toLocalIso(d));
    }

    function updateGithubStatus(message) {
        const el = $('githubStatus');
        if (!el) return;
        if (message) { el.textContent = message; return; }
        const parts = [], tips = [];
        if (githubConfigured()) {
            const where = `${githubConfig.owner}/${githubConfig.repo}/${githubConfig.path}`;
            parts.push(githubConfig.lastSyncAt
                ? `GitHub: synced ${shortTime(githubConfig.lastSyncAt)} • last change ${shortTime(laterIso(state.updatedAt, githubConfig.remoteUpdatedAt))}`
                : `GitHub: ${where} • not synced yet`);
            tips.push(`GitHub: ${where}\nLast sync: ${shortTime(githubConfig.lastSyncAt)}\nLatest version on GitHub: ${shortTime(githubConfig.remoteUpdatedAt)}`);
        }
        if (serverConfigured()) {
            parts.push(serverConfig.lastSyncAt ? `Server: synced ${shortTime(serverConfig.lastSyncAt)}` : 'Server: not synced yet');
            tips.push(`Server: ${serverConfig.url}\nLast sync: ${shortTime(serverConfig.lastSyncAt)}\nLatest version on the server: ${shortTime(serverConfig.remoteUpdatedAt)}`);
        }
        if (!parts.length) { el.textContent = 'Sync: not configured'; el.title = ''; return; }
        el.textContent = parts.join(' • ');
        el.title = `${tips.join('\n\n')}\nLast change on this device: ${shortTime(state.updatedAt)}`;
    }

    function showGithubSettings() {
        $('ghOwner').value = githubConfig.owner || '';
        $('ghRepo').value = githubConfig.repo || '';
        $('ghPath').value = githubConfig.path || 'remmin.json';
        $('ghToken').value = githubConfig.token || '';
        $('ghAutoSync').checked = !!githubConfig.autoSync;
        openModal('githubModal');
    }

    function readGithubForm() {
        return {
            owner: $('ghOwner').value.trim(),
            repo: $('ghRepo').value.trim(),
            path: $('ghPath').value.trim().replace(/^\/+/, ''),
            token: $('ghToken').value.trim(),
            autoSync: $('ghAutoSync').checked,
            sha: githubConfig.sha || null,
            lastSyncAt: githubConfig.lastSyncAt || null,
            remoteUpdatedAt: githubConfig.remoteUpdatedAt || null
        };
    }

    function githubUrl(path = githubConfig.path) {
        return `https://api.github.com/repos/${encodeURIComponent(githubConfig.owner)}/${encodeURIComponent(githubConfig.repo)}/contents/${path.split('/').map(encodeURIComponent).join('/')}`;
    }

    // GitHub sometimes answers 500 / 502 / 503 / 504 (or the connection drops): wait a moment and try again
    async function githubRequest(method, body, url = githubUrl(), accept = 'application/vnd.github+json') {
        const waits = [800, 2500, 6000];
        for (let attempt = 0; ; attempt++) {
            try {
                return await githubRequestOnce(method, body, url, accept);
            } catch (err) {
                const retry = (err.status >= 500 && err.status <= 504) || (err instanceof TypeError && !err.status);
                if (!retry || attempt >= waits.length) {
                    if (err.status >= 500) err.message = `GitHub is having trouble right now (error ${err.status}), Remmin tries again at the next sync`;
                    throw err;
                }
                await new Promise((r) => setTimeout(r, waits[attempt]));
            }
        }
    }
    async function githubRequestOnce(method, body, url, accept) {
        const response = await fetch(url, {
            method,
            // never use a cached copy: we must always see the newest version on GitHub
            cache: 'no-store',
            headers: {
                Accept: accept,
                Authorization: `Bearer ${githubConfig.token}`,
                'X-GitHub-Api-Version': '2022-11-28',
                ...(body ? { 'Content-Type': 'application/json' } : {})
            },
            ...(body ? { body: JSON.stringify(body) } : {})
        });
        if (!response.ok && response.status !== 404) {
            const data = await response.json().catch(() => ({}));
            const error = new Error(data.message || `GitHub error ${response.status}`);
            error.status = response.status;
            throw error;
        }
        if (response.status === 404) return null;
        return accept.includes('raw') ? response.text() : response.json();
    }

    function decodeGithubFile(encoded) {
        const bytes = Uint8Array.from(atob(encoded.replace(/\s/g, '')), (c) => c.charCodeAt(0));
        return new TextDecoder().decode(bytes);
    }

    function encodeGithubFile(value) {
        const bytes = new TextEncoder().encode(value);
        let binary = '';
        for (let i = 0; i < bytes.length; i += 0x8000) {
            binary += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
        }
        return btoa(binary);
    }

    /* ---- snapshots & merging ----
     * A snapshot is { notes, folders, deleted, updatedAt }.
     * Merging works per note/folder: the version with the newest modifiedAt wins,
     * and an item deleted after its last change stays deleted.
     */

    // JSON with sorted keys, so the same data always gives the same text
    function stableStringify(value) {
        if (Array.isArray(value)) return '[' + value.map(stableStringify).join(',') + ']';
        if (value && typeof value === 'object') {
            return '{' + Object.keys(value).sort().filter((k) => value[k] !== undefined)
                .map((k) => JSON.stringify(k) + ':' + stableStringify(value[k])).join(',') + '}';
        }
        return JSON.stringify(value === undefined ? null : value);
    }

    const snapshotKey = (snap) => stableStringify({ notes: snap.notes, folders: snap.folders, deleted: snap.deleted, privatePass: snap.privatePass || null, settings: snap.settings || null,
        trash: (snap.trash || []).map((t) => [t.note.id, t.deletedAt]), trashGone: snap.trashGone || {},
        media: Object.keys(snap.media || {}).sort().map((k) => `${k}:${snap.media[k].length}`) });

    function localSnapshot() {
        return JSON.parse(JSON.stringify({
            notes: state.notes,
            folders: state.folders,
            deleted: state.deleted,
            privatePass: privatePass || null,
            settings: syncedSettings(),
            trash: state.trash,
            trashGone: state.trashGone,
            updatedAt: state.updatedAt,
            media: pickMedia(state.media, state.notes, state.trash)
        }));
    }

    function normalizeSnapshot(data) {
        const notes = Array.isArray(data) ? data : data.notes;
        if (!Array.isArray(notes)) throw new Error('remmin.json on GitHub does not contain a valid note list');
        const snap = {
            notes: notes.map((n) => { migrateNote(n); return n; }),
            folders: (Array.isArray(data) ? [] : (data.folders || [])).map((f) => { migrateFolder(f); return f; }),
            deleted: normalizeDeleted(Array.isArray(data) ? null : data.deleted),
            privatePass: Array.isArray(data) ? null : validPass(data.privatePass),
            settings: Array.isArray(data) ? null : validSettings(data.settings),
            trash: Array.isArray(data) || !Array.isArray(data.trash) ? [] : data.trash
                .filter((t) => t && t.note && typeof t.note.id === 'string' && typeof t.deletedAt === 'string')
                .map((t) => { migrateNote(t.note); return { note: t.note, deletedAt: t.deletedAt }; }),
            trashGone: Array.isArray(data) || !data.trashGone || typeof data.trashGone !== 'object' ? {} : data.trashGone,
            updatedAt: Array.isArray(data) ? null : (data.updatedAt || null),
            media: Array.isArray(data) ? {} : validMedia(data.media)
        };
        return snap;
    }

    const timeOf = (iso) => Date.parse(iso) || 0;
    const laterIso = (a, b) => (timeOf(a) >= timeOf(b) ? a : b) || a || b || null;

    function mergeSnapshots(local, remote) {
        const deleted = { notes: {}, folders: {} };
        const cutoff = Date.now() - 180 * 86400000; // forget deletions after ~6 months
        ['notes', 'folders'].forEach((kind) => {
            [remote.deleted[kind], local.deleted[kind]].forEach((list) => {
                Object.entries(list).forEach(([id, at]) => {
                    if (timeOf(at) < cutoff) return;
                    if (!deleted[kind][id] || timeOf(at) > timeOf(deleted[kind][id])) deleted[kind][id] = at;
                });
            });
        });

        const mergeList = (kind, mine, theirs) => {
            const map = new Map();
            const order = [];
            [...mine, ...theirs].forEach((item) => {
                const current = map.get(item.id);
                if (!current) { map.set(item.id, item); order.push(item.id); }
                else if (timeOf(item.modifiedAt) > timeOf(current.modifiedAt)) map.set(item.id, item);
            });
            return order.map((id) => map.get(id)).filter((item) => {
                const gone = deleted[kind][item.id];
                if (!gone) return true;
                if (timeOf(gone) >= timeOf(item.modifiedAt)) return false;
                delete deleted[kind][item.id]; // changed again after the delete: keep it
                return true;
            });
        };

        const folders = mergeList('folders', local.folders, remote.folders)
            .map((f, i) => ({ f, i }))
            .sort((a, b) => ((typeof a.f.order === 'number' ? a.f.order : a.i) - (typeof b.f.order === 'number' ? b.f.order : b.i)) || a.i - b.i)
            .map((x) => x.f);
        const folderIds = new Set(folders.map((f) => f.id));
        const notes = mergeList('notes', local.notes, remote.notes)
            .map((n) => (n.folderId && !folderIds.has(n.folderId) ? { ...n, folderId: null } : n));

        // the pass that was set (or changed) last wins; with no dates, the one in remmin.json
        const lp = local.privatePass || null, rp = remote.privatePass || null;
        const pass = !lp ? rp : !rp ? lp : (timeOf(lp.setAt) > timeOf(rp.setAt) ? lp : rp);

        // holiday choices + clocks: the newest change wins; with no dates, the ones in remmin.json
        const ls = local.settings || null, rs = remote.settings || null;
        const settings = !ls ? rs : !rs ? ls : (timeOf(ls.at) > timeOf(rs.at) ? ls : rs);

        // the trash: everything from both sides, minus what was restored or deleted forever
        const trashGoneAll = mergeGone(local.trashGone, remote.trashGone);
        const trash = mergeTrash(local.trash, remote.trash, trashGoneAll, new Set(notes.map((n) => n.id)));

        // video files: from both sides (a compressed, smaller version wins), only the ones still used
        const allMedia = { ...(remote.media || {}) };
        Object.entries(local.media || {}).forEach(([k, v]) => { if (!allMedia[k] || v.length < allMedia[k].length) allMedia[k] = v; });
        const media = pickMedia(allMedia, notes, trash);

        return { notes, folders, deleted, privatePass: pass, settings, trash, trashGone: trashGoneAll, updatedAt: laterIso(local.updatedAt, remote.updatedAt), media };
    }

    // Holiday choices + clocks from remmin.json / a .json file
    function applySettings(set) {
        if (set.holidays) state.prefs.holidays = [...set.holidays];
        if (set.clocks) state.prefs.clocks = [...set.clocks];
        if (typeof set.broadcast === 'string') { state.prefs.broadcast = set.broadcast; renderBroadcast(); }
        if (set.imgSize) state.prefs.imgSize = set.imgSize;
        if (set.collageColors) state.prefs.collageColors = { ...set.collageColors };
        if (typeof set.email === 'string') { if (set.email) RemminStore.set(EMAIL_ADDRESS_KEY, set.email); else RemminStore.remove(EMAIL_ADDRESS_KEY); }
        if (typeof set.alarmSound === 'string' && set.alarmSound !== (RemminStore.get(ALARM_SOUND_KEY) || '')) {
            if (set.alarmSound) RemminStore.set(ALARM_SOUND_KEY, set.alarmSound); else RemminStore.remove(ALARM_SOUND_KEY);
            const status = $('alarmSoundStatus');
            if (status) status.textContent = set.alarmSound ? 'Using custom sound (synced)' : 'Using standard sound';
        }
        state.prefs.settingsAt = set.at || state.prefs.settingsAt || null;
        savePrefs();
        syncHolidayCheckboxes();
        if (state.prefs.activeTab === 'calendar') {
            if (state.prefs.calendarMode === 'clock') renderClocks(); else renderCalendar();
        }
    }

    // Put a merged snapshot into the app without counting it as a new local change
    function applySnapshot(snap) {
        const before = stableStringify(noteById(state.currentNoteId) || null);
        const knownFolders = new Set(state.folders.map((f) => f.id));
        const incomingIds = new Set(snap.notes.map((n) => n.id));
        state.notes.filter((n) => !incomingIds.has(n.id)).forEach((n) => {
            state.trash = state.trash.filter((t) => t.note.id !== n.id);
            // deleted on another device: use the time it was deleted there
            const at = (snap.deleted && snap.deleted.notes && snap.deleted.notes[n.id]) || new Date().toISOString();
            state.trash.unshift({ note: JSON.parse(JSON.stringify(n)), deletedAt: at });
        });
        // video files from the other side (or a compressed version)
        let mediaChanged = false;
        Object.entries(snap.media || {}).forEach(([k, v]) => {
            if (state.media[k] && state.media[k].length === v.length) return;
            state.media[k] = v;
            mediaChanged = true;
            if (typeof refreshMediaPlayers === 'function') refreshMediaPlayers(k);
        });
        if (mediaChanged) scheduleSaveMedia();
        state.notes = snap.notes.map((n) => { migrateNote(n); return n; });
        // the synced trash
        state.trashGone = mergeGone(state.trashGone, snap.trashGone);
        state.trash = mergeTrash(state.trash, snap.trash, state.trashGone, incomingIds);
        saveTrash();
        if ($('trashModal').classList.contains('active')) renderTrash();
        state.folders = snap.folders;
        state.deleted = snap.deleted;
        state.updatedAt = snap.updatedAt;
        if (snap.settings && stableStringify(snap.settings) !== stableStringify(syncedSettings())) applySettings(snap.settings);
        if (snap.privatePass && stableStringify(snap.privatePass) !== stableStringify(privatePass)) {
            privatePass = snap.privatePass;
            writeJSON(STORAGE.privatePass, privatePass);
            if (typeof updatePassPanel === 'function') updatePassPanel();
            if (isPrivateLocked(noteById(state.currentNoteId))) renderEditor();
        }
        writeJSON(STORAGE.notes, state.notes);
        writeJSON(STORAGE.folders, state.folders);
        writeJSON(STORAGE.deleted, state.deleted);
        writeJSON(STORAGE.updatedAt, state.updatedAt);

        state.folders.forEach((f) => {
            // folders that are new on this device start expanded
            if (!knownFolders.has(f.id) && !state.prefs.expandedFolders.includes(f.id)) state.prefs.expandedFolders.push(f.id);
        });
        savePrefs();
        if (!noteById(state.currentNoteId)) {
            state.currentNoteId = sortedNotes(state.notes)[0]?.id || null;
            state.currentFolderId = noteById(state.currentNoteId)?.folderId || null;
        }
        renderTree();
        if (stableStringify(noteById(state.currentNoteId) || null) !== before) {
            // drawing / painting / dragging right now: redraw the note once you let go
            if (pointerHeld) pendingEditorRender = true; else renderEditor();
        }
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    }
    let pointerHeld = false, pendingEditorRender = false;
    document.addEventListener('pointerdown', () => { pointerHeld = true; }, true);
    const pointerReleased = () => {
        pointerHeld = false;
        if (!pendingEditorRender) return;
        pendingEditorRender = false;
        setTimeout(renderEditor, 0); // after the stroke has been saved
    };
    document.addEventListener('pointerup', pointerReleased);
    document.addEventListener('pointercancel', pointerReleased);

    // GitHub can briefly hand out the version from before our last push: wait a moment and ask again
    async function fetchRemote() {
        for (let i = 0; i < 6; i++) {
            const r = await fetchRemoteOnce();
            const stale = r && !r.legacy && githubConfig.pushedSha && r.sha !== githubConfig.pushedSha && githubConfig.pushedUpdatedAt
                && r.data && !Array.isArray(r.data) && r.data.updatedAt && r.data.updatedAt < githubConfig.pushedUpdatedAt;
            if (!stale) return r;
            await new Promise((res) => setTimeout(res, 2000));
        }
        const err = new Error('GitHub still shows an older version of the file; trying again later');
        err.quiet = true;
        throw err;
    }
    /* GitHub storage (since 2026-11-19): remmin.json holds the notes; every video / audio is its own file in
     * remmin-media/ (uploaded once, through the Git Data API, which takes files up to 100 MB each).
     * One sync = one commit, so the notes and their media always change together. */
    const ghRepoUrl = (rest) => `https://api.github.com/repos/${encodeURIComponent(githubConfig.owner)}/${encodeURIComponent(githubConfig.repo)}${rest}`;
    async function ghBranch() {
        if (githubConfig.branch) return githubConfig.branch;
        const repo = await githubRequest('GET', null, ghRepoUrl(''));
        if (!repo) throw new Error(`GitHub can't find ${githubConfig.owner}/${githubConfig.repo}`);
        githubConfig.branch = repo.default_branch || 'main';
        return githubConfig.branch;
    }
    // the newest commit of the branch (null: the repo is still empty)
    async function ghHead() {
        const branch = await ghBranch();
        const ref = await githubRequest('GET', null, ghRepoUrl(`/git/ref/heads/${branch.split('/').map(encodeURIComponent).join('/')}`));
        return ref && ref.object ? ref.object.sha : null;
    }
    const ghMediaDir = () => { const pth = githubConfig.path || 'remmin.json'; const i = pth.lastIndexOf('/'); return `${i >= 0 ? pth.slice(0, i + 1) : ''}remmin-media`; };
    // the videos / audio of the remote file: the ones this device has are reused, the others are downloaded
    /* Pictures (since 2026-11-23) also live in remmin-media/: in remmin.json a picture is "remmin-pic:p<id>".
     * Only the synced copy changes: on this device the notes keep the pictures as they are. */
    const PIC_RE = /data:image\/[a-z0-9.+-]+;base64,[A-Za-z0-9+/=]{2000,}/gi;
    const PIC_TOKEN_RE = /remmin-pic:(p[0-9a-z]{4,40})/g;
    const picCache = new Map(); // id → data:image text (pictures seen this session)
    let picCacheSeeded = false;
    function picsOut(text, pics) {
        return text.replace(PIC_RE, (m) => {
            const id = `p${mediaHash(m)}`;
            pics[id] = m;
            picCache.set(id, m);
            return `remmin-pic:${id}`;
        });
    }
    function seedPicCache() {
        if (picCacheSeeded) return;
        picCacheSeeded = true;
        try { picsOut(JSON.stringify([state.notes, state.trash]), {}); } catch (_) { /* only a cache */ }
    }
    async function ghLoadMedia(data) {
        if (!data || Array.isArray(data) || !data.mediaIndex || typeof data.mediaIndex !== 'object') return;
        const media = { ...(data.media && typeof data.media === 'object' ? data.media : {}) };
        const todo = [];
        if (Object.keys(data.mediaIndex).some((id) => id[0] === 'p' && !picCache.has(id))) seedPicCache();
        Object.entries(data.mediaIndex).forEach(([id, info]) => {
            if (!info || typeof info.sha !== 'string') return;
            const have = state.media[id] || (state.mediaGone && state.mediaGone[id]) || picCache.get(id);
            if (have && have.length === info.len) media[id] = have; else todo.push([id, info]);
        });
        for (const [k, [id, info]] of todo.entries()) {
            updateGithubStatus(`GitHub: downloading media ${k + 1}/${todo.length}…`);
            const blob = await githubRequest('GET', null, ghRepoUrl(`/git/blobs/${info.sha}`));
            if (blob && typeof blob.content === 'string') {
                media[id] = `data:${info.mime || 'video/mp4'};base64,${blob.content.replace(/\s/g, '')}`;
                if (id[0] === 'p') picCache.set(id, media[id]);
            }
        }
        data.media = media;
    }
    async function fetchRemoteOnce() {
        const head = await ghHead();
        if (!head) return null;
        const at = `?ref=${head}`; // read exactly this commit (never an older copy)
        let url = githubUrl();
        let meta = await githubRequest('GET', null, url + at);
        let legacy = false;
        if (!meta && githubConfig.legacyPath) {
            // remmin.json doesn't exist yet: start from the old remindate.json / notes.json
            url = githubUrl(githubConfig.legacyPath);
            meta = await githubRequest('GET', null, url + at);
            legacy = true;
        }
        if (!meta) return null;
        let text;
        if (meta.content && meta.encoding === 'base64') text = decodeGithubFile(meta.content);
        else text = await githubRequest('GET', null, url + at, 'application/vnd.github.raw+json'); // files > 1 MB
        let data = text && text.trim() ? JSON.parse(text) : [];
        githubConfig.remoteMediaIndex = data && !Array.isArray(data) && data.mediaIndex && typeof data.mediaIndex === 'object' ? data.mediaIndex : {};
        await ghLoadMedia(data);
        if (data && !Array.isArray(data)) {
            const media = data.media || {};
            if (text.includes('remmin-pic:')) {
                // put the pictures back into the notes
                const missing = new Set();
                const fixed = JSON.parse(text.replace(PIC_TOKEN_RE, (m, id) => {
                    const pic = media[id] || picCache.get(id);
                    if (!pic) missing.add(id);
                    return pic || m;
                }));
                if (missing.size) {
                    const err = new Error(`GitHub: ${missing.size} picture(s) could not be downloaded, trying again later`);
                    err.quiet = true;
                    throw err;
                }
                fixed.media = media;
                data = fixed;
            }
            Object.keys(media).forEach((id) => { if (id[0] === 'p') delete media[id]; });
        }
        if (!legacy) {
            const mediaBytes = Object.values(githubConfig.remoteMediaIndex).reduce((t, m) => t + Math.round((m.len || 0) * 0.75), 0);
            githubConfig.fileSize = (meta.size || 0) + mediaBytes;
            updateStorageMeters();
        }
        // from the old file: always push once (that writes remmin.json)
        return legacy ? { sha: head, data, legacy: true } : { sha: head, data };
    }

    async function pushRemote(snap, parentSha) {
        const parent = parentSha || await ghHead();
        if (!parent) return pushRemoteContents(snap); // an empty repo: the first file the simple way
        // 0) remmin.json without the pictures (each picture becomes its own file, like the videos)
        const pics = {};
        const bodyText = picsOut(JSON.stringify({
            app: 'Remmin',
            version: 3,
            updatedAt: snap.updatedAt || new Date().toISOString(),
            folders: snap.folders,
            notes: snap.notes,
            deleted: snap.deleted,
            ...(snap.privatePass ? { privatePass: snap.privatePass } : {}),
            ...(snap.settings ? { settings: snap.settings } : {}),
            trash: snap.trash || [],
            trashGone: snap.trashGone || {}
        }), pics);
        const all = { ...(snap.media || {}), ...pics };
        // 1) the videos / audio / pictures that GitHub doesn't have yet, each as its own file
        const prevIndex = githubConfig.remoteMediaIndex || {};
        const index = {};
        const ids = Object.keys(all);
        const fresh = ids.filter((id) => !(prevIndex[id] && prevIndex[id].len === all[id].length));
        for (const [k, id] of fresh.entries()) {
            updateGithubStatus(`GitHub: uploading media ${k + 1}/${fresh.length}…`);
            const data = all[id];
            const cut = data.indexOf(';base64,');
            const r = await githubRequest('POST', { content: data.slice(cut + 8), encoding: 'base64' }, ghRepoUrl('/git/blobs'));
            index[id] = { sha: r.sha, mime: data.slice(5, cut), len: data.length };
        }
        ids.forEach((id) => { if (!index[id]) index[id] = prevIndex[id]; });
        // a picture that is still "remmin-pic:" in the notes (never lose its file)
        for (const m of bodyText.matchAll(PIC_TOKEN_RE)) if (!index[m[1]] && prevIndex[m[1]]) index[m[1]] = prevIndex[m[1]];
        // 2) remmin.json, with only the list of media files
        const payload = { updatedAt: snap.updatedAt || new Date().toISOString() };
        const fileText = `${bodyText.slice(0, -1)},"mediaIndex":${JSON.stringify(index)},"appBuild":${JSON.stringify(APP_BUILD)}}`;
        const fileBytes = new TextEncoder().encode(fileText).length;
        if (fileBytes > 95 * 1048576) {
            const err = new Error(`remmin.json would be ${(fileBytes / 1048576).toFixed(0)} MB (GitHub allows 100 MB per file): that is a lot of text, remove some notes or empty the Trash`);
            err.tooBig = true;
            throw err;
        }
        updateGithubStatus('GitHub: pushing…');
        const jsonBlob = await githubRequest('POST', { content: fileText, encoding: 'utf-8' }, ghRepoUrl('/git/blobs'));
        // 3) one commit with the new file, the new media files, and without the media nobody uses any more
        const dir = ghMediaDir();
        const tree = [{ path: githubConfig.path, mode: '100644', type: 'blob', sha: jsonBlob.sha }];
        fresh.forEach((id) => tree.push({ path: `${dir}/${id}`, mode: '100644', type: 'blob', sha: index[id].sha }));
        const baseCommit = await githubRequest('GET', null, ghRepoUrl(`/git/commits/${parent}`));
        // remove only media files that really are there (removing a missing file makes GitHub refuse the whole sync)
        const existing = await ghMediaFiles(baseCommit.tree.sha);
        const stillUsed = new Set([...bodyText.matchAll(PIC_TOKEN_RE)].map((m) => m[1]));
        existing.forEach((sha, name) => { if (!index[name] && !fresh.includes(name) && !stillUsed.has(name)) tree.push({ path: `${dir}/${name}`, mode: '100644', type: 'blob', sha: null }); });
        const newTree = await githubRequest('POST', { base_tree: baseCommit.tree.sha, tree }, ghRepoUrl('/git/trees'));
        const commit = await githubRequest('POST', { message: `Remmin sync ${new Date().toISOString()}`, tree: newTree.sha, parents: [parent] }, ghRepoUrl('/git/commits'));
        try {
            await githubRequest('PATCH', { sha: commit.sha, force: false }, ghRepoUrl(`/git/refs/heads/${(await ghBranch()).split('/').map(encodeURIComponent).join('/')}`));
        } catch (err) {
            // someone else pushed in between (not a fast-forward): fetch again and merge
            if (err.status === 422) { const c = new Error('conflict'); c.status = 409; throw c; }
            throw err;
        }
        githubConfig.sha = commit.sha;
        githubConfig.remoteMediaIndex = index;
        githubConfig.remoteUpdatedAt = payload.updatedAt;
        githubConfig.fileSize = fileBytes + Object.values(index).reduce((t, m) => t + Math.round((m.len || 0) * 0.75), 0);
        githubConfig.pushedSha = commit.sha;
        githubConfig.pushedUpdatedAt = payload.updatedAt;
        delete githubConfig.legacyPath; // remmin.json exists now
    }

    // the files in remmin-media/ of a commit's tree: name → sha
    async function ghMediaFiles(treeSha) {
        const files = new Map();
        let sha = treeSha;
        for (const part of ghMediaDir().split('/')) {
            const t = await githubRequest('GET', null, ghRepoUrl(`/git/trees/${sha}`));
            const entry = t && (t.tree || []).find((x) => x.path === part && x.type === 'tree');
            if (!entry) return files;
            sha = entry.sha;
        }
        const t = await githubRequest('GET', null, ghRepoUrl(`/git/trees/${sha}`));
        ((t && t.tree) || []).forEach((x) => { if (x.type === 'blob') files.set(x.path, x.sha); });
        return files;
    }

    // only for an empty repo (the very first file)
    async function pushRemoteContents(snap, sha) {
        const payload = {
            app: 'Remmin',
            version: 1,
            updatedAt: snap.updatedAt || new Date().toISOString(),
            folders: snap.folders,
            notes: snap.notes,
            deleted: snap.deleted,
            ...(snap.privatePass ? { privatePass: snap.privatePass } : {}),
            ...(snap.settings ? { settings: snap.settings } : {}),
            trash: snap.trash || [],
            trashGone: snap.trashGone || {},
            media: snap.media || {},
            appBuild: APP_BUILD
        };
        const fileText = JSON.stringify(payload, null, 2);
        const fileBytes = new TextEncoder().encode(fileText).length;
        if (fileBytes > 95 * 1048576) {
            const err = new Error(`the sync file would be ${(fileBytes / 1048576).toFixed(0)} MB, GitHub allows at most 100 MB: compress or remove some videos / audio / pictures (click the storage bars to see what is big)`);
            err.tooBig = true;
            throw err;
        }
        const result = await githubRequest('PUT', {
            message: `Remmin sync ${new Date().toISOString()}`,
            content: encodeGithubFile(fileText),
            ...(sha ? { sha } : {})
        });
        if (!result || !result.content) {
            throw new Error(`GitHub can't find ${githubConfig.owner}/${githubConfig.repo}, or your token has no write access to it`);
        }
        githubConfig.sha = result.content.sha;
        githubConfig.remoteUpdatedAt = payload.updatedAt;
        githubConfig.fileSize = fileBytes;
        githubConfig.pushedSha = result.content.sha;
        githubConfig.pushedUpdatedAt = payload.updatedAt;
        delete githubConfig.legacyPath; // remmin.json exists now
    }

    /* ---------------- Server sync (your own server / paid hosting) ----------------
     * Protocol (kept tiny so any server can do it):
     *   GET  <url>  -> 200 + the remmin.json content (ETag header = version), or 404 when empty
     *   PUT  <url>  -> stores the JSON body; with "If-Match: <etag>" it answers 412 if
     *                  someone else saved in between. Answers with the new ETag.
     * Auth: "Authorization: Bearer <token>". scripts/remmin-sync-server.js does exactly this. */
    let serverConfig = readJSON(STORAGE.server, {});
    const serverConfigured = () => !!serverConfig.url;

    async function serverRequest(method, body, ifMatch) {
        let response;
        try {
            response = await fetch(serverConfig.url, {
                method,
                cache: 'no-store',
                headers: {
                    Accept: 'application/json',
                    ...(serverConfig.token ? { Authorization: `Bearer ${serverConfig.token}` } : {}),
                    ...(body ? { 'Content-Type': 'application/json' } : {}),
                    ...(ifMatch ? { 'If-Match': ifMatch } : {})
                },
                ...(body ? { body: JSON.stringify(body) } : {})
            });
        } catch (err) {
            throw new Error(location.protocol === 'https:' && /^http:/i.test(serverConfig.url)
                ? 'the server address must start with https://'
                : "can't reach the server (is it online, and does it allow this website (CORS)?)");
        }
        if (response.status === 404 && method === 'GET') return null;
        if (!response.ok) {
            const error = new Error(response.status === 401 || response.status === 403
                ? 'the server refused the token'
                : response.status === 412 || response.status === 409
                    ? 'someone else saved at the same time'
                    : `server error ${response.status}`);
            error.status = response.status;
            throw error;
        }
        const text = await response.text();
        if (method === 'GET' && text) serverConfig.fileSize = new TextEncoder().encode(text).length;
        else if (body) serverConfig.fileSize = new TextEncoder().encode(JSON.stringify(body)).length;
        updateStorageMeters();
        return { etag: response.headers.get('ETag'), text };
    }

    const syncProviders = {
        github: {
            id: 'github',
            label: 'GitHub',
            configured: () => githubConfigured(),
            cfg: () => githubConfig,
            save: () => writeJSON(STORAGE.github, githubConfig),
            async fetch() {
                const r = await fetchRemote();
                // coming from the old remindate.json / notes.json: always write remmin.json once
                if (r && r.legacy) return { rev: null, data: r.data, forcePush: true };
                return r && { rev: r.sha, data: r.data };
            },
            async push(snap, rev) { await pushRemote(snap, rev); return githubConfig.sha; },
            // (rev = the commit the merge was based on)
            setRev(rev) { githubConfig.sha = rev; },
            // 422 is only a conflict when it is about the sha (otherwise: a real error, e.g. too big)
            isConflict: (err) => err.status === 409 || (err.status === 422 && /sha/i.test(err.message || ''))
        },
        server: {
            id: 'server',
            label: 'Server',
            configured: () => serverConfigured(),
            cfg: () => serverConfig,
            save: () => writeJSON(STORAGE.server, serverConfig),
            async fetch() {
                const r = await serverRequest('GET');
                if (!r) return null;
                const data = r.text && r.text.trim() ? JSON.parse(r.text) : [];
                return { rev: r.etag || null, data };
            },
            async push(snap, rev) {
                const payload = {
                    app: 'Remmin', version: 1,
                    updatedAt: snap.updatedAt || new Date().toISOString(),
                    folders: snap.folders, notes: snap.notes, deleted: snap.deleted,
                    ...(snap.privatePass ? { privatePass: snap.privatePass } : {}),
                    ...(snap.settings ? { settings: snap.settings } : {}),
                    trash: snap.trash || [],
                    trashGone: snap.trashGone || {},
                    media: snap.media || {},
                    appBuild: APP_BUILD
                };
                const r = await serverRequest('PUT', payload, rev);
                serverConfig.remoteUpdatedAt = payload.updatedAt;
                return r.etag || null;
            },
            setRev(rev) { serverConfig.etag = rev; },
            isConflict: (err) => err.status === 412 || err.status === 409
        }
    };
    const configuredProviders = () => Object.values(syncProviders).filter((p) => p.configured());

    // Sync = 1) fetch the newest version, 2) merge it with this device,
    // 3) push the result back (only if the other side doesn't have it yet).
    let syncPending = new Set();
    let pushTimes = [], syncPausedUntil = 0, warnedOldVersion = false;
    async function runSync(p, options = {}) {
        if (!p.configured()) return;
        if (options.silent && Date.now() < syncPausedUntil) return; // auto-sync paused (see below)
        if (githubBusy) { syncPending.add(p.id); return; }
        githubBusy = true;
        let pulled = false, pushed = false;
        try {
            if (options.flush !== false) flushEditor();
            for (let attempt = 0; attempt < 4; attempt++) {
                updateGithubStatus(`${p.label}: fetching…`);
                const local = localSnapshot();
                const remote = await p.fetch();
                // You edited something while we were fetching: start over with the new data
                if (state.updatedAt !== local.updatedAt) continue;

                // the file was written by a newer Remmin: only take its changes, never overwrite it with this older version
                const newerElsewhere = remote && remote.data && !Array.isArray(remote.data) && typeof remote.data.appBuild === 'string' && remote.data.appBuild > APP_BUILD;
                if (newerElsewhere && !warnedOldVersion) {
                    warnedOldVersion = true;
                    toast('Remmin was updated on another device: reload this page (Ctrl+F5) to get the new version. Until then this device only fetches.', { persistent: true, key: 'old-version', buttons: [{ label: 'Reload', onClick: () => location.reload() }] });
                }
                let merged = local;
                let remoteSnap = null;
                if (remote) {
                    remoteSnap = normalizeSnapshot(remote.data);
                    merged = mergeSnapshots(local, remoteSnap);
                    p.cfg().remoteUpdatedAt = remoteSnap.updatedAt;
                }
                if (snapshotKey(merged) !== snapshotKey(local)) {
                    applySnapshot(merged);
                    pulled = true;
                }
                if (!newerElsewhere && (!remote || remote.forcePush || snapshotKey(merged) !== snapshotKey(remoteSnap))) {
                    // safety: many pushes in a short time means something keeps undoing them: pause auto-sync
                    const now = Date.now();
                    pushTimes = pushTimes.filter((t) => now - t < 3 * 60000);
                    if (pushTimes.length >= 8) {
                        syncPausedUntil = now + 5 * 60000;
                        updateGithubStatus(`${p.label}: paused`);
                        toast('Sync paused for 5 minutes: the file kept changing back and forth. Is Remmin open on another device or tab with an older version? Reload it there.', { label: 'Sync now', onClick: () => { syncPausedUntil = 0; pushTimes = []; syncAll(configuredProviders()); } });
                        return;
                    }
                    pushTimes.push(now);
                    updateGithubStatus(`${p.label}: pushing…`);
                    try {
                        p.setRev(await p.push(merged, remote && remote.rev));
                        pushed = true;
                    } catch (err) {
                        // Someone else pushed in between: fetch again and retry
                        if (p.isConflict(err)) continue;
                        throw err;
                    }
                } else {
                    p.setRev(remote.rev);
                }
                p.cfg().lastSyncAt = new Date().toISOString();
                p.save();
                updateGithubStatus();
                // New data came in: pass it on to the other sync targets too
                if (pulled) configuredProviders().forEach((o) => { if (o !== p) syncPending.add(o.id); });
                if (!options.silent) {
                    toast(pulled && pushed ? `Fetched changes from ${p.label} and pushed yours`
                        : pulled ? `Fetched the latest changes from ${p.label}`
                            : pushed ? `Pushed your changes to ${p.label}`
                                : `Already up to date with ${p.label}`);
                }
                return;
            }
            throw new Error('the data kept changing during sync, please try again');
        } catch (err) {
            console.error(`${p.label} sync failed`, err);
            updateGithubStatus(`${p.label}: sync failed`);
            if (err.tooBig) syncPausedUntil = Date.now() + 10 * 60000; // don't keep trying the same too-big file
            if (!(err.quiet && options.silent)) toast(`${p.label} sync failed: ${err.message}`);
        } finally {
            githubBusy = false;
            const next = [...syncPending][0];
            if (next) {
                syncPending.delete(next);
                runSync(syncProviders[next], { silent: true, flush: false });
            }
        }
    }

    const syncGithub = (options) => runSync(syncProviders.github, options);
    const autoProviders = () => configuredProviders().filter((p) => p.cfg().autoSync);
    function syncAll(providers, options = {}) {
        const [first, ...rest] = providers;
        if (!first) return;
        rest.forEach((p) => syncPending.add(p.id));
        runSync(first, options);
    }

    function scheduleGithubSave() {
        if (!autoProviders().length) return;
        clearTimeout(githubTimer);
        githubTimer = setTimeout(() => syncAll(autoProviders(), { silent: true }), 1500);
    }

    // With auto-sync on, also fetch regularly and when you come back to the tab
    let lastAutoPull = 0;
    function autoPull() {
        if (!autoProviders().length || document.hidden) return;
        if (Date.now() - lastAutoPull < 30000) return;
        lastAutoPull = Date.now();
        syncAll(autoProviders(), { silent: true, flush: true });
    }
    window.addEventListener('focus', autoPull);
    document.addEventListener('visibilitychange', autoPull);
    setInterval(autoPull, 120000);

    /* Sync Settings: choose GitHub Sync or Server Sync */
    function showSyncChooser() {
        $('syncChooseGithubState').textContent = githubConfigured() ? (githubConfig.autoSync ? 'On • automatic' : 'On') : 'Not set up';
        $('syncChooseServerState').textContent = serverConfigured() ? (serverConfig.autoSync ? 'On • automatic' : 'On') : 'Not set up';
        openModal('syncChooserModal');
    }
    $('syncChooserClose').addEventListener('click', closeModals);
    $('syncChooserBackdrop').addEventListener('click', closeModals);
    $('syncChooseGithub').addEventListener('click', () => { $('syncChooserModal').classList.remove('active'); showGithubSettings(); });
    $('syncChooseServer').addEventListener('click', () => { $('syncChooserModal').classList.remove('active'); showServerSettings(); });

    function showServerSettings() {
        $('srvUrl').value = serverConfig.url || '';
        $('srvToken').value = serverConfig.token || '';
        $('srvAutoSync').checked = !!serverConfig.autoSync;
        openModal('serverModal');
    }
    function readServerForm() {
        return {
            url: $('srvUrl').value.trim(),
            token: $('srvToken').value.trim(),
            autoSync: $('srvAutoSync').checked,
            // a different address is a different file: forget the old version info
            etag: serverConfig.url === $('srvUrl').value.trim() ? serverConfig.etag || null : null,
            lastSyncAt: serverConfig.url === $('srvUrl').value.trim() ? serverConfig.lastSyncAt || null : null,
            remoteUpdatedAt: serverConfig.url === $('srvUrl').value.trim() ? serverConfig.remoteUpdatedAt || null : null
        };
    }
    function validServerUrl(url) {
        try {
            const u = new URL(url);
            return u.protocol === 'https:' || (u.protocol === 'http:' && /^(localhost|127\.0\.0\.1|\[::1\])$/.test(u.hostname));
        } catch { return false; }
    }
    $('serverClose').addEventListener('click', closeModals);
    $('serverBackdrop').addEventListener('click', closeModals);
    $('serverSaveBtn').addEventListener('click', () => {
        const next = readServerForm();
        if (!validServerUrl(next.url)) { toast('Enter the full server address, starting with https://'); return; }
        serverConfig = next;
        writeJSON(STORAGE.server, serverConfig);
        updateGithubStatus();
        closeModals();
        toast('Server settings saved');
        if (serverConfig.autoSync) runSync(syncProviders.server, { silent: false });
    });
    $('serverClearBtn').addEventListener('click', () => {
        serverConfig = {};
        RemminStore.remove(STORAGE.server);
        $('srvUrl').value = '';
        $('srvToken').value = '';
        $('srvAutoSync').checked = false;
        updateGithubStatus();
        toast('Server settings cleared from this browser');
    });
    $('serverTestBtn').addEventListener('click', async () => {
        const prior = serverConfig;
        serverConfig = readServerForm();
        if (!validServerUrl(serverConfig.url)) {
            serverConfig = prior;
            toast('Enter the full server address, starting with https://');
            return;
        }
        try {
            const r = await serverRequest('GET');
            if (r && r.text.trim()) JSON.parse(r.text);
            toast(r ? 'Server connection works' : 'Server connection works (no notes stored there yet)');
        } catch (err) {
            toast(`Connection failed: ${err.message.replace(/^Unexpected.*/, 'the server did not answer with JSON')}`);
        } finally {
            serverConfig = prior;
        }
    });

    $('githubOpenBtn').addEventListener('click', showSyncChooser);
    $('githubClose').addEventListener('click', closeModals);
    $('githubBackdrop').addEventListener('click', closeModals);
    $('githubSaveBtn').addEventListener('click', () => {
        const next = readGithubForm();
        if (!next.owner || !next.repo || !next.path || !next.token) {
            toast('Enter owner, repository, path, and token');
            return;
        }
        githubConfig = next;
        writeJSON(STORAGE.github, githubConfig);
        updateGithubStatus();
        closeModals();
        toast('GitHub settings saved');
    });
    $('githubClearBtn').addEventListener('click', () => {
        githubConfig = {};
        RemminStore.remove(STORAGE.github);
        $('ghOwner').value = '';
        $('ghRepo').value = '';
        $('ghPath').value = 'remmin.json';
        $('ghToken').value = '';
        $('ghAutoSync').checked = false;
        updateGithubStatus();
        toast('GitHub settings cleared from this browser');
    });
    $('githubTestBtn').addEventListener('click', async () => {
        const prior = githubConfig;
        githubConfig = readGithubForm();
        if (!githubConfigured()) {
            githubConfig = prior;
            toast('Enter owner, repository, path, and token first');
            return;
        }
        try {
            const repoResponse = await fetch(`https://api.github.com/repos/${encodeURIComponent(githubConfig.owner)}/${encodeURIComponent(githubConfig.repo)}`, {
                headers: {
                    Accept: 'application/vnd.github+json',
                    Authorization: `Bearer ${githubConfig.token}`,
                    'X-GitHub-Api-Version': '2022-11-28'
                }
            });
            if (!repoResponse.ok) throw new Error(repoResponse.status === 404
                ? 'Repository not found or token has no access'
                : `GitHub error ${repoResponse.status}`);
            await githubRequest('GET');
            toast('GitHub connection works');
        } catch (err) {
            toast(`Connection failed: ${err.message}`);
        } finally {
            githubConfig = prior;
        }
    });
    $('githubSyncBtn').addEventListener('click', () => {
        const list = configuredProviders();
        if (!list.length) return showSyncChooser();
        flushEditor();
        syncAll(list);
    });

    /* ---------------- back-up ---------------- */

    async function exportData() {
        flushEditor();
        // The file contains everything, also private notes: ask for the Private Note Pass first
        if (!privatePass && state.notes.some((n) => n.private)) {
            toast("Set your 'Private Note Pass' in Settings (Top right)", { label: 'Open Settings', onClick: () => openSettings('pass') });
            return;
        }
        if (privatePass) {
            const pass = await askDialog({
                title: 'Save .json',
                message: 'Enter your Private Note Pass to save all your notes as a file.',
                input: '', password: true, okLabel: 'Save'
            });
            $('dialogInput').type = 'text';
            if (pass === null) return;
            if (!(await checkPass(pass))) { toast('Wrong pass, the file was not saved'); return; }
        }
        const payload = {
            app: 'Remmin',
            version: 1,
            exportedAt: new Date().toISOString(),
            updatedAt: state.updatedAt,
            folders: state.folders,
            notes: state.notes,
            deleted: state.deleted,
            ...(privatePass ? { privatePass } : {}),
            ...(syncedSettings() ? { settings: syncedSettings() } : {}),
            trash: state.trash,
            trashGone: state.trashGone,
            media: pickMedia(state.media, state.notes, state.trash)
        };
        const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `remmin-${dayKey(new Date())}.json`;
        a.rel = 'noopener';
        // Firefox needs the link in the page; Safari/Firefox need the URL to live a moment longer
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 4000);
        toast('Saved your notes as a .json file');
    }

    // Upload a .json file: replace everything, or add it to the notes you already have
    function importData(file) {
        const reader = new FileReader();
        reader.onerror = () => toast('Could not read this file');
        reader.onload = async () => {
            let snap;
            try {
                snap = normalizeSnapshot(JSON.parse(reader.result));
            } catch (err) {
                toast('This is not a Remmin .json file');
                return;
            }
            flushEditor();
            const count = `${snap.notes.length} note${snap.notes.length === 1 ? '' : 's'}` +
                (snap.folders.length ? ` and ${snap.folders.length} folder${snap.folders.length === 1 ? '' : 's'}` : '');
            const choice = await askDialog({
                title: 'Upload .json',
                message: `This file has ${count}. Add them to your notes, or replace everything you have now?`,
                okLabel: 'Add to my notes',
                altLabel: 'Replace all'
            });
            if (!choice) return;
            const now = new Date().toISOString();
            if (choice === 'alt') {
                // replace: the notes/folders that are not in the file count as deleted (also for GitHub sync)
                const keepNotes = new Set(snap.notes.map((n) => n.id));
                const keepFolders = new Set(snap.folders.map((f) => f.id));
                state.notes.forEach((n) => { if (!keepNotes.has(n.id)) { state.deleted.notes[n.id] = now; addToTrash(n); } });
                state.folders.forEach((f) => { if (!keepFolders.has(f.id)) state.deleted.folders[f.id] = now; });
                snap.notes.forEach((n) => { n.modifiedAt = now; delete state.deleted.notes[n.id]; });
                snap.folders.forEach((f) => { f.modifiedAt = now; delete state.deleted.folders[f.id]; });
                state.notes = snap.notes;
                state.folders = snap.folders;
            } else {
                const merged = mergeSnapshots(localSnapshot(), { ...snap, deleted: { notes: {}, folders: {} } });
                state.notes = merged.notes.map((n) => { migrateNote(n); return n; });
                state.folders = merged.folders;
            }
            // no pass on this device yet: take the one from the file
            state.trashGone = mergeGone(state.trashGone, snap.trashGone);
            state.trash = mergeTrash(state.trash, snap.trash, state.trashGone, new Set(state.notes.map((n) => n.id)));
            saveTrash(true);
            if (snap.settings) {
                const mine = syncedSettings();
                if (!mine || timeOf(snap.settings.at) > timeOf(mine.at)) applySettings(snap.settings);
            }
            if (!privatePass && snap.privatePass) { privatePass = snap.privatePass; writeJSON(STORAGE.privatePass, privatePass); updatePassPanel(); }
            if (!noteById(state.currentNoteId)) state.currentNoteId = sortedNotes(state.notes)[0]?.id || null;
            state.currentFolderId = noteById(state.currentNoteId)?.folderId || null;
            state.prefs.expandedFolders = ['__none', ...state.folders.map((f) => f.id)];
            saveFolders();
            saveNotes();
            savePrefs();
            renderTree();
            renderEditor();
            if (state.prefs.activeTab === 'calendar') renderCalendar();
            toast(choice === 'alt' ? `Replaced with ${count}` : `Added ${count}`);
        };
        reader.readAsText(file);
    }

    $('exportBtn').addEventListener('click', exportData);
    $('importBtn').addEventListener('click', () => $('importFile').click());
    $('importFile').addEventListener('change', (e) => {
        if (e.target.files[0]) importData(e.target.files[0]);
        e.target.value = '';
    });

    /* ---------------- alarmen ---------------- */

    // An alarm only rings if its time was at most this long ago. Older (missed)
    // alarms, e.g. when the app was closed or after a sync, are skipped silently.
    const ALARM_GRACE_MS = 10 * 60000;
    const ALARM_MAX_SOUND_MS = 20000;
    const ALARM_REPEAT_MS = 60 * 1000;          // ring again every minute…
    const ALARM_GIVE_UP_MS = 2 * 60 * 60 * 1000; // …until stopped, or for at most 2 hours

    // Mark the most recent (already passed) occurrence as handled, so changing a
    // note's schedule never makes it ring immediately for a time in the past.
    function resetAlarmRuntime(note) {
        delete state.alarms[note.id];
        const occ = note.alarmEnabled ? latestOccurrence(note, Date.now()) : null;
        if (occ) state.alarms[note.id] = { dueTime: occ.getTime(), repeatsLeft: 0, snoozeMinutes: note.alarmSnoozeMinutes, nextAt: null };
        saveAlarms();
    }

    function checkAlarms() {
        const now = Date.now();
        // Re-read so several open tabs don't ring the same alarm twice
        state.alarms = readJSON(STORAGE.alarms, {}) || {};
        let changed = false;

        Object.keys(state.alarms).forEach((id) => {
            const n = noteById(id);
            if (!n || !n.alarmEnabled || !n.scheduledAt) { delete state.alarms[id]; changed = true; }
        });

        state.notes.forEach((note) => {
            if (!note.alarmEnabled || !note.scheduledAt) return;
            const occ = latestOccurrence(note, now);
            if (!occ) return;
            const dueTime = occ.getTime();

            let runtime = state.alarms[note.id];
            if (!runtime || runtime.dueTime !== dueTime) {
                runtime = {
                    dueTime,
                    rings: 0,
                    nextAt: now - dueTime > ALARM_GRACE_MS ? null : dueTime
                };
                state.alarms[note.id] = runtime;
                changed = true;
            }

            if (runtime.nextAt === null || runtime.nextAt > now) return;

            // ring now, and again every minute until "Stop alarm" (or snooze) is pressed
            runtime.rings = (runtime.rings || 0) + 1;
            const stillGoing = now - runtime.dueTime < ALARM_GIVE_UP_MS;
            runtime.nextAt = stillGoing ? now + ALARM_REPEAT_MS : null;
            changed = true;
            saveAlarms();
            showAlarm(note, dueTime, runtime.rings, !stillGoing);
        });

        if (changed) saveAlarms();
    }

    function showAlarm(note, dueTime, ring = 1, lastRing = false) {
        const title = note.title.trim() || 'Untitled';
        const body = note.private ? 'Private note' : (note.content.trim().slice(0, 120) || 'Scheduled note');
        notify(`⏰ ${title}`, body, 'remmin-alarm-' + note.id);
        playAlarmSound();
        const snooze = Math.max(1, Number(note.alarmSnoozeMinutes) || 5);
        const runtimeFor = () => {
            state.alarms = readJSON(STORAGE.alarms, {}) || {};
            const r = state.alarms[note.id];
            return r && r.dueTime === dueTime ? r : null;
        };
        toast(`⏰ ${title} – ${formatTime(toLocalIso(new Date(dueTime)))}${ring > 1 ? ` (${ring}×)` : ''}${lastRing ? ' · last reminder' : ''}`, {
            persistent: true,
            key: 'alarm-' + note.id,
            buttons: [
                {
                    label: `Snooze ${snooze} min`,
                    secondary: true,
                    onClick: () => {
                        stopAlarmSound();
                        const r = runtimeFor();
                        const until = Date.now() + snooze * 60000;
                        if (r) { r.nextAt = until; saveAlarms(); }
                        toast(`Snoozed until ${formatTime(toLocalIso(new Date(until)))}`);
                    }
                },
                {
                    label: 'Stop alarm',
                    onClick: () => {
                        stopAlarmSound();
                        const r = runtimeFor();
                        if (r) { r.nextAt = null; saveAlarms(); }
                    }
                }
            ]
        });
    }

    let alarmAudio = null, alarmContext = null, alarmMaster = null, alarmStopTimer = null;

    function getAudioContext() {
        try {
            alarmContext = alarmContext || new (window.AudioContext || window.webkitAudioContext)();
            if (alarmContext.state === 'suspended') alarmContext.resume();
            return alarmContext;
        } catch (_) { return null; }
    }

    function stopAlarmSound() {
        clearTimeout(alarmStopTimer);
        if (alarmAudio) { alarmAudio.pause(); alarmAudio.currentTime = 0; alarmAudio = null; }
        if (alarmMaster) {
            try {
                alarmMaster.gain.cancelScheduledValues(0);
                alarmMaster.gain.value = 0;
                alarmMaster.disconnect();
            } catch (_) {}
            alarmMaster = null;
        }
    }

    // One short "beep" with a soft attack/release so it doesn't click
    function beep(ctx, out, freq, at, dur) {
        const osc = ctx.createOscillator();
        const env = ctx.createGain();
        osc.type = 'triangle';
        osc.frequency.value = freq;
        env.gain.setValueAtTime(0, at);
        env.gain.linearRampToValueAtTime(1, at + 0.012);
        env.gain.setValueAtTime(1, at + dur - 0.03);
        env.gain.linearRampToValueAtTime(0, at + dur);
        osc.connect(env);
        env.connect(out);
        osc.start(at);
        osc.stop(at + dur + 0.02);
    }

    // Standard alarm: a little two-bar tune ("bi-bi-biep  bi-bi-biep  biep-biep-bieeep")
    // repeated with short pauses, for at most 20 seconds.
    const ALARM_TUNE = [
        // [frequency Hz, start (s), duration (s)]
        [1046.5, 0.00, 0.09], [1318.5, 0.14, 0.09], [1568.0, 0.28, 0.20],   // C E G
        [1046.5, 0.70, 0.09], [1318.5, 0.84, 0.09], [1568.0, 0.98, 0.20],   // C E G
        [1568.0, 1.40, 0.09], [1568.0, 1.54, 0.09], [2093.0, 1.68, 0.35]    // G G C'
    ];
    const ALARM_TUNE_LENGTH = 2.6; // seconds, including the pause after the tune

    function playAlarmSound() {
        stopAlarmSound();
        alarmStopTimer = setTimeout(stopAlarmSound, ALARM_MAX_SOUND_MS);
        const saved = RemminStore.get('remmin.alarmSound.v1');
        if (saved) {
            alarmAudio = new Audio(saved);
            alarmAudio.loop = true;
            const audio = alarmAudio;
            audio.play().catch(() => {
                if (alarmAudio === audio) { alarmAudio = null; playBeepPattern(); }
            });
            return;
        }
        playBeepPattern();
    }

    function playBeepPattern() {
        const ctx = getAudioContext();
        if (!ctx) return;
        alarmMaster = ctx.createGain();
        alarmMaster.gain.value = 0.16;
        alarmMaster.connect(ctx.destination);
        const start = ctx.currentTime + 0.05;
        const maxSec = ALARM_MAX_SOUND_MS / 1000;
        for (let t = 0; t + ALARM_TUNE_LENGTH <= maxSec + 0.5; t += ALARM_TUNE_LENGTH) {
            ALARM_TUNE.forEach(([freq, offset, dur]) => {
                if (t + offset + dur <= maxSec) beep(ctx, alarmMaster, freq, start + t + offset, dur);
            });
        }
    }

    $('alarmSoundFile').addEventListener('change', (e) => {
        const file = e.target.files && e.target.files[0]; if (!file) return;
        const reader = new FileReader();
        reader.onload = () => {
            try {
                RemminStore.set('remmin.alarmSound.v1', reader.result);
                settingsChanged();
                $('alarmSoundStatus').textContent = `Using custom sound: ${file.name}`;
                toast('Custom alarm sound saved');
            } catch (_) { toast('This sound file is too large to store in browser storage'); }
        };
        reader.readAsDataURL(file);
    });
    $('standardAlarmSoundBtn').addEventListener('click', () => {
        RemminStore.remove('remmin.alarmSound.v1');
        settingsChanged();
        $('alarmSoundFile').value = '';
        $('alarmSoundStatus').textContent = 'Using standard sound';
        toast('Standard alarm sound selected');
    });
    $('testAlarmSoundBtn').addEventListener('click', () => {
        if (alarmAudio || alarmMaster) {
            stopAlarmSound();
            $('testAlarmSoundBtn').textContent = 'Test sound';
            return;
        }
        playAlarmSound();
        $('testAlarmSoundBtn').textContent = 'Stop test';
        setTimeout(() => { $('testAlarmSoundBtn').textContent = 'Test sound'; }, ALARM_MAX_SOUND_MS);
    });
    $('openAlarmSoundSettings').addEventListener('click', () => {
        const customSound = RemminStore.get('remmin.alarmSound.v1');
        $('alarmSoundStatus').textContent = customSound ? 'Using a custom sound' : 'Using standard sound';
        $('alarmSoundModal').classList.add('active');
    });
    $('alarmSoundClose').addEventListener('click', closeModals);
    $('alarmSoundBackdrop').addEventListener('click', closeModals);

    function notify(title, body, tag) {
        if (!('Notification' in window)) return;
        if (Notification.permission === 'granted') {
            try {
                // same tag = replaces the previous one instead of stacking up
                new Notification(title, { body, icon: 'remmin_logo.svg', ...(tag ? { tag } : {}) });
            } catch (err) {
                console.warn('Could not show notification', err);
            }
        } else if (Notification.permission !== 'denied') {
            Notification.requestPermission();
        }
    }

    /* ---------------- meldingen (toast) ---------------- */

    let toastTimer = null;

    function toast(message, action) {
        let host = $('toastHost');
        if (!host) {
            host = document.createElement('div');
            host.id = 'toastHost';
            document.body.appendChild(host);
        }
        // Keep alarm toasts (persistent) visible; replace everything else
        host.querySelectorAll('.toast').forEach((t) => {
            if (!t.dataset.persistent || (action?.key && t.dataset.key === action.key)) t.remove();
        });

        const el = document.createElement('div');
        el.className = 'toast';
        if (action?.persistent) el.dataset.persistent = 'true';
        if (action?.key) el.dataset.key = action.key;
        const text = document.createElement('span');
        text.textContent = message;
        el.appendChild(text);

        const buttons = action ? (action.buttons || [{ label: action.label, onClick: action.onClick }]) : [];
        buttons.forEach((b) => {
            const btn = document.createElement('button');
            btn.className = 'toast-action' + (b.secondary ? ' secondary' : '');
            btn.textContent = b.label;
            btn.addEventListener('click', () => {
                el.remove();
                b.onClick();
            });
            el.appendChild(btn);
        });

        host.appendChild(el);
        if (!action?.persistent) {
            clearTimeout(toastTimer);
            toastTimer = setTimeout(() => el.remove(), action ? 20000 : 3500);
        }
    }

    /* ---------------- stray drops ---------------- */
    // A file (or a dragged note) let go outside a drop area must not make the browser
    // open/navigate away from the app (Firefox and Safari do that).
    document.addEventListener('dragover', (e) => {
        if (!e.defaultPrevented && Array.from(e.dataTransfer ? e.dataTransfer.types || [] : []).includes('Files')) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'none';
        }
    });
    document.addEventListener('drop', (e) => {
        // moving text or a picture inside the note itself is fine
        if (e.target && e.target.closest && e.target.closest('#noteContent')) return;
        if (!e.defaultPrevented) e.preventDefault();
    });

    /* ---------------- action menu (the ⋯ buttons on phones) ---------------- */
    let sheetActions = [];
    function showActionSheet(title, actions) {
        sheetActions = actions;
        $('actionSheetTitle').textContent = title;
        $('actionSheetButtons').innerHTML = actions.map((a, i) =>
            `<button type="button" class="btn action-btn${a.danger ? ' danger' : ''}" data-i="${i}">${escapeHtml(a.label)}</button>`).join('');
        openModal('actionSheet');
    }
    $('actionSheetButtons').addEventListener('click', (e) => {
        const btn = e.target.closest('.action-btn');
        if (!btn) return;
        const action = sheetActions[Number(btn.dataset.i)];
        $('actionSheet').classList.remove('active');
        if (action) action.run();
    });
    $('actionSheetCancel').addEventListener('click', () => $('actionSheet').classList.remove('active'));
    $('actionSheetBackdrop').addEventListener('click', () => $('actionSheet').classList.remove('active'));

    /* ---------------- phone: explorer button + tap next to the sidebar closes it ---------------- */
    $('mobileExplorerBtn').addEventListener('click', () => app.classList.toggle('mobile-open'));
    document.querySelector('.main').addEventListener('pointerdown', (e) => {
        if (!isMobile() || !app.classList.contains('mobile-open')) return;
        if (e.target.closest('#mobileExplorerBtn')) return;
        app.classList.remove('mobile-open');
    });

    /* ---------------- drag & drop on touch screens ----------------
     * Phones don't do HTML drag & drop with a finger. Here a long press (0.3 s)
     * picks an item up and moving the finger drags it; under the hood it fires the
     * same dragstart / dragover / drop / dragend events the mouse version uses,
     * so everything (reorder, move to folder, put on a calendar day) works the same.
     */
    (function setupTouchDragDrop() {
        const LONG_PRESS_MS = 300;
        const MOVE_TOLERANCE = 10;
        let pending = null; // finger is down on a draggable item, waiting for the long press
        let drag = null;    // an item is being dragged

        function fire(type, el, x, y, dt, related) {
            if (!el) return false;
            let ev;
            try {
                ev = new DragEvent(type, { bubbles: true, cancelable: true, clientX: x, clientY: y, dataTransfer: dt, relatedTarget: related || null });
            } catch (_) {
                ev = new Event(type, { bubbles: true, cancelable: true });
                Object.defineProperties(ev, {
                    dataTransfer: { value: dt }, clientX: { value: x }, clientY: { value: y }, relatedTarget: { value: related || null }
                });
            }
            el.dispatchEvent(ev);
            return ev.defaultPrevented;
        }

        const touchById = (list, id) => [...list].find((t) => t.identifier === id);

        function cancelPending() {
            if (pending) clearTimeout(pending.timer);
            pending = null;
        }

        function scrollParent(el) {
            for (let n = el; n && n !== document.body; n = n.parentElement) {
                const s = getComputedStyle(n);
                if (/(auto|scroll)/.test(s.overflowY) && n.scrollHeight > n.clientHeight) return n;
            }
            return null;
        }

        function startDrag() {
            const { source, x, y, id } = pending;
            pending = null;
            if (!document.body.contains(source)) return;
            let dt;
            try { dt = new DataTransfer(); } catch (_) { return; }
            const r = source.getBoundingClientRect();
            drag = { source, dt, id, x, y, startX: x, target: null, accepted: false, dx: x - r.left, dy: y - r.top, raf: 0,
                fromSidebar: !!source.closest('#sidebar') };
            if (fire('dragstart', source, x, y, dt)) { drag = null; return; }
            const ghost = source.cloneNode(true);
            ghost.classList.add('touch-drag-ghost');
            ghost.removeAttribute('id');
            ghost.style.width = `${r.width}px`;
            document.body.appendChild(ghost);
            drag.ghost = ghost;
            document.body.classList.add('touch-dragging');
            source.classList.add('touch-drag-source');
            if (navigator.vibrate) { try { navigator.vibrate(20); } catch (_) { /* ignore */ } }
            moveTo(x, y);
            drag.raf = requestAnimationFrame(autoScroll);
        }

        function moveTo(x, y) {
            drag.x = x;
            drag.y = y;
            drag.ghost.style.transform = `translate(${x - drag.dx}px, ${y - drag.dy}px)`;
            // On a phone the sidebar covers almost the whole screen. Pull a note sideways
            // (towards the page) and the sidebar slides away so you can drop it on the calendar;
            // moving up and down keeps it open for reordering.
            const appEl = document.getElementById('app');
            if (drag.fromSidebar && appEl.classList.contains('mobile-open')) {
                const sr = document.getElementById('sidebar').getBoundingClientRect();
                const right = !appEl.classList.contains('sidebar-right');
                const pulled = right ? x - drag.startX > 70 : drag.startX - x > 70;
                if (pulled || x > sr.right + 4 || x < sr.left - 4) appEl.classList.remove('mobile-open');
            }
            const target = document.elementFromPoint(x, y);
            if (target !== drag.target) {
                if (drag.target) fire('dragleave', drag.target, x, y, drag.dt, target);
                if (target) fire('dragenter', target, x, y, drag.dt, drag.target);
                drag.target = target;
            }
            drag.accepted = target ? fire('dragover', target, x, y, drag.dt) : false;
        }

        // keep scrolling while the finger rests near the top/bottom edge of a list or page
        function autoScroll() {
            if (!drag) return;
            const scroller = drag.target && scrollParent(drag.target);
            if (scroller) {
                const r = scroller.getBoundingClientRect();
                const edge = 48;
                let dy = 0;
                if (drag.y < r.top + edge) dy = -Math.ceil((r.top + edge - drag.y) / 4);
                else if (drag.y > r.bottom - edge) dy = Math.ceil((drag.y - (r.bottom - edge)) / 4);
                if (dy) { scroller.scrollTop += dy; moveTo(drag.x, drag.y); }
            }
            drag.raf = requestAnimationFrame(autoScroll);
        }

        function endDrag(drop) {
            const d = drag;
            drag = null;
            cancelAnimationFrame(d.raf);
            if (drop && d.accepted && d.target) fire('drop', d.target, d.x, d.y, d.dt);
            fire('dragend', d.source, d.x, d.y, d.dt);
            if (d.ghost) d.ghost.remove();
            d.source.classList.remove('touch-drag-source');
            document.body.classList.remove('touch-dragging');
        }

        document.addEventListener('touchstart', (e) => {
            if (drag) return;
            if (e.touches.length !== 1) { cancelPending(); return; }
            const el = e.target.nodeType === 1 ? e.target : e.target.parentElement;
            const source = el && el.closest('[draggable="true"]');
            if (!source || el.closest('button, input, select, textarea, a, .voice-handle')) return;
            const t = e.touches[0];
            cancelPending();
            pending = { source, x: t.clientX, y: t.clientY, id: t.identifier, timer: setTimeout(startDrag, LONG_PRESS_MS) };
        }, { passive: true, capture: true });

        document.addEventListener('touchmove', (e) => {
            if (pending) {
                const t = touchById(e.touches, pending.id);
                if (!t || Math.hypot(t.clientX - pending.x, t.clientY - pending.y) > MOVE_TOLERANCE) cancelPending(); // it's a scroll
                return;
            }
            if (!drag) return;
            const t = touchById(e.touches, drag.id);
            if (!t) return;
            e.preventDefault();
            moveTo(t.clientX, t.clientY);
        }, { passive: false, capture: true });

        document.addEventListener('touchend', (e) => {
            if (pending) { cancelPending(); return; } // a normal tap
            if (!drag || touchById(e.touches, drag.id)) return;
            e.preventDefault(); // no "click" after a drag
            endDrag(true);
        }, { passive: false, capture: true });

        document.addEventListener('touchcancel', () => {
            cancelPending();
            if (drag) endDrag(false);
        }, { capture: true });

        // no long-press menu / text selection while picking something up
        document.addEventListener('contextmenu', (e) => {
            if (pending || drag) e.preventDefault();
        }, { capture: true });

        // some phones start their own drag on long press: ours already handles it
        document.addEventListener('dragstart', (e) => {
            if (e.isTrusted && (pending || drag)) { e.preventDefault(); e.stopImmediatePropagation(); }
        }, { capture: true });
    })();

    /* ---------------- sneltoetsen ---------------- */

    document.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
            e.preventDefault();
            flushEditor();
            toast('Note saved');
        }
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'n') {
            e.preventDefault();
            newNote();
        }
        // Ctrl+Z / Cmd+Z undoes the last stroke in draw mode and the last fill in grid mode;
        // Ctrl+Y (or Ctrl+Shift+Z) redoes it
        const key = e.key.toLowerCase();
        const isUndo = (e.ctrlKey || e.metaKey) && !e.shiftKey && !e.altKey && key === 'z';
        const isRedo = (e.ctrlKey || e.metaKey) && !e.altKey && ((key === 'y' && !e.shiftKey) || (key === 'z' && e.shiftKey));
        if (isUndo || isRedo) {
            const mode = state.prefs.mode;
            const typing = e.target.closest && e.target.closest('input, textarea, select, [contenteditable="true"], [contenteditable=""]');
            const modalOpen = document.querySelector('.modal.active');
            if ((mode === 'draw' || mode === 'grid') && state.prefs.activeTab !== 'calendar' && !modalOpen
                && !(typing && typing.offsetParent !== null) && state.currentNoteId
                && !isPrivateLocked(noteById(state.currentNoteId))) {
                e.preventDefault();
                if (isUndo) $(mode === 'draw' ? 'toolUndo' : 'gridUndo').click();
                else if (mode === 'draw') redoDraw();
                else redoGrid();
            }
        }
        if (gridTool === 'select' && (gridPick.length || gridCopyMode) && gridPickActive()) {
            if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); deleteGridPick(); return; }
            if (e.key === 'Escape') { gridPick = []; gridCopyMode = false; renderGrid(); updateGridPickBar(); return; }
            if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'd') { e.preventDefault(); copyGridPick(); return; }
        }
        if (drawPick.length && state.draw.tool === 'select' && drawPickActive()) {
            if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); deletePick(); return; }
            if (e.key === 'Escape') { drawPick = []; renderStrokes(); return; }
            if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'd') { e.preventDefault(); duplicatePick(); return; }
        }
        if (drawSel && state.prefs.mode === 'draw' && !document.querySelector('.modal.active')) {
            const t = e.target.closest && e.target.closest('input, textarea, [contenteditable="true"]');
            if (!(t && t.offsetParent !== null)) {
                if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); deleteSelectedPicture(); return; }
                if (e.key === 'Escape') { drawSel = null; renderStrokes(); return; }
            }
        }
        if (e.key === 'Escape') closeModals();
    });

    /* ---------------- storage meters (above Trash) ---------------- */
    const GITHUB_REPO_LIMIT = 5 * 1024 ** 3; // GitHub: keep a repository under 5 GB (each file under 100 MB)
    let localQuota = null;
    // 6,18 MB (used: always two decimals) · 100 MB / 120 GB (totals: as short as possible)
    const sizeText = (bytes, fixed, unit) => {
        const div = unit === 'GB' ? 1024 ** 3 : 1024 ** 2;
        return (bytes / div).toLocaleString(undefined, fixed ? { minimumFractionDigits: 2, maximumFractionDigits: 2 } : { maximumFractionDigits: 1 });
    };
    function setMeter(id, show, used, total, title) {
        const el = $(id);
        el.hidden = !show;
        if (!show) return;
        const bar = el.querySelector('.storage-meter-fill');
        const num = el.querySelector('.storage-meter-num');
        el.title = title;
        if (used == null) { num.textContent = 'not synced yet'; bar.style.width = '0'; return; }
        const frac = total ? Math.min(1, used / total) : 0;
        if (!total) num.textContent = `${sizeText(used, true, 'MB')} MB used`;
        else if (total >= 1024 ** 3) {
            // a big total in GB; what is used stays in MB while it is small
            num.textContent = used >= 1024 ** 3
                ? `${sizeText(used, true, 'GB')}/${sizeText(total, false, 'GB')} GB used`
                : `${sizeText(used, true, 'MB')} MB/${sizeText(total, false, 'GB')} GB used`;
        } else num.textContent = `${sizeText(used, true, 'MB')}/${sizeText(total, false, 'MB')} MB used`;
        bar.style.width = total ? `${Math.max(frac * 100, used > 0 ? 1.5 : 0)}%` : '0';
        el.classList.toggle('warn', frac >= 0.75 && frac < 0.9);
        el.classList.toggle('full', frac >= 0.9);
        el.title = title;
    }
    let meterTimer = null;
    function updateStorageMeters() {
        clearTimeout(meterTimer);
        meterTimer = setTimeout(() => {
            setMeter('meterGithub', githubConfigured(), Number.isFinite(githubConfig.fileSize) ? githubConfig.fileSize : null,
                GITHUB_REPO_LIMIT, 'Your notes + pictures, videos and audio on GitHub (every file is separate; GitHub wants a repository under 5 GB)');
            setMeter('meterServer', serverConfigured(), Number.isFinite(serverConfig.fileSize) ? serverConfig.fileSize : null,
                null, 'Size of your notes on your server');
            const quota = RemminStore.usingIndexedDB ? localQuota : 5 * 1024 * 1024;
            setMeter('meterLocal', true, RemminStore.bytes(), quota,
                RemminStore.usingIndexedDB
                    ? 'Remmin data saved offline on this device (IndexedDB), out of the space your browser allows'
                    : 'Remmin data in local storage on this device (about 5 MB is allowed)');
        }, 300);
    }
    async function refreshQuota() {
        try {
            if (navigator.storage && navigator.storage.estimate) localQuota = (await navigator.storage.estimate()).quota || null;
        } catch (_) { localQuota = null; }
        updateStorageMeters();
    }
    RemminStore.onChange(updateStorageMeters);

    /* ---- Hubs bar: for now only shows the hubs (what each one does comes later) ---- */
    function applyHubs() {
        const collapsed = !!state.prefs.hubsCollapsed;
        app.classList.toggle('hubs-collapsed', collapsed);
        const left = state.prefs.sidebarSide === 'right'; // the hubs are on the other side than the Explorer
        $('expandHubs').hidden = !collapsed;
        $('expandHubs').classList.toggle('left', left);
        $('expandHubs').textContent = String.fromCharCode(left ? 8250 : 8249);
        $('collapseHubs').textContent = String.fromCharCode(left ? 8249 : 8250);
    }
    $('collapseHubs').addEventListener('click', () => { state.prefs.hubsCollapsed = true; savePrefs(); applyHubs(); fitCanvas(); });
    $('expandHubs').addEventListener('click', () => { state.prefs.hubsCollapsed = false; savePrefs(); applyHubs(); fitCanvas(); });
    applyHubs();
    (() => {
        const bar = $('hubsBar');
        if (!bar) return;
        const tip = document.createElement('div');
        tip.className = 'hub-tip';
        tip.hidden = true;
        document.body.appendChild(tip);
        const showTip = (btn) => {
            const r = btn.getBoundingClientRect();
            const onRight = !$('app').classList.contains('sidebar-right'); // the bar is on the right: the label goes left of it
            tip.textContent = btn.dataset.name;
            tip.hidden = false;
            tip.classList.toggle('right', !onRight);
            tip.style.top = `${r.top + r.height / 2}px`;
            tip.style.left = onRight ? `${r.left - tip.offsetWidth - 12}px` : `${r.right + 12}px`;
        };
        bar.addEventListener('mouseover', (e) => { const b = e.target.closest('.hub-btn'); if (b && !isMobile()) showTip(b); });
        bar.addEventListener('mouseout', (e) => { if (isMobile()) return; if (!e.relatedTarget || !bar.contains(e.relatedTarget) || !e.relatedTarget.closest('.hub-btn')) tip.hidden = true; });
        bar.addEventListener('click', (e) => {
            const b = e.target.closest('.hub-btn');
            if (!b) return;
            bar.querySelectorAll('.hub-btn').forEach((x) => x.classList.toggle('active', x === b));
            if (isMobile()) { showTip(b); clearTimeout(bar.tipTimer); bar.tipTimer = setTimeout(() => { tip.hidden = true; }, 1600); } // on a phone: the name for a moment
        });
    })();

    /* ---- click the storage bars: which notes use the most space (after the Private Note Pass) ---- */
    const fmtSize = (n) => (n >= 1048576 ? `${(n / 1048576).toFixed(n >= 10485760 ? 0 : 1)} MB` : n >= 1024 ? `${Math.round(n / 1024)} KB` : `${n} B`);
    // the pictures (data:image) inside a piece of a note
    function pictureBytes(v) {
        const text = v === undefined || v === null ? '' : typeof v === 'string' ? v : JSON.stringify(v);
        let n = 0;
        const re = /data:image\/[\w.+-]+;base64,[A-Za-z0-9+/=]+/g;
        let m;
        while ((m = re.exec(text))) n += m[0].length;
        return n;
    }
    function noteSizes(note) {
        const len = (v) => (v === undefined || v === null ? 0 : typeof v === 'string' ? v.length : JSON.stringify(v).length);
        const drawParts = [note.strokes, note.grids, note.gridBgs, note.drawing];
        const pics = { Text: pictureBytes(note.html), Draw: drawParts.reduce((t, v) => t + pictureBytes(v), 0), Collage: pictureBytes(note.collage) };
        const parts = {
            Text: len(note.html) - pics.Text,
            Draw: drawParts.reduce((t, v) => t + len(v), 0) - pics.Draw,
            Collage: len(note.collage) - pics.Collage,
            Project: len(note.project),
            Pictures: pics.Text + pics.Draw + pics.Collage
        };
        const all = len(note);
        parts.Other = Math.max(0, all - parts.Text - parts.Draw - parts.Collage - parts.Project - parts.Pictures);
        const ids = new Set();
        const re = /media:([a-z0-9]{4,40})/g;
        const text = JSON.stringify(note);
        let m;
        while ((m = re.exec(text))) ids.add(m[1]);
        let videos = 0, audio = 0;
        ids.forEach((id) => { const d = state.media[id] || ''; if (d.startsWith('data:audio/')) audio += d.length; else videos += d.length; });
        parts.Audio = audio;
        return { parts, videos: videos + audio, ids, total: all + videos + audio };
    }
    function renderStorageOverview() {
        const rows = state.notes.map((n) => ({ note: n, ...noteSizes(n) })).sort((a, b) => b.total - a.total);
        // how many notes use each video (a video used by several notes is stored once)
        const users = new Map();
        rows.forEach((r) => r.ids.forEach((id) => users.set(id, (users.get(id) || 0) + 1)));
        const picturesTotal = rows.reduce((t, r) => t + r.parts.Pictures, 0);
        const notesTotal = rows.reduce((t, r) => t + r.total - r.videos - r.parts.Pictures - r.parts.Audio, 0);
        const videosTotal = Object.values(state.media).reduce((t, v) => t + (v.startsWith('data:audio/') ? 0 : v.length), 0);
        const audioTotal = Object.values(state.media).reduce((t, v) => t + (v.startsWith('data:audio/') ? v.length : 0), 0);
        const trashTotal = JSON.stringify(state.trash || []).length;
        const max = rows.length ? rows[0].total : 1;
        $('storageSummary').innerHTML = `
            <div class="so-row-sum"><span>Notes (without pictures / videos)</span><b>${fmtSize(notesTotal)}</b></div>
            <div class="so-row-sum"><span>Pictures</span><b>${fmtSize(picturesTotal)}</b></div>
            <div class="so-row-sum"><span>Videos (each stored once)</span><b>${fmtSize(videosTotal)}</b></div>
            <div class="so-row-sum"><span>Audio (each stored once)</span><b>${fmtSize(audioTotal)}</b></div>
            <div class="so-row-sum"><span>Trash</span><b>${fmtSize(trashTotal)}</b></div>`;
        $('storageList').innerHTML = rows.length ? rows.map((r, i) => {
            const shared = [...r.ids].filter((id) => (users.get(id) || 0) > 1).length;
            const bits = [...Object.entries(r.parts), ['Videos', r.videos - r.parts.Audio]].filter(([, v]) => v >= 1024)
                .map(([k, v]) => `${k} ${fmtSize(v)}`).join(' · ');
            return `<button type="button" class="so-item" data-id="${escapeHtml(r.note.id)}">
                <span class="so-rank">${i + 1}</span>
                <span class="so-main">
                    <span class="so-title">${escapeHtml(markTitle(r.note, r.note.title) || 'Untitled')}</span>
                    <span class="so-bar"><span style="width:${Math.max(1, r.total / max * 100).toFixed(1)}%"></span></span>
                    <span class="so-bits">${escapeHtml(bits || 'almost nothing')}${shared ? ` · ${shared} video${shared > 1 ? 's' : ''} also used in other notes` : ''}</span>
                </span>
                <span class="so-size">${fmtSize(r.total)}</span>
            </button>`;
        }).join('') : '<div class="so-empty">No notes yet</div>';
    }
    $('storageMeters').addEventListener('click', async () => {
        if (!privatePass) {
            toast("Set your 'Private Note Pass' in Settings first: it is needed to see this", { label: 'Open Settings', onClick: () => openSettings('pass') });
            return;
        }
        const pass = await askDialog({ title: 'Storage overview', message: 'Enter your Private Note Pass to see which notes use the most space.', input: '', password: true, okLabel: 'Show' });
        $('dialogInput').type = 'text';
        if (pass === null || pass === false || pass === '') return;
        if (!(await checkPass(pass))) { toast('Wrong pass'); return; }
        flushEditor();
        renderStorageOverview();
        openModal('storageModal');
    });
    $('storageList').addEventListener('click', (e) => {
        const row = e.target.closest('.so-item');
        if (!row) return;
        closeModals();
        setTab('note');
        selectNote(row.dataset.id);
    });
    $('storageClose').addEventListener('click', closeModals);
    $('storageBackdrop').addEventListener('click', closeModals);

    /* ---------------- start ---------------- */

    load();
    applyLayout();
    // the note that was open last time (if it still exists), otherwise the newest
    state.currentNoteId = (noteById(state.prefs.lastNoteId) ? state.prefs.lastNoteId : null) || sortedNotes(state.notes)[0]?.id || null;
    state.currentFolderId = noteById(state.currentNoteId)?.folderId || null;

    if (!state.prefs.expandedFolders.length) {
        state.prefs.expandedFolders = ['__none', ...state.folders.map((f) => f.id)];
    }

    renderTree();
    renderEditor();
    setTab(state.prefs.activeTab === 'calendar' ? 'calendar' : 'note');

    if ('Notification' in window && Notification.permission === 'default') {
        document.addEventListener('click', () => {
            if (Notification.permission === 'default') Notification.requestPermission();
        }, { once: true });
    }

    // Browsers only allow sound after a user interaction: unlock it on the first click
    document.addEventListener('pointerdown', () => { getAudioContext(); }, { once: true });

    checkAlarms();
    setInterval(checkAlarms, 5000);

    window.addEventListener('beforeunload', flushEditor);
    // phones often close a page without beforeunload: save when it goes to the background
    document.addEventListener('visibilitychange', () => { if (document.hidden) { flushEditor(); RemminStore.flush(); } });
    window.addEventListener('pagehide', () => { flushEditor(); RemminStore.flush(); });
    window.addEventListener('resize', () => {
        fitCanvas();
        if (state.prefs.activeTab === 'calendar') renderCalendar();
    });

    renderBroadcast();
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(renderBroadcast);
    syncReady = true;
    updateGithubStatus();
    refreshQuota();
    setInterval(refreshQuota, 5 * 60 * 1000);
    if (autoProviders().length) {
        setTimeout(() => syncAll(autoProviders(), { silent: true }), 400);
    }
    window.Remmin = { exportData, importData };
});
