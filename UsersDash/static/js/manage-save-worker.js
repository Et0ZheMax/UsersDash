self.addEventListener("install", () => {
    self.skipWaiting();
});

self.addEventListener("activate", (event) => {
    event.waitUntil((async () => {
        await self.clients.claim();
        await flushQueue();
    })());
});

const DB_NAME = "usersdash-manage-sync-db";
const STORE_NAME = "pending_requests";
const SYNC_TAG = "usersdash-manage-config-sync";

function openDb() {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, 1);
        req.onupgradeneeded = () => {
            const db = req.result;
            if (!db.objectStoreNames.contains(STORE_NAME)) {
                db.createObjectStore(STORE_NAME, { keyPath: "id" });
            }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

async function putRecord(record) {
    if (!record || !record.id) return;
    const db = await openDb();
    await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE_NAME, "readwrite");
        tx.objectStore(STORE_NAME).put(record);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
    db.close();
}

async function deleteRecord(id) {
    if (!id) return;
    const db = await openDb();
    await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE_NAME, "readwrite");
        tx.objectStore(STORE_NAME).delete(id);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
    db.close();
}

async function getAllRecords() {
    const db = await openDb();
    const records = await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE_NAME, "readonly");
        const req = tx.objectStore(STORE_NAME).getAll();
        req.onsuccess = () => resolve(req.result || []);
        req.onerror = () => reject(req.error);
    });
    db.close();
    return records;
}

async function flushQueue() {
    const records = await getAllRecords();
    for (const record of records) {
        const method = record.method || "PUT";
        const headers = record.headers || {};
        const body = record.body || {};
        try {
            const resp = await fetch(record.url, {
                method,
                headers,
                body: JSON.stringify(body),
                keepalive: true,
            });
            if (resp.ok) {
                await deleteRecord(record.id);
            }
        } catch (err) {
            // Оставляем запись в очереди для следующей попытки.
        }
    }
}

self.addEventListener("message", (event) => {
    const data = event && event.data ? event.data : {};
    if (data.type === "MANAGE_CONFIG_ENQUEUE" && data.payload) {
        event.waitUntil(putRecord(data.payload));
        return;
    }
    if (data.type === "MANAGE_CONFIG_DONE" && data.payload && data.payload.id) {
        event.waitUntil(deleteRecord(data.payload.id));
    }
});

self.addEventListener("sync", (event) => {
    if (event.tag === SYNC_TAG) {
        event.waitUntil(flushQueue());
    }
});
