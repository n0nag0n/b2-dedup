(() => {
    "use strict";

    const drop = document.getElementById("scratch-drop");
    if (!drop) return;

    const filesInput = document.getElementById("scratch-files");
    const folderInput = document.getElementById("scratch-folder");
    const pickFiles = document.getElementById("scratch-pick-files");
    const pickFolder = document.getElementById("scratch-pick-folder");
    const driveInput = document.getElementById("scratch-drive-name");
    const prefixInput = document.getElementById("scratch-prefix");
    const bucketInput = document.getElementById("scratch-bucket");
    const progress = document.getElementById("scratch-progress");
    const status = document.getElementById("scratch-status");
    const log = document.getElementById("scratch-log");

    const junkDirs = new Set(JSON.parse(drop.dataset.junkDirs || "[]"));
    const junkFiles = new Set(JSON.parse(drop.dataset.junkFiles || "[]"));

    const state = {
        queue: [],
        seen: new Set(),
        totalBytes: 0,
        doneBytes: 0,
        activeLoaded: 0,
        expected: 0,
        finished: 0,
        staged: 0,
        uploading: false,
    };

    function setStatus(text, isError) {
        if (!status) return;
        status.textContent = text;
        status.classList.toggle("error", Boolean(isError));
    }

    function logLine(text) {
        if (!log) return;
        log.hidden = false;
        const lines = (log.textContent ? log.textContent.split("\n") : []);
        lines.push(text);
        while (lines.length > 200) lines.shift();
        log.textContent = lines.join("\n");
        log.scrollTop = log.scrollHeight;
    }

    function formatBytes(bytes) {
        const n = Number(bytes) || 0;
        if (n < 1024) return `${n} B`;
        const units = ["KB", "MB", "GB", "TB"];
        let value = n / 1024;
        let unit = units[0];
        for (let i = 1; i < units.length && value >= 1024; i += 1) {
            value /= 1024;
            unit = units[i];
        }
        return `${value.toFixed(value >= 10 ? 0 : 1)} ${unit}`;
    }

    function normalizeRel(value) {
        return String(value || "").replace(/\\/g, "/").replace(/^\/+/, "");
    }

    function isJunkPath(rel) {
        const parts = rel.split("/").filter(Boolean);
        if (!parts.length) return false;
        for (let i = 0; i < parts.length - 1; i += 1) {
            if (junkDirs.has(parts[i])) return true;
        }
        const name = parts[parts.length - 1];
        return junkFiles.has(name) || name.startsWith("._") || name.startsWith(".Spotlight-");
    }

    function newBatchId() {
        const d = new Date();
        const pad = (n) => String(n).padStart(2, "0");
        const stamp = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`
            + `-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
        const bytes = new Uint8Array(4);
        window.crypto.getRandomValues(bytes);
        const rand = Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("");
        return `${stamp}-${rand}`;
    }

    function renderProgress() {
        if (progress) {
            const loaded = state.doneBytes + state.activeLoaded;
            const pct = state.totalBytes
                ? Math.min(100, Math.round((loaded * 100) / state.totalBytes))
                : 0;
            progress.value = pct;
            progress.textContent = `${pct}%`;
        }
        if (state.uploading) {
            setStatus(`Uploading ${state.finished} of ${state.expected} files…`);
        }
    }

    function addItems(items) {
        let added = 0;
        let junk = 0;
        for (const item of items) {
            const rel = normalizeRel(item.path || (item.file && item.file.name));
            if (!rel || !item.file) continue;
            if (state.seen.has(rel)) {
                logLine(`skipped duplicate path: ${rel}`);
                continue;
            }
            state.seen.add(rel);
            if (isJunkPath(rel)) {
                junk += 1;
                logLine(`skipped junk: ${rel}`);
                continue;
            }
            state.queue.push({ file: item.file, path: rel });
            state.expected += 1;
            state.totalBytes += item.file.size || 0;
            added += 1;
        }
        if (junk) logLine(`Skipped ${junk} junk path${junk === 1 ? "" : "s"} before upload.`);
        if (!added) {
            if (!state.uploading) setStatus("Nothing new to upload.");
            return;
        }
        startQueue();
    }

    function collectEntry(entry, out) {
        if (!entry) return Promise.resolve();
        if (entry.isFile) {
            return new Promise((resolve) => {
                entry.file(
                    (file) => {
                        out.push({ file, path: entry.fullPath || file.name });
                        resolve();
                    },
                    () => resolve(),
                );
            });
        }
        if (!entry.isDirectory) return Promise.resolve();
        const reader = entry.createReader();
        const readBatch = () => new Promise((resolve) => {
            reader.readEntries(resolve, () => resolve([]));
        });
        const walk = async () => {
            let batch = await readBatch();
            while (batch.length) {
                for (const child of batch) {
                    await collectEntry(child, out);
                }
                batch = await readBatch();
            }
        };
        return walk();
    }

    function uploadOne(file, relPath, batchId) {
        return new Promise((resolve, reject) => {
            const body = new FormData();
            body.append("batch", batchId);
            body.append("path", relPath);
            body.append("prefix", prefixInput ? prefixInput.value : "");
            body.append("modified", file.lastModified ? String(file.lastModified) : "");
            body.append("file", file, file.name);

            const xhr = new XMLHttpRequest();
            xhr.open("POST", "/scratch/upload");
            xhr.setRequestHeader("Accept", "application/json");
            xhr.upload.addEventListener("progress", (event) => {
                if (event.lengthComputable) {
                    state.activeLoaded = event.loaded;
                    renderProgress();
                }
            });
            xhr.addEventListener("load", () => {
                state.activeLoaded = 0;
                let data = {};
                try {
                    data = JSON.parse(xhr.responseText);
                } catch (err) {
                    data = {};
                }
                if (xhr.status >= 200 && xhr.status < 300 && data.ok) {
                    resolve(data);
                } else {
                    reject(new Error(data.error || `Upload failed (${xhr.status}).`));
                }
            });
            xhr.addEventListener("error", () => {
                state.activeLoaded = 0;
                reject(new Error("Network error while uploading."));
            });
            xhr.send(body);
        });
    }

    async function finish(batchId) {
        const body = new URLSearchParams();
        body.set("batch", batchId);
        body.set("drive_name", driveInput ? driveInput.value : "");
        body.set("bucket", bucketInput ? bucketInput.value : "");
        setStatus("Upload complete. Starting the ingest job…");
        try {
            const response = await fetch("/scratch/finish", {
                method: "POST",
                headers: { Accept: "application/json" },
                body,
            });
            let data = {};
            try {
                data = await response.json();
            } catch (err) {
                data = {};
            }
            if (data.ok && data.redirect) {
                window.location.assign(data.redirect);
                return;
            }
            setStatus(data.error || "The upload is staged, but the ingest job did not start.", true);
            logLine("The staged files are kept on the Scratch page for a retry.");
        } catch (err) {
            setStatus("The upload is staged, but the ingest job did not start.", true);
            logLine(`Could not start ingest: ${err.message || err}`);
        }
    }

    async function startQueue() {
        if (state.uploading) return;
        state.uploading = true;
        state.doneBytes = 0;
        state.activeLoaded = 0;
        state.finished = 0;
        state.staged = 0;
        state.expected = state.queue.length;
        state.totalBytes = state.queue.reduce(
            (sum, item) => sum + (item.file.size || 0),
            0,
        );
        const batchId = newBatchId();
        setStatus("Preparing upload…");
        renderProgress();

        let failed = false;
        while (state.queue.length) {
            const item = state.queue.shift();
            try {
                const result = await uploadOne(item.file, item.path, batchId);
                state.doneBytes += item.file.size || 0;
                state.finished += 1;
                if (result.skipped) {
                    logLine(`skipped junk: ${result.path || item.path}`);
                } else {
                    state.staged += 1;
                    logLine(`staged ${result.path || item.path} (${formatBytes(result.bytes)})`);
                }
            } catch (err) {
                failed = true;
                state.finished += 1;
                state.doneBytes += item.file.size || 0;
                logLine(`failed ${item.path}: ${err.message || err}`);
                setStatus(
                    `Upload stopped: ${err.message || err} Anything already staged is kept.`,
                    true,
                );
                // Forget the unattempted paths so dropping them again retries
                // cleanly instead of being treated as duplicates.
                state.seen.delete(item.path);
                for (const pending of state.queue) state.seen.delete(pending.path);
                state.queue = [];
                break;
            }
            renderProgress();
        }

        state.uploading = false;
        state.activeLoaded = 0;
        if (failed) return;
        if (state.staged === 0) {
            setStatus("Nothing to ingest — every file was a junk path.");
            return;
        }
        await finish(batchId);
    }

    if (pickFiles && filesInput) {
        pickFiles.addEventListener("click", () => filesInput.click());
    }
    if (pickFolder && folderInput) {
        pickFolder.addEventListener("click", () => folderInput.click());
    }

    if (filesInput) {
        filesInput.addEventListener("change", () => {
            const items = Array.from(filesInput.files || []).map((file) => ({
                file,
                path: file.webkitRelativePath || file.name,
            }));
            filesInput.value = "";
            addItems(items);
        });
    }

    if (folderInput) {
        folderInput.addEventListener("change", () => {
            const items = Array.from(folderInput.files || []).map((file) => ({
                file,
                path: file.webkitRelativePath || file.name,
            }));
            folderInput.value = "";
            addItems(items);
        });
    }

    drop.addEventListener("dragover", (event) => {
        event.preventDefault();
        drop.classList.add("dragging");
    });
    drop.addEventListener("dragleave", () => drop.classList.remove("dragging"));
    drop.addEventListener("click", (event) => {
        if (event.target.closest("button")) return;
        if (event.target === filesInput || event.target === folderInput) return;
        if (filesInput) filesInput.click();
    });
    drop.addEventListener("drop", async (event) => {
        event.preventDefault();
        drop.classList.remove("dragging");
        const transfer = event.dataTransfer;
        if (!transfer) return;

        const collected = [];
        const entries = [];
        if (transfer.items && transfer.items.length) {
            for (const item of transfer.items) {
                if (item.kind !== "file") continue;
                const entry = item.webkitGetAsEntry ? item.webkitGetAsEntry() : null;
                if (entry) entries.push(entry);
            }
        }
        if (entries.length) {
            for (const entry of entries) await collectEntry(entry, collected);
        } else {
            for (const file of transfer.files) {
                collected.push({ file, path: file.name });
            }
        }
        addItems(collected);
    });

    // A drop outside the zone should not navigate away from the page.
    window.addEventListener("dragover", (event) => event.preventDefault());
    window.addEventListener("drop", (event) => event.preventDefault());
})();
