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
    const progress = document.getElementById("scratch-progress");
    const status = document.getElementById("scratch-status");
    const actions = document.getElementById("scratch-actions");
    const usage = document.getElementById("scratch-usage");
    const log = document.getElementById("scratch-log");

    // 248 MB stays under the 256 MB proxy body limit, including the multipart wrapper.
    const CHUNK_BYTES = 248 * 1024 * 1024;
    const MIN_CHUNK_BYTES = 256 * 1024;

    const junkDirs = new Set(JSON.parse(drop.dataset.junkDirs || "[]"));
    const junkFiles = new Set(JSON.parse(drop.dataset.junkFiles || "[]"));

    const state = {
        queue: [],
        seen: new Set(),
        totalBytes: 0,
        doneBytes: 0,
        activeLoaded: 0,
        finished: 0,
        staged: 0,
        stagedBytes: 0,
        failed: [],
        batchId: "",
        uploading: false,
        finishing: false,
        stopped: false,
        chunkSize: CHUNK_BYTES,
        currentName: "",
        currentDetail: "",
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
        if (!state.uploading) return;
        const total = state.finished + (state.currentName ? 1 : 0) + state.queue.length;
        const current = Math.min(total, state.finished + (state.currentName ? 1 : 0));
        let text = `Uploading ${current} of ${total} files`;
        if (state.currentName) text += ` · ${state.currentName}`;
        if (state.currentDetail) text += state.currentDetail;
        setStatus(text);
    }

    function clearActions() {
        if (!actions) return;
        actions.hidden = true;
        actions.replaceChildren();
    }

    function showActions(buttons) {
        if (!actions) return;
        actions.replaceChildren();
        buttons.forEach((spec) => {
            const button = document.createElement("button");
            button.type = "button";
            button.textContent = spec.label;
            if (spec.secondary) button.className = "secondary";
            button.addEventListener("click", spec.onClick);
            actions.appendChild(button);
        });
        actions.hidden = buttons.length === 0;
    }

    function applyUsage(data) {
        if (!usage || !data) return;
        const batches = Number(data.batches) || 0;
        const files = Number(data.files) || 0;
        let text = `${batches} staged ${batches === 1 ? "batch" : "batches"} · ${files.toLocaleString()} files · ${data.bytes_fmt || "0 B"}`;
        if (data.free_fmt) text += ` · ${data.free_fmt} free on that disk`;
        text += " · staging folder ";
        usage.textContent = text;
        const code = document.createElement("code");
        code.className = "wrap";
        code.textContent = data.path || "";
        usage.appendChild(code);
    }

    async function refreshUsage() {
        if (!usage) return;
        try {
            const response = await fetch("/scratch/usage", { headers: { Accept: "application/json" } });
            if (!response.ok) return;
            applyUsage(await response.json());
        } catch (err) {
            // The status line already describes this upload.
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
            state.failed = state.failed.filter((item) => item.path !== rel);
            if (isJunkPath(rel)) {
                junk += 1;
                logLine(`skipped junk: ${rel}`);
                continue;
            }
            state.queue.push({ file: item.file, path: rel });
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

    function selectedDrive() {
        return driveInput ? driveInput.value : "";
    }

    function syncDriveFields() {
        const name = selectedDrive();
        document.querySelectorAll("form[action='/scratch/finish'] input[name='drive_name']").forEach((input) => {
            input.value = name;
        });
        const removeName = document.getElementById("scratch-remove-name");
        if (removeName) removeName.value = name;
        renderDest();
    }

    function renderDest() {
        const dest = document.getElementById("scratch-dest");
        if (!dest) return;
        const drive = selectedDrive() || dest.dataset.drive || "Scratch";
        const prefix = normalizeRel(prefixInput ? prefixInput.value : "").replace(/\/+$/, "");
        dest.textContent = prefix
            ? `New uploads are stored as ${drive}/${prefix}/… The staging folder on this computer is only the waiting area.`
            : `New uploads are stored at the top of ${drive}. Type a folder above to nest them. The staging folder on this computer is only the waiting area.`;
    }

    function sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }

    function plainFailure(status, file) {
        const size = formatBytes(file && file.size);
        if (status === 413) return `the server refused ${size} in one request`;
        if (!status) return "the connection dropped";
        return `upload failed (${status})`;
    }

    function postForm(url, body, file, onProgress) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open("POST", url);
            xhr.setRequestHeader("Accept", "application/json");
            if (onProgress) {
                xhr.upload.addEventListener("progress", (event) => {
                    if (event.lengthComputable) onProgress(event.loaded);
                });
            }
            xhr.addEventListener("load", () => {
                let data = {};
                try {
                    data = JSON.parse(xhr.responseText);
                } catch (err) {
                    data = {};
                }
                if (xhr.status >= 200 && xhr.status < 300 && data.ok) {
                    resolve(data);
                    return;
                }
                const error = new Error(data.error || plainFailure(xhr.status, file));
                error.status = xhr.status;
                error.fatal = xhr.status === 507 && data.fatal !== false;
                if (typeof data.received === "number") error.received = data.received;
                reject(error);
            });
            xhr.addEventListener("error", () => {
                reject(Object.assign(new Error("the connection dropped"), { status: 0 }));
            });
            xhr.send(body);
        });
    }

    async function withTransientRetry(file, send) {
        let last;
        for (let attempt = 0; attempt < 3; attempt += 1) {
            try {
                return await send();
            } catch (err) {
                last = err;
                if (err.fatal || err.status === 400 || err.status === 409 || err.status === 413 || err.status === 507) {
                    throw err;
                }
                await sleep(400 * (attempt + 1));
            }
        }
        throw last;
    }

    function uploadFields(body, file, relPath) {
        body.append("path", relPath);
        body.append("prefix", prefixInput ? prefixInput.value : "");
        body.append("modified", file.lastModified ? String(file.lastModified) : "");
    }

    function uploadWhole(file, relPath, batchId) {
        const body = new FormData();
        body.append("batch", batchId);
        uploadFields(body, file, relPath);
        body.append("file", file, file.name);
        return postForm("/scratch/upload", body, file, (loaded) => {
            state.activeLoaded = Math.min(loaded, file.size || loaded);
            renderProgress();
        });
    }

    async function uploadChunked(file, relPath, batchId) {
        let offset = 0;
        const total = file.size || 0;
        let stuck = 0;
        while (true) {
            const length = total === 0 ? 0 : Math.min(state.chunkSize, total - offset);
            const blob = file.slice(offset, total === 0 ? 0 : offset + length);
            const body = new FormData();
            body.append("batch", batchId);
            uploadFields(body, file, relPath);
            body.append("offset", String(offset));
            body.append("total", String(total));
            body.append("file", blob, file.name);
            let data;
            try {
                data = await withTransientRetry(file, () => postForm(
                    "/scratch/chunk",
                    body,
                    file,
                    (loaded) => {
                        const sent = Math.min(total, offset + loaded);
                        state.activeLoaded = sent;
                        state.currentDetail = total
                            ? ` · ${formatBytes(sent)} of ${formatBytes(total)}`
                            : "";
                        renderProgress();
                    },
                ));
            } catch (err) {
                if (err.status === 409 && typeof err.received === "number" && err.received !== offset) {
                    offset = err.received;
                    stuck = 0;
                    continue;
                }
                if (err.status === 413) {
                    const next = Math.floor(Math.min(state.chunkSize, length || state.chunkSize) / 2);
                    if (next >= MIN_CHUNK_BYTES) {
                        state.chunkSize = next;
                        logLine(`Sending ${relPath} in ${formatBytes(state.chunkSize)} pieces.`);
                        continue;
                    }
                    err.message = `the server refused a ${formatBytes(length)} piece of this ${formatBytes(total)} file`;
                }
                throw err;
            }
            if (data.skipped || data.complete) return data;
            const next = typeof data.received === "number" ? data.received : offset + length;
            if (next <= offset) {
                stuck += 1;
                if (stuck > 2) {
                    throw new Error("the server did not keep the last piece of this file");
                }
                continue;
            }
            stuck = 0;
            offset = next;
            state.activeLoaded = Math.min(total, offset);
            state.currentDetail = total ? ` · ${formatBytes(offset)} of ${formatBytes(total)}` : "";
            renderProgress();
        }
    }

    async function uploadOne(file, relPath, batchId) {
        if ((file.size || 0) > state.chunkSize) {
            return uploadChunked(file, relPath, batchId);
        }
        try {
            return await withTransientRetry(file, () => uploadWhole(file, relPath, batchId));
        } catch (err) {
            if (err.status !== 413) throw err;
            const next = Math.floor((file.size || 0) / 2);
            if (next < MIN_CHUNK_BYTES) {
                err.message = `the server refused this ${formatBytes(file.size)} file`;
                throw err;
            }
            state.chunkSize = next;
            logLine(`${relPath} was refused in one request. Sending ${formatBytes(state.chunkSize)} pieces instead.`);
            return uploadChunked(file, relPath, batchId);
        }
    }

    function showResumeActions() {
        const buttons = [];
        if (state.staged) {
            const noun = state.staged === 1 ? "file" : "files";
            buttons.push({
                label: `Ingest ${state.staged} staged ${noun}`,
                onClick: () => finish(state.batchId),
            });
        }
        const remaining = state.failed.length + state.queue.length;
        if (remaining) {
            buttons.push({
                label: remaining === 1 ? "Retry 1 file" : `Retry ${remaining} files`,
                secondary: Boolean(state.staged),
                onClick: () => {
                    requeueFailed();
                    startQueue();
                },
            });
        }
        showActions(buttons);
    }

    function presentOutcome() {
        const failed = state.failed.length;
        const waiting = state.queue.length;
        const parts = [];
        if (state.staged) {
            const noun = state.staged === 1 ? "file" : "files";
            parts.push(`Staged ${state.staged} ${noun} (${formatBytes(state.stagedBytes)})`);
        } else {
            parts.push("Nothing was staged");
        }
        if (failed) parts.push(`${failed} ${failed === 1 ? "file" : "files"} could not be uploaded`);
        if (waiting) parts.push(`${waiting} ${waiting === 1 ? "file" : "files"} not sent yet`);
        let text = `${parts.join(". ")}.`;
        if (state.stopped && failed) text = `${state.failed[failed - 1].message} ${text}`;
        setStatus(text, failed > 0 || state.stopped);
        if (progress && !waiting) {
            progress.value = 100;
            progress.textContent = "100%";
        }
        showResumeActions();
        refreshUsage();
    }

    function requeueFailed() {
        const queued = new Set(state.queue.map((item) => item.path));
        state.failed.forEach((item) => {
            state.doneBytes -= item.file.size || 0;
            state.finished -= 1;
            if (!queued.has(item.path)) {
                state.queue.push({ file: item.file, path: item.path });
                queued.add(item.path);
            }
            state.seen.add(item.path);
        });
        if (state.doneBytes < 0) state.doneBytes = 0;
        if (state.finished < 0) state.finished = 0;
        state.failed = [];
        state.stopped = false;
    }

    async function finish(batchId) {
        if (state.finishing || state.uploading) return;
        state.finishing = true;
        clearActions();
        const body = new URLSearchParams();
        body.set("batch", batchId);
        body.set("drive_name", selectedDrive());
        setStatus("Starting the ingest job…");
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
            logLine("The staged files are kept on this page. You can start the ingest again.");
            showResumeActions();
        } catch (err) {
            setStatus("The upload is staged, but the ingest job did not start.", true);
            logLine(`Could not start ingest: ${err.message || err}`);
            showResumeActions();
        } finally {
            state.finishing = false;
        }
    }

    async function startQueue() {
        if (state.uploading) return;
        if (!state.queue.length) {
            if (state.failed.length) presentOutcome();
            return;
        }
        state.uploading = true;
        state.stopped = false;
        clearActions();
        if (!state.batchId) state.batchId = newBatchId();
        setStatus("Preparing upload…");
        renderProgress();

        let fatal = false;
        while (state.queue.length) {
            const item = state.queue.shift();
            state.currentName = item.path;
            state.currentDetail = "";
            state.activeLoaded = 0;
            state.failed = state.failed.filter((failed) => failed.path !== item.path);
            renderProgress();
            try {
                const result = await uploadOne(item.file, item.path, state.batchId);
                state.doneBytes += item.file.size || 0;
                state.finished += 1;
                if (result.skipped) {
                    logLine(`skipped junk: ${result.path || item.path}`);
                } else {
                    state.staged += 1;
                    state.stagedBytes += result.bytes || item.file.size || 0;
                    logLine(`staged ${result.path || item.path} (${formatBytes(result.bytes)})`);
                    if (state.staged === 1) refreshUsage();
                }
            } catch (err) {
                state.doneBytes += item.file.size || 0;
                state.finished += 1;
                state.seen.delete(item.path);
                state.failed.push({
                    file: item.file,
                    path: item.path,
                    message: err.message || String(err),
                });
                logLine(`failed ${item.path} (${formatBytes(item.file.size)}): ${err.message || err}`);
                if (err.fatal) {
                    fatal = true;
                    state.stopped = true;
                    break;
                }
            } finally {
                state.currentName = "";
                state.currentDetail = "";
                state.activeLoaded = 0;
            }
            renderProgress();
        }

        state.uploading = false;
        if (fatal || state.failed.length) {
            presentOutcome();
            return;
        }
        if (state.staged === 0) {
            setStatus("Nothing to ingest. Every file was a junk path.");
            refreshUsage();
            return;
        }
        await finish(state.batchId);
    }

    syncDriveFields();
    if (prefixInput) {
        prefixInput.addEventListener("input", renderDest);
    }
    if (driveInput) {
        driveInput.addEventListener("change", () => {
            syncDriveFields();
            const body = new URLSearchParams();
            body.set("drive_name", selectedDrive());
            fetch("/scratch/drives/select", {
                method: "POST",
                headers: { Accept: "application/json" },
                body,
            }).catch(() => {});
        });
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
