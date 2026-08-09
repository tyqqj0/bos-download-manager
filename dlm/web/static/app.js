function app() {
    return {
        view: 'dashboard',
        dashboard: {},
        tasks: [],
        categories: [],
        filters: { status: '', server: '', category: '', search: '' },
        sortField: '',
        sortReverse: false,
        selectedTasks: [],
        selectAll: false,
        showAddModal: false,
        showLogModal: false,
        logServer: '',
        logContent: '',
        lastSync: '',
        showDoctorModal: false,
        doctorLoading: false,
        doctorFixing: false,
        doctorFindings: null,
        toast: { show: false, message: '', type: 'success' },
        // '' means "whatever fleet.DEFAULT_DISPATCH_MODE resolves to server-side"
        // — never hardcode a literal mode here, that just moves the bug that
        // let the UI defeat a DLM_DEFAULT_DISPATCH_MODE flip (see submitAdd
        // and defaultDispatchMode below).
        addForm: { url: '', category: 'other', priority: 'P1', type: 'dataset', dispatch_mode: '', shard_count: 0, no_dispatch: false, parsed: null, error: '' },
        // Populated from /api/dashboard's default_dispatch_mode (fetchDashboard)
        // so the add-form select can show the live server default instead of
        // a client-side guess.
        defaultDispatchMode: '',
        // What the task will actually be dispatched as: the explicit form choice
        // if there is one, otherwise whatever the server default resolves to.
        // Before the first /api/dashboard reply defaultDispatchMode is '', so
        // this reads '' too — the shard input stays hidden rather than flashing
        // for a moment on a pool cluster.
        effectiveDispatchMode() {
            return this.addForm.dispatch_mode || this.defaultDispatchMode;
        },
        transferTasks: [],
        transferSummary: {},
        transferPaused: false,
        shardDetail: null,
        shardTaskId: null,
        shardDispatchMode: 'sharded',

        // Storage tab state
        storageBucket: 'auwomo-data',
        storageBosPath: [],
        storageBosItems: [],
        storageBosLoading: false,
        storageBosSelected: [],
        storageJfsSection: 'managed',
        storageJfsPath: [],
        storageJfsItems: [],
        storageJfsLoading: false,
        storageBottomTab: 'downloads',
        storageBottomOpen: false,
        storageAddUrl: '',
        storageAddCategory: 'other',
        storageAddType: 'dataset',
        storageInited: false,
        storageJfsMoveSource: null,
        storageJfsMoveTarget: '',
        storageJfsMoveCategory: '',
        showMoveModal: false,
        storageJfsMoveLoading: false,

        async init() {
            await this.fetchDashboard();
            await this.fetchTasks();
            setInterval(() => this.fetchDashboard(), 10000);
            setInterval(() => this.fetchTasks(), 15000);
            setInterval(() => { if (this.view === 'transfer') this.fetchTransfer(); }, 15000);
        },

        get progressPct() {
            if (!this.dashboard.total_estimated_tb) return 0;
            return Math.round((this.dashboard.total_downloaded_tb / this.dashboard.total_estimated_tb) * 100);
        },

        get serverKeys() {
            return Object.keys(this.dashboard.servers || {});
        },

        get workersOnly() {
            const servers = this.dashboard.servers || {};
            return Object.entries(servers)
                .filter(([_, srv]) => !srv.local)
                .map(([key, srv]) => ({ key, ...srv }));
        },

        get filteredTasks() {
            let result = this.tasks;
            if (this.filters.status) result = result.filter(t => t.status === this.filters.status);
            if (this.filters.server) result = result.filter(t => t.server === this.filters.server);
            if (this.filters.category) result = result.filter(t => t.category === this.filters.category);
            if (this.filters.search) {
                const q = this.filters.search.toLowerCase();
                result = result.filter(t => t.name.toLowerCase().includes(q) || (t.repo_id || '').toLowerCase().includes(q));
            }
            return result;
        },

        async fetchDashboard() {
            try {
                const res = await fetch('/api/dashboard');
                const data = await res.json();
                if (data.status !== 'loading') {
                    this.dashboard = data;
                    this.lastSync = data.updated_at ? `Updated ${this.timeAgo(new Date(data.updated_at * 1000).toISOString())}` : '';
                    if (data.default_dispatch_mode) this.defaultDispatchMode = data.default_dispatch_mode;
                }
            } catch (e) { console.error('Dashboard fetch error:', e); }
        },

        async fetchTasks() {
            try {
                const res = await fetch('/api/tasks');
                const data = await res.json();
                this.tasks = data.tasks || [];
                if (data.categories) this.categories = data.categories;
                if (this.sortField) this._applySort();
            } catch (e) { console.error('Tasks fetch error:', e); }
        },

        async triggerSync() {
            this.showToast('Syncing...', 'success');
            try {
                const res = await fetch('/api/sync', { method: 'POST' });
                const data = await res.json();
                this.showToast(`Sync done: ${data.changes} changes`, 'success');
                await this.fetchDashboard();
                await this.fetchTasks();
            } catch (e) { this.showToast('Sync failed', 'error'); }
        },

        async retryTask(taskId) {
            try {
                const res = await fetch(`/api/tasks/${taskId}/retry`, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}' });
                if (res.ok) {
                    this.showToast('Task dispatched for retry', 'success');
                    await this.fetchTasks();
                } else {
                    const data = await res.json();
                    this.showToast(data.detail || 'Retry failed', 'error');
                }
            } catch (e) { this.showToast('Retry failed', 'error'); }
        },

        async skipTask(taskId) {
            try {
                const res = await fetch(`/api/tasks/${taskId}/skip`, { method: 'POST' });
                if (res.ok) {
                    this.showToast('Task skipped', 'success');
                    await this.fetchTasks();
                } else {
                    this.showToast('Skip failed', 'error');
                }
            } catch (e) { this.showToast('Skip failed', 'error'); }
        },

        async pauseTask(taskId) {
            try {
                const res = await fetch('/api/queue/pause', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ task_id: taskId }),
                });
                const data = await res.json();
                if (data.ok) {
                    this.showToast('暂停信号已发送，~5s 后生效', 'success');
                    await this.fetchTasks();
                } else {
                    this.showToast(data.error || '暂停失败', 'error');
                }
            } catch (e) { this.showToast('暂停失败', 'error'); }
        },

        async resumeTask(taskId) {
            try {
                const res = await fetch('/api/queue/resume', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ task_id: taskId }),
                });
                const data = await res.json();
                if (data.ok) {
                    this.showToast('任务已恢复', 'success');
                    await this.fetchTasks();
                } else {
                    this.showToast(data.error || '恢复失败', 'error');
                }
            } catch (e) { this.showToast('恢复失败', 'error'); }
        },

        async preemptForTask(taskId) {
            const downloading = this.tasks.filter(t => t.status === 'downloading');
            if (downloading.length === 0) {
                this.showToast('当前没有正在下载的任务，无需插队，直接恢复即可', 'error');
                return;
            }
            const options = downloading.map(t =>
                `${t.name} (${t.server || '?'}, ${this.formatSpeed(t.speed_mbps)})`
            ).join('\n');
            const choice = prompt(
                `选择要暂停的任务（输入序号 1-${downloading.length}）：\n` +
                downloading.map((t, i) =>
                    `${i + 1}. ${t.name} @ ${t.server || '?'} (${this.formatSpeed(t.speed_mbps)}, ${t.priority})`
                ).join('\n')
            );
            if (!choice) return;
            const idx = parseInt(choice) - 1;
            if (isNaN(idx) || idx < 0 || idx >= downloading.length) {
                this.showToast('无效的序号', 'error');
                return;
            }
            const victim = downloading[idx];
            if (!confirm(`确认插队？\n暂停: ${victim.name} @ ${victim.server}\n启动: ${this.tasks.find(t => t.id === taskId)?.name || taskId}`)) return;
            try {
                const res = await fetch('/api/queue/preempt', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        urgent_task_id: taskId,
                        victim_task_id: victim.id,
                        target_server: victim.server,
                    }),
                });
                const data = await res.json();
                if (data.ok) {
                    this.showToast(data.message, 'success');
                    await this.fetchTasks();
                } else {
                    this.showToast(data.error || '插队失败', 'error');
                }
            } catch (e) { this.showToast('插队失败', 'error'); }
        },

        async batchAction(action) {
            if (!confirm(`Confirm ${action} on ${this.selectedTasks.length} tasks?`)) return;
            try {
                const res = await fetch('/api/tasks/batch', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ task_ids: this.selectedTasks, action: action })
                });
                const data = await res.json();
                this.showToast(`Batch ${action}: ${data.results?.length || 0} tasks`, 'success');
                this.selectedTasks = [];
                await this.fetchTasks();
            } catch (e) { this.showToast(`Batch ${action} failed`, 'error'); }
        },

        async parseUrl() {
            if (!this.addForm.url) { this.addForm.parsed = null; return; }
            try {
                const res = await fetch('/api/parse', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ url_or_repo: this.addForm.url })
                });
                this.addForm.parsed = await res.json();
                this.addForm.error = '';
            } catch (e) { this.addForm.parsed = null; }
        },

        async submitAdd() {
            this.addForm.error = '';
            try {
                const body = {
                    url_or_repo: this.addForm.url,
                    category: this.addForm.category,
                    type: this.addForm.type,
                    priority: this.addForm.priority,
                    no_dispatch: this.addForm.no_dispatch,
                };
                // '' means "server default" (see addForm init) — omit the key
                // entirely so the backend applies fleet.DEFAULT_DISPATCH_MODE
                // instead of receiving an explicit empty-string override.
                if (this.addForm.dispatch_mode) body.dispatch_mode = this.addForm.dispatch_mode;
                // 0 = "let the server pick", and only sharded mode has anything
                // to pick — pool sizes its own batches from the filelist. Sent
                // only for sharded so a pool task cannot carry a max_workers
                // that nothing reads (tasks.py:347) and that the UI would then
                // display as if it meant something.
                if (this.effectiveDispatchMode() === 'sharded') {
                    body.shard_count = Number(this.addForm.shard_count) || 0;
                }
                const res = await fetch('/api/tasks', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(body)
                });
                if (res.ok) {
                    const data = await res.json();
                    const paused = data.task?.status === 'paused';
                    this.showToast(
                        `Added: ${data.task?.name || 'task'}${paused ? ' (paused)' : ''}`,
                        'success');
                    this.showAddModal = false;
                    this.addForm = { url: '', category: 'other', priority: 'P1', type: 'dataset', dispatch_mode: '', shard_count: 0, no_dispatch: false, parsed: null, error: '' };
                    await this.fetchTasks();
                } else {
                    const data = await res.json();
                    this.addForm.error = data.detail || 'Add failed';
                }
            } catch (e) { this.addForm.error = 'Network error'; }
        },

        async restartWorker(key) {
            if (!confirm(`Confirm restart worker on ${key}?`)) return;
            try {
                const res = await fetch(`/api/servers/${key}/restart`, { method: 'POST' });
                if (res.ok) {
                    this.showToast(`Worker ${key} restarted`, 'success');
                    await this.fetchDashboard();
                } else {
                    const data = await res.json();
                    this.showToast(data.detail || 'Restart failed', 'error');
                }
            } catch (e) { this.showToast('Restart failed', 'error'); }
        },

        async viewLog(key) {
            this.logServer = key;
            this.logContent = 'Loading...';
            this.showLogModal = true;
            try {
                const res = await fetch(`/api/servers/${key}/log?lines=80`);
                const data = await res.json();
                this.logContent = data.log || 'No log available';
            } catch (e) { this.logContent = 'Failed to fetch log'; }
        },

        confirmDelete(taskId, taskName) {
            if (confirm(`Delete task "${taskName}"? This cannot be undone.`)) {
                this.deleteTask(taskId);
            }
        },

        async deleteTask(taskId) {
            try {
                const res = await fetch(`/api/tasks/${taskId}`, { method: 'DELETE' });
                if (res.ok) {
                    this.showToast('Task deleted', 'success');
                    await this.fetchTasks();
                } else {
                    const data = await res.json();
                    this.showToast(data.detail || 'Delete failed', 'error');
                }
            } catch (e) { this.showToast('Delete failed', 'error'); }
        },

        toggleSort(field) {
            if (this.sortField === field) {
                this.sortReverse = !this.sortReverse;
            } else {
                this.sortField = field;
                this.sortReverse = false;
            }
            this._applySort();
        },

        _applySort() {
            const STATUS_ORDER = { downloading: 0, dispatched: 1, queued: 2, failed: 3, done: 4 };
            const field = this.sortField;
            const rev = this.sortReverse ? -1 : 1;
            if (field === 'status') {
                this.tasks.sort((a, b) => rev * ((STATUS_ORDER[a.status] || 9) - (STATUS_ORDER[b.status] || 9)));
            } else if (field === 'name') {
                this.tasks.sort((a, b) => rev * a.name.localeCompare(b.name));
            } else if (field === 'size') {
                this.tasks.sort((a, b) => rev * ((b.size_gb || 0) - (a.size_gb || 0)));
            } else if (field === 'server') {
                this.tasks.sort((a, b) => rev * (a.server || 'ZZZ').localeCompare(b.server || 'ZZZ'));
            }
        },

        toggleSelectAll() {
            if (this.selectAll) {
                this.selectedTasks = this.filteredTasks.map(t => t.id);
            } else {
                this.selectedTasks = [];
            }
        },

        async runDoctor() {
            this.showDoctorModal = true;
            this.doctorLoading = true;
            this.doctorFindings = null;
            try {
                const res = await fetch('/api/doctor');
                this.doctorFindings = await res.json();
            } catch (e) {
                this.doctorFindings = { error: 'Failed to run diagnostics' };
            }
            this.doctorLoading = false;
        },

        async doctorFix(actions) {
            this.doctorFixing = true;
            try {
                const res = await fetch('/api/doctor', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ actions: actions || [] })
                });
                const data = await res.json();
                // Sum every real fix-result key doctor.py's fix() returns.
                // `restart_dead` was never one of them ("There is deliberately
                // no restart worker action" per that route's docstring) and
                // redispatch_orphaned/redispatch_pool were missing entirely —
                // so the default "Fix All" button (actions: []) always
                // reported 0 even when it redispatched sharded orphans.
                const total = (data.redispatch_orphaned?.length || 0)
                    + (data.redispatch_pool?.length || 0)
                    + (data.reset_stuck?.length || 0)
                    + (data.skip_zombie?.length || 0);
                let message = `Fixed ${total} issues`;
                // M5: reset_stuck/redispatch_orphaned deliberately never act on
                // a pool task (resetting one while its coordinator is still
                // alive double-dispatches and wedges that source for ~15 min —
                // see doctor.py's fix() docstring). The response explains why
                // per skipped task in skipped_pool_tasks; surface it instead of
                // dropping it, or "Fix All" on a pool orphan silently no-ops.
                const skippedPool = data.skipped_pool_tasks || [];
                if (skippedPool.length > 0) {
                    const shown = skippedPool.slice(0, 3).join(' | ');
                    const extra = skippedPool.length - Math.min(3, skippedPool.length);
                    message += `. Skipped ${skippedPool.length} pool task(s): ${shown}` + (extra > 0 ? ` (+${extra} more)` : '');
                }
                this.showToast(message, total === 0 && skippedPool.length > 0 ? 'error' : 'success');
                this.showDoctorModal = false;
                await this.fetchDashboard();
                await this.fetchTasks();
            } catch (e) {
                this.showToast('Fix failed', 'error');
            }
            this.doctorFixing = false;
        },

        async cleanServer(key) {
            if (!confirm(`Clean orphan staging data on ${key}?`)) return;
            try {
                const res = await fetch(`/api/servers/${key}/cleanup`, { method: 'POST' });
                const data = await res.json();
                if (data.cleaned) {
                    this.showToast(`Cleaned ${data.cleaned.length} dirs on ${key}`, 'success');
                } else {
                    this.showToast(data.message || 'Nothing to clean', 'success');
                }
                await this.fetchDashboard();
            } catch (e) { this.showToast('Cleanup failed', 'error'); }
        },

        // Helpers
        statusIcon(status) {
            const icons = { done: '✓', downloading: '↓', dispatched: '→', queued: '○', failed: '✗', skipped: '⏸' };
            return icons[status] || '?';
        },

        statusClass(status) {
            const classes = {
                done: 'bg-green-900 text-green-300',
                downloading: 'bg-blue-900 text-blue-300',
                dispatched: 'bg-indigo-900 text-indigo-300',
                queued: 'bg-gray-700 text-gray-300',
                failed: 'bg-red-900 text-red-300',
                skipped: 'bg-gray-700 text-gray-400',
                'needs-auth': 'bg-yellow-900 text-yellow-300',
                paused: 'bg-gray-600 text-gray-200',
                preempted: 'bg-orange-900 text-orange-300',
            };
            return classes[status] || 'bg-gray-700 text-gray-300';
        },

        // speed_mbps is megaBITS per second: pipeline.py computes
        // speed_bps * 8 / 1_000_000 and the worker heartbeat logs it as "Mbps".
        // This rendered it as "MB/s", i.e. 8x high — a fleet honestly moving
        // 119 MB/s displayed "1030 MB/s". Decimal steps, to match the unit.
        formatSpeed(mbps) {
            if (!mbps || mbps <= 0) return '0 Mbps';
            if (mbps >= 1000) return `${(mbps / 1000).toFixed(1)} Gbps`;
            if (mbps >= 100) return `${Math.round(mbps)} Mbps`;
            if (mbps >= 10) return `${mbps.toFixed(0)} Mbps`;
            return `${mbps.toFixed(1)} Mbps`;
        },

        formatEta(seconds) {
            if (!seconds || seconds <= 0) return '';
            if (seconds < 60) return `${seconds}s`;
            if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
            const h = Math.floor(seconds / 3600);
            const m = Math.round((seconds % 3600) / 60);
            return m > 0 ? `${h}h ${m}m` : `${h}h`;
        },

        formatGB(gb) {
            if (!gb || gb <= 0) return '0';
            if (gb >= 1000) return `${(gb / 1000).toFixed(1)}T`;
            if (gb >= 100) return `${Math.round(gb)}G`;
            if (gb >= 10) return `${Math.round(gb)}G`;
            return `${gb.toFixed(1)}G`;
        },

        formatSize(t) {
            if (t.status === 'done') return t.downloaded_gb > 0 ? this.formatGB(t.downloaded_gb) : (t.size_gb > 0 ? this.formatGB(t.size_gb) : '-');
            if (t.size_gb > 0) return `${this.formatGB(t.downloaded_gb || 0)}/${this.formatGB(t.size_gb)}`;
            if (t.downloaded_gb > 0) return `${this.formatGB(t.downloaded_gb)}/?`;
            return '-';
        },

        formatAlert(al) {
            if (!al || !al.type) return '';
            if (al.type === 'worker_offline') {
                const dur = al.duration_min || 0;
                const time = dur >= 60 ? `${Math.floor(dur / 60)}h ${dur % 60}m` : `${dur}m`;
                return `${al.server || '?'} offline for ${time}`;
            }
            if (al.type === 'task_failed_repeat') {
                return `${al.task || '?'} failed ${al.count || 0}x${al.error ? ` (${al.error})` : ''}`;
            }
            if (al.type === 'disk_low') {
                return `${al.server || '?'} disk low: ${al.free_gb || 0}G free`;
            }
            return al.type;
        },

        // ==================== TRANSFER ====================

        async fetchTransfer() {
            try {
                const resp = await fetch('/api/transfer');
                const data = await resp.json();
                this.transferTasks = data.tasks || [];
                this.transferSummary = data.summary || {};
                this.transferPaused = data.paused || false;
            } catch (e) {
                console.error('fetchTransfer:', e);
            }
        },

        async triggerTransfer(taskId) {
            try {
                const resp = await fetch('/api/transfer/trigger', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ task_ids: [taskId] }),
                });
                const data = await resp.json();
                if (data.error) { this.showToast(data.error, 'error'); return; }
                this.showToast(`已触发搬运: ${data.count} 个任务`);
                await this.fetchTransfer();
            } catch (e) {
                this.showToast('触发失败', 'error');
            }
        },

        async triggerAllTransfer() {
            try {
                const resp = await fetch('/api/transfer/trigger', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({}),
                });
                const data = await resp.json();
                if (data.error) { this.showToast(data.error, 'error'); return; }
                this.showToast(`已触发 ${data.count} 个搬运任务`);
                await this.fetchTransfer();
            } catch (e) {
                this.showToast('触发失败', 'error');
            }
        },

        async retryTransfer(taskId) {
            try {
                const resp = await fetch(`/api/transfer/${taskId}/retry`, { method: 'POST' });
                const data = await resp.json();
                if (data.error) { this.showToast(data.error, 'error'); return; }
                this.showToast(`已重新排队: ${data.name}`);
                await this.fetchTransfer();
            } catch (e) {
                this.showToast('重试失败', 'error');
            }
        },

        async toggleTransferPause() {
            try {
                const resp = await fetch('/api/transfer/pause', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ paused: !this.transferPaused }),
                });
                const data = await resp.json();
                this.transferPaused = data.paused;
                this.showToast(data.paused ? '自动搬运已暂停' : '自动搬运已恢复');
            } catch (e) {
                this.showToast('操作失败', 'error');
            }
        },

        async syncTransfer() {
            try {
                const resp = await fetch('/api/transfer/sync', { method: 'POST' });
                const data = await resp.json();
                this.showToast(`同步完成: ${data.updated || 0} 个更新`);
                await this.fetchTransfer();
            } catch (e) {
                this.showToast('同步失败', 'error');
            }
        },

        timeAgo(isoStr) {
            if (!isoStr) return '';
            const diff = (Date.now() - new Date(isoStr).getTime()) / 1000;
            if (diff < 60) return 'just now';
            if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
            if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
            return `${Math.round(diff / 86400)}d ago`;
        },

        showToast(message, type = 'success') {
            this.toast = { show: true, message, type };
            setTimeout(() => { this.toast.show = false; }, 3000);
        },

        // ==================== STORAGE ====================

        get storageRecent() {
            return this.tasks
                .filter(t => t.status === 'downloading' || t.status === 'dispatched')
                .concat(this.tasks.filter(t => t.status === 'done').slice(-3))
                .slice(0, 8);
        },

        get storageDownloadCount() {
            return this.tasks.filter(t => t.status === 'downloading' || t.status === 'dispatched').length;
        },

        get storageTransferCount() {
            return this.transferTasks.filter(t => t.transfer_status === 'transferring' || t.transfer_status === 'queued').length;
        },

        get storageActiveDownloads() {
            return this.tasks.filter(t => t.status === 'downloading' || t.status === 'dispatched');
        },

        get storageActiveTransfers() {
            return this.transferTasks.filter(t => t.transfer_status != null);
        },

        async initStorage() {
            if (!this.storageInited) {
                this.storageInited = true;
                await this.fetchBos();
                await this.fetchJfs();
                await this.fetchTransfer();
            }
        },

        async fetchBos() {
            this.storageBosLoading = true;
            try {
                const prefix = this.storageBosPath.length > 0 ? this.storageBosPath.join('/') + '/' : '';
                const res = await fetch(`/api/storage/bos?bucket=${encodeURIComponent(this.storageBucket)}&prefix=${encodeURIComponent(prefix)}`);
                const data = await res.json();
                this.storageBosItems = data.items || [];
            } catch (e) {
                console.error('fetchBos:', e);
                this.storageBosItems = [];
            }
            this.storageBosLoading = false;
        },

        bosNavigate(item) {
            const name = item.name;
            this.storageBosPath.push(name);
            this.storageBosSelected = [];
            this.fetchBos();
        },

        async fetchJfs() {
            this.storageJfsLoading = true;
            try {
                let path = '/';
                if (this.storageJfsSection === 'managed') {
                    if (this.storageJfsPath.length === 0) {
                        this.storageJfsItems = [
                            { name: 'auwomo-datasets/raw-data', is_dir: true, size: 0, _root: '/auwomo-datasets/raw-data/' },
                            { name: 'auwomo-model', is_dir: true, size: 0, _root: '/auwomo-model/' },
                        ];
                        this.storageJfsLoading = false;
                        return;
                    }
                    path = '/' + this.storageJfsPath.join('/') + '/';
                } else {
                    if (this.storageJfsPath.length === 0) {
                        path = '/';
                    } else {
                        path = '/' + this.storageJfsPath.join('/') + '/';
                    }
                }
                const res = await fetch(`/api/storage/juicefs?path=${encodeURIComponent(path)}&section=${this.storageJfsSection}`);
                const data = await res.json();
                this.storageJfsItems = data.items || [];
            } catch (e) {
                console.error('fetchJfs:', e);
                this.storageJfsItems = [];
            }
            this.storageJfsLoading = false;
        },

        jfsNavigate(item) {
            if (item._separator) return;
            if (item._root) {
                this.storageJfsPath = item._root.split('/').filter(Boolean);
            } else {
                this.storageJfsPath.push(item.name);
            }
            this.fetchJfs();
        },

        async jfsMove() {
            if (!this.storageJfsMoveSource || !this.storageJfsMoveTarget || !this.storageJfsMoveCategory) return;
            this.storageJfsMoveLoading = true;
            try {
                const source = '/' + this.storageJfsPath.concat(this.storageJfsMoveSource.name).join('/');
                const target = this.storageJfsMoveTarget + this.storageJfsMoveCategory + '/' + this.storageJfsMoveSource.name;
                const res = await fetch('/api/storage/juicefs/move', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ source, target }),
                });
                const data = await res.json();
                if (data.ok) {
                    this.showToast(`Moved to ${target}`);
                    this.showMoveModal = false;
                    this.storageJfsMoveSource = null;
                    this.storageJfsMoveTarget = '';
                    this.storageJfsMoveCategory = '';
                    await this.fetchJfs();
                } else {
                    this.showToast(data.error || 'Move failed', 'error');
                }
            } catch (e) {
                this.showToast('Move failed: network error', 'error');
            }
            this.storageJfsMoveLoading = false;
        },

        async registerSelected() {
            if (this.storageBosSelected.length === 0) return;
            let count = 0;
            for (const prefix of this.storageBosSelected) {
                const parts = prefix.replace(/\/$/, '').split('/');
                const name = parts[parts.length - 1];
                const category = parts.length > 1 ? parts[0] : this.storageAddCategory;
                try {
                    const res = await fetch('/api/storage/register', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            bucket: this.storageBucket,
                            prefix: prefix.replace(/\/$/, ''),
                            name: name,
                            category: category,
                            type: this.storageBucket === 'auwomo-model-open' ? 'model' : 'dataset',
                            auto_transfer: true,
                        }),
                    });
                    const data = await res.json();
                    if (data.ok) count++;
                } catch (e) { console.error('register error:', e); }
            }
            this.showToast(`Registered ${count} items for transfer`);
            this.storageBosSelected = [];
            await this.fetchBos();
        },

        async storageQuickAdd() {
            if (!this.storageAddUrl) return;
            try {
                const res = await fetch('/api/tasks', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        url_or_repo: this.storageAddUrl,
                        category: this.storageAddCategory,
                        type: this.storageAddType,
                        priority: 'P1',
                    }),
                });
                if (res.ok) {
                    const data = await res.json();
                    this.showToast(`Added: ${data.task?.name || 'task'}`);
                    this.storageAddUrl = '';
                    await this.fetchTasks();
                } else {
                    const data = await res.json();
                    this.showToast(data.detail || 'Add failed', 'error');
                }
            } catch (e) { this.showToast('Add failed', 'error'); }
        },

        async fetchShards(taskId) {
            this.shardTaskId = taskId;
            try {
                const res = await fetch(`/api/tasks/${taskId}/shards`);
                const data = await res.json();
                this.shardDetail = data.shards || [];
                this.shardDispatchMode = data.dispatch_mode || 'sharded';
            } catch (e) {
                this.shardDetail = [];
                this.shardDispatchMode = 'sharded';
                console.error('fetchShards:', e);
            }
        },

        closeShards() {
            this.shardDetail = null;
            this.shardTaskId = null;
            this.shardDispatchMode = 'sharded';
        },

        // Per-server aggregation for a pool task's batch rows (decision F) —
        // collapses up to POOL_MAX_BATCHES (1500) rows into one line per
        // server actually holding work, instead of a 1500-row table.
        serverBatches() {
            const byServer = {};
            for (const s of (this.shardDetail || [])) {
                const server = s.server;
                if (!server) continue;
                if (!byServer[server]) {
                    byServer[server] = { server, running: 0, done: 0, speed_mbps: 0, doneBytes: 0, totalBytes: 0 };
                }
                const agg = byServer[server];
                if (s.status === 'running') agg.running += 1;
                else if (s.status === 'done') agg.done += 1;
                agg.speed_mbps += s.speed_mbps || 0;
                agg.doneBytes += s.done_bytes || 0;
                agg.totalBytes += s.total_bytes || 0;
            }
            return Object.values(byServer)
                .sort((a, b) => a.server.localeCompare(b.server))
                .map(agg => ({
                    server: agg.server,
                    running: agg.running,
                    done: agg.done,
                    speed_mbps: agg.speed_mbps,
                    done_pct: agg.totalBytes ? Math.round((agg.doneBytes / agg.totalBytes) * 1000) / 10 : 0,
                }));
        },

        // The hero card's per-server rows, for either dispatch mode. A pool
        // task carries `server_batches` — already aggregated per distinct
        // server by the backend, because up to POOL_MAX_BATCHES (1500) batch
        // rows must not become 1500 chips (decision F) — and a sharded task
        // carries `shard_servers`, one entry per shard. Both expose
        // server/speed_mbps/done_pct, which is all the card renders, so one
        // accessor serves both and the sharded card renders exactly what it
        // did before (G1). Before this existed the card gated on
        // shard_servers alone and a pool task showed no per-server view.
        dlServers(dl) {
            return dl.server_batches || dl.shard_servers || [];
        },

        // "bj1,bj2 (1500 batches)" vs "w1,w2 (6 shards)": total_shards counts
        // batch rows for a pool task, and its server list is deduplicated.
        dlServersLabel(dl) {
            const servers = this.dlServers(dl).map(s => s.server).join(',');
            const unit = dl.server_batches ? 'batches' : 'shards';
            return `${servers} (${dl.total_shards} ${unit})`;
        },

        // The modal's error line: unbounded for a pool task's up-to-1500
        // rows, so cap what is shown there (first few + a count). The
        // sharded table stays as-is — shard counts are small.
        shardErrorSummary() {
            const errored = (this.shardDetail || []).filter(s => s.error);
            if (this.shardDispatchMode === 'pool') {
                const shown = errored.slice(0, 5).map(s => `#${s.shard_index}: ${s.error}`);
                const extra = errored.length - shown.length;
                return shown.join('; ') + (extra > 0 ? ` (+${extra} more)` : '');
            }
            return errored.map(s => `#${s.shard_index}: ${s.error}`).join('; ');
        },

        shardPct(shard) {
            if (!shard.total_files || shard.total_files === 0) return 0;
            return Math.round((shard.done_files / shard.total_files) * 100);
        },

        formatBytes(bytes) {
            if (bytes == null || bytes === 0) return '-';
            if (bytes < 1024) return bytes + ' B';
            if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(0) + ' KB';
            if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
            if (bytes < 1024 * 1024 * 1024 * 1024) return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
            return (bytes / (1024 * 1024 * 1024 * 1024)).toFixed(2) + ' TB';
        },
    };
}
