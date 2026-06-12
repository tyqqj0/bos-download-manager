function app() {
    return {
        view: 'dashboard',
        dashboard: {},
        tasks: [],
        categories: [],
        filters: { status: '', server: '', category: '', search: '' },
        selectedTasks: [],
        selectAll: false,
        showAddModal: false,
        showLogModal: false,
        logServer: '',
        logContent: '',
        selectedServer: '',
        lastSync: '',
        toast: { show: false, message: '', type: 'success' },
        addForm: { url: '', category: 'other', priority: 'P1', type: 'dataset', no_dispatch: false, parsed: null, error: '' },

        async init() {
            await this.fetchDashboard();
            await this.fetchTasks();
            // Auto-refresh
            setInterval(() => this.fetchDashboard(), 10000);
            setInterval(() => this.fetchTasks(), 15000);
        },

        get progressPct() {
            if (!this.dashboard.total_estimated_tb) return 0;
            return Math.round((this.dashboard.total_downloaded_tb / this.dashboard.total_estimated_tb) * 100);
        },

        get serverKeys() {
            return Object.keys(this.dashboard.servers || {});
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
                }
            } catch (e) { console.error('Dashboard fetch error:', e); }
        },

        async fetchTasks() {
            try {
                const res = await fetch('/api/tasks');
                const data = await res.json();
                this.tasks = data.tasks || [];
                if (data.categories) this.categories = data.categories;
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

        async batchAction(action) {
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
                const res = await fetch('/api/tasks', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        url_or_repo: this.addForm.url,
                        category: this.addForm.category,
                        type: this.addForm.type,
                        priority: this.addForm.priority,
                        no_dispatch: this.addForm.no_dispatch,
                    })
                });
                if (res.ok) {
                    const data = await res.json();
                    this.showToast(`Added: ${data.task?.name || 'task'}`, 'success');
                    this.showAddModal = false;
                    this.addForm = { url: '', category: 'other', priority: 'P1', type: 'dataset', no_dispatch: false, parsed: null, error: '' };
                    await this.fetchTasks();
                } else {
                    const data = await res.json();
                    this.addForm.error = data.detail || 'Add failed';
                }
            } catch (e) { this.addForm.error = 'Network error'; }
        },

        async restartWorker(key) {
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

        toggleSort(field) {
            // Simple client-side sort toggle
            const STATUS_ORDER = { downloading: 0, dispatched: 1, queued: 2, failed: 3, done: 4 };
            if (field === 'status') {
                this.tasks.sort((a, b) => (STATUS_ORDER[a.status] || 9) - (STATUS_ORDER[b.status] || 9));
            } else if (field === 'name') {
                this.tasks.sort((a, b) => a.name.localeCompare(b.name));
            } else if (field === 'size') {
                this.tasks.sort((a, b) => (b.size_gb || 0) - (a.size_gb || 0));
            } else if (field === 'server') {
                this.tasks.sort((a, b) => (a.server || 'ZZZ').localeCompare(b.server || 'ZZZ'));
            }
        },

        toggleSelectAll() {
            if (this.selectAll) {
                this.selectedTasks = this.filteredTasks.map(t => t.id);
            } else {
                this.selectedTasks = [];
            }
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
            };
            return classes[status] || 'bg-gray-700 text-gray-300';
        },

        formatSize(t) {
            const fmt = (gb) => {
                if (gb >= 1000) return `${(gb/1000).toFixed(1)}T`;
                if (gb >= 10) return `${Math.round(gb)}G`;
                if (gb > 0) return `${gb.toFixed(1)}G`;
                return '0';
            };
            const dl = t.downloaded_gb || 0;
            const total = t.size_gb || 0;
            if (t.status === 'done') {
                if (dl > 0) return fmt(dl);
                if (total > 0) return fmt(total);
                return 'done';
            }
            if (dl > 0 && total > 0) {
                if (dl > total) return fmt(dl);
                return `${fmt(dl)}/${fmt(total)}`;
            }
            if (dl > 0) return `${fmt(dl)}/?`;
            if (total > 0) return `0/${fmt(total)}`;
            return '-';
        },

        taskProgress(t) {
            if (t.status === 'done') return 100;
            const dl = t.downloaded_gb || 0;
            const total = t.size_gb || 0;
            if (total <= 0) return 0;
            if (dl >= total) return 100;
            return Math.round((dl / total) * 100);
        },

        extractRepo(cmdLine) {
            const parts = cmdLine.split(/\s+/);
            for (let i = 0; i < parts.length; i++) {
                if (parts[i].endsWith('download.sh') || parts[i].endsWith('download-modelscope.sh')) {
                    return parts[i + 1] || cmdLine.slice(0, 40);
                }
            }
            return cmdLine.slice(0, 40);
        },

        getSpeed(serverKey) {
            return (this.dashboard.speeds || {})[serverKey] || 0;
        },

        formatSpeed(mbps) {
            if (mbps <= 0) return '';
            if (mbps >= 1024) return `${(mbps/1024).toFixed(1)} GB/s`;
            if (mbps >= 1) return `${mbps.toFixed(1)} MB/s`;
            return `${(mbps*1024).toFixed(0)} KB/s`;
        },

        timeAgo(isoStr) {
            if (!isoStr) return '';
            const diff = (Date.now() - new Date(isoStr).getTime()) / 1000;
            if (diff < 60) return 'just now';
            if (diff < 3600) return `${Math.round(diff/60)}m ago`;
            if (diff < 86400) return `${Math.round(diff/3600)}h ago`;
            return `${Math.round(diff/86400)}d ago`;
        },

        showToast(message, type = 'success') {
            this.toast = { show: true, message, type };
            setTimeout(() => { this.toast.show = false; }, 3000);
        },
    };
}
