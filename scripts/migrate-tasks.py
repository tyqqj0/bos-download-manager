#!/usr/bin/env python3
"""scripts/migrate-tasks.py — Migrate pending/failed tasks from SQLite to Temporal workflows.

Run on S1 after Temporal is deployed and workers are running.
Usage: python3 scripts/migrate-tasks.py [--retry-failed]
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dlm.queue.snapshot import init_db, get_all_tasks, update_task_progress


async def main():
    from dlm.web.temporal_client import start_download

    init_db()
    tasks = get_all_tasks()

    # Reset all "downloading" tasks to pending (they're zombies from old Celery)
    downloading = [t for t in tasks if t["status"] == "downloading"]
    for t in downloading:
        update_task_progress(t["id"], status="pending", phase="migrated", speed_mbps=0)
        print(f"  Reset zombie: {t['name']}")

    # Start workflows for all pending tasks
    pending = [t for t in tasks if t["status"] in ("pending", "paused", "preempted")]
    pending.sort(key=lambda t: t.get("priority", 5))

    print(f"\nStarting {len(pending)} workflows...")
    for t in pending:
        try:
            await start_download(t)
            print(f"  ✓ {t['name']} (priority={t.get('priority', 5)})")
        except Exception as e:
            if "already running" in str(e).lower() or "already started" in str(e).lower():
                print(f"  ⊘ {t['name']} (already running)")
            else:
                print(f"  ✗ {t['name']}: {e}")

    # Optionally retry failed tasks
    failed = [t for t in tasks if t["status"] == "failed" and (t.get("retry_count") or 0) < 5]
    if failed:
        print(f"\n{len(failed)} failed tasks eligible for retry:")
        for t in failed[:10]:
            print(f"  - {t['name']} (error: {t.get('error', '')[:50]})")
        print("  Run with --retry-failed to restart them")

    if "--retry-failed" in sys.argv:
        for t in failed:
            try:
                update_task_progress(t["id"], status="pending", phase="retrying", error=None)
                await start_download(t)
                print(f"  ✓ Retrying: {t['name']}")
            except Exception as e:
                print(f"  ✗ {t['name']}: {e}")

    print("\nDone! Check Temporal UI: http://154.85.43.52:8233")


if __name__ == "__main__":
    asyncio.run(main())
