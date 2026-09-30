# Purging deleted threads

Deleting a thread hides it, but its events, command receipts and read-model rows stay in `state.sqlite`. On a long-lived server they are most of the file. `t3 purge` removes the rows of threads deleted more than a given number of days ago, then runs `VACUUM`.

```bash
t3 purge --base-dir ~/.t3 --deleted-older-than 30 --dry-run   # counts only
t3 purge --base-dir ~/.t3 --deleted-older-than 30
```

- The server must be stopped. The command refuses when `server-runtime.json` names a live process, and it holds SQLite's exclusive lock for the whole run. It fails at once, without changing anything, if any other process has the database open, and nothing can open the database until it finishes.
- Deleted threads cannot be restored afterwards. Export anything you need first.
- `VACUUM` writes a temporary copy about the size of the database. Point `SQLITE_TMPDIR` at a disk with that much free space when `/tmp` is small or memory-backed.
- The run takes as long as rewriting the whole file; plan for minutes on a multi-gigabyte database.
