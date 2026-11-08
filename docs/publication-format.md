# Publication format

`out/<site>/` is a derived, rebuildable publication of the site's SQLite
database. SQLite remains the source of truth. Each site is a fully
independent snapshot: its own `HEAD`, its own `roots/`, its own `objects/`
and its own `files_ca/`. There is no global `out/HEAD` and no cross-site
manifest.

## Layout

```text
out/<site>/
  HEAD                                   # <root_sha>\n — the commit point
  roots/<aa>/<bb>/<root_sha>.json
  objects/pages/<aa>/<bb>/<pages_sha>.json
  objects/files/<aa>/<bb>/<files_sha>.json
  objects/shell/<aa>/<bb>/<shell_sha>
  objects/archives/<aa>/<bb>/<archive_sha>.zst
  files_ca/...                           # attachments, unchanged
  .publish.lock
```

`<aa> = sha[0..2]`, `<bb> = sha[2..4]`, and the file name contains the full
lowercase 64-character SHA-256. `HEAD` is a plain text file (not a symlink)
containing exactly `<root_sha>\n`.

Objects are immutable. Once written they are never rewritten or deleted.
Old roots, old objects and `files_ca` blobs stay readable for rollback and
for readers that already resolved the previous generation. There is no GC
in this iteration; a crash between writing objects and switching `HEAD`
leaves harmless orphaned objects.

`pages_by_id/` is no longer written. Existing directories remain as
read-only adoption input / rollback material and are not part of the new
contract.

## Format versions

- **v3** adds the first/last content-bearing revision summary
  (`created_by`/`created_at`/`updated_by`/`updated_at`) to page entries.
  Readers that build catalog metadata can use it without decoding page
  archives. The new fields are additive; old readers ignore them.
- **v2** is the immutable content-addressed generation layout.

## Object schemas

All hashed objects are compact JSON (`serde_json::to_vec`) plus a trailing
`\n`. Field order is the struct declaration order; arrays are already sorted
by SQL. Object hashes are computed over these exact bytes.

### Root

```json
{"v":3,"site":"rpcauthority","pages":"<sha>","files":"<sha>","shell":"<sha>"}
```

### Pages section

```json
{"v":3,"site":"rpcauthority","pages":[
  {"id":"1234567890","slug":"some:page","title":"Title","tags":["a","b"],
   "revisions_stored":2,"revisions_known":3,"max_rev":3,
   "created_by":"7","created_at":1700000001,
   "updated_by":"7","updated_at":1700000002,
   "archive":"objects/archives/ab/cd/<sha>.zst","archive_sha256":"<sha>"},
  {"id":"9876543210","slug":"bare","title":"","tags":[],
   "revisions_stored":0,"revisions_known":1,"max_rev":1,
   "created_by":null,"created_at":null,
   "updated_by":null,"updated_at":null,
   "archive":null,"archive_sha256":null}
]}
```

One entry per page, including pages with no stored revisions, sorted by
numeric page id. `tags` keep database order. `archive`/`archive_sha256` are
`null` exactly when `revisions_stored == 0`.

`created_by`/`created_at`/`updated_by`/`updated_at` summarize the first and
last **content-bearing** revisions by `rev_no` (the same revisions packed
into the archive). They are `null` exactly when there are no content-bearing
revisions. The summary is part of the catalog so readers can build page
metadata and satisfy `ListPages` selectors without opening page archives;
the full revision history remains inside each page's archive.

### Files section

```json
{"v":3,"site":"rpcauthority","files":[
  {"path":"http://site.wikidot.com/local--files/x/a.png","sha256":"<sha>",
   "size":12345,"content_type":"image/png","status":"saved",
   "blob":"files_ca/ab/cd/<rest>"}
]}
```

Sorted by `path`. `blob` is derived from `sha256`; it is `null` when the
hash is not 64 lowercase hex characters.

### Shell object

Raw UTF-8 bytes of `Shell::to_text()`, without a JSON wrapper. The hash is
computed over those bytes.

## Commit protocol

1. Take an exclusive `flock` on `out/<site>/.publish.lock`. Contention is a
   retryable error; publication never runs concurrently.
2. Open a separate read-only SQLite connection, `BEGIN DEFERRED`, and read
   pages, files and `meta['shell']` from that one snapshot.
3. For every page decide its archive:
   - no stored revisions → no archive;
   - matching `packed_revs` + non-null `archive_sha256` + object exists →
     reuse;
   - matching `packed_revs` + null `archive_sha256` + legacy
     `pages_by_id/...zst` exists → stream-hash it and hardlink/copy it into
     the object store (migration without repack);
   - otherwise → read the page's revisions from the snapshot, require the
     count to equal `revisions_stored`, and pack a new archive.
4. Write archives, pages section, files section, shell and root as immutable
   objects: unique temp file, fsync file, rename, fsync the directory chain.
   An existing object is checked for size and left untouched.
5. Update `out_state` in one write transaction, only where
   `(packed_revs, archive_sha256)` actually changed. `packed_at` never churns
   on a no-op.
6. Atomically replace `HEAD` with `<root_sha>\n` (temp + file fsync + rename
   + directory fsync). If the bytes are already identical, `HEAD` is not
   touched.

Every object named by the root is durable before step 7. A crash before
step 7 leaves the previous `HEAD` valid; new objects are orphaned but
harmless.

## Reader contract

A reader should:

1. read `out/<site>/HEAD` (one small text file);
2. open `roots/<aa>/<bb>/<root_sha>.json`;
3. follow the three section hashes under `objects/`;
4. resolve page archives and file blobs from the section entries.

That root and everything it names are immutable, so the reader keeps a
consistent generation even if publication later switches `HEAD`.

## Backup order

If backing up a live tree, copy `files_ca/`, `objects/` and `roots/` first,
then `HEAD` last. A `HEAD` restored without every object it names is not a
valid snapshot.

## Migration from the old mutable tree

The v8 schema migration adds `out_state.archive_sha256` with NULL for every
existing row. On the next publication, a page whose `packed_revs` already
matches its stored count is adopted: the existing `pages_by_id/...zst` is
streamed through SHA-256 and hardlinked (or copied) into
`objects/archives/...`. No page is recompressed just because the format
changed. If the old archive is missing, that page is repacked once.

Attachments stay at `files_ca/<aa>/<bb>/<rest>` and are not migrated.
