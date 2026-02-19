# FsDir tree structure and RocksDB interaction
_Exported on 1/22/2026 at 21:36:40 PST from Cursor (2.3.34)_

---

**User**

@docs/CURVINE_SERVER_ARCHITECTURE.md 

```
#### `FsDir` (`src/master/meta/fs_dir.rs:38-44`)

In-memory filesystem tree backed by RocksDB:

```rust
pub struct FsDir {
    root_dir: InodeView,          // Root of directory tree
    inode_id: InodeId,            // ID generator
    store: InodeStore,            // RocksDB persistence
    journal_writer: JournalWriter, // Raft log writer
    evictor: Arc<dyn Evictor>,    // LRU/LFU eviction
}
```

**Inode Types** (`src/master/meta/inode/inode_view.rs`):
```rust
pub enum InodeView {
    File(String, InodeFile),      // Regular file
    Dir(String, InodeDir),        // Directory
    FileEntry(String, i64),       // Hard link entry (name → inode_id)
}
```
```

how does FsDir build a tree structure, and how does it interact with RocksDB?

---

**Cursor**

Let me explore the FsDir implementation and its interaction with RocksDB.

Let me also look at `InodeDir` to understand how children are stored in memory, and `RocksInodeStore` to see the RocksDB schema:

Let me also look at `InodeChildren` to see how children are stored in memory:

Now I have a complete picture. Let me explain how `FsDir` builds its tree structure and interacts with RocksDB.

---

## How FsDir Builds the Tree Structure

### 1. **Dual-Layer Architecture: In-Memory Tree + RocksDB Persistence**

`FsDir` uses a **hybrid architecture** where:
- **In-memory tree**: Fast path lookups via pointer-based tree traversal
- **RocksDB**: Durable persistence and crash recovery

```
┌─────────────────────────────────────────────────────────────────┐
│                      In-Memory Tree                              │
│                                                                 │
│                        root_dir (InodeView)                     │
│                              │                                  │
│              ┌───────────────┼───────────────┐                  │
│              ▼               ▼               ▼                  │
│         Dir("a")        Dir("b")       FileEntry("f1", id=1234) │
│              │               │                                  │
│         ┌────┴────┐     ┌────┴────┐         (lazy load from DB) │
│         ▼         ▼     ▼         ▼                             │
│     File("x") File("y") Dir("c") ...                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ sync via InodeStore
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         RocksDB                                  │
│                                                                 │
│  CF_INODES:    inode_id → serialized InodeView                  │
│  CF_EDGES:     (parent_id, child_name) → child_id               │
│  CF_BLOCK:     (block_id, worker_id) → BlockLocation            │
│  CF_LOCATION:  (worker_id, block_id) → block_id                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2. **InodeView: The Node Type**

From `inode_view.rs`, each node in the tree is an `InodeView`:

```rust
pub enum InodeView {
    File(String, InodeFile),      // Regular file with full metadata
    Dir(String, InodeDir),        // Directory with children
    FileEntry(String, i64),       // Lightweight reference (name → inode_id)
}
```

**Key optimization**: For **files**, the in-memory tree stores only `FileEntry(name, inode_id)` instead of the full `InodeFile`. This reduces memory usage since file metadata is loaded from RocksDB on demand.

### 3. **InodeDir: Directory with Children**

From `inode_dir.rs`, each directory stores its children in an `InodeChildren` container:

```38:39:curvine-server/src/master/meta/inode/inode_dir.rs
    #[serde(skip)]
    children: InodeChildren,
```

The `#[serde(skip)]` is crucial—children are NOT serialized to RocksDB. They're stored separately via edges.

### 4. **InodeChildren: In-Memory Child Storage**

From `inodes_children.rs`, children can be stored two ways:

```rust
pub enum InodeChildren {
    List(Vec<Box<InodeView>>),           // Sorted vector (binary search)
    Map(BTreeMap<String, Box<InodeView>>),  // Tree map (default)
}
```

When adding a child:

```126:166:curvine-server/src/master/meta/inode/inodes_children.rs
    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        // ...
        InodeChildren::Map(map) => match map.entry(inode.name().to_owned()) {
            Entry::Vacant(v) => {
                if inode.is_file() {
                    // Store lightweight FileEntry in map
                    v.insert(Box::new(InodeView::FileEntry(
                        inode.name().to_string(),
                        inode.id(),
                    )));
                    // But return owned pointer to complete object
                    Ok(InodePtr::from_owned(*inode))
                } else {
                    // Directory directly stores complete object
                    let inserted = v.insert(inode.clone());
                    Ok(InodePtr::from_ref(inserted.as_ref()))
                }
            }
            // ...
        }
    }
```

**Memory optimization**: Files are stored as `FileEntry` references; directories are stored fully (since they need children access).

---

## RocksDB Schema

`RocksInodeStore` uses **5 column families**:

| Column Family | Key Format | Value | Purpose |
|--------------|------------|-------|---------|
| `CF_INODES` | `inode_id (i64)` | Serialized `InodeView` | Full inode data |
| `CF_EDGES` | `(parent_id, child_name)` | `child_id (i64)` | Parent→child relationships |
| `CF_BLOCK` | `(block_id, worker_id)` | `BlockLocation` | Block→worker mapping |
| `CF_LOCATION` | `(worker_id, block_id)` | `block_id` | Reverse index for worker cleanup |
| `CF_COMMON` | Prefix + id | Various | Mount points, locks |

---

## How Tree Operations Work

### Creating a Directory (`mkdir`)

```98:128:curvine-server/src/master/meta/fs_dir.rs
pub fn mkdir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
    inp = self.create_parent_dir(inp, opts.parent_opts())?;
    inp = self.create_single_dir(inp, opts)?;
    Ok(inp)
}

fn create_single_dir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
    let dir = InodeDir::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
    inp = self.add_last_inode(inp, Dir(name.clone(), dir.clone()))?;
    self.journal_writer.log_mkdir(op_ms, &parent_path, &dir)?;
    Ok(inp)
}
```

The `add_last_inode` method:
1. Adds the child to parent's in-memory `InodeChildren`
2. Persists to RocksDB via `InodeStore::apply_add()`

### RocksDB Write (apply_add)

```51:84:curvine-server/src/master/meta/store/inode_store.rs
pub fn apply_add(&self, parent: &InodeView, child: &InodeView) -> CommonResult<()> {
    let mut batch = self.store.new_batch();

    batch.write_inode(child)?;           // CF_INODES: id → serialized child
    batch.write_inode(parent)?;          // CF_INODES: id → serialized parent (updated mtime)
    batch.add_child(parent.id(), child.name(), child.id())?;  // CF_EDGES: (parent_id, name) → child_id

    batch.commit()?;
    // ...
}
```

The batch operations in `InodeWriteBatch`:

```280:295:curvine-server/src/master/meta/store/rocks_inode_store.rs
pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
    let key = RocksUtils::i64_to_bytes(inode.id());
    let value = Serde::serialize(inode)?;
    self.put_cf(RocksInodeStore::CF_INODES, key, value)
}

pub fn add_child(&mut self, parent_id: i64, child_name: &str, child_id: i64) -> CommonResult<()> {
    let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
    let value = RocksUtils::i64_to_bytes(child_id);
    self.put_cf(RocksInodeStore::CF_EDGES, key, value)
}
```

---

## Tree Reconstruction from RocksDB (Recovery)

On startup or after Raft snapshot restore, the tree is rebuilt from RocksDB:

```397:465:curvine-server/src/master/meta/store/inode_store.rs
pub fn create_tree(&self) -> CommonResult<(i64, InodeView)> {
    let mut root = FsDir::create_root();
    let mut stack = LinkedList::new();
    stack.push_back((root.as_ptr(), ROOT_INODE_ID, ...));

    while let Some((mut parent, child_id, file_entry)) = stack.pop_front() {
        // Load inode from RocksDB
        let store_inode = self.store.get_inode(child_id)?;
        
        // Add to parent's in-memory children
        let next_parent = parent.add_child(inode)?;

        // If directory, scan edges for children
        if next_parent.is_dir() {
            let childs_iter = self.store.edges_iter(next_parent.id())?;
            for (key, value) in childs_iter {
                let (_, child_name) = RocksUtils::i64_str_from_bytes(&key);
                let child_id = RocksUtils::i64_from_bytes(&value)?;
                stack.push_back((next_parent.clone(), child_id, FileEntry(child_name, child_id)));
            }
        }
    }
    Ok((last_inode_id, root))
}
```

**Process**:
1. Start with empty root directory
2. Use BFS to iterate through all edges in `CF_EDGES`
3. Load each inode from `CF_INODES` 
4. Reconstruct parent-child relationships in memory
5. For files, store only `FileEntry`; for directories, store full `InodeDir`

---

## Summary Diagram

```
                    WRITE PATH (mkdir example)
                    
    FsDir::mkdir()
         │
         ├─► In-Memory: parent.add_child(Dir(...))
         │              └── InodeChildren::Map.insert(name → InodeDir)
         │
         ├─► RocksDB:   InodeStore::apply_add()
         │              ├── CF_INODES[child_id] = serialize(InodeDir)
         │              ├── CF_INODES[parent_id] = serialize(parent) // updated mtime
         │              └── CF_EDGES[(parent_id, name)] = child_id
         │
         └─► Raft:      journal_writer.log_mkdir()
                        └── Replicated to followers

                    READ PATH (list directory)
                    
    FsDir::list_status()
         │
         ├─► In-Memory: dir.children_iter()
         │              └── Returns InodeView references
         │
         └─► For FileEntry: InodeStore::get_inode(id)
                            └── Loads full file metadata from CF_INODES
```

The key insight is that **writes always update both layers atomically** (in-memory + RocksDB via batch), while **reads primarily use the in-memory tree** with lazy loading for file metadata.

```plaintext
┌─────────────────────────────────────────────────────────────────┐
│                      In-Memory Tree                              │
│                                                                 │
│                        root_dir (InodeView)                     │
│                              │                                  │
│              ┌───────────────┼───────────────┐                  │
│              ▼               ▼               ▼                  │
│         Dir("a")        Dir("b")       FileEntry("f1", id=1234) │
│              │               │                                  │
│         ┌────┴────┐     ┌────┴────┐         (lazy load from DB) │
│         ▼         ▼     ▼         ▼                             │
│     File("x") File("y") Dir("c") ...                            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ sync via InodeStore
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         RocksDB                                  │
│                                                                 │
│  CF_INODES:    inode_id → serialized InodeView                  │
│  CF_EDGES:     (parent_id, child_name) → child_id               │
│  CF_BLOCK:     (block_id, worker_id) → BlockLocation            │
│  CF_LOCATION:  (worker_id, block_id) → block_id                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
pub enum InodeView {
    File(String, InodeFile),      // Regular file with full metadata
    Dir(String, InodeDir),        // Directory with children
    FileEntry(String, i64),       // Lightweight reference (name → inode_id)
}
```

```rust
    #[serde(skip)]
    children: InodeChildren,
```

```rust
pub enum InodeChildren {
    List(Vec<Box<InodeView>>),           // Sorted vector (binary search)
    Map(BTreeMap<String, Box<InodeView>>),  // Tree map (default)
}
```

```rust
    pub fn add_child(&mut self, inode: InodeView) -> CommonResult<InodePtr> {
        // ...
        InodeChildren::Map(map) => match map.entry(inode.name().to_owned()) {
            Entry::Vacant(v) => {
                if inode.is_file() {
                    // Store lightweight FileEntry in map
                    v.insert(Box::new(InodeView::FileEntry(
                        inode.name().to_string(),
                        inode.id(),
                    )));
                    // But return owned pointer to complete object
                    Ok(InodePtr::from_owned(*inode))
                } else {
                    // Directory directly stores complete object
                    let inserted = v.insert(inode.clone());
                    Ok(InodePtr::from_ref(inserted.as_ref()))
                }
            }
            // ...
        }
    }
```

```rust
pub fn mkdir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
    inp = self.create_parent_dir(inp, opts.parent_opts())?;
    inp = self.create_single_dir(inp, opts)?;
    Ok(inp)
}

fn create_single_dir(&mut self, mut inp: InodePath, opts: MkdirOpts) -> FsResult<InodePath> {
    let dir = InodeDir::with_opts(self.next_inode_id()?, LocalTime::mills() as i64, opts);
    inp = self.add_last_inode(inp, Dir(name.clone(), dir.clone()))?;
    self.journal_writer.log_mkdir(op_ms, &parent_path, &dir)?;
    Ok(inp)
}
```

```rust
pub fn apply_add(&self, parent: &InodeView, child: &InodeView) -> CommonResult<()> {
    let mut batch = self.store.new_batch();

    batch.write_inode(child)?;           // CF_INODES: id → serialized child
    batch.write_inode(parent)?;          // CF_INODES: id → serialized parent (updated mtime)
    batch.add_child(parent.id(), child.name(), child.id())?;  // CF_EDGES: (parent_id, name) → child_id

    batch.commit()?;
    // ...
}
```

```rust
pub fn write_inode(&mut self, inode: &InodeView) -> CommonResult<()> {
    let key = RocksUtils::i64_to_bytes(inode.id());
    let value = Serde::serialize(inode)?;
    self.put_cf(RocksInodeStore::CF_INODES, key, value)
}

pub fn add_child(&mut self, parent_id: i64, child_name: &str, child_id: i64) -> CommonResult<()> {
    let key = RocksUtils::i64_str_to_bytes(parent_id, child_name);
    let value = RocksUtils::i64_to_bytes(child_id);
    self.put_cf(RocksInodeStore::CF_EDGES, key, value)
}
```

```rust
pub fn create_tree(&self) -> CommonResult<(i64, InodeView)> {
    let mut root = FsDir::create_root();
    let mut stack = LinkedList::new();
    stack.push_back((root.as_ptr(), ROOT_INODE_ID, ...));

    while let Some((mut parent, child_id, file_entry)) = stack.pop_front() {
        // Load inode from RocksDB
        let store_inode = self.store.get_inode(child_id)?;
        
        // Add to parent's in-memory children
        let next_parent = parent.add_child(inode)?;

        // If directory, scan edges for children
        if next_parent.is_dir() {
            let childs_iter = self.store.edges_iter(next_parent.id())?;
            for (key, value) in childs_iter {
                let (_, child_name) = RocksUtils::i64_str_from_bytes(&key);
                let child_id = RocksUtils::i64_from_bytes(&value)?;
                stack.push_back((next_parent.clone(), child_id, FileEntry(child_name, child_id)));
            }
        }
    }
    Ok((last_inode_id, root))
}
```

```plaintext
                    WRITE PATH (mkdir example)
                    
    FsDir::mkdir()
         │
         ├─► In-Memory: parent.add_child(Dir(...))
         │              └── InodeChildren::Map.insert(name → InodeDir)
         │
         ├─► RocksDB:   InodeStore::apply_add()
         │              ├── CF_INODES[child_id] = serialize(InodeDir)
         │              ├── CF_INODES[parent_id] = serialize(parent) // updated mtime
         │              └── CF_EDGES[(parent_id, name)] = child_id
         │
         └─► Raft:      journal_writer.log_mkdir()
                        └── Replicated to followers

                    READ PATH (list directory)
                    
    FsDir::list_status()
         │
         ├─► In-Memory: dir.children_iter()
         │              └── Returns InodeView references
         │
         └─► For FileEntry: InodeStore::get_inode(id)
                            └── Loads full file metadata from CF_INODES
```

