# Buffer Pool Design

## 概要

BufferPoolManagerは、ディスクとメモリ間のページ転送を管理し、頻繁にアクセスされるページをメモリ上にキャッシュすることで、ディスクI/Oを削減します。

## アーキテクチャ

### コンポーネント構成

```
BufferPoolManager
├── PageTable (DashMap<PageId, FrameId>)
├── Frames (HashMap<FrameId, Frame>)
├── Replacer (LRU置換アルゴリズム)
└── PageManager (ディスクI/O)
```

### ゼロコピー設計

1. **PageGuardパターン**
   - `PageReadGuard`: 読み取り専用アクセス（複数同時可能）
   - `PageWriteGuard`: 書き込みアクセス（排他制御）
   - Dropトレイトでunpinを自動化

2. **メモリ効率化**
   - Frameは遅延割り当て（必要時のみ）
   - 直接参照（`&[u8; PAGE_SIZE]`）でコピーを回避
   - Arcによる読み取り専用共有

## 主要構造体

### Frame
```rust
pub struct Frame {
    data: Box<[u8; PAGE_SIZE]>,    // ページデータ
    page_id: Option<PageId>,        // 格納しているページID
    pin_count: AtomicU32,           // ピン数（参照カウント）
    is_dirty: AtomicBool,           // 更新フラグ
}
```

### BufferPoolManager
```rust
pub struct BufferPoolManager {
    page_table: Arc<DashMap<PageId, FrameId>>,    // ページ→フレームマッピング
    frames: Arc<RwLock<HashMap<FrameId, Frame>>>, // フレーム管理
    replacer: Arc<Mutex<Box<dyn Replacer>>>,      // 置換アルゴリズム
    page_manager: Arc<Mutex<PageManager>>,         // ディスクI/O
    next_frame_id: AtomicU32,                      // 次のフレームID
    max_frames: usize,                             // 最大フレーム数
}
```

## 動作フロー

### ページ取得（fetch_page）
1. PageTableでページIDを検索
2. 存在する場合：
   - フレームのpin_countを増加
   - PageGuardを返す
3. 存在しない場合：
   - 空きフレームまたはevict対象を取得
   - ディスクからページを読み込み
   - PageTableに登録
   - PageGuardを返す

### ページ作成（new_page）
1. 新しいページIDを生成
2. 空きフレームを取得
3. フレームを初期化
4. PageTableに登録
5. PageWriteGuardを返す

### ページ解放（unpin）
1. pin_countをデクリメント
2. pin_count == 0の場合、replacerに通知

### フラッシュ（flush_page）
1. is_dirtyフラグをチェック
2. dirtyの場合、ディスクに書き込み
3. is_dirtyフラグをクリア

## 並行性制御

- **PageTable**: DashMapによるスレッドセーフなハッシュマップ
- **Frames**: RwLockで保護（読み取り多重化）
- **Replacer**: Mutexで排他制御
- **Frame内部**: Atomicによるロックフリー操作

## LRU Replacer

### 基本動作
- 最近使用されていないページを置換対象に選択
- pin_count > 0のページは置換対象外

### インターフェース
```rust
pub trait Replacer: Send + Sync {
    fn evict(&mut self) -> Option<FrameId>;
    fn pin(&mut self, frame_id: FrameId);
    fn unpin(&mut self, frame_id: FrameId);
}
```

## HeapPage構造

### レイアウト
```
[Header]
├── page_id (4 bytes)
├── lsn (8 bytes)
├── free_space_pointer (2 bytes)
├── tuple_count (2 bytes)
└── padding

[Tuple Directory] (末尾から成長)
├── slot[0]: (offset, length)
├── slot[1]: (offset, length)
└── ...

[Free Space]

[Tuple Data] (先頭から成長)
├── tuple[0]
├── tuple[1]
└── ...
```

### 特徴
- 可変長タプルサポート
- スロット配列による間接参照
- 空き領域の効率的な管理