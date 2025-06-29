# vibedb Design Documentation

## 概要

vibedbは、教育目的で開発されているRDBMSです。PostgreSQLやMySQLなどの実用的なデータベースシステムの設計を参考にしながら、段階的に機能を実装しています。

## アーキテクチャ

```
┌─────────────────────────────────────┐
│       SQL Parser & Planner          │ ← 将来実装
├─────────────────────────────────────┤
│          Executor Layer             │ ← 将来実装
├─────────────────────────────────────┤
│          Access Layer               │ ← 実装中
│   - TableHeap (テーブル抽象化)      │
│   - Tuple (行の論理表現)            │
├─────────────────────────────────────┤
│         Storage Layer               │ ← 実装済み
│   - BufferPoolManager               │
│   - PageManager                     │
│   - HeapPage                        │
└─────────────────────────────────────┘
```

## 各層の責務

### Storage層（[詳細](storage.md)）
物理的なディスクI/Oとメモリ管理を担当。
- **PageManager**: ディスクへの読み書き
- **BufferPoolManager**: ページキャッシュとメモリ管理
- **HeapPage**: PostgreSQLスタイルのページ構造

### Access層（[詳細](access.md)）
物理層を抽象化し、論理的なテーブル操作を提供。
- **TableHeap**: 複数ページにまたがるテーブル管理
- **Tuple/TupleId**: 行の論理的表現と識別子
- **SeqScan**: 順次スキャン（将来実装）

### Executor層（将来実装）
クエリ実行計画の実行。
- **SeqScanExecutor**: 全表スキャン
- **IndexScanExecutor**: インデックススキャン
- **JoinExecutor**: 結合処理

### Catalog層（将来実装）
メタデータ管理。
- **テーブル定義**
- **カラム情報**
- **制約情報**

## 設計原則

1. **段階的実装**
   - 最小限の機能から開始
   - 各層を独立して開発・テスト可能

2. **実用性重視**
   - 教科書的な実装より実用的な設計を選択
   - PostgreSQLなどの実績ある設計を参考

3. **拡張性**
   - 将来の機能追加を考慮した設計
   - インターフェースの安定性

## 実装状況

### 完了
- ✅ ディスクI/O（PageManager）
- ✅ バッファプール（BufferPoolManager）
- ✅ ページ構造（HeapPage）
- ✅ ゼロコピー設計

### 進行中
- 🚧 Access層（TableHeap, Tuple）

### 計画中
- ⏳ カタログシステム
- ⏳ クエリ実行エンジン
- ⏳ トランザクション管理
- ⏳ インデックス（B+Tree）
- ⏳ WAL（Write-Ahead Logging）

## 参考資料

- [Storage Layer Design](storage.md)
- [Buffer Pool Design](buffer_pool.md)
- [Access Layer Design](access.md)