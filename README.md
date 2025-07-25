# Databricks SQL プロファイラー分析ツール

## 概要

このツールは、Databricks SQL プロファイラーのJSONログファイルを解析し、AI（LLM）を活用してパフォーマンスのボトルネックを特定し、具体的な改善案を提示する高度な分析ツールです。

## 主な機能

### 📊 包括的なパフォーマンス分析
- SQLプロファイラーJSONファイルの詳細解析
- 実行時間、データ処理量、キャッシュ効率などの主要メトリクス抽出
- ボトルネック指標の自動計算（コンパイル時間比率、キャッシュヒット率、データ選択性など）

### 🤖 AI によるボトルネック分析
- **複数LLMプロバイダー対応**: 
  - Databricks Model Serving
  - OpenAI GPT-4
  - Azure OpenAI
  - Anthropic Claude
- 抽出メトリクスからのボトルネック特定
- 具体的な改善案の自動生成

### 🌐 多言語対応
- 日本語・英語での分析レポート出力
- 言語設定による出力の切り替え

### ⚡ 高度な分析機能
- Photon利用率の分析
- スピル検出とシャッフル/並列度の問題特定
- TOP10 最も時間がかかっている処理の特定
- Liquid Clustering の最適化提案

### 🔄 自動エラー修正機能（NEW）
- **EXPLAIN実行時のエラー自動検出**: 生成された最適化クエリの構文チェック
- **LLMによる自動エラー修正**: エラー情報をLLMに再入力して修正クエリを生成
- **最大2回再試行**: エラーが解消されるまで最適化クエリを自動修正
- **フォールバック機能**: 修正失敗時は元の動作可能クエリを使用
- **詳細ログ記録**: 全試行の履歴とエラー詳細を自動記録

## セットアップ

### 前提条件
- Databricks環境（Databricks Runtime 推奨）
- 以下のPythonライブラリ:
  - `pandas`
  - `requests`
  - `pyspark`（Databricks環境では自動的に利用可能）

### 1. リポジトリのクローン
```bash
git clone https://github.com/MitsuhiroItagaki/test02.git
cd test02
```

### 2. LLMエンドポイントの設定
ツール内で以下のいずれかのLLMプロバイダーを設定：

**Databricks Model Serving（推奨）**
```python
LLM_CONFIG["provider"] = "databricks"
LLM_CONFIG["databricks"]["endpoint_name"] = "your-endpoint-name"
```

**OpenAI**
```python
LLM_CONFIG["provider"] = "openai"
LLM_CONFIG["openai"]["api_key"] = "your-api-key"
```

**Azure OpenAI**
```python
LLM_CONFIG["provider"] = "azure_openai"
LLM_CONFIG["azure_openai"]["api_key"] = "your-api-key"
LLM_CONFIG["azure_openai"]["endpoint"] = "your-endpoint"
```

**Anthropic Claude**
```python
LLM_CONFIG["provider"] = "anthropic"
LLM_CONFIG["anthropic"]["api_key"] = "your-api-key"
```

## 使用方法

### 1. SQLプロファイラーJSONファイルの準備
- Databricksでクエリ実行時にSQLプロファイラーを有効化
- 生成されたJSONファイルをDBFS、FileStore、またはUnity Catalog Volumesに配置

### 2. 分析対象ファイルの設定
```python
# ファイルパスの設定例
JSON_FILE_PATH = '/Volumes/catalog/schema/volume/profiler.json'
# または
JSON_FILE_PATH = '/FileStore/shared_uploads/username/profiler_log.json'
```

### 3. 分析実行
1. Databricksノートブックで `databricks_sql_profiler_analysis.py` を開く
2. 設定セクションでファイルパスとLLMエンドポイントを設定
3. 全セルを順次実行

### 4. 結果の確認
- 詳細な分析レポートがMarkdown形式で出力
- ボトルネック要因と具体的な改善案が提示
- パフォーマンスメトリクスの可視化

### 5. 自動エラー修正機能の動作
```
🔄 EXPLAIN実行と自動エラー修正（最大2回試行）
======================================================

🤖 ステップ1: 初回最適化クエリ生成
🔍 試行 1/3: EXPLAIN実行
❌ 試行 1 でエラー発生: syntax error near 'INVALID'
🔧 試行 2 に向けてエラー修正中...
✅ エラー修正クエリを生成しました

🔍 試行 2/3: EXPLAIN実行  
✅ 試行 2 で成功しました！

📊 最終結果: success
🔄 総試行回数: 2
```

- **成功時**: 最適化されたクエリとEXPLAIN結果を出力
- **失敗時**: 元クエリを使用し、詳細な失敗ログを生成
- **全自動**: 手動介入不要でエラー修正を実行

## 出力例

```
📊 Databricks SQLプロファイラー ボトルネック分析結果
===============================================
🔍 クエリID: query_12345
⏰ 分析日時: 2024-01-15 10:30:00
⏱️ 実行時間: 45.6秒

📈 主要メトリクス:
- データ読み込み: 12.5 GB
- キャッシュヒット率: 75.3%
- Photon利用率: 92.1%

🎯 特定されたボトルネック:
1. シャッフル操作による遅延 (15.2秒)
2. 低い並列度による処理効率の低下
3. スピル発生によるディスクI/O

💡 改善案:
1. パーティション数の調整
2. 結合順序の最適化
3. メモリ設定の見直し

🔄 自動エラー修正:
- 試行回数: 2回
- 最終ステータス: 成功
- 生成ファイル: output_optimized_sql_20240115-103000.sql
```

## ファイル構成

- `databricks_sql_profiler_analysis.py` - メイン分析ツール（Databricksノートブック形式）
- `README.md` - このドキュメント

## 貢献

プロジェクトへの貢献を歓迎します。プルリクエストを送信する前に、以下を確認してください：

- コードスタイルに従っている
- 分析機能が正常に動作する
- 適切なドキュメントが追加されている
- 多言語対応が考慮されている

## 技術仕様

- **対応環境**: Databricks Runtime (推奨)
- **対応ファイル形式**: SQLプロファイラーJSON
- **LLM統合**: REST API経由での複数プロバイダー対応
- **出力形式**: Markdown, 構造化されたメトリクス
- **エラー修正**: 自動再試行機能（最大2回）
- **フォールバック**: 元クエリ使用による確実な実行保証
- **ログ機能**: 詳細な試行履歴とエラー分析

## ライセンス

このプロジェクトのライセンスについては、プロジェクト管理者にお問い合わせください。 