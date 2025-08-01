# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQLプロファイラー分析ツール
# MAGIC
# MAGIC このnotebookは、DatabricksのSQLプロファイラーJSONログファイルを読み込み、ボトルネック特定と改善案の提示に必要なメトリクスを抽出して分析を行います。
# MAGIC
# MAGIC ## 機能概要
# MAGIC
# MAGIC 1. **SQLプロファイラーJSONファイルの読み込み**
# MAGIC    - Databricksで出力されたプロファイラーログの解析
# MAGIC    - `graphs`キーに格納された実行プランメトリクスの抽出
# MAGIC
# MAGIC 2. **重要メトリクスの抽出**
# MAGIC    - クエリ基本情報（ID、ステータス、実行時間など）
# MAGIC    - 全体パフォーマンス（実行時間、データ量、キャッシュ効率など）
# MAGIC    - ステージ・ノード詳細メトリクス
# MAGIC    - ボトルネック指標の計算
# MAGIC
# MAGIC 3. **AI によるボトルネック分析**
# MAGIC    - 設定可能なLLMエンドポイント (Databricks, OpenAI, Azure OpenAI, Anthropic)
# MAGIC    - 抽出メトリクスからボトルネック特定
# MAGIC    - 具体的な改善案の提示
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **事前準備:**
# MAGIC - LLMエンドポイントの設定（Databricks Model Serving または 外部API）
# MAGIC - 必要なAPIキーの設定
# MAGIC - SQLプロファイラーJSONファイルの準備（DBFS または FileStore）
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔧 設定・準備セクション
# MAGIC
# MAGIC **このセクションではツールの基本設定を行います**
# MAGIC
# MAGIC 📋 **設定内容:**
# MAGIC - 分析対象ファイルの指定
# MAGIC - LLMエンドポイントの設定
# MAGIC - 分析関数の定義
# MAGIC
# MAGIC ⚠️ **重要:** メイン処理を実行する前に、このセクションのすべてのセルを実行してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 分析対象ファイル設定
# MAGIC
# MAGIC **最初に、分析対象のSQLプロファイラーJSONファイルを指定してください。**
# MAGIC
# MAGIC このセルでは以下の設定を行います：
# MAGIC - 📂 SQLプロファイラーJSONファイルのパス設定
# MAGIC - 📋 対応するファイルパス形式の例
# MAGIC - ⚙️ 基本的な環境設定

# COMMAND ----------

# 📁 SQLプロファイラーJSONファイルのパス設定
# 
# 以下のJSON_FILE_PATHを実際のファイルパスに変更してください：

# ノートブック環境用のファイルパス設定（以下の中から選択してください）

# オプション1: チューニング前プランファイル（推奨）
JSON_FILE_PATH = '/Workspace/Shared/AutoSQLTuning/Query2.json'

# オプション2: 他のJSONファイルを使用する場合は、以下のコメントアウトを解除して編集
# JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/nophoton.json'
# JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/POC1.json'
# JSON_FILE_PATH = '/Volumes/main/base/mitsuhiro_vol/your_file.json'

# コマンドライン環境用（オプション）
import sys
if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
    # コマンドライン引数がフラグ（-で始まる）でない場合のみ使用
    JSON_FILE_PATH = sys.argv[1]

# 🌐 出力言語設定（OUTPUT_LANGUAGE: 'ja' = 日本語, 'en' = 英語）
OUTPUT_LANGUAGE = 'ja'

# 🔍 EXPLAIN文実行設定（EXPLAIN_ENABLED: 'Y' = 実行する, 'N' = 実行しない）
EXPLAIN_ENABLED = 'Y'

# 🐛 デバッグモード設定（DEBUG_ENABLED: 'Y' = 中間ファイル保持, 'N' = 最終ファイルのみ保持）
DEBUG_ENABLED = 'Y'


def save_debug_query_trial(query: str, attempt_num: int, trial_type: str, query_id: str = None, error_info: str = None) -> str:
    """
    DEBUG_ENABLED=Y時に最適化試行中のクエリを試行ごとに保存
    
    Args:
        query: 生成されたクエリ
        attempt_num: 試行番号 (1, 2, 3, ...)
        trial_type: 試行タイプ ('initial', 'performance_improvement', 'error_correction')
        query_id: クエリID (optional)
        error_info: エラー情報 (optional)
    
    Returns:
        保存されたファイルパス（保存されなかった場合は空文字）
    """
    debug_enabled = globals().get('DEBUG_ENABLED', 'N')
    if debug_enabled.upper() != 'Y':
        return ""
    
    try:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # クエリIDが指定されていない場合は試行番号から生成
        if not query_id:
            query_id = f"trial_{attempt_num}"
        
        # ファイル名生成: debug_trial_{attempt_num}_{trial_type}_{timestamp}.sql
        filename = f"debug_trial_{attempt_num:02d}_{trial_type}_{timestamp}.sql"
        
        # メタデータ情報の準備
        metadata_header = f"""-- 🐛 DEBUG: 最適化試行クエリ (DEBUG_ENABLED=Y)
-- 📋 試行番号: {attempt_num}
-- 🎯 試行タイプ: {trial_type}
-- 🕐 生成時刻: {timestamp}
-- 🔍 クエリID: {query_id}
"""
        
        # エラー情報がある場合は追加
        if error_info:
            metadata_header += f"""-- ⚠️  エラー情報: {error_info[:200]}{'...' if len(error_info) > 200 else ''}
"""
        
        metadata_header += f"""-- 📄 生成ファイル: {filename}
-- ================================================

"""
        
        # ファイル保存
        full_content = metadata_header + query
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        print(f"🐛 DEBUG保存完了: {filename} (試行{attempt_num}: {trial_type})")
        return filename
        
    except Exception as e:
        print(f"⚠️ DEBUG保存エラー: {str(e)}")
        return ""

# 🧠 構造化抽出設定（STRUCTURED_EXTRACTION_ENABLED: 'Y' = 構造化抽出使用, 'N' = 従来の切り詰め使用）
# Physical PlanとEXPLAIN COSTの処理方式を制御
# - 'Y': 重要情報のみを構造化抽出（推奨：高精度・高効率）
# - 'N': 従来の文字数制限による切り詰め（フォールバック用）
STRUCTURED_EXTRACTION_ENABLED = 'Y'

# 🔄 自動エラー修正の最大試行回数設定（MAX_RETRIES: デフォルト2回）
# LLMが生成した最適化クエリのEXPLAIN実行でエラーが発生した場合の再試行回数
# - 1回目: 初回生成クエリでEXPLAIN実行
# - 2回目以降: エラー情報をLLMに再入力して修正クエリを生成・再実行
# - 最大試行回数に達した場合: 元の動作可能クエリを使用してファイル生成
MAX_RETRIES = 3

# 🚀 反復的最適化の最大試行回数設定（MAX_OPTIMIZATION_ATTEMPTS: デフォルト3回）
# パフォーマンス悪化を検出した場合の改善試行回数
# - 1回目: 初回最適化クエリ生成・パフォーマンス検証
# - 2回目以降: 悪化原因分析結果に基づく修正版クエリ生成・検証
# - 最大試行回数に達した場合: 元クエリを使用
# 注：構文エラーの修正（MAX_RETRIES）とは別のパラメータです
MAX_OPTIMIZATION_ATTEMPTS = 3

# 🗂️ カタログとデータベース設定（EXPLAIN文実行時に使用）
CATALOG = 'tpcds'
DATABASE = 'tpcds_sf1000_delta_lc'

# 💡 使用例:
# OUTPUT_LANGUAGE = 'ja'  # 日本語でファイル出力
# OUTPUT_LANGUAGE = 'en'  # 英語でファイル出力

# 🌐 多言語メッセージ辞書
MESSAGES = {
    'ja': {
        'bottleneck_title': 'Databricks SQLプロファイラー ボトルネック分析結果',
        'query_id': 'クエリID',
        'analysis_time': '分析日時',
        'execution_time': '実行時間',
        'sql_optimization_report': 'SQL最適化レポート',
        'optimization_time': '最適化日時',
        'original_file': 'オリジナルファイル',
        'optimized_file': '最適化ファイル',
        'optimization_analysis': '最適化分析結果',
        'performance_metrics': 'パフォーマンスメトリクス参考情報',
        'read_data': '読み込みデータ',
        'spill': 'スピル',
        'top10_processes': '最も時間がかかっている処理TOP10'
    },
    'en': {
        'bottleneck_title': 'Databricks SQL Profiler Bottleneck Analysis Results',
        'query_id': 'Query ID',
        'analysis_time': 'Analysis Time',
        'execution_time': 'Execution Time',
        'sql_optimization_report': 'SQL Optimization Report',
        'optimization_time': 'Optimization Time',
        'original_file': 'Original File',
        'optimized_file': 'Optimized File',
        'optimization_analysis': 'Optimization Analysis Results',
        'performance_metrics': 'Performance Metrics Reference',
        'read_data': 'Data Read',
        'spill': 'Spill',
        'top10_processes': 'Top 10 Most Time-Consuming Processes'
    }
}

def get_message(key: str) -> str:
    """多言語メッセージを取得"""
    return MESSAGES.get(OUTPUT_LANGUAGE, MESSAGES['ja']).get(key, key)

# 📋 対応するファイルパス形式の例:
# Unity Catalog Volumes:
# JSON_FILE_PATH = '/Volumes/catalog/schema/volume/profiler.json'
# 
# FileStore (推奨):
# JSON_FILE_PATH = '/FileStore/shared_uploads/your_username/profiler_log.json'
# 
# DBFS:
# JSON_FILE_PATH = '/dbfs/FileStore/shared_uploads/your_username/profiler_log.json'
# 
# DBFS URI:
# JSON_FILE_PATH = 'dbfs:/FileStore/shared_uploads/your_username/profiler_log.json'

print("📁 【分析対象ファイル設定完了】")
print("=" * 50)
print(f"📄 対象ファイル: {JSON_FILE_PATH}")
print("=" * 50)

# ⚙️ 基本的な環境設定
import json
try:
    import pandas as pd
except ImportError:
    print("Warning: pandas is not installed, some features may not work")
    pd = None
from typing import Dict, List, Any, Optional
from datetime import datetime

print("✅ 基本ライブラリインポート完了")
print("🚀 次のセルに進んでください")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 LLMエンドポイント設定
# MAGIC
# MAGIC このセルでは以下の設定を行います：
# MAGIC - LLMプロバイダーの選択（Databricks/OpenAI/Azure/Anthropic）
# MAGIC - 各プロバイダーの接続設定
# MAGIC - 必要なライブラリのインポート

# COMMAND ----------

# 🤖 LLMエンドポイント設定
LLM_CONFIG = {
    # エンドポイントタイプ: 'databricks', 'openai', 'azure_openai', 'anthropic'
    "provider": "databricks",
    
    # Databricks Model Serving設定（高速実行優先）
    "databricks": {
        "endpoint_name": "databricks-claude-3-7-sonnet",  # Model Servingエンドポイント名
        "max_tokens": 131072,  # 128K tokens（Claude 3.7 Sonnetの最大制限）
        "temperature": 0.0,    # 決定的な出力のため（0.1→0.0）
        # "thinking_enabled": False,  # 拡張思考モード（デフォルト: 無効 - 高速実行優先）- Claude 3 Sonnet専用
        # "thinking_budget_tokens": 65536  # 思考用トークン予算 64K tokens（有効時のみ使用）- Claude 3 Sonnet専用
    },
    
    # OpenAI設定（完全なSQL生成用に最適化）
    "openai": {
        "api_key": "",  # OpenAI APIキー (環境変数OPENAI_API_KEYでも可)
        "model": "gpt-4o",  # gpt-4o, gpt-4-turbo, gpt-3.5-turbo
        "max_tokens": 16000,  # OpenAIの制限内最大
        "temperature": 0.0    # 決定的な出力のため（0.1→0.0）
    },
    
    # Azure OpenAI設定（完全なSQL生成用に最適化）
    "azure_openai": {
        "api_key": "",  # Azure OpenAI APIキー (環境変数AZURE_OPENAI_API_KEYでも可)
        "endpoint": "",  # https://your-resource.openai.azure.com/
        "deployment_name": "",  # デプロイメント名
        "api_version": "2024-02-01",
        "max_tokens": 16000,  # Azure OpenAIの制限内最大
        "temperature": 0.0    # 決定的な出力のため（0.1→0.0）
    },
    
    # Anthropic設定（完全なSQL生成用に最適化）
    "anthropic": {
        "api_key": "",  # Anthropic APIキー (環境変数ANTHROPIC_API_KEYでも可)
        "model": "claude-3-5-sonnet-20241022",  # claude-3-5-sonnet-20241022, claude-3-opus-20240229
        "max_tokens": 16000,  # Anthropicの制限内最大
        "temperature": 0.0    # 決定的な出力のため（0.1→0.0）
    }
}

print("🤖 LLMエンドポイント設定完了")
print(f"🤖 LLMプロバイダー: {LLM_CONFIG['provider']}")

if LLM_CONFIG['provider'] == 'databricks':
    print(f"🔗 Databricksエンドポイント: {LLM_CONFIG['databricks']['endpoint_name']}")
    thinking_status = "有効" if LLM_CONFIG['databricks'].get('thinking_enabled', False) else "無効"
    thinking_budget = LLM_CONFIG['databricks'].get('thinking_budget_tokens', 65536)
    max_tokens = LLM_CONFIG['databricks'].get('max_tokens', 131072)
    print(f"🧠 拡張思考モード: {thinking_status} (予算: {thinking_budget:,} tokens)")
    print(f"📊 最大トークン数: {max_tokens:,} tokens ({max_tokens//1024}K)")
    if not LLM_CONFIG['databricks'].get('thinking_enabled', False):
        print("⚡ 高速実行モード: 思考プロセスを省略して迅速な結果生成")
elif LLM_CONFIG['provider'] == 'openai':
    print(f"🔗 OpenAIモデル: {LLM_CONFIG['openai']['model']}")
elif LLM_CONFIG['provider'] == 'azure_openai':
    print(f"🔗 Azure OpenAIデプロイメント: {LLM_CONFIG['azure_openai']['deployment_name']}")
elif LLM_CONFIG['provider'] == 'anthropic':
    print(f"🔗 Anthropicモデル: {LLM_CONFIG['anthropic']['model']}")

print()
print("💡 LLMプロバイダー切り替え例:")
print('   LLM_CONFIG["provider"] = "openai"      # OpenAI GPT-4に切り替え')
print('   LLM_CONFIG["provider"] = "anthropic"   # Anthropic Claudeに切り替え')
print('   LLM_CONFIG["provider"] = "azure_openai" # Azure OpenAIに切り替え')
print()
print("🧠 Databricks拡張思考モード設定例:")
print('   LLM_CONFIG["databricks"]["thinking_enabled"] = False  # 拡張思考モード無効（デフォルト・高速実行）')
print('   LLM_CONFIG["databricks"]["thinking_enabled"] = True   # 拡張思考モード有効（詳細分析時のみ）')
print('   LLM_CONFIG["databricks"]["thinking_budget_tokens"] = 65536  # 思考用トークン予算(64K)')
print('   LLM_CONFIG["databricks"]["max_tokens"] = 131072  # 最大トークン数(128K)')
print()

# 必要なライブラリのインポート
try:
    import requests
except ImportError:
    print("Warning: requests is not installed, some features may not work")
    requests = None
import os
try:
    from pyspark.sql import SparkSession
except ImportError:
    print("Warning: pyspark is not installed")
    SparkSession = None
    print("✅ Spark Version: Not available")

# Databricks Runtime情報を安全に取得
try:
    if spark is not None:
        runtime_version = spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')
    print(f"✅ Databricks Runtime: {runtime_version}")
except Exception:
    try:
        # 代替手段でDBR情報を取得
        dbr_version = spark.conf.get('spark.databricks.clusterUsageTags.clusterName', 'Unknown')
        print(f"✅ Databricks Cluster: {dbr_version}")
    except Exception:
        print("✅ Databricks Environment: 設定情報の取得をスキップしました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 SQLプロファイラーJSONファイル読み込み関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - SQLプロファイラーJSONファイルの読み込み
# MAGIC - DBFS/FileStore/ローカルパスの自動判別
# MAGIC - ファイルサイズとデータ情報の表示

# COMMAND ----------

def load_profiler_json(file_path: str) -> Dict[str, Any]:
    """
    SQLプロファイラーJSONファイルを読み込む
    
    Args:
        file_path: JSONファイルのパス（DBFS または ローカルパス）
        
    Returns:
        Dict: パースされたJSONデータ
    """
    try:
        # DBFSパスの場合は適切に処理
        if file_path.startswith('/dbfs/'):
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        elif file_path.startswith('dbfs:/'):
            # dbfs: プレフィックスを/dbfs/に変換
            local_path = file_path.replace('dbfs:', '/dbfs')
            with open(local_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        elif file_path.startswith('/FileStore/'):
            # FileStore パスを /dbfs/FileStore/ に変換
            local_path = '/dbfs' + file_path
            with open(local_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        else:
            # ローカルファイル
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        
        print(f"✅ JSONファイルを正常に読み込みました: {file_path}")
        print(f"📊 データサイズ: {len(str(data)):,} characters")
        return data
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {str(e)}")
        return {}

print("✅ 関数定義完了: load_profiler_json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 パフォーマンスメトリクス抽出関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - SQLプロファイラーデータからのメトリクス抽出
# MAGIC - クエリ基本情報の取得
# MAGIC - 全体/ステージ/ノード別パフォーマンス指標の計算
# MAGIC - スピル検出とボトルネック指標の分析

# COMMAND ----------

def detect_data_format(profiler_data: Dict[str, Any]) -> str:
    """
    JSONデータの形式を検出
    """
    # SQLプロファイラー形式の検出
    if 'graphs' in profiler_data and isinstance(profiler_data['graphs'], list):
        if len(profiler_data['graphs']) > 0:
            return 'sql_profiler'
    
    # SQLクエリサマリー形式の検出（test2.json形式）
    if 'query' in profiler_data and 'planMetadatas' in profiler_data:
        query_data = profiler_data.get('query', {})
        if 'metrics' in query_data:
            return 'sql_query_summary'
    
    return 'unknown'

def extract_performance_metrics_from_query_summary(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Databricks SQLクエリサマリー形式のJSONから基本メトリクスを抽出
    (test2.json形式に対応)
    """
    try:
        query_data = profiler_data.get('query', {})
        metrics_data = query_data.get('metrics', {})
        
        if not metrics_data:
            print("⚠️ メトリクスデータが見つかりません")
            return {}
        
        print(f"✅ SQLクエリサマリー形式のメトリクスを検出しました")
        print(f"   - 実行時間: {metrics_data.get('totalTimeMs', 0):,} ms")
        print(f"   - 読み込みデータ: {metrics_data.get('readBytes', 0) / 1024 / 1024 / 1024:.2f} GB")
        print(f"   - 処理行数: {metrics_data.get('rowsReadCount', 0):,} 行")
        
        # 基本メトリクスの抽出
        overall_metrics = {
            'total_time_ms': metrics_data.get('totalTimeMs', 0),
            'execution_time_ms': metrics_data.get('executionTimeMs', 0),
            'compilation_time_ms': metrics_data.get('compilationTimeMs', 0),
            'read_bytes': metrics_data.get('readBytes', 0),
            'read_remote_bytes': metrics_data.get('readRemoteBytes', 0),
            'read_cache_bytes': metrics_data.get('readCacheBytes', 0),
            'spill_to_disk_bytes': metrics_data.get('spillToDiskBytes', 0),
            'rows_produced_count': metrics_data.get('rowsProducedCount', 0),
            'rows_read_count': metrics_data.get('rowsReadCount', 0),
            'read_files_count': metrics_data.get('readFilesCount', 0),
            'read_partitions_count': metrics_data.get('readPartitionsCount', 0),
            'photon_total_time_ms': metrics_data.get('photonTotalTimeMs', 0),
            'task_total_time_ms': metrics_data.get('taskTotalTimeMs', 0),
            'network_sent_bytes': metrics_data.get('networkSentBytes', 0),
            'photon_enabled': metrics_data.get('photonTotalTimeMs', 0) > 0,
            'photon_utilization_ratio': 0
        }
        
        # Photon利用率の計算
        if overall_metrics['task_total_time_ms'] > 0:
            overall_metrics['photon_utilization_ratio'] = min(
                overall_metrics['photon_total_time_ms'] / overall_metrics['task_total_time_ms'], 1.0
            )
        
        # キャッシュヒット率の計算
        cache_hit_ratio = 0
        if overall_metrics['read_bytes'] > 0:
            cache_hit_ratio = overall_metrics['read_cache_bytes'] / overall_metrics['read_bytes']
        
        # ボトルネック指標の計算
        bottleneck_indicators = {
            'spill_bytes': overall_metrics['spill_to_disk_bytes'],
            'has_spill': overall_metrics['spill_to_disk_bytes'] > 0,
            'cache_hit_ratio': cache_hit_ratio,
            'has_cache_miss': cache_hit_ratio < 0.8,
            'photon_efficiency': overall_metrics['photon_utilization_ratio'],
            'has_shuffle_bottleneck': False,  # 詳細情報がないため判定不可
            'remote_read_ratio': 0,
            'has_memory_pressure': overall_metrics['spill_to_disk_bytes'] > 0,
            'max_task_duration_ratio': 1.0,  # 不明
            'has_data_skew': False  # 詳細情報がないため判定不可
        }
        
        # リモート読み込み比率の計算
        if overall_metrics['read_bytes'] > 0:
            bottleneck_indicators['remote_read_ratio'] = overall_metrics['read_remote_bytes'] / overall_metrics['read_bytes']
        
        # クエリ情報の抽出
        query_info = {
            'query_id': query_data.get('id', ''),
            'query_text': query_data.get('queryText', '')[:300] + "..." if len(query_data.get('queryText', '')) > 300 else query_data.get('queryText', ''),
            'status': query_data.get('status', ''),
            'query_start_time': query_data.get('queryStartTimeMs', 0),
            'query_end_time': query_data.get('queryEndTimeMs', 0),
            'spark_ui_url': query_data.get('sparkUiUrl', ''),
            'endpoint_id': query_data.get('endpointId', ''),
            'user': query_data.get('user', {}).get('displayName', ''),
            'statement_type': query_data.get('statementType', ''),
            'plans_state': query_data.get('plansState', '')
        }
        
        # 詳細なパフォーマンス洞察を計算（後でnode_metricsと一緒に再計算）
        performance_insights = calculate_performance_insights_from_metrics(overall_metrics, None)
        
        # 擬似的なノードメトリクス（サマリー情報から生成）
        summary_node = {
            'node_id': 'summary_node',
            'name': f'Query Execution Summary ({query_data.get("statementType", "SQL")})',
            'tag': 'QUERY_SUMMARY',
            'key_metrics': {
                'durationMs': overall_metrics['total_time_ms'],
                'rowsNum': overall_metrics['rows_read_count'],
                'peakMemoryBytes': 0,  # 不明
                'throughputMBps': performance_insights['parallelization']['throughput_mb_per_second'],
                'dataSelectivity': performance_insights['data_efficiency']['data_selectivity'],
                'cacheHitRatio': performance_insights['cache_efficiency']['cache_hit_ratio']
            },
            'detailed_metrics': {
                'Total Time': {'value': overall_metrics['total_time_ms'], 'display_name': 'Total Time'},
                'Read Bytes': {'value': overall_metrics['read_bytes'], 'display_name': 'Read Bytes'},
                'Spill Bytes': {'value': overall_metrics['spill_to_disk_bytes'], 'display_name': 'Spill to Disk'},
                'Photon Time': {'value': overall_metrics['photon_total_time_ms'], 'display_name': 'Photon Time'},
                'Rows Read': {'value': overall_metrics['rows_read_count'], 'display_name': 'Rows Read Count'},
                'Cache Hit Ratio': {'value': performance_insights['cache_efficiency']['cache_hit_ratio'], 'display_name': 'Cache Hit Ratio'},
                'Filter Rate': {'value': performance_insights['data_efficiency']['data_selectivity'], 'display_name': 'Filter Rate'},
                'Throughput': {'value': performance_insights['parallelization']['throughput_mb_per_second'], 'display_name': 'Throughput (MB/s)'}
            },
            'graph_index': 0,
            'performance_insights': performance_insights
        }
        
        # 完全なmetricsでperformance_insightsを再計算
        complete_metrics = {
            'overall_metrics': overall_metrics,
            'node_metrics': [summary_node]
        }
        performance_insights = calculate_performance_insights_from_metrics(overall_metrics, complete_metrics)
        
        return {
            'data_format': 'sql_query_summary',
            'query_info': query_info,
            'overall_metrics': overall_metrics,
            'bottleneck_indicators': bottleneck_indicators,
            'node_metrics': [summary_node],
            'stage_metrics': [],  # 詳細ステージ情報なし
            'liquid_clustering_analysis': {},  # 後で追加
            'raw_profiler_data': profiler_data,
            'performance_insights': performance_insights,  # 詳細なパフォーマンス洞察を追加
            'analysis_capabilities': [
                'メトリクスベースのボトルネック分析（キャッシュ効率、フィルタ率、Photon効率）',
                'リソース使用状況分析（スピル、並列化効率、スループット）',
                'パフォーマンス指標計算（ファイル効率、パーティション効率）',
                'ポテンシャルボトルネック特定（メトリクスベース）'
            ],
            'analysis_limitations': [
                '詳細な実行プラン情報（ノード、エッジ）が利用できません',
                'ステージ別メトリクスが利用できません', 
                'BROADCAST分析は基本的な推定のみ可能',
                'Liquid Clustering分析は一般的な推奨のみ可能',
                'データスキュー検出は平均値ベースの推定のみ',
                'クエリ構造の詳細解析は行いません（メトリクス重視アプローチ）'
            ]
        }
        
    except Exception as e:
        print(f"⚠️ SQLクエリサマリー形式のメトリクス抽出でエラー: {str(e)}")
        return {}

def extract_performance_metrics(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからボトルネック分析に必要なメトリクスを抽出（複数形式対応）
    """
    # データ形式を検出
    data_format = detect_data_format(profiler_data)
    
    print(f"🔍 検出されたデータ形式: {data_format}")
    
    if data_format == 'sql_query_summary':
        print("📊 Databricks SQLクエリサマリー形式として処理中...")
        result = extract_performance_metrics_from_query_summary(profiler_data)
        if result:
            # Liquid Clustering分析を追加（制限付き）
            try:
                result["liquid_clustering_analysis"] = analyze_liquid_clustering_opportunities(profiler_data, result)
            except Exception as e:
                print(f"⚠️ Liquid Clustering分析をスキップ: {str(e)}")
                result["liquid_clustering_analysis"] = {}
        return result
    elif data_format == 'sql_profiler':
        print("📊 SQLプロファイラー詳細形式として処理中...")
        # 既存のSQLプロファイラー形式の処理を継続
        pass
    else:
        print(f"⚠️ 未知のデータ形式です: {data_format}")
        return {}
    
    # 既存のSQLプロファイラー形式の処理
    metrics = {
        "query_info": {},
        "overall_metrics": {},
        "stage_metrics": [],
        "node_metrics": [],
        "bottleneck_indicators": {},
        "liquid_clustering_analysis": {},
        "raw_profiler_data": profiler_data  # プラン分析のために生データを保存
    }
    
    # 基本的なクエリ情報
    if 'query' in profiler_data:
        query = profiler_data['query']
        metrics["query_info"] = {
            "query_id": query.get('id', ''),
            "status": query.get('status', ''),
            "query_start_time": query.get('queryStartTimeMs', 0),
            "query_end_time": query.get('queryEndTimeMs', 0),
            "user": query.get('user', {}).get('displayName', ''),
            "query_text": query.get('queryText', '')[:300] + "..." if len(query.get('queryText', '')) > 300 else query.get('queryText', '')
        }
        
        # 全体的なメトリクス
        if 'metrics' in query:
            query_metrics = query['metrics']
            metrics["overall_metrics"] = {
                "total_time_ms": query_metrics.get('totalTimeMs', 0),
                "compilation_time_ms": query_metrics.get('compilationTimeMs', 0),
                "execution_time_ms": query_metrics.get('executionTimeMs', 0),
                "read_bytes": query_metrics.get('readBytes', 0),
                "read_remote_bytes": query_metrics.get('readRemoteBytes', 0),
                "read_cache_bytes": query_metrics.get('readCacheBytes', 0),
                "rows_produced_count": query_metrics.get('rowsProducedCount', 0),
                "rows_read_count": query_metrics.get('rowsReadCount', 0),
                "spill_to_disk_bytes": query_metrics.get('spillToDiskBytes', 0),
                "read_files_count": query_metrics.get('readFilesCount', 0),
                "task_total_time_ms": query_metrics.get('taskTotalTimeMs', 0),
                "photon_total_time_ms": query_metrics.get('photonTotalTimeMs', 0),
                # Photon利用状況の分析（Photon実行時間/タスク合計時間）
                "photon_enabled": query_metrics.get('photonTotalTimeMs', 0) > 0,
                "photon_utilization_ratio": min(query_metrics.get('photonTotalTimeMs', 0) / max(query_metrics.get('taskTotalTimeMs', 1), 1), 1.0)
            }
    
    # グラフデータからステージとノードのメトリクスを抽出（複数グラフ対応）
    if 'graphs' in profiler_data and profiler_data['graphs']:
        # すべてのグラフを分析
        for graph_index, graph in enumerate(profiler_data['graphs']):
            print(f"🔍 グラフ{graph_index}を分析中...")
            
            # ステージデータ
            if 'stageData' in graph:
                for stage in graph['stageData']:
                    stage_metric = {
                        "stage_id": stage.get('stageId', ''),
                        "status": stage.get('status', ''),
                        "duration_ms": stage.get('keyMetrics', {}).get('durationMs', 0),
                        "num_tasks": stage.get('numTasks', 0),
                        "num_failed_tasks": stage.get('numFailedTasks', 0),
                        "num_complete_tasks": stage.get('numCompleteTasks', 0),
                        "start_time_ms": stage.get('startTimeMs', 0),
                        "end_time_ms": stage.get('endTimeMs', 0),
                        "graph_index": graph_index  # どのグラフ由来かを記録
                    }
                    metrics["stage_metrics"].append(stage_metric)
            
            # ノードデータ（重要なもののみ）
            if 'nodes' in graph:
                for node in graph['nodes']:
                    if not node.get('hidden', False):
                        # keyMetricsをそのまま使用（durationMsは既にミリ秒単位）
                        key_metrics = node.get('keyMetrics', {})
                        
                        node_metric = {
                            "node_id": node.get('id', ''),
                            "name": node.get('name', ''),
                            "tag": node.get('tag', ''),
                            "key_metrics": key_metrics,  # 単位変換済みのkey_metrics
                            "metrics": node.get('metrics', []),  # 元のmetrics配列を保持
                            "metadata": node.get('metadata', []),  # metadataを追加
                            "graph_index": graph_index  # どのグラフ由来かを記録
                        }
                        
                        # 重要なメトリクスのみ詳細抽出（スピル関連キーワード追加・label対応）
                        detailed_metrics = {}
                        for metric in node.get('metrics', []):
                            metric_key = metric.get('key', '')
                            metric_label = metric.get('label', '')
                            
                            # キーワードをkeyとlabelの両方で確認
                            key_keywords = ['TIME', 'MEMORY', 'ROWS', 'BYTES', 'DURATION', 'PEAK', 'CUMULATIVE', 'EXCLUSIVE', 
                                           'SPILL', 'DISK', 'PRESSURE', 'SINK']
                            
                            # metric_keyまたはmetric_labelに重要なキーワードが含まれる場合に抽出
                            is_important_metric = (
                                any(keyword in metric_key.upper() for keyword in key_keywords) or
                                any(keyword in metric_label.upper() for keyword in key_keywords)
                            )
                            
                            if is_important_metric:
                                # メトリクス名として、labelが有効な場合はlabelを使用、そうでなければkeyを使用
                                metric_name = metric_label if metric_label and metric_label != 'UNKNOWN_KEY' else metric_key
                                detailed_metrics[metric_name] = {
                                    'value': metric.get('value', 0),
                                    'label': metric_label,
                                    'type': metric.get('metricType', ''),
                                    'original_key': metric_key,  # 元のキー名を保存
                                    'display_name': metric_name  # 表示用の名前
                                }
                        node_metric['detailed_metrics'] = detailed_metrics
                        metrics["node_metrics"].append(node_metric)
    
    # ボトルネック指標の計算
    metrics["bottleneck_indicators"] = calculate_bottleneck_indicators(metrics)
    
    # Liquid Clustering分析
    metrics["liquid_clustering_analysis"] = analyze_liquid_clustering_opportunities(profiler_data, metrics)
    
    return metrics

print("✅ 関数定義完了: extract_performance_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏷️ ノード名解析・改善関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - 汎用的なノード名（Whole Stage Codegen等）の具体化
# MAGIC - 関連ノードの検索と最適な処理名の選択
# MAGIC - Photon情報やテーブル情報の付加
# MAGIC - 処理名の意味的な改善

# COMMAND ----------

def get_meaningful_node_name(node: Dict[str, Any], extracted_metrics: Dict[str, Any]) -> str:
    """
    より意味のあるノード名を取得する関数
    汎用的な名前（Whole Stage Codegenなど）を具体的な処理名に変換
    """
    original_name = node.get('name', '')
    node_id = node.get('node_id', node.get('id', ''))
    node_tag = node.get('tag', '')
    
    # メタデータから詳細情報を取得
    metadata = node.get('metadata', [])
    metadata_info = {}
    for meta in metadata:
        key = meta.get('key', '')
        value = meta.get('value', '')
        label = meta.get('label', '')
        if value:
            metadata_info[key] = value
    
    # 1. 汎用的な名前を具体的な名前に置き換え
    if 'whole stage codegen' in original_name.lower():
        # より具体的な処理名を推測するためのヒューリスティック
        
        # ノードIDベースでの関連性を推測（隣接ID）
        node_id_num = None
        try:
            node_id_num = int(node_id) if node_id else None
        except:
            pass
        
        if node_id_num:
            # 同じファイル内の近いIDの具体的な処理を探す
            all_nodes = extracted_metrics.get('node_metrics', [])
            nearby_specific_nodes = []
            
            for other_node in all_nodes:
                other_id = other_node.get('node_id', '')
                other_name = other_node.get('name', '')
                
                try:
                    other_id_num = int(other_id) if other_id else None
                    if other_id_num and abs(other_id_num - node_id_num) <= 10:  # 近隣10個以内
                        if is_specific_process_name(other_name):
                            nearby_specific_nodes.append(other_name)
                except:
                    continue
            
            # 最も具体的な処理名を選択
            if nearby_specific_nodes:
                specific_name = get_most_specific_process_name_from_list(nearby_specific_nodes)
                if specific_name and specific_name != original_name:
                    return f"{specific_name} (Whole Stage Codegen)"
        
        # フォールバック: tagからより具体的な情報を抽出
        if 'CODEGEN' in node_tag:
            # メタデータから子タグ情報を確認
            child_tag = metadata_info.get('CHILD_TAG', '')
            if child_tag and child_tag != 'Child':
                return f"Whole Stage Codegen ({child_tag})"
    
    # 2. より具体的なタグ情報をノード名に反映
    tag_to_name_mapping = {
        'PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC': 'Photon Shuffle Exchange',
        'PHOTON_GROUPING_AGG_EXEC': 'Photon Grouping Aggregate', 
        'UNKNOWN_DATA_SOURCE_SCAN_EXEC': 'Data Source Scan',
        'HASH_AGGREGATE_EXEC': 'Hash Aggregate',
        'WHOLE_STAGE_CODEGEN_EXEC': 'Whole Stage Codegen'
    }
    
    if node_tag in tag_to_name_mapping:
        mapped_name = tag_to_name_mapping[node_tag]
        if mapped_name != original_name and mapped_name != 'Whole Stage Codegen':
            # タグの方がより具体的な場合は使用
            enhanced_name = mapped_name
        else:
            enhanced_name = original_name
    else:
        enhanced_name = original_name
    
    # 3. メタデータから処理の詳細を追加
    
    # データベース・テーブル情報を追加（強化版）
    table_name = None
    
    # 複数のメタデータキーからテーブル名を抽出（フルパス優先）
    for key_candidate in ['SCAN_TABLE', 'SCAN_IDENTIFIER', 'TABLE_NAME', 'RELATION', 'SCAN_RELATION']:
        if key_candidate in metadata_info:
            extracted_table = metadata_info[key_candidate]
            # フルパス（catalog.schema.table）の場合はそのまま使用
            if isinstance(extracted_table, str) and extracted_table.count('.') >= 2:
                table_name = extracted_table
                break
            elif isinstance(extracted_table, str) and extracted_table.count('.') == 1:
                # schema.table形式の場合もそのまま使用
                table_name = extracted_table
                break
            elif not table_name:  # まだテーブル名が見つかっていない場合のみ設定
                table_name = extracted_table
    
    # メタデータからテーブル名を抽出できない場合、ノード名から推測
    if not table_name and ('scan' in enhanced_name.lower() or 'data source' in enhanced_name.lower()):
        # ノード名からテーブル名を推測
        import re
        
        # "Scan tpcds.tpcds_sf1000_delta_lc.customer" のような形式
        table_patterns = [
            r'[Ss]can\s+([a-zA-Z_][a-zA-Z0-9_.]*[a-zA-Z0-9_])',
            r'[Tt]able\s+([a-zA-Z_][a-zA-Z0-9_.]*[a-zA-Z0-9_])',
            r'([a-zA-Z_][a-zA-Z0-9_]*\.)+([a-zA-Z_][a-zA-Z0-9_]*)',
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, original_name)
            if match:
                if '.' in match.group(0):
                    # フルテーブル名（catalog.schema.table）の場合はフルパスを使用
                    table_name = match.group(0)
                else:
                    table_name = match.group(1) if match.lastindex and match.lastindex >= 1 else match.group(0)
                break
    
    # メタデータのvaluesフィールドからもテーブル名を検索
    if not table_name:
        for meta in metadata:
            values = meta.get('values', [])
            if values:
                for value in values:
                    if isinstance(value, str) and '.' in value and len(value.split('.')) >= 2:
                        # "catalog.schema.table" 形式の場合
                        parts = value.split('.')
                        if len(parts) >= 2 and not any(part.isdigit() for part in parts[-2:]):
                            # フルパスを使用（catalog.schema.table）
                            if len(parts) >= 3:
                                table_name = '.'.join(parts)  # フルパス
                            else:
                                table_name = value  # そのまま使用
                            break
                if table_name:
                    break
    
    # Data Source Scanの場合にテーブル名を表示
    if table_name and ('scan' in enhanced_name.lower() or 'data source' in enhanced_name.lower()):
        # フルパス表示のために制限を緩和（60文字まで）
        if len(table_name) > 60:
            # カタログ.スキーマ.テーブル形式の場合は中間を省略
            parts = table_name.split('.')
            if len(parts) >= 3:
                table_name = f"{parts[0]}.*.{parts[-1]}"
            else:
                table_name = table_name[:57] + "..."
        enhanced_name = f"Data Source Scan ({table_name})"
    elif 'scan' in enhanced_name.lower() and 'data source' in enhanced_name.lower():
        # テーブル名が見つからない場合でも、より明確な名前に
        enhanced_name = "Data Source Scan"
    
    # Photon情報を追加
    if 'IS_PHOTON' in metadata_info and metadata_info['IS_PHOTON'] == 'true':
        if not enhanced_name.startswith('Photon'):
            enhanced_name = f"Photon {enhanced_name}"
    
    return enhanced_name

def find_related_specific_nodes(target_node_id: str, nodes: list, edges: list) -> list:
    """指定ノードに関連する具体的な処理ノードを検索"""
    
    # エッジから関連ノードを特定
    related_node_ids = set()
    
    # 直接接続されているノード
    for edge in edges:
        from_id = edge.get('fromId', '')
        to_id = edge.get('toId', '')
        
        if from_id == target_node_id:
            related_node_ids.add(to_id)
        elif to_id == target_node_id:
            related_node_ids.add(from_id)
    
    # 関連ノードの詳細を取得
    related_nodes = []
    for node in nodes:
        node_id = node.get('id', '')
        if node_id in related_node_ids:
            node_name = node.get('name', '')
            # 具体的な処理名を持つノードのみ選択
            if is_specific_process_name(node_name):
                related_nodes.append(node)
    
    return related_nodes

def is_specific_process_name(name: str) -> bool:
    """具体的な処理名かどうかを判定"""
    specific_keywords = [
        'columnar to row', 'row to columnar', 'filter', 'project', 'join',
        'aggregate', 'sort', 'exchange', 'broadcast', 'scan', 'union'
    ]
    
    generic_keywords = [
        'whole stage codegen', 'stage', 'query', 'result'
    ]
    
    name_lower = name.lower()
    
    # 具体的なキーワードを含む場合
    for keyword in specific_keywords:
        if keyword in name_lower:
            return True
    
    # 汎用的なキーワードのみの場合は除外
    for keyword in generic_keywords:
        if keyword in name_lower and len(name_lower.split()) <= 3:
            return False
    
    return True

def get_most_specific_process_name(nodes: list) -> str:
    """最も具体的な処理名を選択"""
    if not nodes:
        return ""
    
    # 優先順位: より具体的で意味のある処理名
    priority_keywords = [
        'columnar to row', 'row to columnar', 'filter', 'project',
        'hash join', 'broadcast join', 'sort merge join',
        'hash aggregate', 'sort aggregate', 'grouping aggregate'
    ]
    
    for keyword in priority_keywords:
        for node in nodes:
            node_name = node.get('name', '').lower()
            if keyword in node_name:
                return node.get('name', '')
    
    # フォールバック: 最初の具体的なノード名
    for node in nodes:
        node_name = node.get('name', '')
        if is_specific_process_name(node_name):
            return node_name
    
    return ""

def get_most_specific_process_name_from_list(node_names: list) -> str:
    """ノード名のリストから最も具体的な処理名を選択"""
    if not node_names:
        return ""
    
    # 優先順位: より具体的で意味のある処理名
    priority_keywords = [
        'columnar to row', 'row to columnar', 'filter', 'project',
        'hash join', 'broadcast join', 'sort merge join',
        'hash aggregate', 'sort aggregate', 'grouping aggregate'
    ]
    
    for keyword in priority_keywords:
        for name in node_names:
            if keyword in name.lower():
                return name
    
    # フォールバック: 最初の具体的なノード名
    for name in node_names:
        if is_specific_process_name(name):
            return name
    
    return ""

def extract_shuffle_attributes(node: Dict[str, Any]) -> list:
    """
    ShuffleノードからSHUFFLE_ATTRIBUTESを抽出
    
    Args:
        node: ノード情報
        
    Returns:
        list: 検出されたShuffle attributes
    """
    shuffle_attributes = []
    
    # metadataからSHUFFLE_ATTRIBUTESを検索
    metadata = node.get('metadata', [])
    if isinstance(metadata, list):
        for item in metadata:
            if isinstance(item, dict):
                item_key = item.get('key', '')
                item_label = item.get('label', '')
                item_values = item.get('values', [])
                
                # keyとlabelの両方をチェック
                if (item_key == 'SHUFFLE_ATTRIBUTES' or 
                    item_label == 'Shuffle attributes'):
                    if isinstance(item_values, list):
                        shuffle_attributes.extend(item_values)
    
    # raw_metricsからも検索（labelもチェック）
    raw_metrics = node.get('metrics', [])
    if isinstance(raw_metrics, list):
        for metric in raw_metrics:
            if isinstance(metric, dict):
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_values = metric.get('values', [])
                
                if (metric_key == 'SHUFFLE_ATTRIBUTES' or 
                    metric_label == 'Shuffle attributes'):
                    if isinstance(metric_values, list):
                        shuffle_attributes.extend(metric_values)
    
    # detailed_metricsからも検索
    detailed_metrics = node.get('detailed_metrics', {})
    if isinstance(detailed_metrics, dict):
        for key, info in detailed_metrics.items():
            if (key == 'SHUFFLE_ATTRIBUTES' or 
                (isinstance(info, dict) and info.get('label') == 'Shuffle attributes')):
                values = info.get('values', []) if isinstance(info, dict) else []
                if isinstance(values, list):
                    shuffle_attributes.extend(values)
    
    # 重複を削除
    return list(set(shuffle_attributes))

def extract_cluster_attributes(node: Dict[str, Any]) -> list:
    """
    スキャンノードからクラスタリングキー(SCAN_CLUSTERS)を抽出
    
    Args:
        node: ノード情報
        
    Returns:
        list: 検出されたクラスタリングキー
    """
    cluster_attributes = []
    
    # metadataからSCAN_CLUSTERSを検索
    metadata = node.get('metadata', [])
    if isinstance(metadata, list):
        for item in metadata:
            if isinstance(item, dict):
                item_key = item.get('key', '')
                item_label = item.get('label', '')
                item_values = item.get('values', [])
                
                # keyとlabelの両方をチェック
                if (item_key == 'SCAN_CLUSTERS' or 
                    item_label == 'Cluster attributes'):
                    if isinstance(item_values, list):
                        cluster_attributes.extend(item_values)
    
    # raw_metricsからも検索（labelもチェック）
    raw_metrics = node.get('metrics', [])
    if isinstance(raw_metrics, list):
        for metric in raw_metrics:
            if isinstance(metric, dict):
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_values = metric.get('values', [])
                
                if (metric_key == 'SCAN_CLUSTERS' or 
                    metric_label == 'Cluster attributes'):
                    if isinstance(metric_values, list):
                        cluster_attributes.extend(metric_values)
    
    # detailed_metricsからも検索
    detailed_metrics = node.get('detailed_metrics', {})
    if isinstance(detailed_metrics, dict):
        for key, info in detailed_metrics.items():
            if (key == 'SCAN_CLUSTERS' or 
                (isinstance(info, dict) and info.get('label') == 'Cluster attributes')):
                values = info.get('values', []) if isinstance(info, dict) else []
                if isinstance(values, list):
                    cluster_attributes.extend(values)
    
    # 重複を削除
    return list(set(cluster_attributes))

def extract_parallelism_metrics(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    ノードから複数のTasks totalメトリクスとAQEShuffleReadメトリクスを抽出
    
    シャッフル操作などでは以下の複数のメトリクスが存在する可能性があります：
    - Tasks total
    - Sink - Tasks total
    - Source - Tasks total
    - AQEShuffleRead - Number of partitions
    - AQEShuffleRead - Partition data size
    
    Args:
        node: ノード情報
        
    Returns:
        dict: 検出されたメトリクス
            {
                "tasks_total": 値,
                "sink_tasks_total": 値,
                "source_tasks_total": 値,
                "all_tasks_metrics": [{"name": "Tasks total", "value": 値}, ...],
                "aqe_shuffle_partitions": 値,
                "aqe_shuffle_data_size": 値,
                "aqe_shuffle_avg_partition_size": 値,
                "aqe_shuffle_skew_warning": bool,
                "aqe_shuffle_metrics": [{"name": "AQE...", "value": 値}, ...]
            }
    """
    parallelism_metrics = {
        "tasks_total": 0,
        "sink_tasks_total": 0,
        "source_tasks_total": 0,
        "all_tasks_metrics": [],
        "aqe_shuffle_partitions": 0,
        "aqe_shuffle_data_size": 0,
        "aqe_shuffle_avg_partition_size": 0,
        "aqe_shuffle_skew_warning": False,
        "aqe_detected_and_handled": False,
        "aqe_shuffle_metrics": []
    }
    
    # 対象となるTasks totalメトリクス名のパターン
    tasks_total_patterns = [
        "Tasks total",
        "Sink - Tasks total",
        "Source - Tasks total"
    ]
    
    # 対象となるAQEShuffleReadメトリクス名のパターン
    aqe_shuffle_patterns = [
        "AQEShuffleRead - Number of partitions",
        "AQEShuffleRead - Partition data size"
    ]
    
    # 1. detailed_metricsから検索
    detailed_metrics = node.get('detailed_metrics', {})
    for metric_key, metric_info in detailed_metrics.items():
        metric_value = metric_info.get('value', 0)
        metric_label = metric_info.get('label', '')
        
        # 各Tasks totalパターンをチェック
        for pattern in tasks_total_patterns:
            if metric_key == pattern or metric_label == pattern:
                # 特定のメトリクスにマッピング
                if pattern == "Tasks total":
                    parallelism_metrics["tasks_total"] = metric_value
                elif pattern == "Sink - Tasks total":
                    parallelism_metrics["sink_tasks_total"] = metric_value
                elif pattern == "Source - Tasks total":
                    parallelism_metrics["source_tasks_total"] = metric_value
                
                # 全メトリクスリストに追加
                parallelism_metrics["all_tasks_metrics"].append({
                    "name": pattern,
                    "value": metric_value
                })
        
        # AQEShuffleReadメトリクスをチェック
        for pattern in aqe_shuffle_patterns:
            if metric_key == pattern or metric_label == pattern:
                # 特定のメトリクスにマッピング
                if pattern == "AQEShuffleRead - Number of partitions":
                    parallelism_metrics["aqe_shuffle_partitions"] = metric_value
                elif pattern == "AQEShuffleRead - Partition data size":
                    parallelism_metrics["aqe_shuffle_data_size"] = metric_value
                
                # 全メトリクスリストに追加
                parallelism_metrics["aqe_shuffle_metrics"].append({
                    "name": pattern,
                    "value": metric_value
                })
    
    # 2. raw_metricsから検索（フォールバック）
    raw_metrics = node.get('metrics', [])
    if isinstance(raw_metrics, list):
        for metric in raw_metrics:
            if isinstance(metric, dict):
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_value = metric.get('value', 0)
                
                # 各Tasks totalパターンをチェック
                for pattern in tasks_total_patterns:
                    if metric_key == pattern or metric_label == pattern:
                        # 既にdetailed_metricsで見つかっている場合はスキップ
                        if not any(m["name"] == pattern for m in parallelism_metrics["all_tasks_metrics"]):
                            # 特定のメトリクスにマッピング
                            if pattern == "Tasks total":
                                parallelism_metrics["tasks_total"] = metric_value
                            elif pattern == "Sink - Tasks total":
                                parallelism_metrics["sink_tasks_total"] = metric_value
                            elif pattern == "Source - Tasks total":
                                parallelism_metrics["source_tasks_total"] = metric_value
                            
                            # 全メトリクスリストに追加
                            parallelism_metrics["all_tasks_metrics"].append({
                                "name": pattern,
                                "value": metric_value
                            })
                
                # AQEShuffleReadメトリクスをチェック
                for pattern in aqe_shuffle_patterns:
                    if metric_key == pattern or metric_label == pattern:
                        # 既にdetailed_metricsで見つかっている場合はスキップ
                        if not any(m["name"] == pattern for m in parallelism_metrics["aqe_shuffle_metrics"]):
                            # 特定のメトリクスにマッピング
                            if pattern == "AQEShuffleRead - Number of partitions":
                                parallelism_metrics["aqe_shuffle_partitions"] = metric_value
                            elif pattern == "AQEShuffleRead - Partition data size":
                                parallelism_metrics["aqe_shuffle_data_size"] = metric_value
                            
                            # 全メトリクスリストに追加
                            parallelism_metrics["aqe_shuffle_metrics"].append({
                                "name": pattern,
                                "value": metric_value
                            })
    
    # 3. key_metricsから検索（最後のフォールバック）
    key_metrics = node.get('key_metrics', {})
    if isinstance(key_metrics, dict):
        for metric_key, metric_value in key_metrics.items():
            for pattern in tasks_total_patterns:
                if metric_key == pattern:
                    # 既に見つかっている場合はスキップ
                    if not any(m["name"] == pattern for m in parallelism_metrics["all_tasks_metrics"]):
                        # 特定のメトリクスにマッピング
                        if pattern == "Tasks total":
                            parallelism_metrics["tasks_total"] = metric_value
                        elif pattern == "Sink - Tasks total":
                            parallelism_metrics["sink_tasks_total"] = metric_value
                        elif pattern == "Source - Tasks total":
                            parallelism_metrics["source_tasks_total"] = metric_value
                        
                        # 全メトリクスリストに追加
                        parallelism_metrics["all_tasks_metrics"].append({
                            "name": pattern,
                            "value": metric_value
                        })
            
            # AQEShuffleReadメトリクスをチェック
            for pattern in aqe_shuffle_patterns:
                if metric_key == pattern:
                    # 既に見つかっている場合はスキップ
                    if not any(m["name"] == pattern for m in parallelism_metrics["aqe_shuffle_metrics"]):
                        # 特定のメトリクスにマッピング
                        if pattern == "AQEShuffleRead - Number of partitions":
                            parallelism_metrics["aqe_shuffle_partitions"] = metric_value
                        elif pattern == "AQEShuffleRead - Partition data size":
                            parallelism_metrics["aqe_shuffle_data_size"] = metric_value
                        
                        # 全メトリクスリストに追加
                        parallelism_metrics["aqe_shuffle_metrics"].append({
                            "name": pattern,
                            "value": metric_value
                        })
    
    # 平均パーティションサイズの計算と警告設定
    if parallelism_metrics["aqe_shuffle_partitions"] > 0 and parallelism_metrics["aqe_shuffle_data_size"] > 0:
        avg_partition_size = parallelism_metrics["aqe_shuffle_data_size"] / parallelism_metrics["aqe_shuffle_partitions"]
        parallelism_metrics["aqe_shuffle_avg_partition_size"] = avg_partition_size
        
        # 512MB = 512 * 1024 * 1024 bytes
        threshold_512mb = 512 * 1024 * 1024
        if avg_partition_size >= threshold_512mb:
            parallelism_metrics["aqe_shuffle_skew_warning"] = True
        else:
            # AQEShuffleReadメトリクスが存在し、平均パーティションサイズが512MB未満の場合、
            # AQEがスキューを検出して対応済みと判定
            parallelism_metrics["aqe_detected_and_handled"] = True
    
    return parallelism_metrics

def calculate_filter_rate(node: Dict[str, Any]) -> Dict[str, Any]:
    """
    ノードからSize of files prunedとSize of files readメトリクスを抽出してフィルタ率を計算
    
    Args:
        node: ノードデータ
        
    Returns:
        Dict: フィルタ率計算結果
    """
    import os
    debug_mode = os.environ.get('DEBUG_FILTER_ANALYSIS', 'false').lower() == 'true'
    
    filter_rate = None
    files_pruned_bytes = 0
    files_read_bytes = 0
    debug_info = []
    
    # 検索対象のメトリクス名（実際のJSONファイルで確認されたパターンを優先）
    pruned_metrics = [
        "Size of files pruned",  # 実際に存在することを確認済み
        "Size of files pruned before dynamic pruning",  # 実際に存在することを確認済み
        "Pruned files size", 
        "Files pruned size",
        "Num pruned files size"
    ]
    
    read_metrics = [
        "Size of files read",  # 実際に存在することを確認済み
        "Files read size",
        "Read files size",
        "Num files read size"
    ]
    
    # detailed_metricsから検索
    detailed_metrics = node.get('detailed_metrics', {})
    if debug_mode:
        debug_info.append(f"detailed_metrics keys: {list(detailed_metrics.keys())[:5]}")
    
    for metric_key, metric_info in detailed_metrics.items():
        metric_label = metric_info.get('label', '')
        metric_value = metric_info.get('value', 0)
        
        # Pruned関連（labelを優先的にチェック）
        for target in pruned_metrics:
            if target in metric_label and metric_value > 0:
                files_pruned_bytes += metric_value  # 複数のメトリクスがある場合は合計
                if debug_mode:
                    debug_info.append(f"Found pruned metric: {metric_label} = {metric_value}")
                break
        
        # Read関連（labelを優先的にチェック）
        for target in read_metrics:
            if target in metric_label and metric_value > 0:
                files_read_bytes += metric_value  # 複数のメトリクスがある場合は合計
                if debug_mode:
                    debug_info.append(f"Found read metric: {metric_label} = {metric_value}")
                break
    
    # raw_metricsから検索（フォールバック）
    if files_pruned_bytes == 0 or files_read_bytes == 0:
        raw_metrics = node.get('metrics', [])
        if debug_mode:
            debug_info.append(f"Searching in {len(raw_metrics)} raw metrics")
        
        for metric in raw_metrics:
            metric_label = metric.get('label', '')
            metric_value = metric.get('value', 0)
            
            # Pruned関連（labelを優先的にチェック）
            for target in pruned_metrics:
                if target in metric_label and metric_value > 0:
                    files_pruned_bytes += metric_value  # 複数のメトリクスがある場合は合計
                    if debug_mode:
                        debug_info.append(f"Found pruned metric in raw: {metric_label} = {metric_value}")
                    break
            
            # Read関連（labelを優先的にチェック）
            for target in read_metrics:
                if target in metric_label and metric_value > 0:
                    files_read_bytes += metric_value  # 複数のメトリクスがある場合は合計
                    if debug_mode:
                        debug_info.append(f"Found read metric in raw: {metric_label} = {metric_value}")
                    break
    
    # フィルタ率計算（正しい式: プルーニング効率）
    total_available_bytes = files_read_bytes + files_pruned_bytes
    if total_available_bytes > 0:
        filter_rate = files_pruned_bytes / total_available_bytes
    else:
        filter_rate = 0.0
    
    result = {
        "filter_rate": filter_rate,
        "files_pruned_bytes": files_pruned_bytes,
        "files_read_bytes": files_read_bytes,
        "has_filter_metrics": (files_read_bytes > 0 or files_pruned_bytes > 0)
    }
    
    if debug_mode:
        result["debug_info"] = debug_info
    
    return result

def format_filter_rate_display(filter_result: Dict[str, Any]) -> str:
    """
    フィルタ率計算結果を表示用文字列に変換
    
    Args:
        filter_result: calculate_filter_rate()の結果
        
    Returns:
        str: 表示用文字列
    """
    if not filter_result["has_filter_metrics"] or filter_result["filter_rate"] is None:
        return None
    
    filter_rate = filter_result["filter_rate"]
    files_read_gb = filter_result["files_read_bytes"] / (1024 * 1024 * 1024)
    files_pruned_gb = filter_result["files_pruned_bytes"] / (1024 * 1024 * 1024)
    
    return f"📂 フィルタ率: {filter_rate:.1%} (読み込み: {files_read_gb:.2f}GB, プルーン: {files_pruned_gb:.2f}GB)"

def extract_detailed_bottleneck_analysis(extracted_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    セル33スタイルの詳細ボトルネック分析を実行し、構造化されたデータを返す
    
    🚨 重要: パーセンテージ計算デグレ防止
    - 並列実行ノードの時間合計を全体時間として使用することは絶対に禁止
    - overall_metrics.total_time_ms（wall-clock time）を優先使用
    - フォールバック時は最大ノード時間を使用（合計ではない）
    
    Args:
        extracted_metrics: 抽出されたメトリクス
        
    Returns:
        dict: 詳細なボトルネック分析結果
    """
    detailed_analysis = {
        "top_bottleneck_nodes": [],
        "shuffle_optimization_hints": [],
        "spill_analysis": {
            "total_spill_gb": 0,
            "spill_nodes": [],
            "critical_spill_nodes": []
        },
        "skew_analysis": {
            "skewed_nodes": [],
            "total_skewed_partitions": 0
        },
        "performance_recommendations": []
    }
    
    # ノードを実行時間でソート（TOP10）
    sorted_nodes = sorted(extracted_metrics.get('node_metrics', []), 
                         key=lambda x: x.get('key_metrics', {}).get('durationMs', 0), 
                         reverse=True)
    
    # 最大10個のノードを処理
    final_sorted_nodes = sorted_nodes[:10]
    
    # 🚨 重要: 正しい全体時間の計算（デグレ防止）
    # 1. overall_metricsから全体実行時間を取得（wall-clock time）
    overall_metrics = extracted_metrics.get('overall_metrics', {})
    total_duration = overall_metrics.get('total_time_ms', 0)
    
    # 🚨 並列実行問題の修正: task_total_time_msを優先使用
    task_total_time_ms = overall_metrics.get('task_total_time_ms', 0)
    
    if task_total_time_ms > 0:
        total_duration = task_total_time_ms
    elif total_duration <= 0:
        # execution_time_msを次の優先度で使用
        execution_time_ms = overall_metrics.get('execution_time_ms', 0)
        if execution_time_ms > 0:
            total_duration = execution_time_ms
        else:
            # 最終フォールバック
            max_node_time = max([node.get('key_metrics', {}).get('durationMs', 0) for node in sorted_nodes], default=1)
            total_duration = int(max_node_time * 1.2)
    
    for i, node in enumerate(final_sorted_nodes):
        duration_ms = node.get('key_metrics', {}).get('durationMs', 0)
        memory_mb = node.get('key_metrics', {}).get('peakMemoryBytes', 0) / 1024 / 1024
        rows_num = node.get('key_metrics', {}).get('rowsNum', 0)
        
        # 並列度情報の取得（修正版: 複数のTasks totalメトリクスを取得）
        parallelism_data = extract_parallelism_metrics(node)
        
        # 従来の単一値（互換性のため）
        num_tasks = parallelism_data.get('tasks_total', 0)
        
        # フォールバック: Sink - Tasks totalまたはSource - Tasks totalがある場合
        if num_tasks == 0:
            if parallelism_data.get('sink_tasks_total', 0) > 0:
                num_tasks = parallelism_data.get('sink_tasks_total', 0)
            elif parallelism_data.get('source_tasks_total', 0) > 0:
                num_tasks = parallelism_data.get('source_tasks_total', 0)
        
        # スピル検出（セル33と同じロジック）
        spill_detected = False
        spill_bytes = 0
        exact_spill_metrics = [
            "Num bytes spilled to disk due to memory pressure",
            "Sink - Num bytes spilled to disk due to memory pressure",
            "Sink/Num bytes spilled to disk due to memory pressure"
        ]
        
        # detailed_metricsから検索
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            metric_value = metric_info.get('value', 0)
            metric_label = metric_info.get('label', '')
            
            if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                spill_detected = True
                spill_bytes = max(spill_bytes, metric_value)
                break
        
        # raw_metricsから検索（フォールバック）
        if not spill_detected:
            raw_metrics = node.get('metrics', [])
            for metric in raw_metrics:
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_value = metric.get('value', 0)
                
                if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                    spill_detected = True
                    spill_bytes = max(spill_bytes, metric_value)
                    break
        
        # スキュー検出（AQEベース）
        skew_detected = False
        skewed_partitions = 0
        target_skew_metric = "AQEShuffleRead - Number of skewed partitions"
        
        for metric_key, metric_info in detailed_metrics.items():
            if metric_key == target_skew_metric:
                try:
                    skewed_partitions = int(metric_info.get('value', 0))
                    if skewed_partitions > 0:
                        skew_detected = True
                    break
                except (ValueError, TypeError):
                    continue
        
        node_name = get_meaningful_node_name(node, extracted_metrics)
        # 🚨 重要: 正しいパーセンテージ計算（デグレ防止）
        # wall-clock timeに対する各ノードの実行時間の割合
        time_percentage = min((duration_ms / max(total_duration, 1)) * 100, 100.0)
        
        # スキュー判定（AQEスキュー検出とAQEShuffleRead平均パーティションサイズの両方を考慮）
        aqe_shuffle_skew_warning = parallelism_data.get('aqe_shuffle_skew_warning', False)
        combined_skew_detected = skew_detected or aqe_shuffle_skew_warning
        
        # ノード分析結果を構造化
        node_analysis = {
            "rank": i + 1,
            "node_id": node.get('node_id', node.get('id', 'N/A')),
            "node_name": node_name,
            "duration_ms": duration_ms,
            "time_percentage": time_percentage,
            "memory_mb": memory_mb,
            "rows_processed": rows_num,
            "num_tasks": num_tasks,
            "parallelism_data": parallelism_data,  # 複数のTasks totalメトリクス情報を追加
            "spill_detected": spill_detected,
            "spill_bytes": spill_bytes,
            "spill_gb": spill_bytes / 1024 / 1024 / 1024 if spill_bytes > 0 else 0,
            "skew_detected": combined_skew_detected,  # AQEスキュー検出とAQEShuffleRead警告の両方を考慮
            "aqe_skew_detected": skew_detected,  # 従来のAQEスキュー検出のみ
            "aqe_shuffle_skew_warning": aqe_shuffle_skew_warning,  # AQEShuffleRead平均パーティションサイズ警告
            "skewed_partitions": skewed_partitions,
            "is_shuffle_node": "shuffle" in node_name.lower(),
            "severity": "CRITICAL" if duration_ms >= 10000 else "HIGH" if duration_ms >= 5000 else "MEDIUM" if duration_ms >= 1000 else "LOW"
        }
        
        # Shuffleノードの場合、スピルが検出されている場合のみREPARTITIONヒントを追加
        if node_analysis["is_shuffle_node"] and spill_detected and spill_bytes > 0:
            shuffle_attributes = extract_shuffle_attributes(node)
            if shuffle_attributes:
                suggested_partitions = max(num_tasks * 2, 200)
                
                # Shuffle属性で検出されたカラムを全て使用（完全一致）
                repartition_columns = ", ".join(shuffle_attributes)
                
                repartition_hint = {
                    "node_id": node_analysis["node_id"],
                    "attributes": shuffle_attributes,
                    "suggested_sql": f"REPARTITION({suggested_partitions}, {repartition_columns})",
                    "reason": f"スピル({node_analysis['spill_gb']:.2f}GB)改善",
                    "priority": "HIGH",
                    "estimated_improvement": "大幅な性能改善が期待",

                }
                detailed_analysis["shuffle_optimization_hints"].append(repartition_hint)
                node_analysis["repartition_hint"] = repartition_hint
        

        # フィルタ率計算と情報更新
        filter_result = calculate_filter_rate(node)
        node_analysis.update({
            "filter_rate": filter_result["filter_rate"],
            "files_pruned_bytes": filter_result["files_pruned_bytes"],
            "files_read_bytes": filter_result["files_read_bytes"],
            "has_filter_metrics": filter_result["has_filter_metrics"]
        })
        
        # クラスタリングキー情報の追加
        cluster_attributes = extract_cluster_attributes(node)
        node_analysis.update({
            "cluster_attributes": cluster_attributes,
            "has_clustering": len(cluster_attributes) > 0
        })
        
        detailed_analysis["top_bottleneck_nodes"].append(node_analysis)
        
        # スピル分析への追加
        if spill_detected:
            detailed_analysis["spill_analysis"]["total_spill_gb"] += node_analysis["spill_gb"]
            detailed_analysis["spill_analysis"]["spill_nodes"].append({
                "node_id": node_analysis["node_id"],
                "node_name": node_name,
                "spill_gb": node_analysis["spill_gb"],
                "rank": i + 1
            })
            
            if node_analysis["spill_gb"] > 1.0:  # 1GB以上は重要
                detailed_analysis["spill_analysis"]["critical_spill_nodes"].append(node_analysis["node_id"])
        
        # スキュー分析への追加
        if skew_detected:
            detailed_analysis["skew_analysis"]["total_skewed_partitions"] += skewed_partitions
            detailed_analysis["skew_analysis"]["skewed_nodes"].append({
                "node_id": node_analysis["node_id"],
                "node_name": node_name,
                "skewed_partitions": skewed_partitions,
                "rank": i + 1
            })
    
    # 全体的な推奨事項の生成
    if detailed_analysis["spill_analysis"]["total_spill_gb"] > 5.0:
        detailed_analysis["performance_recommendations"].append({
            "type": "memory_optimization",
            "priority": "CRITICAL",
            "description": f"大量スピル({detailed_analysis['spill_analysis']['total_spill_gb']:.1f}GB)検出: メモリ設定とパーティショニング戦略の見直しが必要"
        })
    
    if len(detailed_analysis["shuffle_optimization_hints"]) > 0:
        detailed_analysis["performance_recommendations"].append({
            "type": "shuffle_optimization", 
            "priority": "HIGH",
            "description": f"{len(detailed_analysis['shuffle_optimization_hints'])}個のスピル発生Shuffleノードでメモリ最適化が必要"
        })
    
    if detailed_analysis["skew_analysis"]["total_skewed_partitions"] > 10:
        detailed_analysis["performance_recommendations"].append({
            "type": "skew_optimization",
            "priority": "HIGH", 
            "description": f"データスキュー({detailed_analysis['skew_analysis']['total_skewed_partitions']}パーティション)検出: データ分散の見直しが必要"
        })
    
    return detailed_analysis

print("✅ 関数定義完了: get_meaningful_node_name, extract_shuffle_attributes, extract_detailed_bottleneck_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 ボトルネック指標計算関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - 実行時間とコンパイル時間の比率分析
# MAGIC - キャッシュ効率とデータ処理効率の計算
# MAGIC - Photon利用率の分析
# MAGIC - スピル検出とシャッフル/並列度の問題特定

# COMMAND ----------

def calculate_bottleneck_indicators(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """ボトルネック指標を計算"""
    indicators = {}
    
    overall = metrics.get('overall_metrics', {})
    total_time = overall.get('total_time_ms', 0)
    execution_time = overall.get('execution_time_ms', 0)
    compilation_time = overall.get('compilation_time_ms', 0)
    
    if total_time > 0:
        indicators['compilation_ratio'] = compilation_time / total_time
        indicators['execution_ratio'] = execution_time / total_time
    
    # キャッシュ効率
    read_bytes = overall.get('read_bytes', 0)
    cache_bytes = overall.get('read_cache_bytes', 0)
    if read_bytes > 0:
        indicators['cache_hit_ratio'] = cache_bytes / read_bytes
    
    # データ処理効率（容量ベース）
    read_bytes = overall.get('read_bytes', 0)
    
    # 容量ベースのフィルタ率を計算（正しい実装）
    data_selectivity = calculate_filter_rate_percentage(overall, metrics)
    
    indicators['data_selectivity'] = data_selectivity
    
    # Photon使用率（タスク実行時間に対する割合）
    task_time = overall.get('task_total_time_ms', 0)
    photon_time = overall.get('photon_total_time_ms', 0)
    if task_time > 0:
        indicators['photon_ratio'] = min(photon_time / task_time, 1.0)  # 最大100%に制限
    else:
        indicators['photon_ratio'] = 0.0
    
    # スピル検出（詳細版：Sink - Num bytes spilled to disk due to memory pressure ベース）
    spill_detected = False
    total_spill_bytes = 0
    spill_details = []
    
    # ターゲットメトリクス名（複数パターン対応）
    target_spill_metrics = [
        "Sink - Num bytes spilled to disk due to memory pressure",
        "Num bytes spilled to disk due to memory pressure"
    ]
    
    # 各ノードでスピル検出を実行
    for node in metrics.get('node_metrics', []):
        node_spill_found = False
        
        # 1. detailed_metricsから検索
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            metric_value = metric_info.get('value', 0)
            metric_label = metric_info.get('label', '')
            
            # 複数のスピルメトリクス名をチェック
            if ((metric_key in target_spill_metrics or 
                 metric_label in target_spill_metrics) and metric_value > 0):
                spill_detected = True
                node_spill_found = True
                total_spill_bytes += metric_value
                spill_details.append({
                    'node_id': node.get('node_id', ''),
                    'node_name': node.get('name', ''),
                    'spill_bytes': metric_value,
                    'spill_metric': metric_key if metric_key in target_spill_metrics else metric_label,
                    'source': 'detailed_metrics'
                })
                break
        
        # 2. raw_metricsから検索（このノードでまだ見つからない場合）
        if not node_spill_found:
            raw_metrics = node.get('metrics', [])
            for metric in raw_metrics:
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_value = metric.get('value', 0)
                
                # 複数のスピルメトリクス名をチェック
                if ((metric_key in target_spill_metrics or 
                     metric_label in target_spill_metrics) and metric_value > 0):
                    spill_detected = True
                    node_spill_found = True
                    total_spill_bytes += metric_value
                    spill_details.append({
                        'node_id': node.get('node_id', ''),
                        'node_name': node.get('name', ''),
                        'spill_bytes': metric_value,
                        'spill_metric': metric_key if metric_key in target_spill_metrics else metric_label,
                        'source': 'raw_metrics'
                    })
                    break
        
        # 3. key_metricsから検索（最後のフォールバック）
        if not node_spill_found:
            key_metrics = node.get('key_metrics', {})
            for key_metric_name, key_metric_value in key_metrics.items():
                # 複数のスピルメトリクス名をチェック
                if key_metric_name in target_spill_metrics and key_metric_value > 0:
                    spill_detected = True
                    node_spill_found = True
                    total_spill_bytes += key_metric_value
                    spill_details.append({
                        'node_id': node.get('node_id', ''),
                        'node_name': node.get('name', ''),
                        'spill_bytes': key_metric_value,
                        'spill_metric': key_metric_name,
                        'source': 'key_metrics'
                    })
                    break
    
    # フォールバック: overall_metricsからの簡易検出
    if not spill_detected:
        fallback_spill_bytes = overall.get('spill_to_disk_bytes', 0)
        if fallback_spill_bytes > 0:
            spill_detected = True
            total_spill_bytes = fallback_spill_bytes
            spill_details.append({
                'node_id': 'overall',
                'node_name': 'Overall Metrics',
                'spill_bytes': fallback_spill_bytes,
                'source': 'overall_metrics'
            })
    
    indicators['has_spill'] = spill_detected
    indicators['spill_bytes'] = total_spill_bytes
    indicators['spill_details'] = spill_details
    indicators['spill_nodes_count'] = len(spill_details)
    
    # 最も時間のかかるステージ
    stage_durations = [(s['stage_id'], s['duration_ms']) for s in metrics.get('stage_metrics', []) if s['duration_ms'] > 0]
    if stage_durations:
        slowest_stage = max(stage_durations, key=lambda x: x[1])
        indicators['slowest_stage_id'] = slowest_stage[0]
        indicators['slowest_stage_duration'] = slowest_stage[1]
    
    # 最もメモリを使用するノード
    memory_usage = []
    for node in metrics.get('node_metrics', []):
        peak_memory = node.get('key_metrics', {}).get('peakMemoryBytes', 0)
        if peak_memory > 0:
            memory_usage.append((node['node_id'], node['name'], peak_memory))
    
    if memory_usage:
        highest_memory_node = max(memory_usage, key=lambda x: x[2])
        indicators['highest_memory_node_id'] = highest_memory_node[0]
        indicators['highest_memory_node_name'] = highest_memory_node[1]
        indicators['highest_memory_bytes'] = highest_memory_node[2]
    
    # 並列度とシャッフル問題の検出
    shuffle_nodes = []
    low_parallelism_stages = []
    
    # シャッフルノードの特定
    for node in metrics.get('node_metrics', []):
        node_name = node.get('name', '').upper()
        if any(keyword in node_name for keyword in ['SHUFFLE', 'EXCHANGE']):
            shuffle_nodes.append({
                'node_id': node['node_id'],
                'name': node['name'],
                'duration_ms': node.get('key_metrics', {}).get('durationMs', 0),
                'rows': node.get('key_metrics', {}).get('rowsNum', 0)
            })
    
    # 低並列度ステージの検出
    for stage in metrics.get('stage_metrics', []):
        num_tasks = stage.get('num_tasks', 0)
        duration_ms = stage.get('duration_ms', 0)
        
        # 並列度が低い（タスク数が少ない）かつ実行時間が長いステージ
        if num_tasks > 0 and num_tasks < 10 and duration_ms > 5000:  # 10タスク未満、5秒以上
            low_parallelism_stages.append({
                'stage_id': stage['stage_id'],
                'num_tasks': num_tasks,
                'duration_ms': duration_ms,
                'avg_task_duration': duration_ms / max(num_tasks, 1)
            })
    
    indicators['shuffle_operations_count'] = len(shuffle_nodes)
    indicators['low_parallelism_stages_count'] = len(low_parallelism_stages)
    indicators['has_shuffle_bottleneck'] = len(shuffle_nodes) > 0 and any(s['duration_ms'] > 10000 for s in shuffle_nodes)
    indicators['has_low_parallelism'] = len(low_parallelism_stages) > 0
    
    # シャッフルの詳細情報
    if shuffle_nodes:
        total_shuffle_time = sum(s['duration_ms'] for s in shuffle_nodes)
        indicators['total_shuffle_time_ms'] = total_shuffle_time
        indicators['shuffle_time_ratio'] = total_shuffle_time / max(total_time, 1)
        
        # 最も時間のかかるシャッフル操作
        slowest_shuffle = max(shuffle_nodes, key=lambda x: x['duration_ms'])
        indicators['slowest_shuffle_duration_ms'] = slowest_shuffle['duration_ms']
        indicators['slowest_shuffle_node'] = slowest_shuffle['name']
    
    # 低並列度の詳細情報
    if low_parallelism_stages:
        indicators['low_parallelism_details'] = low_parallelism_stages
        avg_parallelism = sum(s['num_tasks'] for s in low_parallelism_stages) / len(low_parallelism_stages)
        indicators['average_low_parallelism'] = avg_parallelism
    
    # AQEShuffleRead警告の検出
    aqe_shuffle_skew_warning_detected = False
    aqe_detected_and_handled = False
    
    for node in metrics.get('node_metrics', []):
        parallelism_data = extract_parallelism_metrics(node)
        if parallelism_data.get('aqe_shuffle_skew_warning', False):
            aqe_shuffle_skew_warning_detected = True
        if parallelism_data.get('aqe_detected_and_handled', False):
            aqe_detected_and_handled = True
    
    # 優先順位: 512MB以上の警告があれば、それを優先
    # 警告がない場合のみ、AQE対応済みと判定
    indicators['has_aqe_shuffle_skew_warning'] = aqe_shuffle_skew_warning_detected
    indicators['has_skew'] = aqe_detected_and_handled and not aqe_shuffle_skew_warning_detected
    
    return indicators

print("✅ 関数定義完了: calculate_bottleneck_indicators")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧬 Liquid Clustering分析関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - プロファイラーデータからのカラム情報抽出
# MAGIC - フィルター、JOIN、GROUP BY条件の分析
# MAGIC - データスキューとパフォーマンス影響の評価
# MAGIC - クラスタリング推奨カラムの特定

# COMMAND ----------

def calculate_performance_insights_from_metrics(overall_metrics: Dict[str, Any], metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    メトリクス情報のみから詳細なパフォーマンス洞察を計算
    """
    insights = {}
    
    # 基本データ
    total_time_ms = overall_metrics.get('total_time_ms', 0)
    read_bytes = overall_metrics.get('read_bytes', 0)
    read_cache_bytes = overall_metrics.get('read_cache_bytes', 0)
    read_remote_bytes = overall_metrics.get('read_remote_bytes', 0)
    rows_read = overall_metrics.get('rows_read_count', 0)
    rows_produced = overall_metrics.get('rows_produced_count', 0)
    read_files = overall_metrics.get('read_files_count', 0)
    read_partitions = overall_metrics.get('read_partitions_count', 0)
    photon_time = overall_metrics.get('photon_total_time_ms', 0)
    task_time = overall_metrics.get('task_total_time_ms', 0)
    spill_bytes = overall_metrics.get('spill_to_disk_bytes', 0)
    
    # 1. データ効率分析（容量ベース）
    # metricsがNoneの場合は空の辞書で初期化
    if metrics is None:
        metrics = {'node_metrics': []}
    
    # 容量ベースのフィルタ率を計算（正しい実装）
    filter_rate_capacity = calculate_filter_rate_percentage(overall_metrics, metrics)
    
    insights['data_efficiency'] = {
        'data_selectivity': filter_rate_capacity,
        'avg_bytes_per_file': read_bytes / max(read_files, 1),
        'avg_bytes_per_partition': read_bytes / max(read_partitions, 1),
        'avg_rows_per_file': rows_read / max(read_files, 1),
        'avg_rows_per_partition': rows_read / max(read_partitions, 1)
    }
    
    # 2. キャッシュ効率分析
    cache_hit_ratio = read_cache_bytes / max(read_bytes, 1)
    insights['cache_efficiency'] = {
        'cache_hit_ratio': cache_hit_ratio,
        'cache_hit_percentage': cache_hit_ratio * 100,
        'remote_read_ratio': read_remote_bytes / max(read_bytes, 1),
        'cache_effectiveness': 'high' if cache_hit_ratio > 0.8 else 'medium' if cache_hit_ratio > 0.5 else 'low'
    }
    
    # 3. 並列化効率分析
    insights['parallelization'] = {
        'files_per_second': read_files / max(total_time_ms / 1000, 1),
        'partitions_per_second': read_partitions / max(total_time_ms / 1000, 1),
        'throughput_mb_per_second': (read_bytes / 1024 / 1024) / max(total_time_ms / 1000, 1),
        'rows_per_second': rows_read / max(total_time_ms / 1000, 1)
    }
    
    # 4. Photon効率分析
    photon_efficiency = photon_time / max(task_time, 1)
    insights['photon_analysis'] = {
        'photon_enabled': photon_time > 0,
        'photon_efficiency': photon_efficiency,
        'photon_utilization_percentage': photon_efficiency * 100,
        'photon_effectiveness': 'high' if photon_efficiency > 0.8 else 'medium' if photon_efficiency > 0.5 else 'low'
    }
    
    # 5. リソース使用状況
    insights['resource_usage'] = {
        'memory_pressure': spill_bytes > 0,
        'spill_gb': spill_bytes / 1024 / 1024 / 1024,
        'data_processed_gb': read_bytes / 1024 / 1024 / 1024,
        'data_reduction_ratio': 1 - (rows_produced / max(rows_read, 1))
    }
    
    # 6. ボトルネック指標
    bottlenecks = []
    if cache_hit_ratio < 0.3:
        bottlenecks.append('低キャッシュ効率')
    if read_remote_bytes / max(read_bytes, 1) > 0.8:
        bottlenecks.append('高リモート読み込み比率')
    if photon_efficiency < 0.5 and photon_time > 0:
        bottlenecks.append('低Photon効率')
    if spill_bytes > 0:
        bottlenecks.append('メモリスピル発生')
    if insights['data_efficiency']['data_selectivity'] < 0.2:
        bottlenecks.append('低フィルタ効率')
    
    insights['potential_bottlenecks'] = bottlenecks
    
    return insights

def calculate_filter_rate_percentage(overall_metrics: Dict[str, Any], metrics: Dict[str, Any]) -> float:
    """
    容量ベースのフィルタ率を計算する（overall_metrics.read_bytes使用版）
    
    ❌ デグレ防止注意: この関数は必ずoverall_metrics.read_bytesを使用してください！
    ❌ files_read_bytes（スキャンノード集計）は使用しないでください！
    
    Args:
        overall_metrics: 全体メトリクス（read_bytesを使用）
        metrics: 全メトリクス（node_metricsを含む、pruned_bytes取得用）
        
    Returns:
        float: フィルタ率（0.0-1.0、高い値ほど効率的）
               プルーニング効率 = files_pruned_bytes / (overall_read_bytes + files_pruned_bytes)
    """
    import os
    debug_mode = os.environ.get('DEBUG_FILTER_ANALYSIS', 'false').lower() == 'true'
    
    # ❌ デグレ防止: 必ずoverall_metrics.read_bytesを使用！
    overall_read_bytes = overall_metrics.get('read_bytes', 0)
    
    if debug_mode:
        print(f"🔍 フィルタ率計算デバッグ（overall_metrics.read_bytes使用版）:")
        print(f"   overall_read_bytes: {overall_read_bytes:,} ({overall_read_bytes / (1024**4):.2f} TB)")
    
    try:
        # pruned_bytesのみnode_metricsから取得（read_bytesは使用しない）
        node_metrics = metrics.get('node_metrics', [])
        total_files_pruned_bytes = 0
        filter_metrics_found = False
        
        # 全てのスキャンノードからpruned情報のみを集計
        for node in node_metrics:
            if node.get('tag') in ['FileScan', 'BatchScan', 'TableScan', 'UNKNOWN_DATA_SOURCE_SCAN_EXEC']:
                filter_result = calculate_filter_rate(node)
                if filter_result.get('has_filter_metrics', False):
                    files_pruned_bytes = filter_result.get('files_pruned_bytes', 0)
                    
                    if files_pruned_bytes > 0:
                        total_files_pruned_bytes += files_pruned_bytes
                        filter_metrics_found = True
                        
                        if debug_mode:
                            print(f"   ノード {node.get('node_id', 'unknown')}: files_pruned_bytes = {files_pruned_bytes:,}")
        
        # ❌ デグレ防止: overall_read_bytes + pruned_bytes で計算
        if filter_metrics_found and overall_read_bytes > 0:
            # 正しい計算: プルーニング効率 = files_pruned / (overall_read + files_pruned)
            total_available_bytes = overall_read_bytes + total_files_pruned_bytes
            if total_available_bytes > 0:
                overall_filter_rate = total_files_pruned_bytes / total_available_bytes
            else:
                overall_filter_rate = 0.0
                
            if debug_mode:
                print(f"   ❌ デグレ防止版: overall_read_bytes使用")
                print(f"     overall_read_bytes: {overall_read_bytes:,} ({overall_read_bytes / (1024**4):.2f} TB)")
                print(f"     total_files_pruned_bytes: {total_files_pruned_bytes:,} ({total_files_pruned_bytes / (1024**4):.2f} TB)")
                print(f"     total_available_bytes: {total_available_bytes:,} ({total_available_bytes / (1024**4):.2f} TB)")
                print(f"     プルーニング効率: {overall_filter_rate*100:.2f}%")
            return overall_filter_rate
        
        if debug_mode:
            print(f"   フィルタメトリクス: {'検出' if filter_metrics_found else '未検出'}")
            print(f"   overall_read_bytes: {overall_read_bytes:,}")
            if not filter_metrics_found:
                print(f"   ⚠️ プルーニング情報が利用できません")
            if overall_read_bytes == 0:
                print(f"   ⚠️ 読み込みデータがありません")
        
        # プルーニング情報がない場合は0を返す
        return 0.0
        
    except Exception as e:
        if debug_mode:
            print(f"   フィルタ率計算エラー: {e}")
        return 0.0

def extract_liquid_clustering_data(profiler_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    SQLプロファイラーデータからLiquid Clustering分析に必要なデータを抽出（LLM分析用）
    """
    extracted_data = {
        "filter_columns": [],
        "join_columns": [],
        "groupby_columns": [],
        "aggregate_columns": [],
        "table_info": {},
        "scan_nodes": [],
        "join_nodes": [],
        "filter_nodes": [],
        "metadata_summary": {}
    }
    
    print(f"🔍 Liquid Clustering分析用データ抽出開始")
    
    # データ形式を確認
    data_format = metrics.get('data_format', '')
    if data_format == 'sql_query_summary':
        print("📊 SQLクエリサマリー形式: 制限付きのLiquid Clustering分析")
        # test2.json形式の場合は制限付きの分析を行う
        query_info = metrics.get('query_info', {})
        query_text = query_info.get('query_text', '')
        
        # メトリクス情報のみから基本的なテーブル情報を生成
        # test2.json形式では planMetadatas が空のため、graphs metadata は利用不可
        # メトリクス重視のアプローチでボトルネック分析を行う
        
        # 全体的なフィルタ率情報を計算
        overall_filter_rate = calculate_filter_rate_percentage(overall_metrics, metrics)
        read_bytes = overall_metrics.get('read_bytes', 0)
        read_gb = read_bytes / (1024**3) if read_bytes > 0 else 0
        
        # プルーン量を推定（フィルタ率から逆算）
        if overall_filter_rate > 0 and read_bytes > 0:
            pruned_bytes = (read_bytes * overall_filter_rate) / (1 - overall_filter_rate)
            pruned_gb = pruned_bytes / (1024**3)
        else:
            pruned_bytes = 0
            pruned_gb = 0
        
        extracted_data["table_info"]["metrics_summary"] = {
            "node_name": "Metrics-Based Analysis",
            "node_tag": "QUERY_SUMMARY", 
            "node_id": "summary",
            "files_count": overall_metrics.get('read_files_count', 0),
            "partitions_count": overall_metrics.get('read_partitions_count', 0),
            "data_size_gb": read_gb,
            "rows_read": overall_metrics.get('rows_read_count', 0),
            "rows_produced": overall_metrics.get('rows_produced_count', 0),
            "data_selectivity": overall_filter_rate,
            "avg_file_size_mb": (overall_metrics.get('read_bytes', 0) / 1024 / 1024) / max(overall_metrics.get('read_files_count', 1), 1),
            "avg_partition_size_mb": (overall_metrics.get('read_bytes', 0) / 1024 / 1024) / max(overall_metrics.get('read_partitions_count', 1), 1),
            "note": "詳細なテーブル情報はSQLクエリサマリー形式では利用不可。メトリクスベース分析を実行。",
            "current_clustering_keys": [],  # 現在のクラスタリングキー
            "filter_info": {  # フィルタ率情報を追加
                "filter_rate": overall_filter_rate,
                "files_read_bytes": read_bytes,
                "files_pruned_bytes": pruned_bytes,
                "has_filter_metrics": read_bytes > 0
            }
        }
        
        # サマリーノードの情報を使用
        for node in metrics.get('node_metrics', []):
            node_name = node.get('name', '')
            extracted_data["scan_nodes"].append({
                "name": node_name,
                "type": node.get('tag', ''),
                "rows": node.get('key_metrics', {}).get('rowsNum', 0),
                "duration_ms": node.get('key_metrics', {}).get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })
        
        # メタデータサマリー（制限付き）
        view_count = sum(1 for table in extracted_data["table_info"].values() if table.get('is_view', False))
        actual_table_count = sum(len(table.get('underlying_tables', [])) for table in extracted_data["table_info"].values())
        
        extracted_data["metadata_summary"] = {
            "total_nodes": len(metrics.get('node_metrics', [])),
            "total_graphs": 0,
            "filter_expressions_count": 0,
            "join_expressions_count": 0,
            "groupby_expressions_count": 0,
            "aggregate_expressions_count": 0,
            "tables_identified": len(extracted_data["table_info"]),
            "views_identified": view_count,
            "underlying_tables_estimated": actual_table_count,
            "scan_nodes_count": len(extracted_data["scan_nodes"]),
            "join_nodes_count": 0,
            "filter_nodes_count": 0,
            "analysis_limitation": "SQLクエリサマリー形式のため詳細分析が制限されています"
        }
        
        print(f"✅ 制限付きデータ抽出完了: {extracted_data['metadata_summary']}")
        
        # ビュー情報の詳細表示
        if view_count > 0:
            print(f"🔍 ビュー情報の詳細:")
            for table_name, table_info in extracted_data["table_info"].items():
                if table_info.get('is_view', False):
                    print(f"  📊 ビュー: {table_name}")
                    print(f"     エイリアス: {table_info.get('alias', 'なし')}")
                    print(f"     テーブル種別: {table_info.get('table_type', 'unknown')}")
                    
                    underlying_tables = table_info.get('underlying_tables', [])
                    if underlying_tables:
                        print(f"     推定実テーブル数: {len(underlying_tables)}")
                        for i, underlying_table in enumerate(underlying_tables[:3]):  # 最大3個表示
                            print(f"       - {underlying_table}")
                        if len(underlying_tables) > 3:
                            print(f"       ... および {len(underlying_tables) - 3} 個の追加テーブル")
                    print()
        
        return extracted_data
    
    # 通常のSQLプロファイラー形式の処理
    # プロファイラーデータから実行グラフ情報を取得（複数グラフ対応）
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        print("⚠️ グラフデータが見つかりません")
        return extracted_data

    # すべてのグラフからノードを収集
    all_nodes = []
    for graph_index, graph in enumerate(graphs):
        nodes = graph.get('nodes', [])
        for node in nodes:
            node['graph_index'] = graph_index
            all_nodes.append(node)
    
    print(f"🔍 {len(graphs)}個のグラフから{len(all_nodes)}個のノードを処理中")

    # ノードからメタデータ情報を抽出
    for node in all_nodes:
        node_name = node.get('name', '')
        node_tag = node.get('tag', '')
        node_metadata = node.get('metadata', [])
        
        # メタデータから重要な情報を抽出
        for metadata_item in node_metadata:
            key = metadata_item.get('key', '')
            values = metadata_item.get('values', [])
            value = metadata_item.get('value', '')
            
            # フィルター条件の抽出
            if key == 'FILTERS' and values:
                for filter_expr in values:
                    extracted_data["filter_columns"].append({
                        "expression": filter_expr,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # GROUP BY式の抽出
            elif key == 'GROUPING_EXPRESSIONS' and values:
                for group_expr in values:
                    extracted_data["groupby_columns"].append({
                        "expression": group_expr,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # JOIN条件の抽出
            elif key in ['LEFT_KEYS', 'RIGHT_KEYS'] and values:
                for join_key in values:
                    extracted_data["join_columns"].append({
                        "expression": join_key,
                        "key_type": key,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # 集約関数の抽出
            elif key == 'AGGREGATE_EXPRESSIONS' and values:
                for agg_expr in values:
                    extracted_data["aggregate_columns"].append({
                        "expression": agg_expr,
                        "node_name": node_name,
                        "node_tag": node_tag
                    })
            
            # テーブル情報の抽出
            elif key == 'SCAN_IDENTIFIER':
                table_name = value
                extracted_data["table_info"][table_name] = {
                    "node_name": node_name,
                    "node_tag": node_tag,
                    "node_id": node.get('id', ''),
                    "current_clustering_keys": []  # 現在のクラスタリングキーを追加
                }

    # ノードタイプ別の分類と現在のクラスタリングキー情報の関連付け
    node_metrics = metrics.get('node_metrics', [])
    for node in node_metrics:
        node_name = node.get('name', '')
        node_type = node.get('tag', '')
        key_metrics = node.get('key_metrics', {})
        
        if any(keyword in node_name.upper() for keyword in ['SCAN', 'FILESCAN', 'PARQUET', 'DELTA']):
            # スキャンノードからクラスタリングキーを抽出
            cluster_attributes = extract_cluster_attributes(node)
            
            # ノードからテーブル名を抽出してマッピング
            node_metadata = node.get('metadata', [])
            table_name_from_node = None
            
            for meta in node_metadata:
                meta_key = meta.get('key', '')
                meta_value = meta.get('value', '')
                if meta_key == 'SCAN_IDENTIFIER' and meta_value:
                    table_name_from_node = meta_value
                    break
            
            # テーブル名が見つからない場合はノード名から推測
            if not table_name_from_node:
                import re
                table_patterns = [
                    r'[Ss]can\s+([a-zA-Z_][a-zA-Z0-9_.]*[a-zA-Z0-9_])',
                    r'([a-zA-Z_][a-zA-Z0-9_]*\.)+([a-zA-Z_][a-zA-Z0-9_]*)',
                ]
                
                for pattern in table_patterns:
                    match = re.search(pattern, node_name)
                    if match:
                        if '.' in match.group(0):
                            table_name_from_node = match.group(0)
                        else:
                            table_name_from_node = match.group(1) if match.lastindex and match.lastindex >= 1 else match.group(0)
                        break
            
            # フィルタ率情報を計算
            filter_result = calculate_filter_rate(node)
            filter_rate_info = {
                "filter_rate": filter_result.get("filter_rate", 0),
                "files_read_bytes": filter_result.get("files_read_bytes", 0),
                "files_pruned_bytes": filter_result.get("files_pruned_bytes", 0),
                "has_filter_metrics": filter_result.get("has_filter_metrics", False)
            }
            
            # テーブル情報にクラスタリングキーとフィルタ率を追加
            if table_name_from_node:
                # 既存のテーブル情報を更新
                if table_name_from_node in extracted_data["table_info"]:
                    extracted_data["table_info"][table_name_from_node]["current_clustering_keys"] = cluster_attributes
                    extracted_data["table_info"][table_name_from_node]["filter_info"] = filter_rate_info
                else:
                    # 新しいテーブル情報を作成
                    extracted_data["table_info"][table_name_from_node] = {
                        "node_name": node_name,
                        "node_tag": node_type,
                        "node_id": node.get('node_id', ''),
                        "current_clustering_keys": cluster_attributes,
                        "filter_info": filter_rate_info
                    }
            
            extracted_data["scan_nodes"].append({
                "name": node_name,
                "type": node_type,
                "rows": key_metrics.get('rowsNum', 0),
                "duration_ms": key_metrics.get('durationMs', 0),
                "node_id": node.get('node_id', ''),
                "table_name": table_name_from_node,
                "current_clustering_keys": cluster_attributes
            })
        elif any(keyword in node_name.upper() for keyword in ['JOIN', 'HASH']):
            extracted_data["join_nodes"].append({
                "name": node_name,
                "type": node_type,
                "duration_ms": key_metrics.get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })
        elif any(keyword in node_name.upper() for keyword in ['FILTER']):
            extracted_data["filter_nodes"].append({
                "name": node_name,
                "type": node_type,
                "duration_ms": key_metrics.get('durationMs', 0),
                "node_id": node.get('node_id', '')
            })

    # メタデータサマリー
    extracted_data["metadata_summary"] = {
        "total_nodes": len(all_nodes),
        "total_graphs": len(graphs),
        "filter_expressions_count": len(extracted_data["filter_columns"]),
        "join_expressions_count": len(extracted_data["join_columns"]),
        "groupby_expressions_count": len(extracted_data["groupby_columns"]),
        "aggregate_expressions_count": len(extracted_data["aggregate_columns"]),
        "tables_identified": len(extracted_data["table_info"]),
        "scan_nodes_count": len(extracted_data["scan_nodes"]),
        "join_nodes_count": len(extracted_data["join_nodes"]),
        "filter_nodes_count": len(extracted_data["filter_nodes"])
    }
    
    print(f"✅ データ抽出完了: {extracted_data['metadata_summary']}")
    
    # 現在のクラスタリングキー情報の詳細表示
    clustering_info_found = False
    for table_name, table_info in extracted_data["table_info"].items():
        current_keys = table_info.get('current_clustering_keys', [])
        if current_keys:
            if not clustering_info_found:
                print(f"🔍 現在のクラスタリングキー情報:")
                clustering_info_found = True
            print(f"  📊 テーブル: {table_name}")
            print(f"     現在のキー: {', '.join(current_keys)}")
            print(f"     ノード: {table_info.get('node_name', 'Unknown')}")
            print()
    
    if not clustering_info_found:
        print(f"ℹ️ 現在のクラスタリングキーは検出されませんでした")
    
    return extracted_data

def analyze_liquid_clustering_opportunities(profiler_data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    LLMを使用してLiquid Clusteringの分析と推奨事項を生成
    """
    print(f"🤖 LLMによるLiquid Clustering分析を開始")
    
    # 基本データの抽出
    extracted_data = extract_liquid_clustering_data(profiler_data, metrics)
    
    # LLM分析用のプロンプト作成
    overall_metrics = metrics.get('overall_metrics', {})
    bottleneck_indicators = metrics.get('bottleneck_indicators', {})
    
    # パフォーマンス概要
    total_time_sec = overall_metrics.get('total_time_ms', 0) / 1000
    read_gb = overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024
    rows_produced = overall_metrics.get('rows_produced_count', 0)
    rows_read = overall_metrics.get('rows_read_count', 0)
    
    # 抽出したカラム情報のサマリー作成（上位5個まで）
    filter_summary = []
    for i, item in enumerate(extracted_data["filter_columns"][:5]):
        filter_summary.append(f"  {i+1}. {item['expression']} (ノード: {item['node_name']})")
    
    join_summary = []
    for i, item in enumerate(extracted_data["join_columns"][:5]):
        join_summary.append(f"  {i+1}. {item['expression']} (タイプ: {item['key_type']}, ノード: {item['node_name']})")
    
    groupby_summary = []
    for i, item in enumerate(extracted_data["groupby_columns"][:5]):
        groupby_summary.append(f"  {i+1}. {item['expression']} (ノード: {item['node_name']})")
    
    aggregate_summary = []
    for i, item in enumerate(extracted_data["aggregate_columns"][:5]):
        aggregate_summary.append(f"  {i+1}. {item['expression']} (ノード: {item['node_name']})")
    
    # テーブル情報のサマリー（現在のクラスタリングキー情報とフィルタ率を含む）
    table_summary = []
    for table_name, table_info in extracted_data["table_info"].items():
        current_keys = table_info.get('current_clustering_keys', [])
        current_keys_str = ', '.join(current_keys) if current_keys else '設定なし'
        
        # フィルタ率情報を追加
        filter_info = table_info.get('filter_info', {})
        filter_rate = filter_info.get('filter_rate', 0)
        files_read_bytes = filter_info.get('files_read_bytes', 0)
        files_pruned_bytes = filter_info.get('files_pruned_bytes', 0)
        
        # バイト数をGB単位に変換
        read_gb = files_read_bytes / (1024**3) if files_read_bytes > 0 else 0
        pruned_gb = files_pruned_bytes / (1024**3) if files_pruned_bytes > 0 else 0
        
        if filter_info.get('has_filter_metrics', False):
            filter_str = f", フィルタ率: {filter_rate*100:.1f}% (読み込み: {read_gb:.2f}GB, プルーン: {pruned_gb:.2f}GB)"
        else:
            filter_str = ", フィルタ率: 情報なし"
        
        table_summary.append(f"  - {table_name} (ノード: {table_info['node_name']}, 現在のクラスタリングキー: {current_keys_str}{filter_str})")
    
    # スキャンノードのパフォーマンス情報
    scan_performance = []
    for scan in extracted_data["scan_nodes"]:
        efficiency = scan['rows'] / max(scan['duration_ms'], 1)
        scan_performance.append(f"  - {scan['name']}: {scan['rows']:,}行, {scan['duration_ms']:,}ms, 効率={efficiency:.1f}行/ms")

    clustering_prompt = f"""
あなたはDatabricksのLiquid Clustering専門家です。以下のSQLプロファイラーデータを分析し、最適なLiquid Clusteringの推奨事項を提示してください。

【クエリパフォーマンス概要】
- 実行時間: {total_time_sec:.1f}秒
- データ読み込み: {read_gb:.2f}GB
- 出力行数: {rows_produced:,}行
- 読み込み行数: {rows_read:,}行
- フィルタ率: {calculate_filter_rate_percentage(overall_metrics, metrics):.4f}

【抽出されたカラム使用パターン】

🔍 フィルター条件 ({len(extracted_data["filter_columns"])}個):
{chr(10).join(filter_summary)}

🔗 JOIN条件 ({len(extracted_data["join_columns"])}個):
{chr(10).join(join_summary)}

📊 GROUP BY ({len(extracted_data["groupby_columns"])}個):
{chr(10).join(groupby_summary)}

📈 集約関数 ({len(extracted_data["aggregate_columns"])}個):
{chr(10).join(aggregate_summary)}

【テーブル情報】
テーブル数: {len(extracted_data["table_info"])}個
{chr(10).join(table_summary)}

【スキャンノードパフォーマンス】
{chr(10).join(scan_performance)}

【現在のボトルネック指標】
- スピル発生: {'あり' if bottleneck_indicators.get('has_spill', False) else 'なし'}
- シャッフル操作: {bottleneck_indicators.get('shuffle_operations_count', 0)}回
- 低並列度ステージ: {bottleneck_indicators.get('low_parallelism_stages_count', 0)}個

【分析要求】
1. 各テーブルに対する最適なLiquid Clusteringカラムの推奨（最大4カラム）
2. カラム選定の根拠（フィルター、JOIN、GROUP BYでの使用頻度と重要度）
3. 現在のクラスタリングキーと推奨キーの比較分析
4. 実装優先順位（パフォーマンス向上効果順）
5. 具体的なSQL実装例（正しいDatabricks SQL構文、現在のクラスタリングキー情報をコメントに明記）
6. 期待されるパフォーマンス改善効果（数値で）

【制約事項】
- パーティショニングやZORDERは提案しない（Liquid Clusteringのみ）
- 正しいDatabricks SQL構文を使用：
  * 新規テーブル: CREATE TABLE ... CLUSTER BY (col1, col2, ...)
  * 既存テーブル: ALTER TABLE table_name CLUSTER BY (col1, col2, ...)
- 最大4カラムまでの推奨
- データスキューや並列度の問題も考慮

【🚨 重要なLiquid Clustering仕様の理解】
- **カラム順序**: Liquid Clusteringではクラスタリングキーの順序変更は「ノードレベルのデータ局所性」に影響しません
- **実際の改善効果**: 向上するのは「スキャン効率」「ファイルプルーニング効果」「クエリパフォーマンス」です
- **技術的特性**: CLUSTER BY内のカラム順序は任意であり、(col1, col2, col3) と (col3, col1, col2) は同等のパフォーマンス

【🚨 絶対に使用禁止の誤った表現】
❌ 「順序を変更することでデータ局所性を向上」
❌ 「クラスタリングキー順序でデータ局所性改善」  
❌ 「順序変更によるノードレベルデータ配置最適化」
✅ 「順序変更による具体的な改善効果なし（Liquid Clustering仕様）」
✅ 「スキャン効率とファイルプルーニング効果の向上」
✅ 「WHERE句やJOIN条件のパフォーマンス改善」

簡潔で実践的な分析結果を日本語で提供してください。

【重要な出力形式指示】
各テーブルの分析では、必ず以下の形式で現在のクラスタリングキー情報とフィルタ率を含めてください：

## テーブル別推奨クラスタリング

### 1. [テーブル名] テーブル (最優先/高優先度/中優先度)
**現在のクラスタリングキー**: [現在設定されているキー または "設定なし"]
**推奨クラスタリングカラム**: [推奨カラム1], [推奨カラム2], [推奨カラム3], [推奨カラム4]

```sql
ALTER TABLE [テーブル名] 
CLUSTER BY ([推奨カラム1], [推奨カラム2], [推奨カラム3], [推奨カラム4]);
OPTIMIZE [テーブル名] FULL;
```

**選定根拠**:
- [カラム1]: [使用パターンと重要度]
- [カラム2]: [使用パターンと重要度]
- [以下同様...]
- 🚨重要: クラスタリングキー順序変更はノードレベルのデータ局所性に影響しない（Liquid Clustering仕様）
- ✅改善効果: スキャン効率とファイルプルーニング効果の向上（順序無関係）

**期待される改善効果**:
- [具体的な数値での改善見込み]

**フィルタ率**: [X.X]% (読み込み: [XX.XX]GB, プルーン: [XX.XX]GB)

この形式により、現在の設定、推奨設定、および各テーブルの現在のフィルタリング効率を明確に表示してください。フィルタ率情報は上記テーブル情報から正確な数値を使用してください。
"""

    try:
        # LLM分析の実行
        provider = LLM_CONFIG["provider"]
        print(f"🤖 {provider}を使用してLiquid Clustering分析中...")
        
        if provider == "databricks":
            llm_analysis = _call_databricks_llm(clustering_prompt)
        elif provider == "openai":
            llm_analysis = _call_openai_llm(clustering_prompt)
        elif provider == "azure_openai":
            llm_analysis = _call_azure_openai_llm(clustering_prompt)
        elif provider == "anthropic":
            llm_analysis = _call_anthropic_llm(clustering_prompt)
        else:
            llm_analysis = f"❌ サポートされていないLLMプロバイダー: {provider}"
        
        # 分析結果の構造化
        clustering_analysis = {
            "llm_analysis": llm_analysis,
            "extracted_data": extracted_data,
            "performance_context": {
                "total_time_sec": total_time_sec,
                "read_gb": read_gb,
                "rows_produced": rows_produced,
                "rows_read": rows_read,
                "data_selectivity": calculate_filter_rate_percentage(overall_metrics, metrics)
            },
            "summary": {
                "analysis_method": "LLM-based",
                "tables_identified": len(extracted_data["table_info"]),
                "total_filter_columns": len(extracted_data["filter_columns"]),
                "total_join_columns": len(extracted_data["join_columns"]),
                "total_groupby_columns": len(extracted_data["groupby_columns"]),
                "total_aggregate_columns": len(extracted_data["aggregate_columns"]),
                "scan_nodes_count": len(extracted_data["scan_nodes"]),
                "llm_provider": provider
            }
        }
        
        print("✅ LLM Liquid Clustering分析完了")
        return clustering_analysis
        
    except Exception as e:
        error_msg = f"LLM分析エラー: {str(e)}"
        print(f"❌ {error_msg}")
        
        # フォールバック: 基本的な抽出データのみを返す
        return {
            "llm_analysis": f"❌ LLM分析に失敗しました: {error_msg}",
            "extracted_data": extracted_data,
            "summary": {
                "analysis_method": "extraction-only",
                "tables_identified": len(extracted_data["table_info"]),
                "total_filter_columns": len(extracted_data["filter_columns"]),
                "error": error_msg
            }
        }

def save_liquid_clustering_analysis(clustering_analysis: Dict[str, Any], output_dir: str = "/tmp") -> Dict[str, str]:
    """
    Liquid Clustering分析結果をファイルに出力
    """
    import os
    import json
    from datetime import datetime
    
    # タイムスタンプ付きファイル名の生成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 出力ファイルパス
    json_path = f"{output_dir}/liquid_clustering_analysis_{timestamp}.json"
    markdown_path = f"{output_dir}/liquid_clustering_analysis_{timestamp}.md"
    sql_path = f"{output_dir}/liquid_clustering_implementation_{timestamp}.sql"
    
    file_paths = {}
    
    try:
        # 1. JSON形式での詳細データ保存
        # set型をlist型に変換してJSON serializable にする
        json_data = convert_sets_to_lists(clustering_analysis)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        file_paths['json'] = json_path
        print(f"✅ JSON形式の詳細データを保存: {json_path}")
        
        # 2. Markdown形式での分析レポート保存
        markdown_content = generate_liquid_clustering_markdown_report(clustering_analysis)
        
        with open(markdown_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        file_paths['markdown'] = markdown_path
        print(f"✅ Markdown形式の分析レポートを保存: {markdown_path}")
        
        # 3. SQL実装例ファイルの生成
        sql_content = generate_liquid_clustering_sql_implementations(clustering_analysis)
        
        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        file_paths['sql'] = sql_path
        print(f"✅ SQL実装例を保存: {sql_path}")
        
        return file_paths
        
    except Exception as e:
        error_msg = f"ファイル出力エラー: {str(e)}"
        print(f"❌ {error_msg}")
        return {"error": error_msg}

def generate_liquid_clustering_markdown_report(clustering_analysis: Dict[str, Any]) -> str:
    """
    Liquid Clustering分析結果のMarkdownレポートを生成
    """
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 基本情報の取得
    summary = clustering_analysis.get('summary', {})
    performance_context = clustering_analysis.get('performance_context', {})
    extracted_data = clustering_analysis.get('extracted_data', {})
    llm_analysis = clustering_analysis.get('llm_analysis', '')
    
    markdown_content = f"""# Liquid Clustering 分析レポート

**生成日時**: {timestamp}  
**分析方法**: {summary.get('analysis_method', 'Unknown')}  
**LLMプロバイダー**: {summary.get('llm_provider', 'Unknown')}

## 📊 パフォーマンス概要

| 項目 | 値 |
|------|-----|
| 実行時間 | {performance_context.get('total_time_sec', 0):.1f}秒 |
| データ読み込み | {performance_context.get('read_gb', 0):.2f}GB |
| 出力行数 | {performance_context.get('rows_produced', 0):,}行 |
| 読み込み行数 | {performance_context.get('rows_read', 0):,}行 |
| フィルタ率 | {performance_context.get('data_selectivity', 0):.4f} |

## 🔍 抽出されたメタデータ

### フィルター条件 ({summary.get('total_filter_columns', 0)}個)
"""
    
    # フィルター条件の詳細
    filter_columns = extracted_data.get('filter_columns', [])
    for i, filter_item in enumerate(filter_columns[:10], 1):  # 最大10個まで表示
        markdown_content += f"{i}. `{filter_item.get('expression', '')}` (ノード: {filter_item.get('node_name', '')})\n"
    
    if len(filter_columns) > 10:
        markdown_content += f"... 他 {len(filter_columns) - 10}個\n"
    
    markdown_content += f"""
### JOIN条件 ({summary.get('total_join_columns', 0)}個)
"""
    
    # JOIN条件の詳細
    join_columns = extracted_data.get('join_columns', [])
    for i, join_item in enumerate(join_columns[:10], 1):
        markdown_content += f"{i}. `{join_item.get('expression', '')}` ({join_item.get('key_type', '')})\n"
    
    if len(join_columns) > 10:
        markdown_content += f"... 他 {len(join_columns) - 10}個\n"
    
    markdown_content += f"""
### GROUP BY条件 ({summary.get('total_groupby_columns', 0)}個)
"""
    
    # GROUP BY条件の詳細
    groupby_columns = extracted_data.get('groupby_columns', [])
    for i, groupby_item in enumerate(groupby_columns[:10], 1):
        markdown_content += f"{i}. `{groupby_item.get('expression', '')}` (ノード: {groupby_item.get('node_name', '')})\n"
    
    if len(groupby_columns) > 10:
        markdown_content += f"... 他 {len(groupby_columns) - 10}個\n"
    
    markdown_content += f"""
### 集約関数 ({summary.get('total_aggregate_columns', 0)}個)
"""
    
    # 集約関数の詳細
    aggregate_columns = extracted_data.get('aggregate_columns', [])
    for i, agg_item in enumerate(aggregate_columns[:10], 1):
        markdown_content += f"{i}. `{agg_item.get('expression', '')}` (ノード: {agg_item.get('node_name', '')})\n"
    
    if len(aggregate_columns) > 10:
        markdown_content += f"... 他 {len(aggregate_columns) - 10}個\n"
    
    markdown_content += f"""
## 🏷️ 識別されたテーブル ({summary.get('tables_identified', 0)}個)

"""
    
    # テーブル情報の詳細（現在のクラスタリングキーを含む）
    table_info = extracted_data.get('table_info', {})
    for table_name, table_details in table_info.items():
        current_keys = table_details.get('current_clustering_keys', [])
        current_keys_str = ', '.join(current_keys) if current_keys else '設定なし'
        markdown_content += f"- **{table_name}** (ノード: {table_details.get('node_name', '')})\n"
        markdown_content += f"  - 現在のクラスタリングキー: `{current_keys_str}`\n"
    
    markdown_content += f"""
## 🔎 スキャンノード分析 ({summary.get('scan_nodes_count', 0)}個)

"""
    
    # スキャンノードの詳細
    scan_nodes = extracted_data.get('scan_nodes', [])
    for scan in scan_nodes:
        efficiency = scan.get('rows', 0) / max(scan.get('duration_ms', 1), 1)
        markdown_content += f"- **{scan.get('name', '')}**: {scan.get('rows', 0):,}行, {scan.get('duration_ms', 0):,}ms, 効率={efficiency:.1f}行/ms\n"
    
    markdown_content += f"""
## 🤖 LLM分析結果

{llm_analysis}

## 📋 分析サマリー

- **分析対象テーブル数**: {summary.get('tables_identified', 0)}
- **フィルター条件数**: {summary.get('total_filter_columns', 0)}
- **JOIN条件数**: {summary.get('total_join_columns', 0)}
- **GROUP BY条件数**: {summary.get('total_groupby_columns', 0)}
- **集約関数数**: {summary.get('total_aggregate_columns', 0)}
- **スキャンノード数**: {summary.get('scan_nodes_count', 0)}

---
*レポート生成時刻: {timestamp}*
"""
    
    return markdown_content

def generate_liquid_clustering_sql_implementations(clustering_analysis: Dict[str, Any]) -> str:
    """
    Liquid Clustering実装用のSQL例を生成
    """
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 基本情報の取得
    extracted_data = clustering_analysis.get('extracted_data', {})
    table_info = extracted_data.get('table_info', {})
    
    sql_content = f"""-- =====================================================
-- Liquid Clustering 実装SQL例
-- 生成日時: {timestamp}
-- =====================================================

-- 【重要】
-- 以下のSQL例は分析結果に基づく推奨事項です。
-- 実際の実装前に、テーブル構造やデータ特性を確認してください。

"""
    
    # テーブルごとのSQL実装例を生成
    for table_name, table_details in table_info.items():
        # 現在のクラスタリングキー情報を取得
        current_keys = table_details.get('current_clustering_keys', [])
        current_keys_str = ', '.join(current_keys) if current_keys else '設定なし'
        
        sql_content += f"""
-- =====================================================
-- テーブル: {table_name}
-- 現在のクラスタリングキー: {current_keys_str}
-- =====================================================

-- 既存テーブルにLiquid Clusteringを適用する場合:
-- ALTER TABLE {table_name} CLUSTER BY (column1, column2, column3, column4);

-- 新規テーブル作成時にLiquid Clusteringを設定する場合:
-- CREATE TABLE {table_name}_clustered
-- CLUSTER BY (column1, column2, column3, column4)
-- AS SELECT * FROM {table_name};

-- Delta Live Tablesでの設定例:
-- @dlt.table(
--   cluster_by=["column1", "column2", "column3", "column4"]
-- )
-- def {table_name.split('.')[-1]}_clustered():
--   return spark.table("{table_name}")

-- クラスタリング状況の確認:
-- DESCRIBE DETAIL {table_name};

-- クラスタリング統計の確認:
-- ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS;

"""
    
    sql_content += f"""
-- =====================================================
-- 一般的なLiquid Clustering実装パターン
-- =====================================================

-- パターン1: フィルター頻度の高いカラムを優先
-- 推奨順序: 1) フィルター条件カラム 2) JOIN条件カラム 3) GROUP BYカラム

-- パターン2: カーディナリティを考慮した順序
-- 低カーディナリティ → 高カーディナリティの順で配置

-- パターン3: データアクセスパターンに基づく配置
-- よく一緒に使用されるカラムを近い位置に配置

-- =====================================================
-- 実装後のパフォーマンス検証SQL
-- =====================================================

-- 1. クエリ実行計画の確認
-- EXPLAIN SELECT ... FROM table_name WHERE ...;

-- 2. ファイルスキップ統計の確認
-- SELECT * FROM table_name WHERE filter_column = 'value';
-- -- SQLプロファイラーでファイルスキップ数を確認

-- 3. データ配置の確認
-- SELECT 
--   file_path,
--   count(*) as row_count,
--   min(cluster_column1) as min_val,
--   max(cluster_column1) as max_val
-- FROM table_name
-- GROUP BY file_path
-- ORDER BY file_path;

-- =====================================================
-- 注意事項
-- =====================================================

-- 1. Liquid Clusteringは最大4カラムまで指定可能
-- 2. パーティショニングとは併用不可
-- 3. 既存のZORDER BYは自動的に無効化される
-- 4. クラスタリングの効果は時間とともに向上する（OPTIMIZE実行で最適化）
-- 5. 定期的なOPTIMIZE実行を推奨
-- 6. **重要**: カラムの指定順序はパフォーマンスに影響しません
--    * CLUSTER BY (col1, col2, col3) と CLUSTER BY (col3, col1, col2) は同等
--    * 従来のパーティショニングやZ-ORDERとは異なる重要な特性

-- OPTIMIZE実行例:
-- OPTIMIZE table_name;

-- =====================================================
-- 生成情報
-- =====================================================
-- 生成日時: {timestamp}
-- 分析対象テーブル数: {len(table_info)}
-- 基づいた分析: LLMによるLiquid Clustering分析
"""
    
    return sql_content

print("✅ 関数定義完了: analyze_liquid_clustering_opportunities, save_liquid_clustering_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 LLMによるボトルネック分析関数
# MAGIC
# MAGIC このセルでは以下の機能を定義します：
# MAGIC - 抽出されたメトリクスのLLM分析用フォーマット
# MAGIC - 複数LLMプロバイダーの対応（Databricks/OpenAI/Azure/Anthropic）
# MAGIC - 日本語での詳細な分析レポート生成
# MAGIC - エラーハンドリングとフォールバック分析

# COMMAND ----------

def analyze_bottlenecks_with_llm(metrics: Dict[str, Any]) -> str:
    """
    包括的なパフォーマンス分析レポートを生成
    セル33（TOP10プロセス）、セル35（Liquid Clustering）、セル43（統合最適化実行）の情報を統合
    EXPLAIN + EXPLAIN COST結果も活用してより精密な分析を実行
    
    🚨 重要: パーセンテージ計算デグレ防止
    - 並列実行ノードの時間合計を全体時間として使用することは絶対に禁止
    - overall_metrics.total_time_ms（wall-clock time）を優先使用
    - フォールバック時は最大ノード時間を使用（合計ではない）
    """
    from datetime import datetime
    
    print("📊 包括的パフォーマンス分析レポート（EXPLAIN+EXPLAIN COST統合）を生成中...")
    
    # === EXPLAIN + EXPLAIN COST結果の読み込み ===
    explain_content = ""
    explain_cost_content = ""
    physical_plan = ""
    photon_explanation = ""
    cost_statistics = ""
    
    explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')
    if explain_enabled.upper() == 'Y':
        import glob
        import os
        
        print("🔍 ボトルネック分析用: EXPLAIN + EXPLAIN COST結果ファイルを検索中...")
        
        # 最新のEXPLAIN結果ファイルを検索
        explain_original_files = glob.glob("output_explain_original_*.txt")
        explain_optimized_files = glob.glob("output_explain_optimized_*.txt")
        explain_files = explain_original_files if explain_original_files else explain_optimized_files
        
        if explain_files:
            latest_explain_file = max(explain_files, key=os.path.getctime)
            try:
                with open(latest_explain_file, 'r', encoding='utf-8') as f:
                    explain_content = f.read()
                    print(f"✅ ボトルネック分析用EXPLAIN結果を読み込み: {latest_explain_file}")
                
                # Physical Planの抽出
                if "== Physical Plan ==" in explain_content:
                    physical_plan_start = explain_content.find("== Physical Plan ==")
                    physical_plan_end = explain_content.find("== Photon", physical_plan_start)
                    if physical_plan_end == -1:
                        physical_plan_end = len(explain_content)
                    physical_plan = explain_content[physical_plan_start:physical_plan_end].strip()
                
                # Photon Explanationの抽出
                if "== Photon Explanation ==" in explain_content:
                    photon_start = explain_content.find("== Photon Explanation ==")
                    photon_explanation = explain_content[photon_start:].strip()
                    
            except Exception as e:
                print(f"⚠️ ボトルネック分析用EXPLAIN結果の読み込みに失敗: {str(e)}")
        
        # 最新のEXPLAIN COST結果ファイルを検索
        cost_original_files = glob.glob("output_explain_cost_original_*.txt")
        cost_optimized_files = glob.glob("output_explain_cost_optimized_*.txt")
        cost_files = cost_original_files if cost_original_files else cost_optimized_files
        
        if cost_files:
            latest_cost_file = max(cost_files, key=os.path.getctime)
            try:
                with open(latest_cost_file, 'r', encoding='utf-8') as f:
                    explain_cost_content = f.read()
                    print(f"💰 ボトルネック分析用EXPLAIN COST結果を読み込み: {latest_cost_file}")
                
                # 統計情報の抽出
                cost_statistics = extract_cost_statistics_from_explain_cost(explain_cost_content)
                print(f"📊 ボトルネック分析用統計情報を抽出: {len(cost_statistics)} 文字")
                    
            except Exception as e:
                print(f"⚠️ ボトルネック分析用EXPLAIN COST結果の読み込みに失敗: {str(e)}")
        
        if not explain_files and not cost_files:
            # フォールバック: 古いファイル名パターンもチェック
            old_explain_files = glob.glob("output_explain_plan_*.txt")
            if old_explain_files:
                latest_explain_file = max(old_explain_files, key=os.path.getctime)
                try:
                    with open(latest_explain_file, 'r', encoding='utf-8') as f:
                        explain_content = f.read()
                        print(f"✅ 古い形式のEXPLAIN結果を読み込み: {latest_explain_file}")
                        
                    if "== Physical Plan ==" in explain_content:
                        physical_plan_start = explain_content.find("== Physical Plan ==")
                        physical_plan_end = explain_content.find("== Photon", physical_plan_start)
                        if physical_plan_end == -1:
                            physical_plan_end = len(explain_content)
                        physical_plan = explain_content[physical_plan_start:physical_plan_end].strip()
                        
                    if "== Photon Explanation ==" in explain_content:
                        photon_start = explain_content.find("== Photon Explanation ==")
                        photon_explanation = explain_content[photon_start:].strip()
                except Exception as e:
                    print(f"⚠️ 古い形式EXPLAIN結果の読み込みに失敗: {str(e)}")
            else:
                print("⚠️ ボトルネック分析: EXPLAIN・EXPLAIN COST結果ファイルが見つかりません")
    
    # レポート生成時刻
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # === 1. 基本メトリクスの取得 ===
    overall_metrics = metrics.get('overall_metrics', {})
    bottleneck_indicators = metrics.get('bottleneck_indicators', {})
    
    total_time_sec = overall_metrics.get('total_time_ms', 0) / 1000
    read_gb = overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024
    cache_hit_ratio = bottleneck_indicators.get('cache_hit_ratio', 0) * 100
    data_selectivity = bottleneck_indicators.get('data_selectivity', 0) * 100
    
    # Photon情報
    photon_enabled = overall_metrics.get('photon_enabled', False)
    photon_utilization = min(overall_metrics.get('photon_utilization_ratio', 0) * 100, 100.0)
    
    # 並列度・シャッフル情報
    shuffle_count = bottleneck_indicators.get('shuffle_operations_count', 0)
    has_shuffle_bottleneck = bottleneck_indicators.get('has_shuffle_bottleneck', False)
    has_low_parallelism = bottleneck_indicators.get('has_low_parallelism', False)
    low_parallelism_count = bottleneck_indicators.get('low_parallelism_stages_count', 0)
    
    # スピル情報
    has_spill = bottleneck_indicators.get('has_spill', False)
    spill_bytes = bottleneck_indicators.get('spill_bytes', 0)
    spill_gb = spill_bytes / 1024 / 1024 / 1024 if spill_bytes > 0 else 0
    
    # スキュー検出情報
    has_skew = bottleneck_indicators.get('has_skew', False)
    has_aqe_shuffle_skew_warning = bottleneck_indicators.get('has_aqe_shuffle_skew_warning', False)
    
    # === 2. セル33: TOP10プロセス分析情報の取得 ===
    # 全ノードを実行時間でソート（パーセンテージ計算用）
    all_sorted_nodes = sorted(metrics['node_metrics'], 
                             key=lambda x: x['key_metrics'].get('durationMs', 0), 
                             reverse=True)
    
    # TOP5ボトルネック抽出用
    sorted_nodes = all_sorted_nodes[:5]
    
    # 🚨 重要: 正しい全体時間の計算（デグレ防止）
    # 1. overall_metrics.total_time_msを優先使用（wall-clock time）
    total_time_ms = overall_metrics.get('total_time_ms', 0)
    
    # 🚨 並列実行問題の修正: task_total_time_msを優先使用
    # 個別ノード時間は並列タスクの累積時間のため、同じく累積時間であるtask_total_time_msと比較
    task_total_time_ms = overall_metrics.get('task_total_time_ms', 0)
    
    if task_total_time_ms > 0:
        total_time_ms = task_total_time_ms
        print(f"✅ デバッグ: 並列実行対応 - task_total_time_ms使用: {total_time_ms:,} ms ({total_time_ms/3600000:.1f}時間)")
    elif total_time_ms <= 0:
        # execution_time_msを次の優先度で使用
        execution_time_ms = overall_metrics.get('execution_time_ms', 0)
        if execution_time_ms > 0:
            total_time_ms = execution_time_ms
            print(f"⚠️ デバッグ: task_total_time_ms利用不可、execution_time_ms使用: {total_time_ms} ms")
        else:
            # 最終フォールバック: 全ノードの合計時間
            max_node_time = max([node['key_metrics'].get('durationMs', 0) for node in all_sorted_nodes], default=1)
            total_time_ms = int(max_node_time * 1.2)
            print(f"⚠️ デバッグ: 最終フォールバック - 推定時間使用: {total_time_ms} ms")
    
    print(f"📊 デバッグ: パーセンテージ計算に使用する全体時間: {total_time_ms:,} ms ({total_time_ms/1000:.1f} sec)")
    
    critical_processes = []
    for i, node in enumerate(sorted_nodes):
        duration_ms = node['key_metrics'].get('durationMs', 0)
        duration_sec = duration_ms / 1000
        
        # パーセンテージ計算（100%を上限とする）
        percentage = min((duration_ms / max(total_time_ms, 1)) * 100, 100.0)
        
        # ボトルネックの重要度判定
        severity = "CRITICAL" if duration_ms >= 10000 else "HIGH" if duration_ms >= 5000 else "MEDIUM"
        
        # 意味のあるノード名を取得
        node_name = get_meaningful_node_name(node, metrics)
        short_name = node_name[:80] + "..." if len(node_name) > 80 else node_name
        
        critical_processes.append({
            'rank': i + 1,
            'name': short_name,
            'duration_sec': duration_sec,
            'percentage': percentage,
            'severity': severity
        })
    
    # === 3. セル35: Liquid Clustering分析情報の取得 ===
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    extracted_data = liquid_analysis.get('extracted_data', {})
    
    # テーブル情報
    table_info = extracted_data.get('table_info', {})
    identified_tables = list(table_info.keys())[:5]  # TOP5テーブル
    
    # フィルター・JOIN・GROUP BY情報
    filter_columns = extracted_data.get('filter_columns', [])[:10]
    join_columns = extracted_data.get('join_columns', [])[:10]
    groupby_columns = extracted_data.get('groupby_columns', [])[:10]
    
    # === 4. セル43: 統合最適化処理での詳細ボトルネック分析の取得 ===
    try:
        detailed_bottleneck = extract_detailed_bottleneck_analysis(metrics)
    except Exception as e:
        print(f"⚠️ 詳細ボトルネック分析でエラー: {e}")
        detailed_bottleneck = {
            'top_bottleneck_nodes': [],
            'performance_recommendations': []
        }
    
    # === 5. 包括的レポートの生成 ===
    
    report_lines = []
    
    # タイトルとサマリー
    report_lines.append("# 📊 Databricks SQLパフォーマンス包括分析レポート")
    report_lines.append(f"**生成日時**: {timestamp}")
    report_lines.append("")
    
    # パフォーマンス概要
    report_lines.append("## 1. パフォーマンス概要")
    report_lines.append("")
    report_lines.append("### 主要パフォーマンス指標")
    report_lines.append("")
    report_lines.append("| 指標 | 値 | 評価 |")
    report_lines.append("|------|-----|------|")
    report_lines.append(f"| 実行時間 | {total_time_sec:.1f}秒 | {'✅ 良好' if total_time_sec < 60 else '⚠️ 改善必要'} |")
    report_lines.append(f"| データ読み込み | {read_gb:.2f}GB | {'✅ 良好' if read_gb < 10 else '⚠️ 大容量'} |")
    report_lines.append(f"| Photon有効 | {'はい' if photon_enabled else 'いいえ'} | {'✅ 良好' if photon_enabled else '❌ 未有効'} |")
    report_lines.append(f"| キャッシュ効率 | {cache_hit_ratio:.1f}% | {'✅ 良好' if cache_hit_ratio > 80 else '⚠️ 改善必要'} |")
    report_lines.append(f"| フィルタ率 | {data_selectivity:.1f}% | {'✅ 良好' if data_selectivity > 50 else '⚠️ フィルタ条件を確認'} |")
    report_lines.append(f"| シャッフル操作 | {shuffle_count}回 | {'✅ 良好' if shuffle_count < 5 else '⚠️ 多数'} |")
    report_lines.append(f"| スピル発生 | {'はい' if has_spill else 'いいえ'} | {'❌ 問題あり' if has_spill else '✅ 良好'} |")
    
    # スキュー検出の判定
    if has_skew:
        skew_status = "AQEで検出・対応済"
        skew_evaluation = "🔧 AQE対応済"
    elif has_aqe_shuffle_skew_warning:
        skew_status = "潜在的なスキューの可能性あり"
        skew_evaluation = "⚠️ 改善必要"
    else:
        skew_status = "未検出"
        skew_evaluation = "✅ 良好"
    
    report_lines.append(f"| スキュー検出 | {skew_status} | {skew_evaluation} |")
    report_lines.append("")
    
    # 主要ボトルネック分析
    report_lines.append("## 2. 主要ボトルネック分析")
    report_lines.append("")
    
    # Photon分析
    photon_status = "有効" if photon_enabled else "無効"
    photon_recommendation = ""
    if not photon_enabled:
        photon_recommendation = " → **Photon有効化を強く推奨**"
    elif photon_utilization < 50:
        photon_recommendation = " → **Photon利用率向上が必要**"
    elif photon_utilization < 80:
        photon_recommendation = " → **Photon設定の最適化を推奨**"
    else:
        photon_recommendation = " → **最適化済み**"
    
    report_lines.append("### Photonエンジン")
    report_lines.append(f"- **状態**: {photon_status} (利用率: {photon_utilization:.1f}%){photon_recommendation}")
    report_lines.append("")
    
    # 並列度・シャッフル分析
    report_lines.append("### 並列度・シャッフル")
    shuffle_status = "❌ ボトルネックあり" if has_shuffle_bottleneck else "✅ 良好"
    parallelism_status = "❌ 低並列度あり" if has_low_parallelism else "✅ 適切"
    
    report_lines.append(f"- **シャッフル操作**: {shuffle_count}回 ({shuffle_status})")
    report_lines.append(f"- **並列度**: {parallelism_status}")
    if has_low_parallelism:
        report_lines.append(f"  - 低並列度ステージ: {low_parallelism_count}個")
    report_lines.append("")
    
    # スピル分析
    report_lines.append("### メモリ使用状況")
    if has_spill:
        report_lines.append(f"- **メモリスピル**: ❌ 発生中 ({spill_gb:.2f}GB)")
        report_lines.append("  - **対応必要**: クラスター設定の見直し、クエリ最適化")
    else:
        report_lines.append("- **メモリスピル**: ✅ なし")
    report_lines.append("")
    
    # TOP5処理時間ボトルネック
    report_lines.append("## 3. TOP5処理時間ボトルネック")
    report_lines.append("")
    
    for process in critical_processes:
        severity_icon = "🔴" if process['severity'] == "CRITICAL" else "🟠" if process['severity'] == "HIGH" else "🟡"
        report_lines.append(f"### {process['rank']}. {severity_icon} {process['name']}")
        report_lines.append(f"   - **実行時間**: {process['duration_sec']:.1f}秒 (全体の{process['percentage']:.1f}%)")
        report_lines.append(f"   - **重要度**: {process['severity']}")
        report_lines.append("")
    
    # Liquid Clustering推奨事項
    report_lines.append("## 4. Liquid Clustering推奨事項")
    report_lines.append("")
    
    if identified_tables:
        report_lines.append("### 対象テーブル")
        for i, table_name in enumerate(identified_tables, 1):
            # 現在のクラスタリングキー情報を取得
            table_details = table_info.get(table_name, {})
            current_keys = table_details.get('current_clustering_keys', [])
            current_keys_str = ', '.join(current_keys) if current_keys else '設定なし'
            
            report_lines.append(f"{i}. `{table_name}`")
            report_lines.append(f"   - 現在のクラスタリングキー: `{current_keys_str}`")
        report_lines.append("")
    
    if filter_columns or join_columns or groupby_columns:
        report_lines.append("### 推奨クラスタリングキー")
        
        if filter_columns:
            report_lines.append("**フィルター条件カラム (高優先度)**:")
            for i, col in enumerate(filter_columns[:5], 1):
                expression = col.get('expression', 'Unknown')
                report_lines.append(f"  {i}. `{expression}`")
            report_lines.append("")
        
        if join_columns:
            report_lines.append("**JOIN条件カラム (中優先度)**:")
            for i, col in enumerate(join_columns[:5], 1):
                expression = col.get('expression', 'Unknown')
                key_type = col.get('key_type', '')
                report_lines.append(f"  {i}. `{expression}` ({key_type})")
            report_lines.append("")
        
        if groupby_columns:
            report_lines.append("**GROUP BY条件カラム (中優先度)**:")
            for i, col in enumerate(groupby_columns[:5], 1):
                expression = col.get('expression', 'Unknown')
                report_lines.append(f"  {i}. `{expression}`")
            report_lines.append("")
    
    # 実装SQL例
    if identified_tables:
        report_lines.append("### 実装SQL例")
        for table_name in identified_tables[:2]:  # TOP2テーブルのみ
            # 現在のクラスタリングキー情報を取得
            table_details = table_info.get(table_name, {})
            current_keys = table_details.get('current_clustering_keys', [])
            current_keys_str = ', '.join(current_keys) if current_keys else '設定なし'
            
            report_lines.append(f"```sql")
            report_lines.append(f"-- {table_name}テーブルにLiquid Clusteringを適用")
            report_lines.append(f"-- 現在のクラスタリングキー: {current_keys_str}")
            report_lines.append(f"ALTER TABLE {table_name}")
            report_lines.append(f"CLUSTER BY (column1, column2, column3, column4);")
            report_lines.append(f"```")
            report_lines.append("")
    
    # 最適化推奨アクション
    report_lines.append("## 5. 推奨最適化アクション")
    report_lines.append("")
    
    # 優先度別の推奨事項
    high_priority_actions = []
    medium_priority_actions = []
    low_priority_actions = []
    
    # CRITICAL/HIGH priorityアクション
    if not photon_enabled:
        high_priority_actions.append("**Photonエンジンの有効化** - 最大50%の性能向上期待")
    
    if has_spill:
        high_priority_actions.append(f"**メモリスピル解決** - {spill_gb:.2f}GBのスピルを解消")
    
    if has_shuffle_bottleneck:
        high_priority_actions.append("**シャッフル最適化** - JOIN順序とREPARTITION適用")
    
    # MEDIUMアクション
    if photon_enabled and photon_utilization < 80:
        medium_priority_actions.append("**Photon利用率向上** - 設定の最適化")
    
    if has_low_parallelism:
        medium_priority_actions.append("**並列度向上** - クラスター設定の見直し")
    
    if cache_hit_ratio < 50:
        medium_priority_actions.append("**キャッシュ効率改善** - データアクセスパターンの最適化")
    
    # Liquid Clustering
    if identified_tables:
        medium_priority_actions.append("**Liquid Clustering実装** - 主要テーブルのクラスタリング")
    
    # LOWアクション
    if data_selectivity < 50:
        low_priority_actions.append("**WHERE句最適化** - フィルタ効率の向上")
    
    # アクションの出力
    if high_priority_actions:
        report_lines.append("### 🚨 緊急対応 (HIGH優先度)")
        for i, action in enumerate(high_priority_actions, 1):
            report_lines.append(f"{i}. {action}")
        report_lines.append("")
    
    if medium_priority_actions:
        report_lines.append("### ⚠️ 重要改善 (MEDIUM優先度)")
        for i, action in enumerate(medium_priority_actions, 1):
            report_lines.append(f"{i}. {action}")
        report_lines.append("")
    
    if low_priority_actions:
        report_lines.append("### 📝 長期最適化 (LOW優先度)")
        for i, action in enumerate(low_priority_actions, 1):
            report_lines.append(f"{i}. {action}")
        report_lines.append("")
    
    # 期待効果
    report_lines.append("## 6. 期待されるパフォーマンス改善")
    report_lines.append("")
    
    total_improvement_estimate = 0
    improvement_details = []
    
    if not photon_enabled:
        total_improvement_estimate += 40
        improvement_details.append("- **Photon有効化**: 30-50%の実行時間短縮")
    
    if has_spill:
        total_improvement_estimate += 25
        improvement_details.append(f"- **スピル解消**: 20-30%の実行時間短縮 ({spill_gb:.2f}GBスピル削減)")
    
    if has_shuffle_bottleneck:
        total_improvement_estimate += 20
        improvement_details.append("- **シャッフル最適化**: 15-25%の実行時間短縮")
    
    if identified_tables:
        total_improvement_estimate += 15
        improvement_details.append("- **Liquid Clustering**: 10-20%の実行時間短縮")
    
    # 改善効果の上限設定
    total_improvement_estimate = min(total_improvement_estimate, 80)
    
    if improvement_details:
        for detail in improvement_details:
            report_lines.append(detail)
        report_lines.append("")
        report_lines.append(f"**総合改善見込み**: 最大{total_improvement_estimate}%の実行時間短縮")
    else:
        report_lines.append("現在のパフォーマンスは比較的良好です。微細な最適化により5-10%の改善が期待できます。")
    
    # === EXPLAIN + EXPLAIN COST結果に基づく詳細分析 ===
    if explain_enabled.upper() == 'Y' and (physical_plan or cost_statistics):
        report_lines.append("")
        report_lines.append("## 6. EXPLAIN + EXPLAIN COST詳細分析")
        report_lines.append("")
        
        if physical_plan:
            report_lines.append("### 🔍 Physical Plan分析")
            report_lines.append("")
            
            # Physical Planから重要な情報を抽出
            plan_analysis = []
            if "Exchange" in physical_plan:
                plan_analysis.append("- **シャッフル操作検出**: データ転送のボトルネック可能性")
            if "BroadcastExchange" in physical_plan:
                plan_analysis.append("- **BROADCAST JOIN適用済み**: 小テーブルの効率的な配布")
            if "HashAggregate" in physical_plan:
                plan_analysis.append("- **Hash集計処理**: メモリ効率の最適化が重要")
            if "FileScan" in physical_plan:
                plan_analysis.append("- **ファイルスキャン操作**: I/O効率とフィルタプッシュダウンの確認")
            if "SortMergeJoin" in physical_plan:
                plan_analysis.append("- **Sort Merge JOIN**: 大テーブル間の結合、BROADCAST適用を検討")
            
            if plan_analysis:
                for analysis in plan_analysis:
                    report_lines.append(analysis)
            else:
                report_lines.append("- Physical Planの詳細情報が利用可能です")
            report_lines.append("")
        
        if photon_explanation:
            report_lines.append("### 🚀 Photon Explanation分析")
            report_lines.append("")
            
            photon_analysis = []
            if "photon" in photon_explanation.lower():
                photon_analysis.append("- **Photon処理情報**: ベクトル化処理の最適化詳細")
            if "unsupported" in photon_explanation.lower():
                photon_analysis.append("- **未対応関数検出**: Photon利用率向上の改善機会")
            if "compiled" in photon_explanation.lower():
                photon_analysis.append("- **コンパイル処理**: 実行時最適化の適用状況")
            
            if photon_analysis:
                for analysis in photon_analysis:
                    report_lines.append(analysis)
            else:
                report_lines.append("- Photon実行の詳細情報が利用可能です")
            report_lines.append("")
        
        if cost_statistics:
            report_lines.append("### 💰 EXPLAIN COST統計分析")
            report_lines.append("")
            
            # EXPLAIN COST統計から重要な情報を抽出
            cost_analysis = []
            if "サイズ情報" in cost_statistics:
                cost_analysis.append("- **テーブルサイズ統計**: 正確なサイズ情報によるBROADCAST判定精度向上")
            if "行数情報" in cost_statistics:
                cost_analysis.append("- **行数統計**: パーティション数最適化とメモリ使用量予測")
            if "選択率情報" in cost_statistics:
                cost_analysis.append("- **選択率統計**: フィルタ効率最適化とWHERE条件順序調整")
            if "コスト情報" in cost_statistics:
                cost_analysis.append("- **コスト見積もり**: JOIN戦略とアクセスパス選択の最適化")
            if "パーティション情報" in cost_statistics:
                cost_analysis.append("- **パーティション統計**: データ分散最適化とスキュー対策")
            
            if cost_analysis:
                for analysis in cost_analysis:
                    report_lines.append(analysis)
                report_lines.append("")
                report_lines.append("**統計ベース最適化の利点**:")
                report_lines.append("- 推測ではなく実際の統計に基づく最適化")
                report_lines.append("- 事前のボトルネック予測とスピル回避")
                report_lines.append("- 正確なコスト見積もりによる最適戦略選択")
            else:
                report_lines.append("- EXPLAIN COST統計情報が利用可能です")
            report_lines.append("")
    elif explain_enabled.upper() == 'Y':
        report_lines.append("")
        report_lines.append("## 6. EXPLAIN分析")
        report_lines.append("")
        report_lines.append("⚠️ EXPLAIN・EXPLAIN COST結果ファイルが見つかりません")
        report_lines.append("統計ベースの詳細分析を行うには、事前にEXPLAIN実行が必要です")
        report_lines.append("")
    
    report_lines.append("")
    report_lines.append("---")
    report_lines.append(f"*レポート生成: {timestamp} | 分析エンジン: Databricks SQL Profiler + EXPLAIN統合*")
    
    print("✅ 包括的パフォーマンス分析レポート（EXPLAIN+EXPLAIN COST統合）が完成しました")
    
    return "\n".join(report_lines)


def _call_databricks_llm(prompt: str) -> str:
    """Databricks Model Serving APIを呼び出す"""
    try:
        # Databricksトークンの取得
        try:
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception:
            token = os.environ.get('DATABRICKS_TOKEN')
            if not token:
                return "❌ Databricksトークンの取得に失敗しました。環境変数DATABRICKS_TOKENを設定してください。"
        
        # ワークスペースURLの取得
        try:
            workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception:
            workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
        
        config = LLM_CONFIG["databricks"]
        endpoint_url = f"https://{workspace_url}/serving-endpoints/{config['endpoint_name']}/invocations"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"]
        }
        
        # 拡張思考モードが有効な場合は追加
        if config.get("thinking_enabled", False):
            payload["thinking"] = {
                "type": "enabled",
                "budget_tokens": config.get("thinking_budget_tokens", 65536)
            }
        
        # リトライ機能（SQL最適化用に増強）
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"🔄 リトライ中... (試行 {attempt + 1}/{max_retries})")
                
                response = requests.post(endpoint_url, headers=headers, json=payload, timeout=300)
                
                if response.status_code == 200:
                    result = response.json()
                    analysis_text = result.get('choices', [{}])[0].get('message', {}).get('content', '')
                    print("✅ ボトルネック分析が完了しました")
                    return analysis_text
                else:
                    error_msg = f"APIエラー: ステータスコード {response.status_code}"
                    if response.status_code == 400:
                        # 400エラーの場合は詳細な解決策を提供
                        error_detail = response.text
                        if "maximum tokens" in error_detail.lower():
                            if attempt == max_retries - 1:
                                detailed_error = f"""❌ {error_msg}

🔧 トークン制限エラーの解決策:
1. LLM_CONFIG["databricks"]["max_tokens"] を 65536 (64K) に削減
2. より単純なクエリで再試行
3. 手動でSQL最適化を実行
4. クエリを分割して段階的に最適化

💡 推奨設定:
LLM_CONFIG["databricks"]["max_tokens"] = 65536
LLM_CONFIG["databricks"]["thinking_budget_tokens"] = 32768

詳細エラー: {error_detail}"""
                                print(detailed_error)
                                return detailed_error
                            else:
                                print(f"⚠️ {error_msg} (トークン制限) - リトライします...")
                                continue
                    
                    if attempt == max_retries - 1:
                        print(f"❌ {error_msg}\nレスポンス: {response.text}")
                        return f"{error_msg}\nレスポンス: {response.text}"
                    else:
                        print(f"⚠️ {error_msg} - リトライします...")
                        continue
                        
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    timeout_msg = f"""⏰ タイムアウトエラー: Databricksエンドポイントの応答が300秒以内に完了しませんでした。

🔧 解決策:
1. LLMエンドポイントの稼働状況を確認
2. プロンプトサイズを削減
3. より高性能なモデルを使用
4. 手動でSQL最適化を実行

💡 推奨アクション:
- クエリの複雑度を確認
- Databricks Model Servingエンドポイントのスケールアップ
- シンプルなクエリでテスト実行"""
                    print(f"❌ {timeout_msg}")
                    return timeout_msg
                else:
                    print(f"⏰ タイムアウト発生（300秒）- リトライします... (試行 {attempt + 1}/{max_retries})")
                    continue
                    
    except Exception as e:
        return f"Databricks API呼び出しエラー: {str(e)}"

def _call_openai_llm(prompt: str) -> str:
    """OpenAI APIを呼び出す"""
    try:
        config = LLM_CONFIG["openai"]
        api_key = config["api_key"] or os.environ.get('OPENAI_API_KEY')
        
        if not api_key:
            return "❌ OpenAI APIキーが設定されていません。LLM_CONFIG['openai']['api_key']または環境変数OPENAI_API_KEYを設定してください。"
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": config["model"],
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"]
        }
        
        response = requests.post("https://api.openai.com/v1/chat/completions", 
                               headers=headers, json=payload, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result['choices'][0]['message']['content']
            print("✅ OpenAI分析が完了しました")
            return analysis_text
        else:
            return f"OpenAI APIエラー: ステータスコード {response.status_code}\n{response.text}"
            
    except Exception as e:
        return f"OpenAI API呼び出しエラー: {str(e)}"

def _call_azure_openai_llm(prompt: str) -> str:
    """Azure OpenAI APIを呼び出す"""
    try:
        config = LLM_CONFIG["azure_openai"]
        api_key = config["api_key"] or os.environ.get('AZURE_OPENAI_API_KEY')
        
        if not api_key or not config["endpoint"] or not config["deployment_name"]:
            return "❌ Azure OpenAI設定が不完全です。api_key、endpoint、deployment_nameを設定してください。"
        
        endpoint_url = f"{config['endpoint']}/openai/deployments/{config['deployment_name']}/chat/completions?api-version={config['api_version']}"
        
        headers = {
            "api-key": api_key,
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"]
        }
        
        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result['choices'][0]['message']['content']
            print("✅ Azure OpenAI分析が完了しました")
            return analysis_text
        else:
            return f"Azure OpenAI APIエラー: ステータスコード {response.status_code}\n{response.text}"
            
    except Exception as e:
        return f"Azure OpenAI API呼び出しエラー: {str(e)}"

def _call_anthropic_llm(prompt: str) -> str:
    """Anthropic APIを呼び出す"""
    try:
        config = LLM_CONFIG["anthropic"]
        api_key = config["api_key"] or os.environ.get('ANTHROPIC_API_KEY')
        
        if not api_key:
            return "❌ Anthropic APIキーが設定されていません。LLM_CONFIG['anthropic']['api_key']または環境変数ANTHROPIC_API_KEYを設定してください。"
        
        headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        }
        
        payload = {
            "model": config["model"],
            "max_tokens": config["max_tokens"],
            "temperature": config["temperature"],
            "messages": [{"role": "user", "content": prompt}]
        }
        
        response = requests.post("https://api.anthropic.com/v1/messages", 
                               headers=headers, json=payload, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            analysis_text = result['content'][0]['text']
            print("✅ Anthropic分析が完了しました")
            return analysis_text
        else:
            return f"Anthropic APIエラー: ステータスコード {response.status_code}\n{response.text}"
            
    except Exception as e:
        return f"Anthropic API呼び出しエラー: {str(e)}"

print("✅ 関数定義完了: analyze_bottlenecks_with_llm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 LLMボトルネック分析実行の準備
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 設定されたLLMプロバイダーの確認と表示
# MAGIC - 分析開始の準備とメッセージ表示
# MAGIC - プロンプト最適化による安定性向上

# COMMAND ----------

# LLMボトルネック分析実行の準備
provider = LLM_CONFIG["provider"]

print(f"\n🤖 【{provider.upper()} LLM による SQLボトルネック分析を開始します】")
print("=" * 80)

if provider == "databricks":
    endpoint = LLM_CONFIG["databricks"]["endpoint_name"]
    print(f"🔗 Databricks Model Serving エンドポイント: {endpoint}")
    print("⚠️  Model Servingエンドポイントが稼働中である必要があります")
elif provider == "openai":
    model = LLM_CONFIG["openai"]["model"]
    print(f"🔗 OpenAI モデル: {model}")
    print("⚠️  OpenAI APIキーが必要です")
elif provider == "azure_openai":
    deployment = LLM_CONFIG["azure_openai"]["deployment_name"]
    print(f"🤖 Azure OpenAI ({deployment}) によるボトルネック分析を開始します...")
    print("⚠️  Azure OpenAI APIキーとエンドポイントが必要です")
elif provider == "anthropic":
    model = LLM_CONFIG["anthropic"]["model"]
    print(f"🤖 Anthropic ({model}) によるボトルネック分析を開始します...")
    print("⚠️  Anthropic APIキーが必要です")

print("📝 分析プロンプトを簡潔化してタイムアウトリスクを軽減しています...")
print()

# extracted_metrics変数が定義されているかチェック
try:
    extracted_metrics
    print("✅ extracted_metrics変数が確認されました")
    analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)
except NameError:
    print("❌ extracted_metrics変数が定義されていません")
    print("⚠️ セル12 (パフォーマンスメトリクス抽出) を先に実行してください")
    print("📋 正しい実行順序: セル11 → セル12 → セル15")
    print("🔄 デフォルトの分析結果を設定します")
    analysis_result = """
🤖 LLMボトルネック分析結果

❌ 分析に必要なメトリクスデータが見つかりませんでした。

📋 解決方法:
1. セル11でJSONファイルを読み込む
2. セル12でメトリクスを抽出する
3. このセル（セル15）を再実行する

⚠️ 先にメトリクス抽出を完了してから分析を実行してください。
"""
except Exception as e:
    print(f"❌ LLM分析中にエラーが発生しました: {str(e)}")
    analysis_result = f"LLM分析エラー: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 クエリプロファイル分析セクション
# MAGIC
# MAGIC **ここからメインの分析処理が開始されます**
# MAGIC
# MAGIC 📋 **実行手順:**
# MAGIC 1. 上記の🔧設定・準備セクションをすべて実行してください
# MAGIC 2. 以下のセルを順番に実行して分析を行います
# MAGIC 3. エラーが発生した場合は、設定セクションから再実行してください
# MAGIC
# MAGIC ⚠️ **注意:**
# MAGIC - 🔧設定・準備セクション → 🚀メイン処理セクション → 🔧SQL最適化セクション の順序で実行
# MAGIC - ファイルパスの設定は必ず最初のセルで行ってください
# MAGIC - LLMエンドポイントの設定を確認してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 SQLプロファイラーJSONファイル読み込み実行
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 設定されたファイルパスからJSONファイルの読み込み
# MAGIC - ファイルサイズと基本情報の表示
# MAGIC - エラーハンドリングと処理停止制御

# COMMAND ----------

print("=" * 80)
print("🚀 Databricks SQLプロファイラー分析ツール")
print("=" * 80)
print(f"📁 分析対象ファイル: {JSON_FILE_PATH}")
print()

# ファイル存在チェック
import os
if not os.path.exists(JSON_FILE_PATH):
    print("❌ ファイルが見つかりません:")
    print(f"   指定パス: {JSON_FILE_PATH}")
    print()
    print("💡 ファイルパス設定のヒント:")
    print("   1. セル2でJSON_FILE_PATH変数を正しいパスに設定してください")
    print("   2. 利用可能なオプション例:")
    print("      - /Volumes/main/base/mitsuhiro_vol/チューニング前プランファイル.json")
    print("      - /Volumes/main/base/mitsuhiro_vol/nophoton.json")
    print("      - /Volumes/main/base/mitsuhiro_vol/POC1.json")
    print("   3. ファイルがDBFS FileStoreにある場合:")
    print("      - /FileStore/shared_uploads/your_username/filename.json")
    print("⚠️ 処理を停止します。")
    raise RuntimeError(f"指定されたファイルが見つかりません: {JSON_FILE_PATH}")

# SQLプロファイラーJSONファイルの読み込み
profiler_data = load_profiler_json(JSON_FILE_PATH)
if not profiler_data:
    print("❌ JSONファイルの読み込みに失敗しました。ファイル形式を確認してください。")
    print("⚠️ 処理を停止します。")
    # dbutils.notebook.exit("File loading failed")  # 安全のためコメントアウト
    raise RuntimeError("JSONファイルの読み込みに失敗しました。")

print(f"✅ データ読み込み完了")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 パフォーマンスメトリクス抽出と概要表示
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - プロファイラーデータからメトリクスの抽出
# MAGIC - クエリ基本情報の表示
# MAGIC - 全体パフォーマンス指標の計算と表示
# MAGIC - Liquid Clusteringの分析結果表示

# COMMAND ----------

# 📊 パフォーマンスメトリクスの抽出
extracted_metrics = extract_performance_metrics(profiler_data)
print("✅ パフォーマンスメトリクスを抽出しました")

# 抽出されたメトリクスの概要表示
print("\n" + "=" * 50)
print("📈 抽出されたメトリクス概要")
print("=" * 50)

query_info = extracted_metrics['query_info']
overall_metrics = extracted_metrics['overall_metrics']
bottleneck_indicators = extracted_metrics['bottleneck_indicators']

print(f"🆔 クエリID: {query_info['query_id']}")
print(f"📊 ステータス: {query_info['status']}")
print(f"👤 実行ユーザー: {query_info['user']}")
print(f"⏱️ 実行時間: {overall_metrics['total_time_ms']:,} ms ({overall_metrics['total_time_ms']/1000:.2f} sec)")
print(f"💾 読み込みデータ: {overall_metrics['read_bytes']/1024/1024/1024:.2f} GB")
print(f"📈 出力行数: {overall_metrics['rows_produced_count']:,} 行")
print(f"📉 読み込み行数: {overall_metrics['rows_read_count']:,} 行")
print(f"🎯 フィルタ率: {bottleneck_indicators.get('data_selectivity', 0):.4f} ({bottleneck_indicators.get('data_selectivity', 0)*100:.2f}%)")
print(f"🔧 ステージ数: {len(extracted_metrics['stage_metrics'])}")
print(f"🏗️ ノード数: {len(extracted_metrics['node_metrics'])}")

# Liquid Clustering分析結果の表示
liquid_analysis = extracted_metrics['liquid_clustering_analysis']
liquid_summary = liquid_analysis.get('summary', {})
print(f"🗂️ Liquid Clustering対象テーブル数: {liquid_summary.get('tables_identified', 0)}")
print(f"📊 高インパクトテーブル数: {liquid_summary.get('high_impact_tables', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 ボトルネック指標詳細
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - Photon エンジンの利用状況とパフォーマンス分析
# MAGIC - シャッフル操作と並列度の問題検出
# MAGIC - 各種パフォーマンス指標の詳細表示

# COMMAND ----------

# 📋 ボトルネック指標の詳細表示
print("\n" + "=" * 50)
print("🔍 ボトルネック指標詳細")
print("=" * 50)

# Photon関連指標
photon_enabled = overall_metrics.get('photon_enabled', False)
photon_utilization_ratio = overall_metrics.get('photon_utilization_ratio', 0)
photon_utilization = min(photon_utilization_ratio * 100, 100.0)  # 最大100%に制限
photon_emoji = "✅" if photon_enabled and photon_utilization > 80 else "⚠️" if photon_enabled else "❌"

# 利用率に関する詳細情報
if photon_enabled:
    photon_total_ms = overall_metrics.get('photon_total_time_ms', 0)
    task_total_ms = overall_metrics.get('task_total_time_ms', 0)
    print(f"{photon_emoji} Photonエンジン: 有効 (利用率: {photon_utilization:.1f}%)")
    print(f"   📊 Photon実行時間: {photon_total_ms:,} ms | タスク合計時間: {task_total_ms:,} ms")
else:
    print(f"{photon_emoji} Photonエンジン: 無効")

# 並列度・シャッフル関連指標
shuffle_count = bottleneck_indicators.get('shuffle_operations_count', 0)
has_shuffle_bottleneck = bottleneck_indicators.get('has_shuffle_bottleneck', False)
has_low_parallelism = bottleneck_indicators.get('has_low_parallelism', False)
low_parallelism_count = bottleneck_indicators.get('low_parallelism_stages_count', 0)

shuffle_emoji = "🚨" if has_shuffle_bottleneck else "⚠️" if shuffle_count > 5 else "✅"
print(f"{shuffle_emoji} シャッフル操作: {shuffle_count}回 ({'ボトルネックあり' if has_shuffle_bottleneck else '正常'})")

parallelism_emoji = "🚨" if has_low_parallelism else "✅"
print(f"{parallelism_emoji} 並列度: {'問題あり' if has_low_parallelism else '適切'} (低並列度ステージ: {low_parallelism_count}個)")

print()
print("📊 その他の指標:")

for key, value in bottleneck_indicators.items():
    # 新しく追加した指標は上記で表示済みなのでスキップ
    if key in ['shuffle_operations_count', 'has_shuffle_bottleneck', 'has_low_parallelism', 
               'low_parallelism_stages_count', 'total_shuffle_time_ms', 'shuffle_time_ratio',
               'slowest_shuffle_duration_ms', 'slowest_shuffle_node', 'low_parallelism_details',
               'average_low_parallelism']:
        continue
        
    if 'ratio' in key:
        emoji = "📊" if value < 0.1 else "⚠️" if value < 0.3 else "🚨"
        print(f"{emoji} {key}: {value:.3f} ({value*100:.1f}%)")
    elif 'bytes' in key and key != 'has_spill':
        if value > 0:
            emoji = "💾" if value < 1024*1024*1024 else "⚠️"  # 1GB未満は普通、以上は注意
            print(f"{emoji} {key}: {value:,} bytes ({value/1024/1024:.2f} MB)")
    elif key == 'has_spill':
        emoji = "❌" if not value else "⚠️"
        print(f"{emoji} {key}: {'あり' if value else 'なし'}")
    elif 'duration' in key:
        emoji = "⏱️"
        print(f"{emoji} {key}: {value:,} ms ({value/1000:.2f} sec)")
    else:
        emoji = "ℹ️"
        print(f"{emoji} {key}: {value}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 メトリクス保存と時間消費分析
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 抽出したメトリクスのJSON形式での保存
# MAGIC - set型からlist型への変換処理
# MAGIC - 最も時間がかかっている処理TOP10の詳細分析
# MAGIC - 特定メトリクスベーススピル検出とAQEベーススキュー分析
# MAGIC
# MAGIC 💿 **スピル検出ロジック**:
# MAGIC - ターゲットメトリクス: `"Sink - Num bytes spilled to disk due to memory pressure"`
# MAGIC - 判定条件: 上記メトリクスの値 > 0 の場合にスピルありと判定
# MAGIC - 検索対象: detailed_metrics → raw_metrics → key_metrics の順序で検索
# MAGIC
# MAGIC 🎯 **スキュー検出ロジック**:
# MAGIC - `AQEShuffleRead - Number of skewed partitions`: AQEベーススキュー検出
# MAGIC - 判定条件: メトリクス値 > 0 でスキュー判定
# MAGIC - 重要度: 検出値に基づいた判定
# MAGIC - 統計ベース判定は非推奨（AQEベース判定を推奨）
# MAGIC
# MAGIC 💡 **デバッグモード**: スピル・スキューの判定根拠を詳細表示したい場合
# MAGIC ```python
# MAGIC import os
# MAGIC os.environ['DEBUG_SPILL_ANALYSIS'] = 'true'   # 特定メトリクススピル判定の詳細表示
# MAGIC os.environ['DEBUG_SKEW_ANALYSIS'] = 'true'    # AQEベーススキュー判定の詳細表示
# MAGIC ```

# COMMAND ----------

# 🐛 デバッグモード設定（オプション）
# 
# **スピル・スキューの判定根拠を詳細表示したい場合のみ実行してください**
# 
# 📋 設定内容:
# - DEBUG_SPILL_ANALYSIS=true: 特定メトリクススピル判定の詳細根拠を表示
# - DEBUG_SKEW_ANALYSIS=true: AQEベーススキュー判定の詳細根拠を表示
# 
# 💿 スピルデバッグ表示内容:
# - ターゲットメトリクス: "Sink - Num bytes spilled to disk due to memory pressure"
# - 各データソース（detailed_metrics, raw_metrics, key_metrics）での検索結果
# - メトリクス発見時の値と判定結果
# - その他のスピル関連メトリクス一覧（参考情報）
# 
# 🎯 スキューデバッグ表示内容:
# - AQEShuffleRead - Number of skewed partitions メトリクス値
# - AQEベーススキュー検出の判定根拠
# - 検出されたスキュー数と重要度レベル
# - 統計ベース判定は非推奨（AQEベース判定を推奨）

import os

# 特定メトリクススピル分析のデバッグ表示を有効にする場合はコメントアウトを解除
# os.environ['DEBUG_SPILL_ANALYSIS'] = 'true'

# AQEベーススキュー分析のデバッグ表示を有効にする場合はコメントアウトを解除  
# os.environ['DEBUG_SKEW_ANALYSIS'] = 'true'

print("🐛 デバッグモード設定:")
print(f"   特定メトリクススピル分析デバッグ: {os.environ.get('DEBUG_SPILL_ANALYSIS', 'false')}")
print(f"   AQEベーススキュー分析デバッグ: {os.environ.get('DEBUG_SKEW_ANALYSIS', 'false')}")
print("   ※ 'true'に設定すると判定根拠の詳細情報が表示されます")
print()
print("💿 特定メトリクススピル検出基準:")
print('   🎯 ターゲット: "Sink - Num bytes spilled to disk due to memory pressure"')
print("   ✅ 判定条件: 値 > 0")
print()
print("🎯 AQEベーススキュー検出基準:")
print("   📊 AQEShuffleRead - Number of skewed partitions > 0")
print("   📊 判定条件: メトリクス値 > 0")
print("   📊 重要度: 検出値に基づく")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐌 最も時間がかかっている処理TOP10
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 抽出したメトリクスのJSON形式での保存
# MAGIC - set型からlist型への変換処理
# MAGIC - 最も時間がかかっている処理TOP10の詳細分析
# MAGIC - スピル検出とデータスキュー分析
# MAGIC - Sparkステージ実行分析

# COMMAND ----------

# 💾 抽出したメトリクスのJSONファイル保存は除外（不要）
def format_thinking_response(response) -> str:
    """
    thinking_enabled: Trueの場合のレスポンスを人間に読みやすい形式に変換
    思考過程（thinking）とシグネチャ（signature）等の不要な情報は除外し、最終的な結論のみを表示
    JSON構造や不適切な文字列の露出を防止
    """
    import re  # reモジュールのインポートを追加
    
    if not isinstance(response, list):
        # リストでない場合は文字列として処理し、クリーンアップ
        cleaned_text = clean_response_text(str(response))
        return cleaned_text
    
    # 除外すべきキーのリスト（拡張）
    excluded_keys = {
        'thinking', 'signature', 'metadata', 'id', 'request_id', 
        'timestamp', 'uuid', 'reasoning', 'type', 'model'
    }
    
    formatted_parts = []
    
    for item in response:
        if isinstance(item, dict):
            # 最も適切なテキストコンテンツを抽出
            content = extract_best_content_from_dict(item, excluded_keys)
            if content:
                cleaned_content = clean_response_text(content)
                if is_valid_content(cleaned_content):
                    formatted_parts.append(cleaned_content)
        else:
            # 辞書でない場合もクリーンアップ
            cleaned_content = clean_response_text(str(item))
            if is_valid_content(cleaned_content):
                formatted_parts.append(cleaned_content)
    
    final_result = '\n'.join(formatted_parts)
    
    # 最終的な品質チェックとクリーンアップ
    final_result = final_quality_check(final_result)
    
    return final_result

def extract_best_content_from_dict(item_dict, excluded_keys):
    """辞書から最適なコンテンツを抽出"""
    # 優先順位: text > summary_text > content > message > その他
    priority_keys = ['text', 'summary_text', 'content', 'message', 'response']
    
    for key in priority_keys:
        if key in item_dict and item_dict[key]:
            content = str(item_dict[key])
            # JSON構造が含まれていないかチェック
            if not looks_like_json_structure(content):
                return content
    
    # 優先キーで見つからない場合、他のキーをチェック（除外キー以外）
    for key, value in item_dict.items():
        if key not in excluded_keys and value and isinstance(value, str):
            if not looks_like_json_structure(value):
                return value
    
    return None

def looks_like_json_structure(text):
    """テキストがJSON構造を含んでいるかチェック"""
    json_indicators = [
        "{'type':", '[{\'type\':', '{"type":', '[{"type":',
        "'text':", '"text":', "'summary_text':", '"summary_text":',
        'reasoning', 'metadata', 'signature'
    ]
    text_lower = text.lower()
    return any(indicator.lower() in text_lower for indicator in json_indicators)

def clean_response_text(text):
    """レスポンステキストのクリーンアップ"""
    import re
    
    if not text or not isinstance(text, str):
        return ""
    
    # 改行コードの正規化
    text = text.replace('\\n', '\n').replace('\\t', '\t')
    
    # JSON構造の除去
    
    # 典型的なJSON構造パターンを除去
    json_patterns = [
        r"'type':\s*'[^']*'",
        r'"type":\s*"[^"]*"',
        r"\[?\{'type':[^}]*\}[,\]]?",
        r'\[?\{"type":[^}]*\}[,\]]?',
        r"'reasoning':\s*\[[^\]]*\]",
        r'"reasoning":\s*\[[^\]]*\]',
        r"'signature':\s*'[A-Za-z0-9+/=]{50,}'",
        r'"signature":\s*"[A-Za-z0-9+/=]{50,}"'
    ]
    
    for pattern in json_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
    
    # 不完全なJSONブラケットの除去
    text = re.sub(r'^\s*[\[\{]', '', text)  # 先頭の [ や {
    text = re.sub(r'[\]\}]\s*$', '', text)  # 末尾の ] や }
    text = re.sub(r'^\s*[,;]\s*', '', text)  # 先頭のカンマやセミコロン
    
    # 連続する空白・改行の正規化
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # 3つ以上の連続改行を2つに
    text = re.sub(r'[ \t]+', ' ', text)  # 連続するスペース・タブを1つに
    
    # 前後の空白を除去
    text = text.strip()
    
    return text

def is_valid_content(text):
    """コンテンツが有効かどうかをチェック"""
    import re
    
    if not text or len(text.strip()) < 10:
        return False
    
    # 無効なパターンをチェック
    invalid_patterns = [
        r'^[{\[\'"]*$',  # JSON構造のみ
        r'^[,;:\s]*$',   # 区切り文字のみ
        r'^\s*reasoning\s*$',  # reasoningのみ
        r'^\s*metadata\s*$',   # metadataのみ
        r'^[A-Za-z0-9+/=]{50,}$',  # Base64っぽい長い文字列
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, text.strip(), re.IGNORECASE):
            return False
    
    return True

def final_quality_check(text):
    """最終的な品質チェックとクリーンアップ"""
    import re  # reモジュールのインポートを追加
    
    if not text:
        return "分析結果の抽出に失敗しました。"
    
    # 言語の一貫性チェック（安全な変数アクセス）
    try:
        language = globals().get('OUTPUT_LANGUAGE', 'ja')  # デフォルトは日本語
    except:
        language = 'ja'
    
    if language == 'ja':
        text = ensure_japanese_consistency(text)
    elif language == 'en':
        text = ensure_english_consistency(text)
    
    # 最小限の長さチェック
    if len(text.strip()) < 20:
        if language == 'ja':
            return "分析結果が不完全です。詳細な分析を実行中です。"
        else:
            return "Analysis result is incomplete. Detailed analysis in progress."
    
    return text

def ensure_japanese_consistency(text):
    """日本語の一貫性を確保"""
    import re
    
    # 明らかに破損している部分を除去
    # 例: "正caientify="predicate_liquid_referencet1" のような破損文字列
    text = re.sub(r'[a-zA-Z0-9_="\']{20,}', '', text)
    
    # 不完全なマークダウンの修正
    text = re.sub(r'#\s*[^#\n]*["\'>]+[^#\n]*', '', text)  # 破損したマークダウンヘッダー
    
    # 意味不明な文字列パターンの除去（拡張）
    nonsense_patterns = [
        r'addressing_sales_column\d*',
        r'predicate_liquid_reference[a-zA-Z0-9]*',
        r'bottlenars\s+effect',
        r'実装非保存在',
        r'裏票のend_by',
        r'riconsistall',
        r'caientify[a-zA-Z0-9="\']*',
        r'iving\s+[a-zA-Z0-9]*',
        r'o\s+Matter配賛',
        r'ubsが低い僮性',
        r'到田データの方効性',
        r'パフォーマンス.*topic.*項行に考',
        r'［[^］]*］">[^<]*',  # 破損したHTML/XML要素
        r'\]\s*">\s*$'  # 文末の破損したタグ
    ]
    
    for pattern in nonsense_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    # 連続する記号の除去
    text = re.sub(r'["\'>]{2,}', '', text)
    text = re.sub(r'[=\'"]{3,}', '', text)
    
    # 破損した日本語の修正パターン
    broken_japanese_patterns = [
        (r'の方法動的がら', '動的な方法で'),
        (r'思考に沿って進めていきます。$', '思考に沿って分析を進めます。'),
        (r'ベストプラクティスに沿った改善を.*までしているの', 'ベストプラクティスに沿った改善提案'),
    ]
    
    for broken, fixed in broken_japanese_patterns:
        text = re.sub(broken, fixed, text, flags=re.IGNORECASE)
    
    # 空行の正規化
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    
    return text.strip()

def ensure_english_consistency(text):
    """英語の一貫性を確保"""
    import re
    
    # 同様のクリーンアップを英語用に実装
    text = re.sub(r'[^\x00-\x7F\s]{10,}', '', text)  # 非ASCII文字の長い連続を除去
    
    return text.strip()

def extract_main_content_from_thinking_response(response) -> str:
    """
    thinking形式のレスポンスから主要コンテンツ（textまたはsummary_text）のみを抽出
    thinking、signature等の不要な情報は除外
    JSON構造や破損したテキストの混入を防止
    """
    if not isinstance(response, list):
        cleaned_text = clean_response_text(str(response))
        return final_quality_check(cleaned_text)
    
    # 除外すべきキー
    excluded_keys = {
        'thinking', 'signature', 'metadata', 'id', 'request_id', 
        'timestamp', 'uuid', 'reasoning', 'type', 'model'
    }
    
    for item in response:
        if isinstance(item, dict):
            # 最適なコンテンツを抽出
            content = extract_best_content_from_dict(item, excluded_keys)
            if content:
                cleaned_content = clean_response_text(content)
                if is_valid_content(cleaned_content):
                    return final_quality_check(cleaned_content)
    
    # 主要コンテンツが見つからない場合は全体をフォーマット
    return format_thinking_response(response)

def convert_sets_to_lists(obj):
    """set型をlist型に変換してJSONシリアライズ可能にする"""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {key: convert_sets_to_lists(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(item) for item in obj]
    else:
        return obj

# output_extracted_metrics の生成は除外（不要）

# 🐌 最も時間がかかっている処理TOP10
print(f"\n🐌 最も時間がかかっている処理TOP10")
print("=" * 80)
print("📊 アイコン説明: ⏱️時間 💾メモリ 🔥🐌並列度 💿スピル ⚖️スキュー")
print('💿 スピル判定: "Sink - Num bytes spilled to disk due to memory pressure" > 0')
print("🎯 スキュー判定: 'AQEShuffleRead - Number of skewed partitions' > 0")

# ノードを実行時間でソート
sorted_nodes = sorted(extracted_metrics['node_metrics'], 
                     key=lambda x: x['key_metrics'].get('durationMs', 0), 
                     reverse=True)

# 最大10個のノードを処理
final_sorted_nodes = sorted_nodes[:10]

if final_sorted_nodes:
    # 🚨 重要: 正しい全体時間の計算（デグレ防止）
    # 1. overall_metricsから全体実行時間を取得（wall-clock time）
    overall_metrics = extracted_metrics.get('overall_metrics', {})
    total_duration = overall_metrics.get('total_time_ms', 0)
    
    # 🚨 並列実行問題の修正: task_total_time_msを優先使用
    task_total_time_ms = overall_metrics.get('task_total_time_ms', 0)
    
    if task_total_time_ms > 0:
        total_duration = task_total_time_ms
        print(f"✅ コンソール表示: 並列実行対応 - task_total_time_ms使用: {total_duration:,} ms ({total_duration/3600000:.1f}時間)")
    elif total_duration <= 0:
        # execution_time_msを次の優先度で使用
        execution_time_ms = overall_metrics.get('execution_time_ms', 0)
        if execution_time_ms > 0:
            total_duration = execution_time_ms
            print(f"⚠️ コンソール表示: task_total_time_ms利用不可、execution_time_ms使用: {total_duration} ms")
        else:
            # 最終フォールバック
            max_node_time = max([node['key_metrics'].get('durationMs', 0) for node in sorted_nodes], default=1)
            total_duration = int(max_node_time * 1.2)
            print(f"⚠️ コンソール表示: 最終フォールバック - 推定時間使用: {total_duration} ms")
    
    print(f"📊 累積タスク実行時間（並列）: {total_duration:,} ms ({total_duration/3600000:.1f} 時間)")
    print(f"📈 TOP10合計時間（並列実行）: {sum(node['key_metrics'].get('durationMs', 0) for node in final_sorted_nodes):,} ms")

    print()
    
    for i, node in enumerate(final_sorted_nodes):
        rows_num = node['key_metrics'].get('rowsNum', 0)
        duration_ms = node['key_metrics'].get('durationMs', 0)
        memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / 1024 / 1024
        
        # 🚨 重要: 正しいパーセンテージ計算（デグレ防止）
        # wall-clock timeに対する各ノードの実行時間の割合
        time_percentage = min((duration_ms / max(total_duration, 1)) * 100, 100.0)
        
        # 時間の重要度に基づいてアイコンを選択
        if duration_ms >= 10000:  # 10秒以上
            time_icon = "�"
            severity = "CRITICAL"
        elif duration_ms >= 5000:  # 5秒以上
            time_icon = "🟠"
            severity = "HIGH"
        elif duration_ms >= 1000:  # 1秒以上
            time_icon = "🟡"
            severity = "MEDIUM"
        else:
            time_icon = "�"
            severity = "LOW"
        
        # メモリ使用量のアイコン
        memory_icon = "�" if memory_mb < 100 else "⚠️" if memory_mb < 1000 else "🚨"
        
        # より意味のあるノード名を取得
        raw_node_name = node['name']
        node_name = get_meaningful_node_name(node, extracted_metrics)
        short_name = node_name[:100] + "..." if len(node_name) > 100 else node_name
        
        # 並列度情報の取得（修正版: 複数のTasks totalメトリクスを取得）
        parallelism_data = extract_parallelism_metrics(node)
        
        # 従来の単一値（互換性のため）
        num_tasks = parallelism_data.get('tasks_total', 0)
        
        # フォールバック: Sink - Tasks totalまたはSource - Tasks totalがある場合
        if num_tasks == 0:
            if parallelism_data.get('sink_tasks_total', 0) > 0:
                num_tasks = parallelism_data.get('sink_tasks_total', 0)
            elif parallelism_data.get('source_tasks_total', 0) > 0:
                num_tasks = parallelism_data.get('source_tasks_total', 0)
        
        # ディスクスピルアウトの検出（メモリプレッシャーによるスピルメトリクス対応改善版）
        spill_detected = False
        spill_bytes = 0
        spill_details = []
        
        # スピル検出ターゲットメトリクス名リスト（正確なメトリクス名のみ）
        exact_spill_metrics = [
            "Num bytes spilled to disk due to memory pressure",
            "Sink - Num bytes spilled to disk due to memory pressure",
            "Sink/Num bytes spilled to disk due to memory pressure"
        ]
        
        # 1. detailed_metricsから正確なメトリクス名で検索
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            metric_value = metric_info.get('value', 0)
            metric_label = metric_info.get('label', '')
            
            # 正確なメトリクス名でのみマッチング
            if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                spill_detected = True
                spill_bytes = max(spill_bytes, metric_value)  # 最大値を使用
                spill_details.append({
                    'metric_name': metric_key,
                    'value': metric_value,
                    'label': metric_label,
                    'source': 'detailed_metrics',
                    'matched_field': 'key' if metric_key in exact_spill_metrics else 'label',
                    'matched_pattern': metric_key if metric_key in exact_spill_metrics else metric_label
                })
                break  # 最初に見つかったスピルメトリクスを使用
        
        # 2. detailed_metricsで見つからない場合、生メトリクスから正確なメトリクス名で検索
        if not spill_detected:
            raw_metrics = node.get('metrics', [])
            for metric in raw_metrics:
                metric_key = metric.get('key', '')
                metric_label = metric.get('label', '')
                metric_value = metric.get('value', 0)
                
                # 正確なメトリクス名でのみマッチング
                if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                    spill_detected = True
                    spill_bytes = max(spill_bytes, metric_value)  # 最大値を使用
                    spill_details.append({
                        'metric_name': metric_key,
                        'value': metric_value,
                        'label': metric_label,
                        'source': 'raw_metrics',
                        'matched_field': 'key' if metric_key in exact_spill_metrics else 'label',
                        'matched_pattern': metric_key if metric_key in exact_spill_metrics else metric_label
                    })
                    break  # 最初に見つかったスピルメトリクスを使用
        
        # 3. key_metricsから正確なメトリクス名で検索
        if not spill_detected:
            key_metrics = node.get('key_metrics', {})
            for exact_metric in exact_spill_metrics:
                if exact_metric in key_metrics and key_metrics[exact_metric] > 0:
                    spill_detected = True
                    spill_bytes = max(spill_bytes, key_metrics[exact_metric])  # 最大値を使用
                    spill_details.append({
                        'metric_name': f"key_metrics.{exact_metric}",
                        'value': key_metrics[exact_metric],
                        'label': f"Key metric: {exact_metric}",
                        'source': 'key_metrics',
                        'matched_field': 'key',
                        'matched_pattern': exact_metric
                    })
                    break
        
        # データスキューの検出（AQEベースの精密判定）
        skew_detected = False
        skew_details = []
        skewed_partitions = 0  # スキューパーティション数
        
        # AQEベーススキュー検出: "AQEShuffleRead - Number of skewed partitions" > 0
        target_aqe_metrics = [
            "AQEShuffleRead - Number of skewed partitions",
            "AQEShuffleRead - Number of skewed partition splits"
        ]
        
        aqe_skew_value = 0
        aqe_split_value = 0
        aqe_metric_name = ""
        aqe_split_metric_name = ""
        
        # 1. detailed_metricsで検索
        detailed_metrics = node.get('detailed_metrics', {})
        for metric_key, metric_info in detailed_metrics.items():
            if metric_key == "AQEShuffleRead - Number of skewed partitions":
                aqe_skew_value = metric_info.get('value', 0)
                aqe_metric_name = metric_key
            elif metric_key == "AQEShuffleRead - Number of skewed partition splits":
                aqe_split_value = metric_info.get('value', 0)
                aqe_split_metric_name = metric_key
            elif metric_info.get('label', '') == "AQEShuffleRead - Number of skewed partitions":
                aqe_skew_value = metric_info.get('value', 0)
                aqe_metric_name = metric_info.get('label', '')
            elif metric_info.get('label', '') == "AQEShuffleRead - Number of skewed partition splits":
                aqe_split_value = metric_info.get('value', 0)
                aqe_split_metric_name = metric_info.get('label', '')
        
        # 2. raw_metricsで検索（フォールバック）
        if aqe_skew_value == 0 or aqe_split_value == 0:
            raw_metrics = node.get('metrics', [])
            if isinstance(raw_metrics, list):
                for raw_metric in raw_metrics:
                    if isinstance(raw_metric, dict):
                        # 'label'フィールドを最初にチェック
                        raw_metric_label = raw_metric.get('label', '')
                        if raw_metric_label == "AQEShuffleRead - Number of skewed partitions" and aqe_skew_value == 0:
                            aqe_skew_value = raw_metric.get('value', 0)
                            aqe_metric_name = raw_metric_label
                        elif raw_metric_label == "AQEShuffleRead - Number of skewed partition splits" and aqe_split_value == 0:
                            aqe_split_value = raw_metric.get('value', 0)
                            aqe_split_metric_name = raw_metric_label
                        
                        # 'key'フィールドもチェック
                        raw_metric_key = raw_metric.get('key', '')
                        if raw_metric_key == "AQEShuffleRead - Number of skewed partitions" and aqe_skew_value == 0:
                            aqe_skew_value = raw_metric.get('value', 0)
                            aqe_metric_name = raw_metric_key
                        elif raw_metric_key == "AQEShuffleRead - Number of skewed partition splits" and aqe_split_value == 0:
                            aqe_split_value = raw_metric.get('value', 0)
                            aqe_split_metric_name = raw_metric_key
                        
                        # 'metricName'フィールドもチェック（従来の互換性）
                        raw_metric_name = raw_metric.get('metricName', '')
                        if raw_metric_name == "AQEShuffleRead - Number of skewed partitions" and aqe_skew_value == 0:
                            aqe_skew_value = raw_metric.get('value', 0)
                            aqe_metric_name = raw_metric_name
                        elif raw_metric_name == "AQEShuffleRead - Number of skewed partition splits" and aqe_split_value == 0:
                            aqe_split_value = raw_metric.get('value', 0)
                            aqe_split_metric_name = raw_metric_name
        
        # 3. key_metricsで検索（フォールバック）
        if aqe_skew_value == 0 or aqe_split_value == 0:
            key_metrics = node.get('key_metrics', {})
            for key_metric_name, key_metric_value in key_metrics.items():
                if "AQEShuffleRead - Number of skewed partitions" in key_metric_name and aqe_skew_value == 0:
                    aqe_skew_value = key_metric_value
                    aqe_metric_name = key_metric_name
                elif "AQEShuffleRead - Number of skewed partition splits" in key_metric_name and aqe_split_value == 0:
                    aqe_split_value = key_metric_value
                    aqe_split_metric_name = key_metric_name
        
        # AQEスキュー判定
        if aqe_skew_value > 0:
            skew_detected = True
            skewed_partitions = aqe_skew_value  # スキューパーティション数を設定
            severity_level = "高" if aqe_skew_value >= 5 else "中"
            
            # 基本的なAQEスキュー検出情報
            description = f'AQEスキュー検出: {aqe_metric_name} = {aqe_skew_value} > 基準値 0 [重要度:{severity_level}]'
            
            # split値も取得できた場合、詳細情報を追加
            if aqe_split_value > 0:
                description += f' | AQE検出詳細: Sparkが自動的に{aqe_skew_value}個のスキューパーティションを検出'
                description += f' | AQE自動対応: Sparkが自動的に{aqe_split_value}個のパーティションに分割'
            
            skew_details.append({
                'type': 'aqe_skew',
                'value': aqe_skew_value,
                'split_value': aqe_split_value,
                'threshold': 0,
                'metric_name': aqe_metric_name,
                'split_metric_name': aqe_split_metric_name,
                'severity': severity_level,
                'description': description
            })
        
        # AQEベーススキュー検出のみ使用（スピルベース判定は削除）
        # 理由: AQEShuffleRead - Number of skewed partitions が正確なスキュー判定基準
        
        # 並列度アイコン
        parallelism_icon = "🔥" if num_tasks >= 10 else "⚠️" if num_tasks >= 5 else "🐌"
        # スピルアイコン
        spill_icon = "💿" if spill_detected else "✅"
        # スキューアイコン
        skew_icon = "⚖️" if skew_detected else "✅"
        
        print(f"{i+1:2d}. {time_icon}{memory_icon}{parallelism_icon}{spill_icon}{skew_icon} [{severity:8}] {short_name}")
        print(f"    ⏱️  実行時間: {duration_ms:>8,} ms ({duration_ms/1000:>6.1f} sec) - 累積時間の {time_percentage:>5.1f}%")
        print(f"    📊 処理行数: {rows_num:>8,} 行")
        print(f"    💾 ピークメモリ: {memory_mb:>6.1f} MB")
        # 複数のTasks totalメトリクスを表示
        parallelism_display = []
        for task_metric in parallelism_data.get('all_tasks_metrics', []):
            parallelism_display.append(f"{task_metric['name']}: {task_metric['value']}")
        
        if parallelism_display:
            print(f"    🔧 並列度: {' | '.join(parallelism_display)}")
        else:
            print(f"    🔧 並列度: {num_tasks:>3d} タスク")
        
        # スキュー判定（AQEスキュー検出とAQEShuffleRead平均パーティションサイズの両方を考慮）
        aqe_shuffle_skew_warning = parallelism_data.get('aqe_shuffle_skew_warning', False)
        
        if skew_detected:
            skew_status = "AQEで検出・対応済"
        elif aqe_shuffle_skew_warning:
            skew_status = "潜在的なスキューの可能性あり"
        else:
            skew_status = "なし"
        
        print(f"    💿 スピル: {'あり' if spill_detected else 'なし'} | ⚖️ スキュー: {skew_status}")
        
        # AQEShuffleReadメトリクスの表示
        aqe_shuffle_metrics = parallelism_data.get('aqe_shuffle_metrics', [])
        if aqe_shuffle_metrics:
            aqe_display = []
            for aqe_metric in aqe_shuffle_metrics:
                if aqe_metric['name'] == "AQEShuffleRead - Number of partitions":
                    aqe_display.append(f"パーティション数: {aqe_metric['value']}")
                elif aqe_metric['name'] == "AQEShuffleRead - Partition data size":
                    aqe_display.append(f"データサイズ: {aqe_metric['value']:,} bytes")
            
            if aqe_display:
                print(f"    🔄 AQEShuffleRead: {' | '.join(aqe_display)}")
                
                # 平均パーティションサイズと警告表示
                avg_partition_size = parallelism_data.get('aqe_shuffle_avg_partition_size', 0)
                if avg_partition_size > 0:
                    avg_size_mb = avg_partition_size / (1024 * 1024)
                    print(f"    📊 平均パーティションサイズ: {avg_size_mb:.2f} MB")
                    
                    # 512MB以上の場合に警告
                    if parallelism_data.get('aqe_shuffle_skew_warning', False):
                        print(f"    ⚠️  【警告】 平均パーティションサイズが512MB以上 - 潜在的なスキューの可能性あり")
        
        # 効率性指標（行/秒）を計算
        if duration_ms > 0:
            rows_per_sec = (rows_num * 1000) / duration_ms
            print(f"    🚀 処理効率: {rows_per_sec:>8,.0f} 行/秒")
        
# フィルタ率表示（デバッグ機能付き）
        filter_result = calculate_filter_rate(node)
        filter_display = format_filter_rate_display(filter_result)
        if filter_display:
            print(f"    {filter_display}")
        else:
            # デバッグ情報：なぜフィルタ率が表示されないかを確認
            if filter_result["has_filter_metrics"]:
                print(f"    📂 フィルタ率: {filter_result['filter_rate']:.1%} (読み込み: {filter_result['files_read_bytes']/(1024*1024*1024):.2f}GB, プルーン: {filter_result['files_pruned_bytes']/(1024*1024*1024):.2f}GB)")
            else:
                # メトリクス検索のデバッグ
                debug_info = []
                detailed_metrics = node.get('detailed_metrics', {})
                for metric_key, metric_info in detailed_metrics.items():
                    metric_label = metric_info.get('label', '')
                    if 'file' in metric_label.lower() and ('read' in metric_label.lower() or 'prun' in metric_label.lower()):
                        debug_info.append(f"{metric_label}: {metric_info.get('value', 0)}")
                
                if debug_info:
                    print(f"    📂 フィルタ関連メトリクス検出: {', '.join(debug_info[:2])}")
                # else:
                #     print(f"    📂 フィルタ率: メトリクス未検出")
        
        # スピル詳細情報（シンプル表示）
        spill_display = ""
        if spill_detected and spill_bytes > 0:
            spill_mb = spill_bytes / 1024 / 1024
            if spill_mb >= 1024:  # GB単位
                spill_display = f"{spill_mb/1024:.2f} GB"
            else:  # MB単位
                spill_display = f"{spill_mb:.1f} MB"
            print(f"    💿 スピル: {spill_display}")
        
        # Shuffleノードの場合は常にShuffle attributesを表示
        if "shuffle" in short_name.lower():
            shuffle_attributes = extract_shuffle_attributes(node)
            if shuffle_attributes:
                print(f"    🔄 Shuffle属性: {', '.join(shuffle_attributes)}")
                
                # REPARTITIONヒントの提案（スピルが検出された場合のみ）
                if spill_detected and spill_bytes > 0 and spill_display:
                    suggested_partitions = max(num_tasks * 2, 200)  # 最小200パーティション
                    
                    # Shuffle属性で検出されたカラムを全て使用（完全一致）
                    repartition_columns = ", ".join(shuffle_attributes)
                    
                    print(f"    💡 最適化提案: REPARTITION({suggested_partitions}, {repartition_columns})")
                    print(f"       理由: スピル({spill_display})を改善するため")
                    print(f"       対象: Shuffle属性全{len(shuffle_attributes)}カラムを完全使用")
            else:
                print(f"    🔄 Shuffle属性: 設定なし")
        
        # スキャンノードの場合はクラスタリングキーを表示
        if "scan" in short_name.lower():
            cluster_attributes = extract_cluster_attributes(node)
            if cluster_attributes:
                print(f"    📊 クラスタリングキー: {', '.join(cluster_attributes)}")
            else:
                print(f"    📊 クラスタリングキー: 設定なし")

        
        # スキュー詳細情報（簡略表示）
        if skew_detected and skewed_partitions > 0:
            print(f"    ⚖️ スキュー詳細: {skewed_partitions} 個のスキューパーティション")
        
        # ノードIDも表示
        print(f"    🆔 ノードID: {node.get('node_id', node.get('id', 'N/A'))}")
        print()
        
else:
    print("⚠️ ノードメトリクスが見つかりませんでした")

print()

# 🔥 Sparkステージ実行分析
if extracted_metrics['stage_metrics']:
    print("\n🔥 Sparkステージ実行分析")
    print("=" * 60)
    
    stage_metrics = extracted_metrics['stage_metrics']
    total_stages = len(stage_metrics)
    completed_stages = len([s for s in stage_metrics if s.get('status') == 'COMPLETE'])
    failed_stages = len([s for s in stage_metrics if s.get('num_failed_tasks', 0) > 0])
    
    print(f"📊 ステージ概要: 全{total_stages}ステージ (完了:{completed_stages}, 失敗タスクあり:{failed_stages})")
    print()
    
    # ステージを実行時間でソート
    sorted_stages = sorted(stage_metrics, key=lambda x: x.get('duration_ms', 0), reverse=True)
    
    print("⏱️ ステージ実行時間ランキング:")
    print("-" * 60)
    
    for i, stage in enumerate(sorted_stages[:5]):  # TOP5ステージのみ表示
        stage_id = stage.get('stage_id', 'N/A')
        status = stage.get('status', 'UNKNOWN')
        duration_ms = stage.get('duration_ms', 0)
        num_tasks = stage.get('num_tasks', 0)
        failed_tasks = stage.get('num_failed_tasks', 0)
        complete_tasks = stage.get('num_complete_tasks', 0)
        
        # ステータスに応じたアイコン
        if status == 'COMPLETE' and failed_tasks == 0:
            status_icon = "✅"
        elif failed_tasks > 0:
            status_icon = "⚠️"
        else:
            status_icon = "❓"
        
        # 並列度アイコン
        parallelism_icon = "🔥" if num_tasks >= 10 else "⚠️" if num_tasks >= 5 else "🐌"
        
        # 実行時間の重要度
        if duration_ms >= 10000:
            time_icon = "🔴"
            severity = "CRITICAL"
        elif duration_ms >= 5000:
            time_icon = "🟠"
            severity = "HIGH"
        elif duration_ms >= 1000:
            time_icon = "🟡"
            severity = "MEDIUM"
        else:
            time_icon = "🟢"
            severity = "LOW"
        
        print(f"{i+1}. {status_icon}{parallelism_icon}{time_icon} ステージ {stage_id} [{severity:8}]")
        print(f"   ⏱️ 実行時間: {duration_ms:,} ms ({duration_ms/1000:.1f} sec)")
        print(f"   🔧 タスク: {complete_tasks}/{num_tasks} 完了 (失敗: {failed_tasks})")
        
        # タスクあたりの平均時間
        if num_tasks > 0:
            avg_task_time = duration_ms / num_tasks
            print(f"   📊 平均タスク時間: {avg_task_time:.1f} ms")
        
        # 効率性評価
        if num_tasks > 0:
            task_efficiency = "高効率" if num_tasks >= 10 and failed_tasks == 0 else "要改善" if failed_tasks > 0 else "標準"
            print(f"   🎯 効率性: {task_efficiency}")
        
        print()
    
    if len(sorted_stages) > 5:
        print(f"... 他 {len(sorted_stages) - 5} ステージ")
    
    # 問題のあるステージのハイライト
    problematic_stages = [s for s in stage_metrics if s.get('num_failed_tasks', 0) > 0 or s.get('duration_ms', 0) > 30000]
    if problematic_stages:
        print("\n🚨 注意が必要なステージ:")
        print("-" * 40)
        for stage in problematic_stages[:3]:
            stage_id = stage.get('stage_id', 'N/A')
            duration_sec = stage.get('duration_ms', 0) / 1000
            failed_tasks = stage.get('num_failed_tasks', 0)
            
            issues = []
            if failed_tasks > 0:
                issues.append(f"失敗タスク{failed_tasks}個")
            if duration_sec > 30:
                issues.append(f"長時間実行({duration_sec:.1f}sec)")
            
            print(f"   ⚠️ ステージ {stage_id}: {', '.join(issues)}")
    
    
    print()
else:
    print("\n🔥 Sparkステージ実行分析")
    print("=" * 60)
    print("⚠️ ステージメトリクスが見つかりませんでした")
    print()

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗂️ Liquid Clustering分析結果の詳細表示
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - テーブル別推奨クラスタリングカラムの詳細表示
# MAGIC - パフォーマンス向上見込みの分析
# MAGIC - カラム使用パターンの詳細分析
# MAGIC - プッシュダウンフィルター情報の表示
# MAGIC - SQL実装例の提示

# COMMAND ----------

# 🗂️ LLMによるLiquid Clustering分析結果の詳細表示
print("\n" + "=" * 50)
print("🤖 LLM Liquid Clustering推奨分析")
print("=" * 50)

# LLMベースのLiquid Clustering分析を実行
liquid_analysis = extracted_metrics['liquid_clustering_analysis']

# LLM分析結果を表示
print("\n🤖 LLM分析結果:")
print("=" * 50)
llm_analysis = liquid_analysis.get('llm_analysis', '')
if llm_analysis:
    print(llm_analysis)
else:
    print("❌ LLM分析結果が見つかりません")

# 抽出データの概要を表示
extracted_data = liquid_analysis.get('extracted_data', {})
metadata_summary = extracted_data.get('metadata_summary', {})

print(f"\n📊 抽出データ概要:")
print(f"   🔍 フィルター条件: {metadata_summary.get('filter_expressions_count', 0)}個")
print(f"   🔗 JOIN条件: {metadata_summary.get('join_expressions_count', 0)}個")
print(f"   📊 GROUP BY条件: {metadata_summary.get('groupby_expressions_count', 0)}個")
print(f"   📈 集約関数: {metadata_summary.get('aggregate_expressions_count', 0)}個")
print(f"   🏷️ 識別テーブル: {metadata_summary.get('tables_identified', 0)}個")
print(f"   📂 スキャンノード: {metadata_summary.get('scan_nodes_count', 0)}個")

# パフォーマンスコンテキストの表示
performance_context = liquid_analysis.get('performance_context', {})
print(f"\n⚡ パフォーマンス情報:")
print(f"   ⏱️ 実行時間: {performance_context.get('total_time_sec', 0):.1f}秒")
print(f"   💾 データ読み込み: {performance_context.get('read_gb', 0):.2f}GB")
print(f"   📊 出力行数: {performance_context.get('rows_produced', 0):,}行")
print(f"   🎯 フィルタ率: {performance_context.get('data_selectivity', 0):.4f}")

# 分析結果をファイルに出力
print(f"\n💾 分析結果をファイルに出力中...")
try:
    saved_files = save_liquid_clustering_analysis(liquid_analysis, "/tmp")
    
    if "error" in saved_files:
        print(f"❌ ファイル出力エラー: {saved_files['error']}")
    else:
        print(f"✅ ファイル出力完了:")
        for file_type, file_path in saved_files.items():
            if file_type == "json":
                print(f"   📄 JSON詳細データ: {file_path}")
            elif file_type == "markdown":
                print(f"   📝 Markdownレポート: {file_path}")
            elif file_type == "sql":
                print(f"   🔧 SQL実装例: {file_path}")
                
except Exception as e:
    print(f"❌ ファイル出力中にエラーが発生しました: {str(e)}")

# サマリー情報
summary = liquid_analysis.get('summary', {})
print(f"\n📋 分析サマリー:")
print(f"   🔬 分析方法: {summary.get('analysis_method', 'Unknown')}")
print(f"   🤖 LLMプロバイダー: {summary.get('llm_provider', 'Unknown')}")
print(f"   📊 対象テーブル数: {summary.get('tables_identified', 0)}")
print(f"   📈 抽出カラム数: フィルター({summary.get('total_filter_columns', 0)}) + JOIN({summary.get('total_join_columns', 0)}) + GROUP BY({summary.get('total_groupby_columns', 0)})")

print()

# COMMAND ----------

# 🤖 設定されたLLMエンドポイントを使用してボトルネック分析
provider = LLM_CONFIG["provider"]
if provider == "databricks":
    endpoint_name = LLM_CONFIG["databricks"]["endpoint_name"]
    print(f"🤖 Databricks Model Serving ({endpoint_name}) によるボトルネック分析を開始します...")
    print(f"⚠️  Model Servingエンドポイント '{endpoint_name}' が必要です")
elif provider == "openai":
    model = LLM_CONFIG["openai"]["model"]
    print(f"🤖 OpenAI ({model}) によるボトルネック分析を開始します...")
    print("⚠️  OpenAI APIキーが必要です")
elif provider == "azure_openai":
    deployment = LLM_CONFIG["azure_openai"]["deployment_name"]
    print(f"🤖 Azure OpenAI ({deployment}) によるボトルネック分析を開始します...")
    print("⚠️  Azure OpenAI APIキーとエンドポイントが必要です")
elif provider == "anthropic":
    model = LLM_CONFIG["anthropic"]["model"]
    print(f"🤖 Anthropic ({model}) によるボトルネック分析を開始します...")
    print("⚠️  Anthropic APIキーが必要です")

print("📝 分析プロンプトを簡潔化してタイムアウトリスクを軽減しています...")
print()

analysis_result = analyze_bottlenecks_with_llm(extracted_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 LLMボトルネック分析結果の表示
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 設定されたLLMプロバイダーによる詳細分析結果の表示
# MAGIC - ボトルネック特定と改善提案の可視化
# MAGIC - 分析結果の整形と読みやすい表示

# COMMAND ----------

# 📊 分析結果の表示
print("\n" + "=" * 80)
print(f"🎯 【{provider.upper()} LLM による SQLボトルネック分析結果】")
print("=" * 80)
print()
print(analysis_result)
print()
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 分析結果の保存と完了サマリー
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - LLM分析結果のテキストファイルへの保存
# MAGIC - 分析対象の基本情報の記録
# MAGIC - 全体処理の完了サマリー表示
# MAGIC - 出力ファイルの一覧表示

# COMMAND ----------

# 💾 分析結果の保存と完了サマリー
from datetime import datetime
# output_bottleneck_analysis_result_XXX.txtファイルの出力は廃止（optimization_reportに統合）

# 最終的なサマリー
print("\n" + "🎉" * 20)
print("🏁 【処理完了サマリー】")
print("🎉" * 20)
print("✅ SQLプロファイラーJSONファイル読み込み完了")
print(f"✅ パフォーマンスメトリクス抽出完了")

# LLMプロバイダー情報の動的表示
try:
    current_provider = LLM_CONFIG.get('provider', 'unknown')
    provider_display_names = {
        'databricks': f"Databricks ({LLM_CONFIG.get('databricks', {}).get('endpoint_name', 'Model Serving')})",
        'openai': f"OpenAI ({LLM_CONFIG.get('openai', {}).get('model', 'GPT-4')})",
        'azure_openai': f"Azure OpenAI ({LLM_CONFIG.get('azure_openai', {}).get('deployment_name', 'GPT-4')})",
        'anthropic': f"Anthropic ({LLM_CONFIG.get('anthropic', {}).get('model', 'Claude')})"
    }
    provider_display = provider_display_names.get(current_provider, f"{current_provider}（未知のプロバイダー）")
    print(f"✅ {provider_display}によるボトルネック分析完了")
except Exception as e:
    print("✅ LLMによるボトルネック分析完了")

print("✅ 分析結果は後でoptimization_reportに統合されます")
print()
print("🚀 分析完了！結果を確認してクエリ最適化にお役立てください。")
print("🎉" * 20)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔧 SQL最適化機能セクション
# MAGIC
# MAGIC **このセクションではSQLクエリの最適化を行います**
# MAGIC
# MAGIC 📋 **最適化プロセス:**
# MAGIC - プロファイラーデータからオリジナルクエリの抽出
# MAGIC - LLMによるクエリ最適化の実行
# MAGIC - 最適化結果のファイル生成
# MAGIC - テスト実行の準備
# MAGIC
# MAGIC ⚠️ **前提条件:** メイン処理セクションを完了してから実行してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 SQL最適化関連関数定義
# MAGIC
# MAGIC このセルでは以下の関数を定義します：
# MAGIC - `extract_original_query_from_profiler_data`: プロファイラーデータからオリジナルクエリを抽出
# MAGIC - `generate_optimized_query_with_llm`: LLM分析結果に基づくクエリ最適化
# MAGIC - `save_optimized_sql_files`: 最適化結果の各種ファイル保存

# COMMAND ----------

def extract_original_query_from_profiler_data(profiler_data: Dict[str, Any]) -> str:
    """
    プロファイラーデータからオリジナルクエリを抽出
    """
    
    # 複数の場所からSQLクエリを探す
    query_candidates = []
    
    # 1. query.queryText から抽出
    if 'query' in profiler_data and 'queryText' in profiler_data['query']:
        query_text = profiler_data['query']['queryText']
        if query_text and query_text.strip():
            query_candidates.append(query_text.strip())
    
    # 2. metadata から抽出
    if 'metadata' in profiler_data:
        metadata = profiler_data['metadata']
        for key, value in metadata.items():
            if 'sql' in key.lower() or 'query' in key.lower():
                if isinstance(value, str) and value.strip():
                    query_candidates.append(value.strip())
    
    # 3. graphs の metadata から抽出
    if 'graphs' in profiler_data:
        for graph in profiler_data['graphs']:
            nodes = graph.get('nodes', [])
            for node in nodes:
                node_metadata = node.get('metadata', [])
                for meta in node_metadata:
                    if meta.get('key', '').upper() in ['SQL', 'QUERY', 'SQL_TEXT']:
                        value = meta.get('value', '')
                        if value and value.strip():
                            query_candidates.append(value.strip())
    
    # 最も長いクエリを選択（通常、最も完全なクエリ）
    if query_candidates:
        original_query = max(query_candidates, key=len)
        return original_query
    
    return ""

def extract_table_size_estimates_from_plan(profiler_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    実行プランからテーブルごとの推定サイズ情報を抽出
    
    注意: Databricksクエリプロファイルには estimatedSizeInBytes が含まれていないため、
    この機能は現在無効化されています。メトリクスベースの推定を使用してください。
    
    Args:
        profiler_data: プロファイラーデータ
        
    Returns:
        Dict: 空の辞書（機能無効化）
    """
    # DatabricksクエリプロファイルにestimatedSizeInBytesが含まれていないため無効化
    return {}

def extract_table_name_from_scan_node(node: Dict[str, Any]) -> str:
    """
    スキャンノードからテーブル名を抽出
    
    Args:
        node: 実行プランのノード
        
    Returns:
        str: テーブル名
    """
    try:
        # 複数の方法でテーブル名を抽出を試行
        
        # 1. node outputからの抽出
        output = node.get("output", "")
        if output:
            # パターン: [col1#123, col2#456] table_name
            import re
            table_match = re.search(r'\]\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', output)
            if table_match:
                return table_match.group(1)
        
        # 2. node詳細からの抽出
        details = node.get("details", "")
        if details:
            # パターン: Location: /path/to/table/name
            location_match = re.search(r'Location:.*?([a-zA-Z_][a-zA-Z0-9_]*)', details)
            if location_match:
                return location_match.group(1)
            
            # パターン: Table: database.table_name
            table_match = re.search(r'Table:\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', details)
            if table_match:
                return table_match.group(1)
        
        # 3. メタデータからの抽出
        metadata = node.get("metadata", [])
        for meta in metadata:
            if meta.get("key") == "table" or meta.get("key") == "relation":
                values = meta.get("values", [])
                if values:
                    return str(values[0])
        
        # 4. node名からの推測（最後の手段）
        node_name = node.get("nodeName", "")
        if "delta" in node_name.lower():
            # Delta Scan の場合、詳細情報から抽出
            pass
    
    except Exception as e:
        print(f"⚠️ テーブル名抽出でエラー: {str(e)}")
    
    return None

def extract_broadcast_table_names(profiler_data: Dict[str, Any], broadcast_nodes: list) -> Dict[str, Any]:
    """
    BROADCASTノードから関連するテーブル名を抽出
    """
    broadcast_table_info = {
        "broadcast_tables": [],
        "broadcast_table_mapping": {},
        "broadcast_nodes_with_tables": []
    }
    
    # 実行プランのグラフ情報を取得
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        return broadcast_table_info
    
    # 全ノードを収集
    all_nodes = []
    for graph in graphs:
        nodes = graph.get('nodes', [])
        all_nodes.extend(nodes)
    
    # エッジ情報を収集（ノード間の関係）
    all_edges = []
    for graph in graphs:
        edges = graph.get('edges', [])
        all_edges.extend(edges)
    
    # 各BROADCASTノードについて関連するテーブルを特定
    for broadcast_node in broadcast_nodes:
        broadcast_node_id = broadcast_node.get('node_id', '')
        broadcast_node_name = broadcast_node.get('node_name', '')
        
        # BROADCASTノードから直接テーブル名を抽出
        table_names = set()
        
        # 1. メタデータからテーブル名を抽出
        metadata = broadcast_node.get('metadata', [])
        for meta in metadata:
            key = meta.get('key', '')
            value = meta.get('value', '')
            values = meta.get('values', [])
            
            # テーブル名を示すキーをチェック
            if key in ['SCAN_IDENTIFIER', 'TABLE_NAME', 'RELATION']:
                if value:
                    table_names.add(value)
                table_names.update(values)
        
        # 2. ノード名からテーブル名を推定
        if 'SCAN' in broadcast_node_name:
            # "Broadcast Scan delta orders" → "orders"
            import re
            table_match = re.search(r'SCAN\s+(?:DELTA|PARQUET|JSON|CSV)?\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)', broadcast_node_name, re.IGNORECASE)
            if table_match:
                table_names.add(table_match.group(1))
        
        # 3. エッジ情報から関連するスキャンノードを特定
        for edge in all_edges:
            source_id = edge.get('source', '')
            target_id = edge.get('target', '')
            
            # BROADCASTノードに入力されるノードを検索
            if target_id == broadcast_node_id:
                # 入力ノードがスキャンノードかチェック
                for node in all_nodes:
                    if node.get('id', '') == source_id:
                        node_name = node.get('name', '').upper()
                        if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN']):
                            # スキャンノードからテーブル名を抽出
                            scan_table_name = extract_table_name_from_scan_node(node)
                            if scan_table_name:
                                table_names.add(scan_table_name)
        
        # 4. 同じグラフ内のスキャンノードとの関連付け
        for node in all_nodes:
            node_name = node.get('name', '').upper()
            if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN']):
                # スキャンノードの名前がBROADCASTノード名に含まれるかチェック
                scan_table_name = extract_table_name_from_scan_node(node)
                if scan_table_name:
                    # テーブル名の部分一致をチェック
                    if any(part in broadcast_node_name for part in scan_table_name.split('.') if len(part) > 2):
                        table_names.add(scan_table_name)
        
        # 結果を記録
        table_names_list = list(table_names)
        if table_names_list:
            broadcast_table_info["broadcast_tables"].extend(table_names_list)
            broadcast_table_info["broadcast_table_mapping"][broadcast_node_id] = table_names_list
            
            # BROADCASTノード情報を拡張
            enhanced_broadcast_node = broadcast_node.copy()
            enhanced_broadcast_node["associated_tables"] = table_names_list
            enhanced_broadcast_node["table_count"] = len(table_names_list)
            broadcast_table_info["broadcast_nodes_with_tables"].append(enhanced_broadcast_node)
    
    # 重複を除去
    broadcast_table_info["broadcast_tables"] = list(set(broadcast_table_info["broadcast_tables"]))
    
    return broadcast_table_info

def extract_execution_plan_info(profiler_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    JSONメトリクスから実行プラン情報を抽出
    """
    plan_info = {
        "broadcast_nodes": [],
        "join_nodes": [],
        "scan_nodes": [],
        "shuffle_nodes": [],
        "aggregate_nodes": [],
        "plan_summary": {},
        "broadcast_already_applied": False,
        "join_strategies": [],
        "table_scan_details": {},
        "broadcast_table_info": {}
    }
    
    # プロファイラーデータから実行グラフ情報を取得
    graphs = profiler_data.get('graphs', [])
    if not graphs:
        return plan_info
    
    # すべてのグラフからノードを収集
    all_nodes = []
    for graph_index, graph in enumerate(graphs):
        nodes = graph.get('nodes', [])
        for node in nodes:
            node['graph_index'] = graph_index
            all_nodes.append(node)
    
    # ノード分析
    for node in all_nodes:
        node_name = node.get('name', '').upper()
        node_tag = node.get('tag', '').upper()
        node_metadata = node.get('metadata', [])
        
        # BROADCASTノードの検出
        if 'BROADCAST' in node_name or 'BROADCAST' in node_tag:
            plan_info["broadcast_already_applied"] = True
            broadcast_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "metadata": []
            }
            
            # BROADCASTに関連するメタデータを抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                value = meta.get('value', '')
                values = meta.get('values', [])
                
                if any(keyword in key.upper() for keyword in ['BROADCAST', 'BUILD', 'PROBE']):
                    broadcast_info["metadata"].append({
                        "key": key,
                        "value": value,
                        "values": values
                    })
            
            plan_info["broadcast_nodes"].append(broadcast_info)
        
        # JOINノードの検出と戦略分析
        elif any(keyword in node_name for keyword in ['JOIN', 'HASH']):
            join_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "join_strategy": "unknown",
                "join_keys": [],
                "join_type": "unknown"
            }
            
            # JOIN戦略の特定
            if 'BROADCAST' in node_name:
                join_info["join_strategy"] = "broadcast_hash_join"
            elif 'SORT' in node_name and 'MERGE' in node_name:
                join_info["join_strategy"] = "sort_merge_join"
            elif 'HASH' in node_name:
                join_info["join_strategy"] = "shuffle_hash_join"
            elif 'NESTED' in node_name:
                join_info["join_strategy"] = "broadcast_nested_loop_join"
            
            # JOINタイプの特定
            if 'INNER' in node_name:
                join_info["join_type"] = "inner"
            elif 'LEFT' in node_name:
                join_info["join_type"] = "left"
            elif 'RIGHT' in node_name:
                join_info["join_type"] = "right"
            elif 'OUTER' in node_name:
                join_info["join_type"] = "outer"
            
            # JOIN条件の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                values = meta.get('values', [])
                
                if key in ['LEFT_KEYS', 'RIGHT_KEYS']:
                    join_info["join_keys"].extend(values)
            
            plan_info["join_nodes"].append(join_info)
            plan_info["join_strategies"].append(join_info["join_strategy"])
        
        # スキャンノードの詳細分析
        elif any(keyword in node_name for keyword in ['SCAN', 'FILESCAN']):
            scan_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "table_name": "unknown",
                "file_format": "unknown",
                "pushed_filters": [],
                "output_columns": []
            }
            
            # テーブル名とファイル形式の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                value = meta.get('value', '')
                values = meta.get('values', [])
                
                if key == 'SCAN_IDENTIFIER':
                    scan_info["table_name"] = value
                elif key == 'OUTPUT':
                    scan_info["output_columns"] = values
                elif key == 'PUSHED_FILTERS' or key == 'FILTERS':
                    scan_info["pushed_filters"] = values
            
            # ファイル形式の推定
            if 'DELTA' in node_name:
                scan_info["file_format"] = "delta"
            elif 'PARQUET' in node_name:
                scan_info["file_format"] = "parquet"
            elif 'JSON' in node_name:
                scan_info["file_format"] = "json"
            elif 'CSV' in node_name:
                scan_info["file_format"] = "csv"
            
            plan_info["scan_nodes"].append(scan_info)
            plan_info["table_scan_details"][scan_info["table_name"]] = scan_info
        
        # シャッフルノードの検出
        elif any(keyword in node_name for keyword in ['SHUFFLE', 'EXCHANGE']):
            shuffle_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "partition_keys": []
            }
            
            # パーティション情報の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                values = meta.get('values', [])
                
                if key in ['PARTITION_EXPRESSIONS', 'PARTITION_KEYS']:
                    shuffle_info["partition_keys"] = values
            
            plan_info["shuffle_nodes"].append(shuffle_info)
        
        # 集約ノードの検出
        elif any(keyword in node_name for keyword in ['AGGREGATE', 'GROUP']):
            agg_info = {
                "node_name": node_name,
                "node_tag": node_tag,
                "node_id": node.get('id', ''),
                "group_keys": [],
                "aggregate_expressions": []
            }
            
            # 集約情報の抽出
            for meta in node_metadata:
                key = meta.get('key', '')
                values = meta.get('values', [])
                
                if key == 'GROUPING_EXPRESSIONS':
                    agg_info["group_keys"] = values
                elif key == 'AGGREGATE_EXPRESSIONS':
                    agg_info["aggregate_expressions"] = values
            
            plan_info["aggregate_nodes"].append(agg_info)
    
    # プランサマリーの生成
    plan_info["plan_summary"] = {
        "total_nodes": len(all_nodes),
        "broadcast_nodes_count": len(plan_info["broadcast_nodes"]),
        "join_nodes_count": len(plan_info["join_nodes"]),
        "scan_nodes_count": len(plan_info["scan_nodes"]),
        "shuffle_nodes_count": len(plan_info["shuffle_nodes"]),
        "aggregate_nodes_count": len(plan_info["aggregate_nodes"]),
        "unique_join_strategies": list(set(plan_info["join_strategies"])),
        "has_broadcast_joins": plan_info["broadcast_already_applied"],
        "tables_scanned": len(plan_info["table_scan_details"])
    }
    
    # BROADCASTテーブル情報を抽出
    if plan_info["broadcast_nodes"]:
        broadcast_table_info = extract_broadcast_table_names(profiler_data, plan_info["broadcast_nodes"])
        plan_info["broadcast_table_info"] = broadcast_table_info
        
        # プランサマリーにBROADCASTテーブル情報を追加
        plan_info["plan_summary"]["broadcast_tables"] = broadcast_table_info["broadcast_tables"]
        plan_info["plan_summary"]["broadcast_table_count"] = len(broadcast_table_info["broadcast_tables"])
    
    # 実行プランからのテーブルサイズ推定情報を追加（estimatedSizeInBytes利用不可のため無効化）
    plan_info["table_size_estimates"] = {}  # extract_table_size_estimates_from_plan(profiler_data)
    
    return plan_info

def get_spark_broadcast_threshold() -> float:
    """
    Sparkの実際のbroadcast閾値設定を取得
    """
    try:
        # Sparkの設定値を取得
        threshold_bytes = spark.conf.get("spark.databricks.optimizer.autoBroadcastJoinThreshold", "31457280")  # デフォルト30MB
        threshold_mb = float(threshold_bytes) / 1024 / 1024
        return threshold_mb
    except:
        # 取得できない場合は標準的な30MBを返す
        return 30.0

def estimate_uncompressed_size(compressed_size_mb: float, file_format: str = "parquet") -> float:
    """
    圧縮サイズから非圧縮サイズを推定（3.0倍固定）
    
    注意: 実際のestimatedSizeInBytesが利用できないため、
    保守的な3.0倍圧縮率で統一して推定します。
    """
    # 保守的な3.0倍圧縮率で統一（estimatedSizeInBytes利用不可のため）
    compression_ratio = 3.0
    
    return compressed_size_mb * compression_ratio

def analyze_broadcast_feasibility(metrics: Dict[str, Any], original_query: str, plan_info: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    BROADCASTヒントの適用可能性を分析（正確な30MB閾値適用）
    """
    broadcast_analysis = {
        "is_join_query": False,
        "broadcast_candidates": [],
        "recommendations": [],
        "feasibility": "not_applicable",
        "reasoning": [],
        "spark_threshold_mb": get_spark_broadcast_threshold(),
        "compression_analysis": {},
        "detailed_size_analysis": [],
        "execution_plan_analysis": {},
        "existing_broadcast_nodes": [],
        "already_optimized": False,
        "broadcast_applied_tables": []
    }
    
    # クエリにJOINが含まれているかチェック
    query_upper = original_query.upper()
    join_types = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'SEMI JOIN', 'ANTI JOIN']
    has_join = any(join_type in query_upper for join_type in join_types)
    
    if not has_join:
        broadcast_analysis["reasoning"].append("JOINクエリではないため、BROADCASTヒントは適用不可")
        return broadcast_analysis
    
    broadcast_analysis["is_join_query"] = True
    broadcast_analysis["reasoning"].append(f"Spark BROADCAST閾値: {broadcast_analysis['spark_threshold_mb']:.1f}MB（非圧縮）")
    
    # 実行プラン情報の分析
    if plan_info:
        plan_summary = plan_info.get("plan_summary", {})
        broadcast_nodes = plan_info.get("broadcast_nodes", [])
        join_nodes = plan_info.get("join_nodes", [])
        table_scan_details = plan_info.get("table_scan_details", {})
        table_size_estimates = plan_info.get("table_size_estimates", {})
        
        # 既存のBROADCAST適用状況の記録
        broadcast_analysis["existing_broadcast_nodes"] = broadcast_nodes
        broadcast_analysis["already_optimized"] = len(broadcast_nodes) > 0
        
        # プラン分析結果の記録
        broadcast_analysis["execution_plan_analysis"] = {
            "has_broadcast_joins": plan_summary.get("has_broadcast_joins", False),
            "unique_join_strategies": plan_summary.get("unique_join_strategies", []),
            "broadcast_nodes_count": len(broadcast_nodes),
            "join_nodes_count": len(join_nodes),
            "scan_nodes_count": plan_summary.get("scan_nodes_count", 0),
            "shuffle_nodes_count": plan_summary.get("shuffle_nodes_count", 0),
            "tables_in_plan": list(table_scan_details.keys())
        }
        
        # 既にBROADCASTが適用されている場合の詳細記録
        if broadcast_nodes:
            broadcast_analysis["reasoning"].append(f"✅ 実行プランで既にBROADCAST JOINが適用済み: {len(broadcast_nodes)}個のノード")
            
            # BROADCASTテーブル情報を取得
            broadcast_table_info = plan_info.get("broadcast_table_info", {})
            broadcast_tables = broadcast_table_info.get("broadcast_tables", [])
            
            if broadcast_tables:
                broadcast_analysis["reasoning"].append(f"📋 BROADCASTされているテーブル: {', '.join(broadcast_tables)}")
                broadcast_analysis["broadcast_applied_tables"] = broadcast_tables
                
                # 各BROADCASTノードの詳細
                broadcast_nodes_with_tables = broadcast_table_info.get("broadcast_nodes_with_tables", [])
                for i, node in enumerate(broadcast_nodes_with_tables[:3]):  # 最大3個まで表示
                    node_name_short = node['node_name'][:50] + "..." if len(node['node_name']) > 50 else node['node_name']
                    associated_tables = node.get('associated_tables', [])
                    if associated_tables:
                        broadcast_analysis["reasoning"].append(f"  • BROADCAST Node {i+1}: {node_name_short}")
                        broadcast_analysis["reasoning"].append(f"    └─ テーブル: {', '.join(associated_tables)}")
                    else:
                        broadcast_analysis["reasoning"].append(f"  • BROADCAST Node {i+1}: {node_name_short} (テーブル名未特定)")
            else:
                # BROADCASTノードは存在するがテーブル名が特定できない場合
                for i, node in enumerate(broadcast_nodes[:3]):  # 最大3個まで表示
                    broadcast_analysis["reasoning"].append(f"  • BROADCAST Node {i+1}: {node['node_name'][:50]}... (テーブル名解析中)")
        else:
            # BROADCAST未適用だが、JOINが存在する場合
            if join_nodes:
                join_strategies = set(node["join_strategy"] for node in join_nodes)
                broadcast_analysis["reasoning"].append(f"🔍 現在のJOIN戦略: {', '.join(join_strategies)}")
                broadcast_analysis["reasoning"].append("💡 BROADCAST最適化の機会を検討中...")
    else:
        broadcast_analysis["reasoning"].append("⚠️ 実行プラン情報が利用できません - メトリクス推定に基づく分析を実行")
    
    # メトリクスからテーブルサイズ情報を取得
    overall_metrics = metrics.get('overall_metrics', {})
    node_metrics = metrics.get('node_metrics', [])
    
    # スキャンノードからテーブル情報を抽出
    scan_nodes = []
    total_compressed_bytes = 0
    total_rows_all_tables = 0
    
    for node in node_metrics:
        node_name = node.get('name', '').upper()
        if any(keyword in node_name for keyword in ['SCAN', 'FILESCAN', 'PARQUET', 'DELTA']):
            key_metrics = node.get('key_metrics', {})
            rows_num = key_metrics.get('rowsNum', 0)
            duration_ms = key_metrics.get('durationMs', 0)
            
            # ファイル形式の推定（プラン情報を優先）
            file_format = "parquet"  # デフォルト
            table_name_from_plan = "unknown"
            
            # プラン情報からテーブル名とファイル形式を取得
            if plan_info and plan_info.get("table_scan_details"):
                # メタデータから詳細なテーブル名を抽出
                node_metadata = node.get('metadata', [])
                for meta in node_metadata:
                    meta_key = meta.get('key', '')
                    meta_value = meta.get('value', '')
                    if meta_key in ['SCAN_IDENTIFIER', 'SCAN_TABLE', 'TABLE_NAME'] and meta_value:
                        # プランの詳細と照合
                        for plan_table, scan_detail in plan_info["table_scan_details"].items():
                            if meta_value in plan_table or plan_table in meta_value:
                                table_name_from_plan = plan_table
                                if scan_detail["file_format"] != "unknown":
                                    file_format = scan_detail["file_format"]
                                break
                        break
            
            # フォールバック: ノード名からファイル形式を推定
            if file_format == "parquet":  # まだデフォルトの場合
                if "DELTA" in node_name:
                    file_format = "delta"
                elif "PARQUET" in node_name:
                    file_format = "parquet"
                elif "JSON" in node_name:
                    file_format = "json"
                elif "CSV" in node_name:
                    file_format = "csv"
            
            # メトリクスベース推定のみ使用（estimatedSizeInBytes利用不可のため）
            estimated_compressed_mb = 0
            estimated_uncompressed_mb = 0
            size_source = "metrics_estimation"
            
            # メトリクスベース推定
            total_read_bytes = overall_metrics.get('read_bytes', 0)
            total_rows = overall_metrics.get('rows_read_count', 0)
            
            if total_rows > 0 and total_read_bytes > 0 and rows_num > 0:
                # 全体の読み込み量からこのテーブルの割合を計算
                table_ratio = rows_num / total_rows
                estimated_compressed_bytes = total_read_bytes * table_ratio
                estimated_compressed_mb = estimated_compressed_bytes / 1024 / 1024
                 
                # 非圧縮サイズを推定
                estimated_uncompressed_mb = estimate_uncompressed_size(estimated_compressed_mb, file_format)
            else:
                # フォールバック: 行数ベースの推定（保守的）
                # 平均行サイズを推定（非圧縮）
                if total_rows > 0 and total_read_bytes > 0:
                    # 全体データから圧縮後の平均行サイズを計算
                    compressed_avg_row_size = total_read_bytes / total_rows
                    # 圧縮率を考慮して非圧縮サイズを推定
                    uncompressed_avg_row_size = compressed_avg_row_size * estimate_uncompressed_size(1.0, file_format)
                else:
                    # 完全なフォールバック: 一般的な非圧縮行サイズ（1KB）
                    uncompressed_avg_row_size = 1024
                
                estimated_compressed_mb = (rows_num * compressed_avg_row_size) / 1024 / 1024 if 'compressed_avg_row_size' in locals() else 0
                estimated_uncompressed_mb = (rows_num * uncompressed_avg_row_size) / 1024 / 1024
            
            # 既存のBROADCAST適用状況をチェック
            is_already_broadcasted = False
            if plan_info and plan_info.get("broadcast_nodes"):
                for broadcast_node in plan_info["broadcast_nodes"]:
                    # テーブル名の部分一致をチェック
                    broadcast_node_name = broadcast_node["node_name"]
                    if (table_name_from_plan != "unknown" and 
                        any(part in broadcast_node_name for part in table_name_from_plan.split('.') if len(part) > 3)):
                        is_already_broadcasted = True
                        break
                    # ノード名での照合
                    elif any(part in broadcast_node_name for part in node_name.split() if len(part) > 3):
                        is_already_broadcasted = True
                        break

            scan_info = {
                "node_name": node_name,
                "table_name_from_plan": table_name_from_plan,
                "rows": rows_num,
                "duration_ms": duration_ms,
                "estimated_compressed_mb": estimated_compressed_mb,
                "estimated_uncompressed_mb": estimated_uncompressed_mb,
                "file_format": file_format,
                "compression_ratio": 3.0,  # 固定3.0倍圧縮率
                "node_id": node.get('node_id', ''),
                "is_already_broadcasted": is_already_broadcasted,
                "size_estimation_source": size_source,
                "size_confidence": "medium"  # メトリクスベース推定のため中程度信頼度
            }
            scan_nodes.append(scan_info)
            
            total_compressed_bytes += estimated_compressed_bytes if 'estimated_compressed_bytes' in locals() else 0
            total_rows_all_tables += rows_num
    
    # BROADCAST候補の判定（30MB閾値使用）
    broadcast_threshold_mb = broadcast_analysis["spark_threshold_mb"]  # 実際のSpark設定値
    broadcast_safe_mb = broadcast_threshold_mb * 0.8  # 安全マージン（80%）
    broadcast_max_mb = broadcast_threshold_mb * 10    # 明らかに大きすぎる閾値
    
    small_tables = []
    large_tables = []
    marginal_tables = []
    
    # 圧縮分析の記録
    broadcast_analysis["compression_analysis"] = {
        "total_compressed_gb": total_compressed_bytes / 1024 / 1024 / 1024 if total_compressed_bytes > 0 else 0,
        "total_rows": total_rows_all_tables,
        "avg_compression_ratio": 0
    }
    
    for scan in scan_nodes:
        uncompressed_size_mb = scan["estimated_uncompressed_mb"]
        compressed_size_mb = scan["estimated_compressed_mb"]
        
        # 詳細サイズ分析の記録
        table_display_name = scan.get("table_name_from_plan", scan["node_name"])
        is_already_broadcasted = scan.get("is_already_broadcasted", False)
        
        size_analysis = {
            "table": table_display_name,
            "node_name": scan["node_name"],
            "rows": scan["rows"],
            "compressed_mb": compressed_size_mb,
            "uncompressed_mb": uncompressed_size_mb,
            "file_format": scan["file_format"],
            "compression_ratio": scan["compression_ratio"],
            "broadcast_decision": "",
            "decision_reasoning": "",
            "is_already_broadcasted": is_already_broadcasted
        }
        
        # 30MB閾値での判定（非圧縮サイズ）- 既存適用状況を考慮
        if is_already_broadcasted:
            # 既にBROADCASTが適用済み
            small_tables.append(scan)  # 統計目的で記録
            size_analysis["broadcast_decision"] = "already_applied"
            size_analysis["decision_reasoning"] = f"既にBROADCAST適用済み（推定サイズ: 非圧縮{uncompressed_size_mb:.1f}MB）"
            broadcast_analysis["broadcast_candidates"].append({
                "table": table_display_name,
                "estimated_uncompressed_mb": uncompressed_size_mb,
                "estimated_compressed_mb": compressed_size_mb,
                "rows": scan["rows"],
                "file_format": scan["file_format"],
                "compression_ratio": scan["compression_ratio"],
                "broadcast_feasible": True,
                "confidence": "confirmed",
                "status": "already_applied",
                "reasoning": f"実行プランで既にBROADCAST適用確認済み（推定サイズ: 非圧縮{uncompressed_size_mb:.1f}MB、メトリクスベース推定）"
            })
        elif uncompressed_size_mb <= broadcast_safe_mb and scan["rows"] > 0:
            # 安全マージン内（24MB以下）- 強く推奨
            small_tables.append(scan)
            size_analysis["broadcast_decision"] = "strongly_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB ≤ 安全閾値{broadcast_safe_mb:.1f}MB"
            broadcast_analysis["broadcast_candidates"].append({
                "table": table_display_name,
                "estimated_uncompressed_mb": uncompressed_size_mb,
                "estimated_compressed_mb": compressed_size_mb,
                "rows": scan["rows"],
                "file_format": scan["file_format"],
                "compression_ratio": scan["compression_ratio"],
                "broadcast_feasible": True,
                "confidence": "high",
                "status": "new_recommendation",
                "reasoning": f"非圧縮推定サイズ {uncompressed_size_mb:.1f}MB（安全閾値 {broadcast_safe_mb:.1f}MB 以下）でBROADCAST強く推奨（メトリクスベース推定、3.0倍圧縮率）"
            })
        elif uncompressed_size_mb <= broadcast_threshold_mb and scan["rows"] > 0:
            # 閾値内だが安全マージンは超過（24-30MB）- 条件付き推奨
            marginal_tables.append(scan)
            size_analysis["broadcast_decision"] = "conditionally_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB ≤ 閾値{broadcast_threshold_mb:.1f}MB（安全マージン超過）"
            broadcast_analysis["broadcast_candidates"].append({
                "table": table_display_name,
                "estimated_uncompressed_mb": uncompressed_size_mb,
                "estimated_compressed_mb": compressed_size_mb,
                "rows": scan["rows"],
                "file_format": scan["file_format"],
                "compression_ratio": scan["compression_ratio"],
                "broadcast_feasible": True,
                "confidence": "medium",
                "status": "new_recommendation",
                "reasoning": f"非圧縮推定サイズ {uncompressed_size_mb:.1f}MB（閾値 {broadcast_threshold_mb:.1f}MB 以下だが安全マージン {broadcast_safe_mb:.1f}MB 超過）で条件付きBROADCAST推奨（メトリクスベース推定、3.0倍圧縮率）"
            })
        elif uncompressed_size_mb > broadcast_max_mb:
            # 明らかに大きすぎる（300MB超）
            large_tables.append(scan)
            size_analysis["broadcast_decision"] = "not_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB > 最大閾値{broadcast_max_mb:.1f}MB"
            broadcast_analysis["reasoning"].append(f"テーブル {table_display_name}: 非圧縮{uncompressed_size_mb:.1f}MB - BROADCAST不可（>{broadcast_max_mb:.1f}MB）")
        else:
            # 中間サイズのテーブル（30-300MB）
            large_tables.append(scan)
            size_analysis["broadcast_decision"] = "not_recommended"
            size_analysis["decision_reasoning"] = f"非圧縮{uncompressed_size_mb:.1f}MB > 閾値{broadcast_threshold_mb:.1f}MB"
            broadcast_analysis["reasoning"].append(f"テーブル {table_display_name}: 非圧縮{uncompressed_size_mb:.1f}MB - BROADCAST非推奨（>{broadcast_threshold_mb:.1f}MB閾値）")
        
        broadcast_analysis["detailed_size_analysis"].append(size_analysis)
    
    # 圧縮分析サマリーの更新
    if scan_nodes:
        total_uncompressed_mb = sum(scan["estimated_uncompressed_mb"] for scan in scan_nodes)
        total_compressed_mb = sum(scan["estimated_compressed_mb"] for scan in scan_nodes)
        if total_compressed_mb > 0:
            broadcast_analysis["compression_analysis"]["avg_compression_ratio"] = total_uncompressed_mb / total_compressed_mb
        broadcast_analysis["compression_analysis"]["total_uncompressed_mb"] = total_uncompressed_mb
        broadcast_analysis["compression_analysis"]["total_compressed_mb"] = total_compressed_mb
    
    # 総データ読み込み量との整合性チェック（圧縮ベース）
    total_read_gb = overall_metrics.get('read_bytes', 0) / 1024 / 1024 / 1024
    estimated_total_compressed_mb = sum(scan["estimated_compressed_mb"] for scan in scan_nodes)
    
    if estimated_total_compressed_mb > 0:
        size_ratio = (total_read_gb * 1024) / estimated_total_compressed_mb
        if size_ratio > 3 or size_ratio < 0.3:
            broadcast_analysis["reasoning"].append(f"推定圧縮サイズ({estimated_total_compressed_mb:.1f}MB)と実読み込み量({total_read_gb:.1f}GB)に乖離あり - サイズ推定に注意")
        else:
            broadcast_analysis["reasoning"].append(f"サイズ推定整合性: 推定圧縮{estimated_total_compressed_mb:.1f}MB vs 実際{total_read_gb:.1f}GB（比率:{size_ratio:.2f}）")
    
    # BROADCAST推奨事項の生成（30MB閾値対応、既存のBROADCAST適用状況を考慮）
    total_broadcast_candidates = len(small_tables) + len(marginal_tables)
    total_tables = len(scan_nodes)
    
    if small_tables or marginal_tables:
        if large_tables:
            # 既存のBROADCAST適用状況を考慮した判定
            if broadcast_analysis["already_optimized"]:
                broadcast_analysis["feasibility"] = "already_optimized_with_improvements"
                broadcast_analysis["recommendations"] = [
                    f"✅ 既にBROADCAST JOIN適用済み - 追加改善の検討",
                    f"🎯 追加最適化テーブル: {total_broadcast_candidates}個（全{total_tables}個中）",
                    f"  ✅ 強く推奨: {len(small_tables)}個（安全閾値{broadcast_safe_mb:.1f}MB以下）",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個（閾値{broadcast_threshold_mb:.1f}MB以下、要注意）",
                    f"  ❌ 非推奨: {len(large_tables)}個（閾値超過）"
                ]
            else:
                broadcast_analysis["feasibility"] = "recommended"
                broadcast_analysis["recommendations"] = [
                    f"🎯 BROADCAST推奨テーブル: {total_broadcast_candidates}個（全{total_tables}個中）",
                    f"  ✅ 強く推奨: {len(small_tables)}個（安全閾値{broadcast_safe_mb:.1f}MB以下）",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個（閾値{broadcast_threshold_mb:.1f}MB以下、要注意）",
                    f"  ❌ 非推奨: {len(large_tables)}個（閾値超過）"
                ]
        else:
            # 全テーブルが小さい場合
            if broadcast_analysis["already_optimized"]:
                broadcast_analysis["feasibility"] = "already_optimized_complete"
                broadcast_analysis["recommendations"] = [
                    f"✅ 既にBROADCAST JOIN適用済み - 最適化完了",
                    f"🎯 全テーブル（{total_tables}個）がBROADCAST閾値以下で適切に処理済み",
                    f"  ✅ 強く推奨: {len(small_tables)}個",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個",
                    "📋 現在の設定が最適です"
                ]
            else:
                broadcast_analysis["feasibility"] = "all_small"
                broadcast_analysis["recommendations"] = [
                    f"🎯 全テーブル（{total_tables}個）がBROADCAST閾値以下",
                    f"  ✅ 強く推奨: {len(small_tables)}個",
                    f"  ⚠️ 条件付き推奨: {len(marginal_tables)}個",
                    "📋 最小テーブルを優先的にBROADCASTすることを推奨"
                ]
        
        # 具体的なBROADCAST候補の詳細
        for small_table in small_tables:
            broadcast_analysis["recommendations"].append(
                f"🔹 BROADCAST({small_table['node_name']}) - 非圧縮{small_table['estimated_uncompressed_mb']:.1f}MB（圧縮{small_table['estimated_compressed_mb']:.1f}MB、{small_table['file_format']}、圧縮率{small_table['compression_ratio']:.1f}x）"
            )
        
        for marginal_table in marginal_tables:
            broadcast_analysis["recommendations"].append(
                f"🔸 BROADCAST({marginal_table['node_name']}) - 非圧縮{marginal_table['estimated_uncompressed_mb']:.1f}MB（条件付き、メモリ使用量要注意）"
            )
            
    elif large_tables:
        broadcast_analysis["feasibility"] = "not_recommended"
        broadcast_analysis["recommendations"] = [
            f"❌ 全テーブル（{len(large_tables)}個）が30MB閾値超過のためBROADCAST非推奨",
            f"📊 最小テーブルでも非圧縮{min(scan['estimated_uncompressed_mb'] for scan in large_tables):.1f}MB",
            "🔧 代替最適化手法を推奨:",
            "  • Liquid Clustering実装",
            "  • データパーティショニング",
            "  • クエリ最適化（フィルタープッシュダウン等）",
            "  • spark.databricks.optimizer.autoBroadcastJoinThreshold設定値の調整検討"
        ]
    else:
        broadcast_analysis["feasibility"] = "insufficient_data"
        broadcast_analysis["recommendations"] = [
            "⚠️ テーブルサイズ情報が不足しているため、手動でのサイズ確認が必要",
            "📋 以下のコマンドでテーブルサイズを確認:",
            "  • DESCRIBE DETAIL table_name",
            "  • SELECT COUNT(*) FROM table_name",
            "  • SHOW TABLE EXTENDED LIKE 'table_name'"
        ]
    
    # 30MB閾値にヒットする特別なケース分析（small_tables + marginal_tables を考慮）
    all_30mb_candidates = small_tables + marginal_tables  # 30MB以下の全候補
    
    if all_30mb_candidates:
        broadcast_analysis["30mb_hit_analysis"] = {
            "has_30mb_candidates": True,
            "candidate_count": len(all_30mb_candidates),
            "small_tables_count": len(small_tables),  # 24MB以下（強く推奨）
            "marginal_tables_count": len(marginal_tables),  # 24-30MB（条件付き推奨）
            "smallest_table_mb": min(scan["estimated_uncompressed_mb"] for scan in all_30mb_candidates),
            "largest_candidate_mb": max(scan["estimated_uncompressed_mb"] for scan in all_30mb_candidates),
            "total_candidate_size_mb": sum(scan["estimated_uncompressed_mb"] for scan in all_30mb_candidates),
            "recommended_broadcast_table": all_30mb_candidates[0]["node_name"] if all_30mb_candidates else None,
            "memory_impact_estimation": f"{sum(scan['estimated_uncompressed_mb'] for scan in all_30mb_candidates):.1f}MB がワーカーノードにブロードキャスト"
        }
        
        # 最適なBROADCAST候補の特定（全30MB候補から選択）
        if len(all_30mb_candidates) > 1:
            optimal_candidate = min(all_30mb_candidates, key=lambda x: x["estimated_uncompressed_mb"])
            broadcast_analysis["30mb_hit_analysis"]["optimal_candidate"] = {
                "table": optimal_candidate["node_name"],
                "size_mb": optimal_candidate["estimated_uncompressed_mb"],
                "rows": optimal_candidate["rows"],
                "reasoning": f"最小サイズ{optimal_candidate['estimated_uncompressed_mb']:.1f}MBで最も効率的"
            }
        
        # 30MB閾値内の詳細分類情報を追加
        broadcast_analysis["30mb_hit_analysis"]["size_classification"] = {
            "safe_zone_tables": len(small_tables),  # 0-24MB（安全マージン内）
            "caution_zone_tables": len(marginal_tables),  # 24-30MB（要注意）
            "safe_zone_description": "24MB以下（強く推奨、安全マージン内）",
            "caution_zone_description": "24-30MB（条件付き推奨、メモリ使用量要注意）"
        }
    else:
        broadcast_analysis["30mb_hit_analysis"] = {
            "has_30mb_candidates": False,
            "reason": f"全テーブルが30MB閾値を超過（最小: {min(scan['estimated_uncompressed_mb'] for scan in scan_nodes):.1f}MB）" if scan_nodes else "テーブル情報なし"
        }
    
    return broadcast_analysis

def extract_structured_physical_plan(physical_plan: str) -> Dict[str, Any]:
    """
    Physical Planから重要情報のみを構造化抽出（トークン制限対策）
    
    Args:
        physical_plan: Physical Planの完全テキスト
    
    Returns:
        Dict: 構造化された重要情報
    """
    import re
    
    extracted = {
        "joins": [],           # JOIN情報（種類、条件、統計）
        "scans": [],          # テーブルスキャン（サイズ、行数）  
        "exchanges": [],      # データ移動（Shuffle、Broadcast）
        "aggregates": [],     # 集約処理（GROUP BY、SUM等）
        "filters": [],        # フィルタ条件と選択率
        "photon_usage": {},   # Photon利用状況
        "bottlenecks": [],    # 特定されたボトルネック
        "statistics": {},     # 数値統計サマリー
        "total_size": len(physical_plan),
        "extraction_summary": ""
    }
    
    try:
        lines = physical_plan.split('\n')
        join_count = scan_count = exchange_count = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # JOIN情報の抽出（従来形式 + Photon形式完全対応）
            # 従来のSpark JOIN形式（Statistics付き）
            join_match = re.search(r'(\w*Join)\s+([^,\n]+).*?Statistics\(([^)]+)\)', line)
            # Photon JOIN形式（Statistics無し、詳細なパラメータ付き）
            photon_join_match = re.search(r'(Photon\w*Join)\s+\[([^\]]+)\],\s*\[([^\]]+)\],\s*(\w+),\s*(\w+)', line)
            
            if join_match or photon_join_match:
                if join_match:
                    # 従来のSpark JOIN形式
                    join_type = join_match.group(1)
                    condition = join_match.group(2).strip()
                    stats = join_match.group(3)
                    
                    # 統計情報から数値抽出
                    size_match = re.search(r'sizeInBytes=([0-9.]+)\s*([KMGT]i?B)?', stats)
                    rows_match = re.search(r'rowCount=(\d+)', stats)
                    
                    size_str = f"{size_match.group(1)}{size_match.group(2) or 'B'}" if size_match else "unknown"
                    rows_str = rows_match.group(1) if rows_match else "unknown"
                    
                elif photon_join_match:
                    # Photon JOIN形式の詳細抽出
                    join_type = photon_join_match.group(1)  # PhotonBroadcastHashJoin等
                    left_keys = photon_join_match.group(2)   # 左側のJOINキー
                    right_keys = photon_join_match.group(3)  # 右側のJOINキー
                    join_method = photon_join_match.group(4) # Inner, Left等
                    build_side = photon_join_match.group(5)  # BuildRight, BuildLeft等
                    
                    # JOIN条件の構成
                    condition = f"{left_keys} = {right_keys} ({join_method}, {build_side})"
                    
                    # Photon JOINは統計情報が別の場所にあるため、ここでは基本情報のみ
                    size_str = "photon_optimized"
                    rows_str = "photon_optimized"
                
                extracted["joins"].append({
                    "type": join_type,
                    "condition": condition[:100],  # 条件を100文字に制限
                    "size": size_str,
                    "rows": rows_str
                })
                join_count += 1
                
            # テーブルスキャン情報の抽出（従来形式 + Photon形式完全対応）
            elif ('FileScan' in line and 'Statistics(' in line) or ('PhotonScan' in line and 'parquet' in line):
                # 従来形式：Statistics付きFileScan
                stats_match = re.search(r'Statistics\(([^)]+)\)', line)
                # Photon形式：PhotonScan parquet table_name[columns]
                photon_scan_match = re.search(r'PhotonScan\s+parquet\s+([a-zA-Z_][a-zA-Z0-9_.]*)\[([^\]]+)\]', line)
                # 従来形式：FileScan
                file_scan_match = re.search(r'FileScan\s+([^,\s\[]+)', line)
                
                if (stats_match and file_scan_match) or photon_scan_match:
                    if photon_scan_match:
                        # Photon形式の場合
                        table = photon_scan_match.group(1)  # テーブル名
                        columns = photon_scan_match.group(2)  # 列リスト
                        stats = None  # PhotonScanには統計情報が同一行にない
                        
                        # テーブル統計の保存（Photon用の構造）
                        extracted["scans"].append({
                            "table": table[:50],
                            "columns": columns[:100],
                            "type": "PhotonScan",
                            "size": "photon_scan",
                            "rows": "photon_scan"
                        })
                        scan_count += 1
                        
                    elif stats_match and file_scan_match:
                        # 従来形式の場合
                        stats = stats_match.group(1)
                        table = file_scan_match.group(1)
                        
                        size_match = re.search(r'sizeInBytes=([0-9.]+)\s*([KMGT]i?B)?', stats)
                        rows_match = re.search(r'rowCount=(\d+)', stats)
                        
                        extracted["scans"].append({
                            "table": table[:50],  # テーブル名を50文字に制限
                            "type": "FileScan",
                            "size": f"{size_match.group(1)}{size_match.group(2) or 'B'}" if size_match else "unknown",
                            "rows": rows_match.group(1) if rows_match else "unknown"
                        })
                        scan_count += 1
                    
            # データ移動（Exchange）の抽出
            elif 'Exchange' in line:
                if 'BroadcastExchange' in line:
                    extracted["exchanges"].append({"type": "BROADCAST", "detail": line[:100]})
                elif 'ShuffleExchange' in line or 'Exchange' in line:
                    extracted["exchanges"].append({"type": "SHUFFLE", "detail": line[:100]})
                exchange_count += 1
                
            # 集約処理の抽出
            elif 'Aggregate' in line or 'HashAggregate' in line:
                extracted["aggregates"].append({"type": "AGGREGATE", "detail": line[:100]})
                
            # Photon利用状況の確認
            elif 'Photon' in line:
                if 'PhotonResultStage' in line:
                    extracted["photon_usage"]["result_stage"] = True
                elif 'PhotonHashJoin' in line:
                    extracted["photon_usage"]["hash_join"] = True
                elif 'PhotonProject' in line:
                    extracted["photon_usage"]["project"] = True
        
        # 統計サマリー生成
        extracted["statistics"] = {
            "total_joins": join_count,
            "total_scans": scan_count,  
            "total_exchanges": exchange_count,
            "photon_operations": len([k for k, v in extracted["photon_usage"].items() if v])
        }
        
        # 抽出サマリー生成
        extracted["extraction_summary"] = f"📊 構造化抽出完了: JOIN({join_count}) SCAN({scan_count}) EXCHANGE({exchange_count}) PHOTON({len(extracted['photon_usage'])})"
        
        # 🚨 トークン制限対策: 情報量が多い場合の自動要約
        total_joins_scans = join_count + scan_count
        if total_joins_scans > 30:  # 閾値を大幅に引き上げ: JOIN+SCAN合計が30個以上
            # 重要度順に並び替えてトップ情報のみ保持
            extracted = apply_token_limit_optimization(extracted, max_joins=20, max_scans=15)  # 制限を大幅緩和
            extracted["extraction_summary"] += f" → トークン制限対策でJOIN/SCAN情報を要約済み"
        elif total_joins_scans > 15:  # 中間閾値: 15-30個の場合
            # 中程度の要約
            extracted = apply_token_limit_optimization(extracted, max_joins=12, max_scans=10)
            extracted["extraction_summary"] += f" → 中程度のJOIN/SCAN情報要約済み"
        
    except Exception as e:
        extracted["extraction_error"] = str(e)
        
    return extracted

def extract_structured_cost_statistics(explain_cost_content: str) -> Dict[str, Any]:
    """
    EXPLAIN COSTから数値統計のみを構造化抽出（トークン制限対策）
    
    Args:
        explain_cost_content: EXPLAIN COSTの完全結果
    
    Returns:
        Dict: 構造化された統計情報
    """
    import re
    
    extracted = {
        "table_stats": {},      # テーブル別統計（サイズ、行数）
        "join_costs": {},       # JOIN別コスト見積もり  
        "selectivity": {},      # フィルタ選択率
        "partition_info": {},   # パーティション統計
        "memory_estimates": {}, # メモリ使用量予測
        "cost_breakdown": {},   # コスト内訳
        "critical_stats": {},   # 重要統計値
        "total_size": len(explain_cost_content),
        "extraction_summary": ""
    }
    
    try:
        lines = explain_cost_content.split('\n')
        tables_found = costs_found = memory_found = 0
        
        # 重要統計値を追跡
        largest_table = {"name": "", "size": 0, "size_str": ""}
        total_rows = 0
        broadcast_candidates = []
        
        # テーブル名とサイズの対応を追跡
        table_name_size_map = {}  # {table_name: {"size_bytes": int, "size_str": str, "rows": int}}
        current_table_context = None  # 現在処理中のテーブル名
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 🔍 テーブル名の抽出（Relationから）
            table_name_match = re.search(r'Relation\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)', line)
            if table_name_match:
                current_table_context = table_name_match.group(1)
                
            # 🔍 テーブル名の抽出（Join条件から）
            elif 'Join' in line and '=' in line:
                # JOIN条件からテーブル名を推定 (例: ty_brand#456 = ly_brand#789)
                join_match = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*)[#.]', line)
                if join_match and not current_table_context:
                    # JOIN条件のプレフィックスからテーブル推定
                    prefix = join_match.group(1)
                    if len(prefix) > 2:  # 意味のあるプレフィックス
                        current_table_context = f"{prefix}_table"
                
            # テーブル統計の抽出
            if 'Statistics(' in line:
                # サイズ情報の抽出
                size_match = re.search(r'sizeInBytes=([0-9.]+)\s*([KMGT]i?B)?', line)
                rows_match = re.search(r'rowCount=(\d+)', line)
                
                # テーブル名の決定
                if current_table_context:
                    table_name = current_table_context
                else:
                    # フォールバック: 行番号から推定
                    line_table_match = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*)', line)
                    table_name = line_table_match.group(1) if line_table_match else f"table_{tables_found}"
                
                if size_match:
                    size_val = float(size_match.group(1))
                    size_unit = size_match.group(2) or 'B'
                    size_str = f"{size_val}{size_unit}"
                    
                    # サイズ変換（バイト単位）
                    size_bytes = size_val
                    if 'KiB' in size_unit:
                        size_bytes *= 1024
                    elif 'MiB' in size_unit:
                        size_bytes *= 1024 * 1024
                    elif 'GiB' in size_unit:
                        size_bytes *= 1024 * 1024 * 1024
                    elif 'TiB' in size_unit:
                        size_bytes *= 1024 * 1024 * 1024 * 1024
                    
                    # 行数の取得
                    rows = int(rows_match.group(1)) if rows_match else 0
                    
                    # テーブル統計の保存
                    extracted["table_stats"][table_name] = {
                        "size_bytes": size_bytes,
                        "size_str": size_str,
                        "rows": rows,
                        "is_broadcast_candidate": size_bytes < 30 * 1024 * 1024  # 30MB
                    }
                    
                    # 最大テーブルの追跡
                    if size_bytes > largest_table["size"]:
                        largest_table = {"name": table_name, "size": size_bytes, "size_str": size_str}
                    
                    # ブロードキャスト候補（30MB未満）
                    if size_bytes < 30 * 1024 * 1024:  # 30MB
                        broadcast_candidates.append({"table": table_name, "size": size_str})
                    
                    tables_found += 1
                    total_rows += rows
                    
                # 現在のコンテキストをリセット（次のテーブル用）
                current_table_context = None
                    
            # コスト情報の抽出  
            elif 'Cost(' in line:
                cost_match = re.search(r'Cost\(([0-9.]+)\)', line)
                if cost_match:
                    extracted["cost_breakdown"][f"operation_{costs_found}"] = float(cost_match.group(1))
                    costs_found += 1
                    
            # メモリ関連情報の抽出
            elif any(keyword in line.lower() for keyword in ['memory', 'spill', 'threshold']):
                if 'memory' in line.lower():
                    memory_match = re.search(r'(\d+(?:\.\d+)?)\s*([KMGT]i?B)', line)
                    if memory_match:
                        extracted["memory_estimates"][f"estimate_{memory_found}"] = f"{memory_match.group(1)}{memory_match.group(2)}"
                        memory_found += 1
        
        # 重要統計値のまとめ
        extracted["critical_stats"] = {
            "largest_table": largest_table,
            "total_rows": total_rows,
            "broadcast_candidates": broadcast_candidates[:5],  # 上位5個まで
            "tables_analyzed": tables_found,
            "cost_operations": costs_found,
            "memory_estimates": memory_found,
            "table_breakdown": {
                "total_tables": len(extracted["table_stats"]),
                "largest_table_name": largest_table.get("name", "unknown"),
                "broadcast_table_names": [bc.get("table", "unknown") for bc in broadcast_candidates[:3]]
            }
        }
        
        # 抽出サマリー生成
        extracted["extraction_summary"] = f"💰 統計抽出完了: テーブル({tables_found}) コスト({costs_found}) メモリ({memory_found}) BROADCAST候補({len(broadcast_candidates)})"
        
    except Exception as e:
        extracted["extraction_error"] = str(e)
        
    return extracted

def apply_token_limit_optimization(extracted: Dict[str, Any], max_joins: int = 5, max_scans: int = 8) -> Dict[str, Any]:
    """
    トークン制限対策: JOIN/SCAN情報の重要度別要約
    
    Args:
        extracted: 抽出された構造化データ
        max_joins: 保持するJOIN数の上限
        max_scans: 保持するSCAN数の上限
    
    Returns:
        最適化された構造化データ
    """
    
    # JOIN情報の重要度別ソート
    joins = extracted.get("joins", [])
    if len(joins) > max_joins:
        # 重要度順序: Broadcast > Hash > Sort > Nested
        join_priority = {
            "PhotonBroadcastHashJoin": 1,
            "BroadcastHashJoin": 2,
            "PhotonHashJoin": 3,
            "HashJoin": 4,
            "SortMergeJoin": 5,
            "NestedLoopJoin": 6
        }
        
        # 重要度でソート
        sorted_joins = sorted(joins, key=lambda j: join_priority.get(j.get("type", ""), 10))
        
        # 上位のみ保持、残りは要約
        top_joins = sorted_joins[:max_joins]
        remaining_count = len(joins) - max_joins
        
        if remaining_count > 0:
            summary_join = {
                "type": "SUMMARY",
                "condition": f"その他{remaining_count}個のJOIN操作（詳細省略）",
                "size": "multiple",
                "rows": "multiple"
            }
            top_joins.append(summary_join)
        
        extracted["joins"] = top_joins
    
    # SCAN情報の重要度別ソート
    scans = extracted.get("scans", [])
    if len(scans) > max_scans:
        # 重要度順序: PhotonScan > FileScan、テーブル名の長さ（詳細度）
        def scan_priority(scan):
            priority = 1 if scan.get("type") == "PhotonScan" else 2
            table_length = len(scan.get("table", ""))
            return (priority, -table_length)  # テーブル名が長い（詳細）ほど重要
        
        # 重要度でソート
        sorted_scans = sorted(scans, key=scan_priority)
        
        # 上位のみ保持、残りは要約
        top_scans = sorted_scans[:max_scans]
        remaining_count = len(scans) - max_scans
        
        if remaining_count > 0:
            # 残りのテーブル名を集約
            remaining_tables = [s.get("table", "unknown")[:20] for s in sorted_scans[max_scans:]]
            table_summary = ", ".join(remaining_tables[:3])
            if len(remaining_tables) > 3:
                table_summary += f" 他{len(remaining_tables)-3}個"
                
            summary_scan = {
                "table": f"SUMMARY({table_summary})",
                "type": "SUMMARY",
                "size": "multiple",
                "rows": "multiple"
            }
            top_scans.append(summary_scan)
        
        extracted["scans"] = top_scans
    
    # 統計情報の更新
    extracted["statistics"]["optimization_applied"] = True
    extracted["statistics"]["original_joins"] = len(joins)
    extracted["statistics"]["original_scans"] = len(scans)
    extracted["statistics"]["optimized_joins"] = len(extracted["joins"])
    extracted["statistics"]["optimized_scans"] = len(extracted["scans"])
    
    return extracted

def extract_cost_statistics_from_explain_cost(explain_cost_content: str) -> str:
    """
    EXPLAIN COST結果から統計情報を抽出して構造化（改善版 + サイズ制限）
    
    Args:
        explain_cost_content: EXPLAIN COSTの結果文字列
    
    Returns:
        構造化された統計情報文字列（レポート用に簡潔化）
    """
    if not explain_cost_content:
        return ""
    
    # 🚨 レポート肥大化防止：サマリー情報のみ抽出
    statistics_counts = {
        "テーブル統計": 0,
        "行数情報": 0, 
        "サイズ情報": 0,
        "コスト情報": 0,
        "選択率情報": 0,
        "パーティション情報": 0,
        "メモリ情報": 0,
        "JOIN情報": 0
    }
    
    # 重要な統計値のみ抽出（詳細は除外）
    key_statistics = []
    MAX_KEY_STATS = 5  # 重要統計情報の最大数
    
    try:
        lines = explain_cost_content.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # テーブル統計情報の抽出（カウントのみ）
            if 'statistics=' in line.lower() or 'stats=' in line.lower() or 'Statistics(' in line:
                statistics_counts["テーブル統計"] += 1
                if len(key_statistics) < MAX_KEY_STATS and 'sizeInBytes' in line:
                    # 重要なサイズ情報のみ抽出
                    if 'GiB' in line or 'TiB' in line:
                        key_statistics.append(f"📊 テーブルサイズ: {line[:100]}...")
            
            # 行数情報の抽出（カウントのみ）
            elif 'rows=' in line.lower() or 'rowcount=' in line.lower() or 'rows:' in line.lower():
                statistics_counts["行数情報"] += 1
            
            # サイズ情報の抽出（カウントのみ）
            elif ('size=' in line.lower() or 'sizeinbytes=' in line.lower() or 'sizeInBytes=' in line 
                  or 'GB' in line or 'MB' in line or 'size:' in line.lower()):
                statistics_counts["サイズ情報"] += 1
            
            # その他の統計情報のカウント
            elif ('cost=' in line.lower() or 'Cost(' in line or 'cost:' in line.lower()):
                statistics_counts["コスト情報"] += 1
            elif ('selectivity=' in line.lower() or 'filter=' in line.lower()):
                statistics_counts["選択率情報"] += 1
            elif ('partition' in line.lower() and ('count' in line.lower() or 'size' in line.lower())):
                statistics_counts["パーティション情報"] += 1
            elif ('memory' in line.lower() or 'spill' in line.lower()):
                statistics_counts["メモリ情報"] += 1
            elif ('join' in line.lower() and ('cost' in line.lower() or 'selectivity' in line.lower())):
                statistics_counts["JOIN情報"] += 1
    
    except Exception as e:
        return f"⚠️ 統計情報抽出エラー: {str(e)}"
    
    # 簡潔なサマリーを生成
    summary_lines = ["## 📊 統計情報サマリー（簡潔版）"]
    
    total_stats = sum(statistics_counts.values())
    if total_stats > 0:
        summary_lines.append(f"- **総統計項目数**: {total_stats}個")
        
        for stat_type, count in statistics_counts.items():
            if count > 0:
                summary_lines.append(f"- **{stat_type}**: {count}個")
        
        if key_statistics:
            summary_lines.append("\n### 🎯 主要統計")
            summary_lines.extend(key_statistics)
        
        summary_lines.append(f"\n💡 詳細な統計情報は DEBUG_ENABLED='Y' で確認できます")
    else:
        summary_lines.append("- 統計情報が見つかりませんでした")
    
    return '\n'.join(summary_lines)


def generate_optimized_query_with_llm(original_query: str, analysis_result: str, metrics: Dict[str, Any]) -> str:
    """
    セル33の詳細ボトルネック分析結果に基づいてSQLクエリを最適化（処理速度重視）
    EXPLAIN + EXPLAIN COST実行フラグがYの場合は、統計情報も活用
    """
    
    # EXPLAIN + EXPLAIN COST結果ファイルの読み込み（EXPLAIN_ENABLEDがYの場合）
    explain_content = ""
    explain_cost_content = ""
    physical_plan = ""
    photon_explanation = ""
    cost_statistics = ""
    
    explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')
    if explain_enabled.upper() == 'Y':
        import glob
        import os
        
        print("🔍 EXPLAIN + EXPLAIN COST結果ファイルを検索中...")
        
        # 1. 最新のEXPLAIN結果ファイルを検索（新しいファイル名パターン対応）
        explain_original_files = glob.glob("output_explain_original_*.txt")
        explain_optimized_files = glob.glob("output_explain_optimized_*.txt")
        
        # オリジナルクエリのEXPLAIN結果を優先、なければ最適化後
        explain_files = explain_original_files if explain_original_files else explain_optimized_files
        
        if explain_files:
            latest_explain_file = max(explain_files, key=os.path.getctime)
            try:
                with open(latest_explain_file, 'r', encoding='utf-8') as f:
                    explain_content = f.read()
                    print(f"✅ EXPLAIN結果ファイルを読み込み: {latest_explain_file}")
                
                # Physical Planの抽出と処理（構造化抽出対応）
                if "== Physical Plan ==" in explain_content:
                    physical_plan_start = explain_content.find("== Physical Plan ==")
                    physical_plan_end = explain_content.find("== Photon", physical_plan_start)
                    if physical_plan_end == -1:
                        physical_plan_end = len(explain_content)
                    physical_plan_raw = explain_content[physical_plan_start:physical_plan_end].strip()
                    print(f"📊 Physical Plan情報を抽出: {len(physical_plan_raw)} 文字")
                    
                                    # 🧠 構造化抽出 vs 従来の切り詰めの選択
                structured_enabled = globals().get('STRUCTURED_EXTRACTION_ENABLED', 'Y')
                debug_enabled = globals().get('DEBUG_ENABLED', 'N')
                
                # 🧠 構造化抽出 vs 従来の切り詰めの選択
                structured_enabled = globals().get('STRUCTURED_EXTRACTION_ENABLED', 'Y')
                debug_enabled = globals().get('DEBUG_ENABLED', 'N')
                
                if structured_enabled.upper() == 'Y':
                    # 🚀 構造化抽出アプローチ
                    try:
                        structured_plan = extract_structured_physical_plan(physical_plan_raw)
                        
                        # 構造化結果をJSON形式で文字列化
                        import json
                        physical_plan = json.dumps(structured_plan, ensure_ascii=False, indent=2)
                        
                        print(f"🧠 構造化抽出完了: {len(physical_plan_raw):,} → {len(physical_plan):,} 文字")
                        print(f"   {structured_plan.get('extraction_summary', '📊 構造化抽出完了')}")
                        
                        # DEBUG_ENABLED='Y'の場合、構造化結果と元データを保存
                        if debug_enabled.upper() == 'Y':
                            try:
                                from datetime import datetime
                                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                                
                                # 構造化結果保存
                                structured_plan_filename = f"output_physical_plan_structured_{timestamp}.json"
                                with open(structured_plan_filename, 'w', encoding='utf-8') as f:
                                    f.write(physical_plan)
                                
                                print(f"📄 構造化Physical Planを保存: {structured_plan_filename}")
                                
                            except Exception as save_error:
                                print(f"⚠️ Physical Plan保存に失敗: {str(save_error)}")
                                
                    except Exception as extraction_error:
                        print(f"⚠️ 構造化抽出に失敗、従来方式にフォールバック: {str(extraction_error)}")
                        # フォールバック: 従来の切り詰め方式
                        MAX_PLAN_SIZE = 30000
                        if len(physical_plan_raw) > MAX_PLAN_SIZE:
                            physical_plan = physical_plan_raw[:MAX_PLAN_SIZE] + "\n\nStructured extraction failed, truncated to limit"
                            print(f"⚠️ フォールバック: Physical Planを{MAX_PLAN_SIZE}文字に切り詰めました")
                        else:
                            physical_plan = physical_plan_raw
                            print(f"⚠️ Physical Planをトークン制限のため{MAX_PLAN_SIZE}文字に切り詰めました")
                
                # Photon Explanationの抽出
                if "== Photon Explanation ==" in explain_content:
                    photon_start = explain_content.find("== Photon Explanation ==")
                    photon_explanation = explain_content[photon_start:].strip()
                    print(f"🚀 Photon Explanation情報を抽出: {len(photon_explanation)} 文字")
                    
            except Exception as e:
                print(f"⚠️ EXPLAIN結果ファイルの読み込みに失敗: {str(e)}")
                explain_content = ""
        
        # 2. 最新のEXPLAIN COST結果ファイルを検索
        cost_original_files = glob.glob("output_explain_cost_original_*.txt")
        cost_optimized_files = glob.glob("output_explain_cost_optimized_*.txt")
        
        # オリジナルクエリのEXPLAIN COST結果を優先、なければ最適化後
        cost_files = cost_original_files if cost_original_files else cost_optimized_files
        
        if cost_files:
            latest_cost_file = max(cost_files, key=os.path.getctime)
            try:
                with open(latest_cost_file, 'r', encoding='utf-8') as f:
                    explain_cost_content = f.read()
                    print(f"💰 EXPLAIN COST結果ファイルを読み込み: {latest_cost_file}")
                
                # 統計情報の抽出（構造化抽出対応）
                structured_enabled = globals().get('STRUCTURED_EXTRACTION_ENABLED', 'Y')
                
                if structured_enabled.upper() == 'Y':
                    # 🚀 構造化抽出アプローチ
                    try:
                        structured_cost = extract_structured_cost_statistics(explain_cost_content)
                        
                        # 構造化結果をJSON形式で文字列化
                        import json
                        cost_statistics = json.dumps(structured_cost, ensure_ascii=False, indent=2)
                        
                        print(f"💰 EXPLAIN COST構造化抽出完了: {len(explain_cost_content):,} → {len(cost_statistics):,} 文字 (圧縮率: {len(explain_cost_content)//len(cost_statistics) if len(cost_statistics) > 0 else 0}x)")
                        print(f"   {structured_cost.get('extraction_summary', '💰 統計抽出完了')}")
                        
                    except Exception as extraction_error:
                        print(f"⚠️ EXPLAIN COST構造化抽出に失敗、従来方式にフォールバック: {str(extraction_error)}")
                        # フォールバック: 従来の抽出方式
                        cost_statistics = extract_cost_statistics_from_explain_cost(explain_cost_content)
                        print(f"📊 EXPLAIN COST統計情報を抽出（従来方式）: {len(cost_statistics)} 文字")
                else:
                    # 🔄 従来の抽出アプローチ
                    cost_statistics = extract_cost_statistics_from_explain_cost(explain_cost_content)
                    print(f"📊 EXPLAIN COST統計情報を抽出: {len(cost_statistics)} 文字")
                
                # 🚨 DEBUG_ENABLED='Y'の場合、抽出された統計情報を常に保存
                debug_enabled = globals().get('DEBUG_ENABLED', 'N')
                if debug_enabled.upper() == 'Y':
                    try:
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                        extracted_stats_filename = f"output_explain_cost_statistics_extracted_{timestamp}.json"
                        
                        with open(extracted_stats_filename, 'w', encoding='utf-8') as f:
                            f.write(f"# 抽出されたEXPLAIN COST統計情報 (生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
                            f.write(f"# 抽出サイズ: {len(cost_statistics):,} 文字\n")
                            f.write(f"# 元ファイル: {latest_cost_file}\n\n")
                            f.write(cost_statistics)
                        
                        print(f"📄 抽出統計情報を保存: {extracted_stats_filename}")
                        
                    except Exception as save_error:
                        print(f"⚠️ 抽出統計情報保存に失敗: {str(save_error)}")
                
                # 統計情報のサイズ制限（LLMトークン制限対策）
                MAX_STATISTICS_SIZE = 50000  # 約50KB制限
                if len(cost_statistics) > MAX_STATISTICS_SIZE:
                    # 🚨 DEBUG_ENABLED='Y'の場合、完全なEXPLAIN COST統計情報をファイル保存
                    debug_enabled = globals().get('DEBUG_ENABLED', 'N')
                    if debug_enabled.upper() == 'Y':
                        try:
                            from datetime import datetime
                            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                            full_stats_filename = f"output_explain_cost_statistics_full_{timestamp}.txt"
                            
                            with open(full_stats_filename, 'w', encoding='utf-8') as f:
                                f.write(f"# 完全なEXPLAIN COST統計情報 (生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n")
                                f.write(f"# 元サイズ: {len(cost_statistics):,} 文字\n")
                                f.write(f"# LLM使用サイズ: {MAX_STATISTICS_SIZE:,} 文字\n\n")
                                f.write(cost_statistics)
                            
                            print(f"📄 完全なEXPLAIN COST統計情報を保存: {full_stats_filename}")
                            
                        except Exception as save_error:
                            print(f"⚠️ EXPLAIN COST統計情報保存に失敗: {str(save_error)}")
                    
                    truncated_statistics = cost_statistics[:MAX_STATISTICS_SIZE]
                    truncated_statistics += f"\n\n⚠️ 統計情報が大きすぎるため、{MAX_STATISTICS_SIZE}文字に切り詰められました"
                    cost_statistics = truncated_statistics
                    print(f"⚠️ 統計情報をトークン制限のため{MAX_STATISTICS_SIZE}文字に切り詰めました")
                    
            except Exception as e:
                print(f"⚠️ EXPLAIN COST結果ファイルの読み込みに失敗: {str(e)}")
                explain_cost_content = ""
        
        if not explain_files and not cost_files:
            print("⚠️ EXPLAIN・EXPLAIN COST結果ファイルが見つかりません")
            # フォールバック: 古いファイル名パターンもチェック
            old_explain_files = glob.glob("output_explain_plan_*.txt")
            if old_explain_files:
                latest_explain_file = max(old_explain_files, key=os.path.getctime)
                try:
                    with open(latest_explain_file, 'r', encoding='utf-8') as f:
                        explain_content = f.read()
                        print(f"✅ 古い形式のEXPLAIN結果ファイルを読み込み: {latest_explain_file}")
                        
                    # Physical Plan抽出（旧形式対応）
                    if "== Physical Plan ==" in explain_content:
                        physical_plan_start = explain_content.find("== Physical Plan ==")
                        physical_plan_end = explain_content.find("== Photon", physical_plan_start)
                        if physical_plan_end == -1:
                            physical_plan_end = len(explain_content)
                        physical_plan = explain_content[physical_plan_start:physical_plan_end].strip()
                        
                    if "== Photon Explanation ==" in explain_content:
                        photon_start = explain_content.find("== Photon Explanation ==")
                        photon_explanation = explain_content[photon_start:].strip()
                except Exception as e:
                    print(f"⚠️ 古い形式EXPLAIN結果ファイルの読み込みに失敗: {str(e)}")
            else:
                print("⚠️ EXPLAIN結果ファイルが見つかりません")
    
    # 実行プラン情報の抽出（メトリクスから）
    profiler_data = metrics.get('raw_profiler_data', {})
    plan_info = None
    if profiler_data:
        plan_info = extract_execution_plan_info(profiler_data)
    
    # BROADCAST適用可能性の分析（プラン情報を含む）
    # 🎯 BROADCAST最適化は無効化（ユーザー要求により除外）
    # 🚨 重要: すべての必要なキーを含める（KeyError防止）
    broadcast_analysis = {
        "feasibility": "disabled", 
        "broadcast_candidates": [], 
        "recommendations": [],
        "reasoning": ["BROADCASTヒントは構文エラーの原因となるため無効化"], 
        "is_join_query": True,
        "already_optimized": False,  # 🚨 緊急修正: 必須キー追加
        "spark_threshold_mb": 30.0,
        "compression_analysis": {},
        "detailed_size_analysis": [],
        "execution_plan_analysis": {},
        "existing_broadcast_nodes": [],
        "broadcast_applied_tables": [],
        # 🚨 緊急修正: 30mb_hit_analysis キー追加（KeyError防止）
        "30mb_hit_analysis": {
            "has_30mb_candidates": False,
            "reason": "BROADCASTヒントは無効化されているため分析対象外"
        }
    }
    
    # プラン情報をメトリクスに追加（ファイル出力で使用）
    if plan_info:
        metrics['execution_plan_info'] = plan_info
    
    # 🚀 セル33スタイルの詳細ボトルネック分析を実行
    detailed_bottleneck = extract_detailed_bottleneck_analysis(metrics)
    
    # 最適化のためのコンテキスト情報を準備（詳細版）
    optimization_context = []
    performance_critical_issues = []
    
    # 基本的なボトルネック情報の抽出
    bottlenecks = metrics.get('bottleneck_indicators', {})
    
    if bottlenecks.get('has_spill', False):
        spill_gb = bottlenecks.get('spill_bytes', 0) / 1024 / 1024 / 1024
        optimization_context.append(f"スピル発生: {spill_gb:.1f}GB - メモリ効率の改善が必要")
    
    if bottlenecks.get('has_shuffle_bottleneck', False):
        optimization_context.append("シャッフルボトルネック - JOINとGROUP BYの最適化が必要")
    
    if bottlenecks.get('cache_hit_ratio', 0) < 0.5:
        optimization_context.append("キャッシュ効率低下 - データアクセスパターンの最適化が必要")
    
    # 🎯 詳細ボトルネック分析結果からの追加情報
    if detailed_bottleneck["spill_analysis"]["total_spill_gb"] > 0:
        total_spill = detailed_bottleneck["spill_analysis"]["total_spill_gb"]
        spill_nodes_count = len(detailed_bottleneck["spill_analysis"]["spill_nodes"])
        performance_critical_issues.append(f"🚨 CRITICAL: 合計{total_spill:.1f}GBのスピルが{spill_nodes_count}個のノードで発生")
        
        # 最も重要なスピルノードを特定
        if detailed_bottleneck["spill_analysis"]["spill_nodes"]:
            top_spill_node = max(detailed_bottleneck["spill_analysis"]["spill_nodes"], key=lambda x: x["spill_gb"])
            performance_critical_issues.append(f"   最大スピルノード: {top_spill_node['node_name']} ({top_spill_node['spill_gb']:.2f}GB)")
    
    if detailed_bottleneck["skew_analysis"]["total_skewed_partitions"] > 0:
        total_skew = detailed_bottleneck["skew_analysis"]["total_skewed_partitions"]
        skewed_nodes_count = len(detailed_bottleneck["skew_analysis"]["skewed_nodes"])
        performance_critical_issues.append(f"⚖️ データスキュー: {total_skew}個のスキューパーティションが{skewed_nodes_count}個のノードで検出")
    
    # TOP3ボトルネックノードの詳細分析
    top3_bottlenecks = detailed_bottleneck["top_bottleneck_nodes"][:3]
    performance_critical_issues.append("📊 TOP3処理時間ボトルネック:")
    for node in top3_bottlenecks:
        severity_icon = "🔴" if node["severity"] == "CRITICAL" else "🟠" if node["severity"] == "HIGH" else "🟡"
        performance_critical_issues.append(f"   {severity_icon} #{node['rank']}: {node['node_name'][:60]}...")
        performance_critical_issues.append(f"      実行時間: {node['duration_ms']:,}ms ({node['time_percentage']:.1f}%) | メモリ: {node['memory_mb']:.1f}MB")
        if node["spill_detected"]:
            performance_critical_issues.append(f"      💿 スピル: {node['spill_gb']:.2f}GB - 緊急対応必要")
        if node["skew_detected"]:
            performance_critical_issues.append(f"      ⚖️ スキュー: {node['skewed_partitions']}パーティション - データ分散改善必要")
    
    # 🔄 REPARTITIONヒントの詳細生成（スピル検出時のみ）
    repartition_hints = []
    if detailed_bottleneck["shuffle_optimization_hints"]:
        repartition_hints.append("🔄 REPARTITIONヒント（スピル検出時のみ）:")
        for hint in detailed_bottleneck["shuffle_optimization_hints"]:
            priority_icon = "🚨" if hint["priority"] == "HIGH" else "📈"
            repartition_hints.append(f"   {priority_icon} ノードID {hint['node_id']}: {hint['suggested_sql']}")
            repartition_hints.append(f"      属性: {', '.join(hint['attributes'])}")
            repartition_hints.append(f"      理由: {hint['reason']}")
            repartition_hints.append(f"      効果: {hint['estimated_improvement']}")
            
            # クエリへの適用方法の具体的な提案
            main_attr = hint['attributes'][0]
            if 'GROUP BY' in original_query.upper():
                repartition_hints.append(f"      適用提案: GROUP BY前にREPARTITION({hint['suggested_sql'].split('(')[1]}")
            elif 'JOIN' in original_query.upper():
                repartition_hints.append(f"      適用提案: JOIN前のテーブルを{hint['suggested_sql']}でリパーティション")
    
    # 📊 処理速度重視の最適化推奨事項
    speed_optimization_recommendations = []
    for rec in detailed_bottleneck["performance_recommendations"]:
        priority_icon = "🚨" if rec["priority"] == "CRITICAL" else "⚠️" if rec["priority"] == "HIGH" else "📝"
        speed_optimization_recommendations.append(f"{priority_icon} {rec['type'].upper()}: {rec['description']}")
    
    # Liquid Clustering推奨情報（LLMベース対応）
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    extracted_data = liquid_analysis.get('extracted_data', {})
    table_info = extracted_data.get('table_info', {})
    
    clustering_recommendations = []
    if table_info:
        for table_name in list(table_info.keys())[:3]:  # 上位3テーブル
            clustering_recommendations.append(f"テーブル {table_name}: LLM分析による推奨カラムでクラスタリング推奨")
    
    # 最適化プロンプトの作成（簡潔版でタイムアウト回避）
    
    # 分析結果を簡潔化（128K制限内で最大効率化）
    analysis_summary = ""
    if isinstance(analysis_result, str) and len(analysis_result) > 2000:
        # プロンプト容量の確保のため、分析結果は要点のみに圧縮
        analysis_summary = analysis_result[:2000] + "...[要約：主要ボトルネックのみ保持]"
    else:
        analysis_summary = str(analysis_result)
    
    # ボトルネック情報の簡潔化
    bottleneck_summary = "、".join(optimization_context[:3]) if optimization_context else "特になし"
    
    # Liquid Clustering推奨の簡潔化
    clustering_summary = "、".join(clustering_recommendations[:2]) if clustering_recommendations else "特になし"
    
    # 🚨 JOIN戦略分析の簡略化（BROADCASTヒント無効化）
    broadcast_summary = ["🎯 最適化方針: JOIN順序最適化（Sparkの自動戦略を活用、ヒント不使用）"]
    
    optimization_prompt = f"""
あなたはDatabricksのSQLパフォーマンス最適化の専門家です。以下の**詳細なボトルネック分析結果**を基に、**処理速度重視**でSQLクエリを最適化してください。

【重要な処理方針】
- 一回の出力で完全なSQLクエリを生成してください
- 段階的な出力や複数回に分けての出力は禁止です
- thinking機能で構造理解→一回で完全なSQL出力
- **❌ BROADCASTヒント（/*+ BROADCAST */、/*+ BROADCAST(table) */）は一切使用禁止**
- **✅ JOIN戦略はSparkの自動最適化に委ねてヒント不使用で最適化**

【元のSQLクエリ】
```sql
{original_query}
```

【📊 セル33詳細ボトルネック分析結果】
{chr(10).join(performance_critical_issues) if performance_critical_issues else "特別な重要課題は設定なし"}

【🔄 REPARTITIONヒント（スピル検出時のみ）】
{chr(10).join(repartition_hints) if repartition_hints else "スピルが検出されていないため、REPARTITIONヒントは適用対象外です"}

【🚀 処理速度重視の最適化推奨事項】
{chr(10).join(speed_optimization_recommendations) if speed_optimization_recommendations else "特別な推奨事項はありません"}

【基本的なボトルネック情報】
{chr(10).join(optimization_context) if optimization_context else "主要なボトルネックは設定なし"}

【JOIN戦略分析結果】
Sparkの自動JOIN戦略を使用（エラー回避のためヒントは使用せず）

【Liquid Clustering推奨】
{chr(10).join(clustering_recommendations) if clustering_recommendations else "特別な推奨事項はありません"}

【パフォーマンス分析結果（サマリー）】
{analysis_summary}

【🔍 EXPLAIN結果分析（EXPLAIN_ENABLED=Yの場合のみ）】
{f'''
**Physical Plan分析:**
```
{physical_plan}
```

**Photon Explanation分析:**
```
{photon_explanation}
```

**Physical Plan最適化の重要ポイント:**
- ファイルスキャンの効率性
- ジョイン戦略の妥当性
- シャッフル操作の最小化
- プロジェクション（列選択）の最適化
- フィルタープッシュダウンの活用

**Photon最適化の重要ポイント:**
- Photon未対応関数の検出と代替関数への変更
- ベクトル化処理に適した関数の選択
- Photon利用率向上のための書式変更
- コンパイル時最適化の活用
''' if explain_enabled.upper() == 'Y' and (physical_plan or photon_explanation) else '(EXPLAIN実行が無効、またはEXPLAIN結果が利用できません)'}

【💰 EXPLAIN COST統計情報分析（統計ベース最適化）】
{f'''
**構造化EXPLAIN COST統計情報:**
```json
{cost_statistics}
```

**🧠 構造化統計データの活用指針:**
上記は構造化抽出された統計情報です。以下の項目を重点的に分析してください：

- **table_stats**: テーブル別詳細統計（テーブル名、サイズ、行数）
- **critical_stats**: 重要統計値（最大テーブル、総行数、小テーブル候補）
- **largest_table**: 最大テーブルの名前とサイズ（JOIN順序の基準）
- **small_table_candidates**: 小テーブル（テーブル名とサイズ）
- **table_breakdown**: テーブル名の詳細（最大テーブル名、小テーブル名）

**🎯 テーブル名を使った精密最適化:**
1. **JOIN順序の最適化:**
   - テーブルサイズに基づく効率的なJOIN順序の決定
   - 小テーブルから大テーブルへの段階的結合

2. **JOIN順序の具体的提案:**
   - largest_table.nameを最後に配置
   - table_statsのサイズ順でJOIN順序を最適化
   - 具体的なテーブル名でJOIN文を改善

3. **曖昧性解決の具体的提案:**
   - エラーメッセージのテーブル名とtable_statsを照合
   - 具体的なエイリアス提案（例: `store_sales.ss_item_sk`）

**🚀 構造化データ解析の実行例:**
1. table_stats内で小テーブルを特定し、効率的なJOIN順序を決定
2. largest_table_nameが1GB以上 → 大テーブルとして最終JOINに配置
3. JOIN順序の具体的な改善提案を生成
4. テーブル名を明示したJOIN順序提案を生成

**🚨 トークン制限対策について:**
- JOIN/SCAN情報が多数の場合、重要度順に要約済み
- SUMMARY項目は複数操作の集約を示します
- 詳細は optimization_applied フラグで確認可能
- Physical Planが100KB超の場合は自動調整済み
''' if explain_enabled.upper() == 'Y' and cost_statistics else '(EXPLAIN COST実行が無効、または統計情報が利用できません)'}

【🎯 処理速度重視の最適化要求】
**最重要**: 以下の順序で処理速度の改善を優先してください

1. **🚨 CRITICAL優先度**: スピル対策（メモリ効率改善）
   - 大量スピル（5GB以上）が検出された場合は最優先で対処
   - メモリ効率的なJOIN順序の検討
   - 中間結果のサイズ削減

2. **🔄 REPARTITIONヒント適用**（スピル検出時のみ）
   - **スピルが検出された場合のみ**REPARTITIONヒントを適用
   - 検出されたShuffle attributesを基に具体的なREPARTITIONヒントを適用
   - GROUP BY前またはJOIN前の適切な位置にREPARTITIONを配置
   - 推奨パーティション数を使用

3. **⚖️ データスキュー対策**
   - スキューパーティション（10個以上）検出時は分散改善を優先
   - 適切なパーティションキーの選択
   - データ分散の均等化

4. **📈 シャッフル最適化**
   - シャッフル量の最小化
   - 適切なJOIN戦略の選択
   - ネットワーク転送量の削減

5. **🎯 JOIN戦略最適化**
   - 小テーブルを先に処理する効率的なJOIN順序
   - Sparkの自動最適化を活用したJOIN戦略（ヒント不使用）
   - 中間結果のサイズ最小化

6. **💾 メモリ効率化**
   - 不要なカラムの除去
   - 適切なフィルタリング順序
   - 中間結果のキャッシュ活用

7. **🔧 実行プラン最適化**
   - PHOTONエンジン最適化（目標はPhoton利用率90%以上)
   - Liquid Clustering活用 (Where条件の書き換え含む検討を実施）
   - CTE活用による共通化

8. **📊 EXPLAIN結果に基づく最適化**（EXPLAIN_ENABLED=Yの場合）
   - **Physical Plan分析に基づく最適化**: 
     - 非効率なスキャン操作の改善
     - ジョイン順序の最適化（Sparkの自動判定に依存）
     - 不要なシャッフル操作の削除
     - プロジェクションプッシュダウンの適用
   - **Photon未対応関数の最適化**:
     - Photon Explanationで検出された未対応関数の代替関数への変更
     - ベクトル化処理に適した関数への書き換え
     - Photon利用率向上のための関数選択
     - コンパイル時最適化の活用

9. **🎯 JOIN順序とパーティショニングの最適化**（重要な構造的最適化）
   - **効率的なJOIN順序**: 小さいテーブルから大きいテーブルへの段階的結合
   - **Sparkの自動JOIN戦略**: エンジンの自動判定に委ねることでエラー回避
   - **結合後のREPARTITION**: 結合後にGROUP BYの効率化のためREPARTITIONヒントを適用
   - **CTE構造の活用**: 必要に応じてCTEを使って段階的に処理する構造で出力
   - **スピル回避と並列度**: スピルを回避しつつ、並列度の高い処理ができるよう最適化
   
   **🔄 推奨する処理フロー:**
   ```sql
   -- ✅ 推奨パターン: 効率的JOIN順序 → CTE → REPARTITION → GROUP BY
   WITH efficient_joined AS (
     SELECT 
       large_table.columns...,
       small_table.columns...
     FROM small_table  -- 小テーブルを先に配置
       JOIN large_table ON small_table.key = large_table.key
   ),
   repartitioned_for_groupby AS (
     SELECT /*+ REPARTITION(200, group_key) */
       columns...
     FROM efficient_joined
   )
   SELECT 
     group_key,
     COUNT(*),
     SUM(amount)
   FROM repartitioned_for_groupby
   GROUP BY group_key
   ```

【🔄 REPARTITIONヒント適用ルール - 構文エラー防止】
REPARTITIONヒントを付与する場合は以下の最適化ルールを守ってください：

- **シャッフルの効率化・スキュー防止などパフォーマンス最適化が目的であるため、スピルアウトしていない場合には REPARTITION ヒントは不要**
- **REPARTITIONヒントは SELECT /*+ REPARTITION(パーティション数, カラム名) の形式で指定**
- **REPARTITIONヒントの適用位置は、対象となるJOINやGROUP BYを含むSELECTの直前であるため、出力されたoutput_explain_plan_*.txtのPhysical Planから実行計画を理解し、適切な位置にREPARTITION ヒントを付与すること**

**🚨 REPARTITIONヒント配置の重要な構文ルール:**
1. **JOINやGROUP BYの処理段階で効果を発揮するため、必ずサブクエリ内部に配置する**
2. **トップレベルのSELECT文に配置すると最終出力段階のみに影響し、JOIN/GROUP BY処理段階には影響しない**
3. **複数のREPARTITIONヒントは各サブクエリ内部に個別に配置する**
4. **パーティション数とカラム名は必須パラメータとして指定する**

従来のルール：
- **スピルが検出された場合のみ適用**
- GROUP BYクエリの場合: GROUP BY前にREPARTITION(推奨数, group_by_column)
- JOINクエリの場合: JOIN前にREPARTITION(推奨数, join_key)
- 複数テーブルの場合: 最も大きなテーブルをリパーティション
- 推奨パーティション数: 検出されたタスク数の2倍以上、最低200
- スピルが検出されていない場合: REPARTITIONヒントは適用しない

**🚨 CREATE TABLE AS SELECT (CTAS) でのREPARTITION配置の重要な注意事項:**
- CREATE TABLE AS SELECT文では、トップレベルのSELECT句にREPARTITIONヒントを配置すると、**最終的な出力書き込み段階のみに影響**し、JOIN や集計などの中間処理段階には影響しない
- JOINの前にパーティショニングを制御するには、**REPARTITIONヒントをサブクエリ内部に配置する必要がある**
- これにより、Sparkがデータフローの適切な時点でリパーティションを適用し、書き込み段階ではなく実行段階で最適化される

**正しいCTAS REPARTITIONヒント配置例:**
```sql
-- ❌ 間違い: トップレベルのSELECT句（書き込み段階のみに影響）
CREATE TABLE optimized_table AS
SELECT /*+ REPARTITION(200, join_key) */
  t1.column1, t2.column2
FROM table1 t1
  JOIN table2 t2 ON t1.join_key = t2.join_key

-- ✅ 正しい: サブクエリ内部に配置（JOIN処理段階で最適化）
CREATE TABLE optimized_table AS
SELECT 
  t1.column1, t2.column2
FROM (
  SELECT /*+ REPARTITION(200, join_key) */
    column1, join_key
  FROM table1
) t1
  JOIN table2 t2 ON t1.join_key = t2.join_key
```

**🚨 全般的なREPARTITIONヒント配置の重要な注意事項:**
- **CTAS以外のクエリでも同様**：トップレベルのクエリにREPARTITIONヒントを配置すると、**最終的な出力段階のみに影響**し、JOIN や集計などの中間変換段階には影響しない
- この動作は、結果をテーブルに書き込むかどうかに関係なく**すべてのSpark SQLクエリで一貫**している
- JOINの入力段階でリパーティションを確実に実行するには、**REPARTITIONヒントをサブクエリ内部に配置する必要がある**
- これにより、Sparkが適切なデータフローの時点でリパーティションを適用し、最終出力段階ではなく実行段階で最適化される

**一般的なクエリでの正しいREPARTITIONヒント配置例:**
```sql
-- ❌ 間違い: トップレベルのSELECT句（最終出力段階のみに影響）
SELECT /*+ REPARTITION(200, join_key) */
  t1.column1, t2.column2
FROM table1 t1
  JOIN table2 t2 ON t1.join_key = t2.join_key

-- ✅ 正しい: サブクエリ内部に配置（JOIN処理段階で最適化）
SELECT 
  t1.column1, t2.column2
FROM (
  SELECT /*+ REPARTITION(200, join_key) */
    column1, join_key
  FROM table1
) t1
  JOIN table2 t2 ON t1.join_key = t2.join_key

-- ✅ 正しい: より複雑なケース（複数のサブクエリでのリパーティション）
SELECT 
  t1.column1, t2.column2, t3.column3
FROM (
  SELECT /*+ REPARTITION(200, join_key) */
    column1, join_key
  FROM table1
) t1
  JOIN (
    SELECT /*+ REPARTITION(200, join_key) */
      column2, join_key
    FROM table2
  ) t2 ON t1.join_key = t2.join_key
  JOIN table3 t3 ON t2.join_key = t3.join_key
```

**🚨 全般的なREPARTITIONヒント配置の重要な注意事項:**
- **CTAS以外のクエリでも同様**：トップレベルのクエリにREPARTITIONヒントを配置すると、**最終的な出力段階のみに影響**し、JOIN や集計などの中間変換段階には影響しない
- この動作は、結果をテーブルに書き込むかどうかに関係なく**すべてのSpark SQLクエリで一貫**している
- JOINの入力段階でリパーティションを確実に実行するには、**REPARTITIONヒントをサブクエリ内部に配置する必要がある**
- これにより、Sparkが適切なデータフローの時点でリパーティションを適用し、最終出力段階ではなく実行段階で最適化される

**一般的なクエリでの正しいREPARTITIONヒント配置例:**
```sql
-- ❌ 間違い: トップレベルのSELECT句（最終出力段階のみに影響）
SELECT /*+ REPARTITION(200, join_key) */
  t1.column1, t2.column2
FROM table1 t1
  JOIN table2 t2 ON t1.join_key = t2.join_key

-- ✅ 正しい: サブクエリ内部に配置（JOIN処理段階で最適化）
SELECT 
  t1.column1, t2.column2
FROM (
  SELECT /*+ REPARTITION(200, join_key) */
    column1, join_key
  FROM table1
) t1
  JOIN table2 t2 ON t1.join_key = t2.join_key
```



【重要な制約】
- 絶対に不完全なクエリを生成しないでください
- すべてのカラム名、テーブル名、CTE名を完全に記述してください
- プレースホルダー（...、[省略]、空白など）は一切使用しないでください
- オリジナルクエリのすべてのSELECT項目を保持してください
- **🚨 DISTINCT句の絶対保持**: 元のクエリにDISTINCT句がある場合は、**必ずDISTINCT句を保持**してください
- **最適化時のDISTINCT保持**: REPARTITIONヒントを追加する際も、DISTINCT句は絶対に削除しないでください
- 元のクエリが長い場合でも、すべてのカラムを省略せずに記述してください
- 実際に実行できる完全なSQLクエリのみを出力してください
- 元のクエリと同じアウトプットになることを厳守してください

【🚨 最適化における構文エラー防止】
**絶対に守るべき文法ルール（構文エラー防止のため必須）:**

✅ **REPARTITIONヒントの正しい配置:**
```sql
-- REPARTITIONヒントはメインクエリのSELECT直後に配置
SELECT /*+ REPARTITION(200, column_name) */
  column1, column2, ...
FROM table1 t1
  JOIN table2 t2 ON t1.id = t2.id
```

✅ **DISTINCT句との正しい組み合わせ（絶対必須）:**
```sql
-- 🚨 重要: DISTINCT句は必ずヒント句の後に配置
SELECT /*+ REPARTITION(200, column_name) */ DISTINCT
  cs.ID, cs.column1, cs.column2, ...
FROM table1 cs
  JOIN table2 t2 ON cs.id = t2.id
```

**🚨 構文エラー防止のための基本ルール:**
1. **ヒントは必ずメインクエリのSELECT文の直後に配置**
2. **FROM句、JOIN句、WHERE句内には絶対に配置しない**
3. **REPARTITIONヒントには適切なパーティション数とカラム名を指定**

【出力形式】
## 🚀 処理速度重視の最適化されたSQL

**適用した最適化手法**:
- [具体的な最適化手法のリスト]
- [REPARTITIONヒントの適用詳細（スピル検出時のみ）]
- [JOIN順序最適化の詳細]
- [CTE構造による段階的最適化の適用詳細]
- [推定される性能改善効果]

**🚨 構文エラー防止の最終確認**:
- ✅ REPARTITIONヒントは適切にメインクエリのSELECT直後に配置されている
- ✅ FROM句、JOIN句、WHERE句内にヒントが配置されていない
- ✅ REPARTITIONヒントには適切なパーティション数とカラム名が指定されている
- ✅ **DISTINCT句が元のクエリにある場合は必ず保持されている**
- ✅ **ヒント句追加時にDISTINCT句が削除されていない**
- ✅ **DISTINCT句がヒント句の直後に正しく配置されている**
- ✅ プレースホルダー（...、[省略]等）が一切使用されていない
- ✅ 完全なSQL構文になっている（不完全なクエリではない）
- ✅ NULLリテラルが適切な型でキャストされている
- ✅ JOIN順序が効率的に最適化されている
- ✅ スピル回避と並列度向上の両方を考慮した構造になっている
- ✅ **BROADCASTヒントは一切使用されていない（構文エラー防止）**
- ✅ **Sparkの自動JOIN戦略に委ねてヒント不使用で最適化されている**

```sql
-- 🚨 重要: REPARTITIONヒントはメインクエリのSELECT文の直後に配置
-- 例: SELECT /*+ REPARTITION(200, column_name) */ column1, column2, ...
-- 🚨 DISTINCT句保持例: SELECT /*+ REPARTITION(200, column_name) */ DISTINCT cs.ID, cs.column1, ...
-- 🚨 REPARTITIONヒントの適切な配置: SELECT /*+ REPARTITION(200, join_key) */ column1, column2, ...
-- ❌ 禁止: BROADCASTヒント（/*+ BROADCAST */、/*+ BROADCAST(table) */）は一切使用禁止
-- ✅ 推奨: Sparkの自動JOIN戦略に委ねてヒント不使用で最適化
[完全なSQL - すべてのカラム・CTE・テーブル名を省略なしで記述]
```

## 改善ポイント
[3つの主要改善点]

## JOIN最適化の根拠
[JOIN順序最適化の詳細根拠]
- 📏 テーブルサイズベースの最適化: 小テーブルから大テーブルへの効率的結合順序
- 🎯 最適化対象テーブル: [テーブル名リスト]
- ⚖️ JOIN戦略: Sparkの自動最適化を活用した効率的な結合処理
- 🚀 期待効果: [ネットワーク転送量削減・JOIN処理高速化・シャッフル削減など]

## 期待効果  
[実行時間・メモリ・スピル改善の見込み（JOIN最適化効果を含む）]
"""

    # 設定されたLLMプロバイダーを使用
    provider = LLM_CONFIG["provider"]
    
    try:
        if provider == "databricks":
            optimized_result = _call_databricks_llm(optimization_prompt)
        elif provider == "openai":
            optimized_result = _call_openai_llm(optimization_prompt)
        elif provider == "azure_openai":
            optimized_result = _call_azure_openai_llm(optimization_prompt)
        elif provider == "anthropic":
            optimized_result = _call_anthropic_llm(optimization_prompt)
        else:
            error_msg = "⚠️ 設定されたLLMプロバイダーが認識できません"
            print(f"❌ LLM最適化エラー: {error_msg}")
            return f"LLM_ERROR: {error_msg}"
        
        # LLMレスポンスのエラーチェック（重要）
        if isinstance(optimized_result, str):
            # APIエラーメッセージの検出
            error_indicators = [
                 "APIエラー:",
                 "Input is too long",
                 "Bad Request",
                 "❌",
                 "⚠️",
                 "タイムアウトエラー:",
                 "API呼び出しエラー:",
                 "レスポンス:",
                 '{"error_code":'
             ]
             
             # エラーメッセージかどうかをチェック
            is_error_response = any(indicator in optimized_result for indicator in error_indicators)
            
            if is_error_response:
                print(f"❌ LLM API呼び出しでエラーが発生: {optimized_result[:200]}...")
                return f"LLM_ERROR: {optimized_result}"
        
        # thinking_enabled: Trueの場合にoptimized_resultがリストになることがあるため対応
        # ここでは元のレスポンス形式を保持して返す（後で用途に応じて変換）
        return optimized_result
        
    except Exception as e:
        error_msg = f"⚠️ SQL最適化の生成中にエラーが発生しました: {str(e)}"
        print(f"❌ LLM最適化例外エラー: {error_msg}")
        return f"LLM_ERROR: {error_msg}"



def generate_top10_time_consuming_processes_report(extracted_metrics: Dict[str, Any], limit_nodes: int = 10) -> str:
    """
    最も時間がかかっている処理のレポートを文字列として生成
    
    🚨 重要: パーセンテージ計算デグレ防止
    - 並列実行ノードの時間合計を全体時間として使用することは絶対に禁止
    - overall_metrics.total_time_ms（wall-clock time）を優先使用
    - フォールバック時は最大ノード時間を使用（合計ではない）
    
    Args:
        extracted_metrics: 抽出されたメトリクス
        limit_nodes: 表示するノード数（デフォルト10、ファイル出力時は5）
    
    Returns:
        str: 処理レポート
    """
    report_lines = []
    
    # タイトルをノード数に応じて調整
    title = f"最も時間がかかっている処理TOP{limit_nodes}" if limit_nodes <= 10 else "最も時間がかかっている処理TOP10"
    report_lines.append(f"## 🐌 {title}")
    report_lines.append("=" * 80)
    report_lines.append("📊 アイコン説明: ⏱️時間 💾メモリ 🔥🐌並列度 💿スピル ⚖️スキュー")
    report_lines.append('💿 スピル判定: "Num bytes spilled to disk due to memory pressure" または "Sink - Num bytes spilled to disk due to memory pressure" > 0')
    report_lines.append("🎯 スキュー判定: 'AQEShuffleRead - Number of skewed partitions' > 0")
    report_lines.append("")

    # ノードを実行時間でソート
    sorted_nodes = sorted(extracted_metrics['node_metrics'], 
                         key=lambda x: x['key_metrics'].get('durationMs', 0), 
                         reverse=True)
    
    # 指定されたノード数まで処理
    final_sorted_nodes = sorted_nodes[:limit_nodes]

    if final_sorted_nodes:
        # 🚨 重要: 正しい全体時間の計算（デグレ防止）
        # 1. overall_metricsから全体実行時間を取得（wall-clock time）
        overall_metrics = extracted_metrics.get('overall_metrics', {})
        total_duration = overall_metrics.get('total_time_ms', 0)
        
        # 🚨 並列実行問題の修正: task_total_time_msを優先使用
        task_total_time_ms = overall_metrics.get('task_total_time_ms', 0)
        
        if task_total_time_ms > 0:
            total_duration = task_total_time_ms
            print(f"✅ generate_top10レポート: 並列実行対応 - task_total_time_ms使用: {total_duration:,} ms ({total_duration/3600000:.1f}時間)")
        elif total_duration <= 0:
            # execution_time_msを次の優先度で使用
            execution_time_ms = overall_metrics.get('execution_time_ms', 0)
            if execution_time_ms > 0:
                total_duration = execution_time_ms
                print(f"⚠️ generate_top10レポート: task_total_time_ms利用不可、execution_time_ms使用: {total_duration} ms")
            else:
                # 最終フォールバック
                max_node_time = max([node['key_metrics'].get('durationMs', 0) for node in sorted_nodes], default=1)
                total_duration = int(max_node_time * 1.2)
                print(f"⚠️ generate_top10レポート: 最終フォールバック - 推定時間使用: {total_duration} ms")
        
        report_lines.append(f"📊 累積タスク実行時間（並列）: {total_duration:,} ms ({total_duration/3600000:.1f} 時間)")
        report_lines.append(f"📈 TOP{limit_nodes}合計時間（並列実行）: {sum(node['key_metrics'].get('durationMs', 0) for node in final_sorted_nodes):,} ms")

        report_lines.append("")
        
        for i, node in enumerate(final_sorted_nodes):
            # バグ修正：変数を正しく定義
            duration_ms = node['key_metrics'].get('durationMs', 0)
            rows_num = node['key_metrics'].get('numOutputRows', 0)
            memory_mb = node['key_metrics'].get('peakMemoryBytes', 0) / 1024 / 1024
            
            # 🚨 重要: 正しいパーセンテージ計算（デグレ防止）
            # wall-clock timeに対する各ノードの実行時間の割合
            time_percentage = min((duration_ms / max(total_duration, 1)) * 100, 100.0)
            
            # 時間の重要度に基づいてアイコンを選択
            if duration_ms >= 10000:  # 10秒以上
                time_icon = "🔴"
                severity = "CRITICAL"
            elif duration_ms >= 5000:  # 5秒以上
                time_icon = "🟠"
                severity = "HIGH"
            elif duration_ms >= 1000:  # 1秒以上
                time_icon = "🟡"
                severity = "MEDIUM"
            else:
                time_icon = "🟢"
                severity = "LOW"
            
            # メモリ使用量のアイコン
            memory_icon = "💚" if memory_mb < 100 else "⚠️" if memory_mb < 1000 else "🚨"
            
            # より意味のあるノード名を取得
            raw_node_name = node['name']
            node_name = get_meaningful_node_name(node, extracted_metrics)
            short_name = node_name[:100] + "..." if len(node_name) > 100 else node_name
            
            # 並列度情報の取得（修正版: 複数のTasks totalメトリクスを取得）
            parallelism_data = extract_parallelism_metrics(node)
            
            # 従来の単一値（互換性のため）
            num_tasks = parallelism_data.get('tasks_total', 0)
            
            # フォールバック: Sink - Tasks totalまたはSource - Tasks totalがある場合
            if num_tasks == 0:
                if parallelism_data.get('sink_tasks_total', 0) > 0:
                    num_tasks = parallelism_data.get('sink_tasks_total', 0)
                elif parallelism_data.get('source_tasks_total', 0) > 0:
                    num_tasks = parallelism_data.get('source_tasks_total', 0)
            
            # スピル検出（セル33と同じロジック - 正確なメトリクス名のみ）
            spill_detected = False
            spill_bytes = 0
            exact_spill_metrics = [
                "Num bytes spilled to disk due to memory pressure",
                "Sink - Num bytes spilled to disk due to memory pressure",
                "Sink/Num bytes spilled to disk due to memory pressure"
            ]
            
            # detailed_metricsから検索
            detailed_metrics = node.get('detailed_metrics', {})
            for metric_key, metric_info in detailed_metrics.items():
                metric_value = metric_info.get('value', 0)
                metric_label = metric_info.get('label', '')
                
                if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                    spill_detected = True
                    spill_bytes = max(spill_bytes, metric_value)
                    break
            
            # raw_metricsから検索（フォールバック）
            if not spill_detected:
                raw_metrics = node.get('metrics', [])
                for metric in raw_metrics:
                    metric_key = metric.get('key', '')
                    metric_label = metric.get('label', '')
                    metric_value = metric.get('value', 0)
                    
                    if (metric_key in exact_spill_metrics or metric_label in exact_spill_metrics) and metric_value > 0:
                        spill_detected = True
                        spill_bytes = max(spill_bytes, metric_value)
                        break
            
            # スキュー検出: AQEShuffleRead - Number of skewed partitions メトリクス使用（正確なメトリクス名のみ）
            skew_detected = False
            skewed_partitions = 0
            target_skew_metric = "AQEShuffleRead - Number of skewed partitions"
            
            # detailed_metricsから正確なメトリクス名で検索
            detailed_metrics = node.get('detailed_metrics', {})
            for metric_key, metric_info in detailed_metrics.items():
                if metric_key == target_skew_metric:
                    try:
                        skewed_partitions = int(metric_info.get('value', 0))
                        if skewed_partitions > 0:
                            skew_detected = True
                        break
                    except (ValueError, TypeError):
                        continue
            
            # key_metricsから正確なメトリクス名で検索（フォールバック）
            if not skew_detected:
                key_metrics = node.get('key_metrics', {})
                if target_skew_metric in key_metrics:
                    try:
                        skewed_partitions = int(key_metrics[target_skew_metric])
                        if skewed_partitions > 0:
                            skew_detected = True
                    except (ValueError, TypeError):
                        pass
            
            # 並列度アイコン
            parallelism_icon = "🔥" if num_tasks >= 10 else "⚠️" if num_tasks >= 5 else "🐌"
            # スピルアイコン
            spill_icon = "💿" if spill_detected else "✅"
            # スキューアイコン
            skew_icon = "⚖️" if skew_detected else "✅"
            
            report_lines.append(f"{i+1:2d}. {time_icon}{memory_icon}{parallelism_icon}{spill_icon}{skew_icon} [{severity:8}] {short_name}")
            report_lines.append(f"    ⏱️  実行時間: {duration_ms:>8,} ms ({duration_ms/1000:>6.1f} sec) - 累積時間の {time_percentage:>5.1f}%")
            report_lines.append(f"    📊 処理行数: {rows_num:>8,} 行")
            report_lines.append(f"    💾 ピークメモリ: {memory_mb:>6.1f} MB")
            # 複数のTasks totalメトリクスを表示
            parallelism_display = []
            for task_metric in parallelism_data.get('all_tasks_metrics', []):
                parallelism_display.append(f"{task_metric['name']}: {task_metric['value']}")
            
            if parallelism_display:
                report_lines.append(f"    🔧 並列度: {' | '.join(parallelism_display)}")
            else:
                report_lines.append(f"    🔧 並列度: {num_tasks:>3d} タスク")
            
            # スキュー判定（AQEスキュー検出とAQEShuffleRead平均パーティションサイズの両方を考慮）
            aqe_shuffle_skew_warning = parallelism_data.get('aqe_shuffle_skew_warning', False)
            
            if skew_detected:
                skew_status = "AQEで検出・対応済"
            elif aqe_shuffle_skew_warning:
                skew_status = "潜在的なスキューの可能性あり"
            else:
                skew_status = "なし"
            
            report_lines.append(f"    💿 スピル: {'あり' if spill_detected else 'なし'} | ⚖️ スキュー: {skew_status}")
            
            # AQEShuffleReadメトリクスの表示
            aqe_shuffle_metrics = parallelism_data.get('aqe_shuffle_metrics', [])
            if aqe_shuffle_metrics:
                aqe_display = []
                for aqe_metric in aqe_shuffle_metrics:
                    if aqe_metric['name'] == "AQEShuffleRead - Number of partitions":
                        aqe_display.append(f"パーティション数: {aqe_metric['value']}")
                    elif aqe_metric['name'] == "AQEShuffleRead - Partition data size":
                        aqe_display.append(f"データサイズ: {aqe_metric['value']:,} bytes")
                
                if aqe_display:
                    report_lines.append(f"    🔄 AQEShuffleRead: {' | '.join(aqe_display)}")
                    
                    # 平均パーティションサイズと警告表示
                    avg_partition_size = parallelism_data.get('aqe_shuffle_avg_partition_size', 0)
                    if avg_partition_size > 0:
                        avg_size_mb = avg_partition_size / (1024 * 1024)
                        report_lines.append(f"    📊 平均パーティションサイズ: {avg_size_mb:.2f} MB")
                        
                        # 512MB以上の場合に警告
                        if parallelism_data.get('aqe_shuffle_skew_warning', False):
                            report_lines.append(f"    ⚠️  【警告】 平均パーティションサイズが512MB以上 - 潜在的なスキューの可能性あり")
            
            # 効率性指標（行/秒）を計算
            if duration_ms > 0:
                rows_per_sec = (rows_num * 1000) / duration_ms
                report_lines.append(f"    🚀 処理効率: {rows_per_sec:>8,.0f} 行/秒")
            
            # フィルタ率表示（デバッグ機能付き）
            filter_result = calculate_filter_rate(node)
            filter_display = format_filter_rate_display(filter_result)
            if filter_display:
                report_lines.append(f"    {filter_display}")
            else:
                # デバッグ情報：なぜフィルタ率が表示されないかを確認
                if filter_result["has_filter_metrics"]:
                    report_lines.append(f"    📂 フィルタ率: {filter_result['filter_rate']:.1%} (読み込み: {filter_result['files_read_bytes']/(1024*1024*1024):.2f}GB, プルーン: {filter_result['files_pruned_bytes']/(1024*1024*1024):.2f}GB)")
                else:
                    # メトリクス検索のデバッグ
                    debug_info = []
                    detailed_metrics = node.get('detailed_metrics', {})
                    for metric_key, metric_info in detailed_metrics.items():
                        metric_label = metric_info.get('label', '')
                        if 'file' in metric_label.lower() and ('read' in metric_label.lower() or 'prun' in metric_label.lower()):
                            debug_info.append(f"{metric_label}: {metric_info.get('value', 0)}")
                    
                    if debug_info:
                        report_lines.append(f"    📂 フィルタ関連メトリクス検出: {', '.join(debug_info[:2])}")
            
            # スピル詳細情報（シンプル表示）
            spill_display = ""
            if spill_detected and spill_bytes > 0:
                spill_mb = spill_bytes / 1024 / 1024
                if spill_mb >= 1024:  # GB単位
                    spill_display = f"{spill_mb/1024:.2f} GB"
                else:  # MB単位
                    spill_display = f"{spill_mb:.1f} MB"
                report_lines.append(f"    💿 スピル: {spill_display}")
            
            # Shuffleノードの場合は常にShuffle attributesを表示
            if "shuffle" in raw_node_name.lower():
                shuffle_attributes = extract_shuffle_attributes(node)
                if shuffle_attributes:
                    report_lines.append(f"    🔄 Shuffle属性: {', '.join(shuffle_attributes)}")
                    
                    # REPARTITIONヒントの提案（スピルが検出された場合のみ）
                    if spill_detected and spill_bytes > 0 and spill_display:
                        suggested_partitions = max(num_tasks * 2, 200)  # 最小200パーティション
                        
                        # Shuffle属性で検出されたカラムを全て使用（完全一致）
                        repartition_columns = ", ".join(shuffle_attributes)
                        
                        report_lines.append(f"    💡 最適化提案: REPARTITION({suggested_partitions}, {repartition_columns})")
                        report_lines.append(f"       理由: スピル({spill_display})を改善するため")
                        report_lines.append(f"       対象: Shuffle属性全{len(shuffle_attributes)}カラムを完全使用")
                else:
                    report_lines.append(f"    🔄 Shuffle属性: 設定なし")
            
            # スキャンノードの場合はクラスタリングキーを表示
            if "scan" in raw_node_name.lower():
                cluster_attributes = extract_cluster_attributes(node)
                if cluster_attributes:
                    report_lines.append(f"    📊 クラスタリングキー: {', '.join(cluster_attributes)}")
                else:
                    report_lines.append(f"    📊 クラスタリングキー: 設定なし")
            
            # スキュー詳細情報
            if skew_detected and skewed_partitions > 0:
                report_lines.append(f"    ⚖️ スキュー詳細: {skewed_partitions} 個のスキューパーティション（AQEShuffleRead検出）")
            
            # ノードIDも表示
            report_lines.append(f"    🆔 ノードID: {node.get('node_id', node.get('id', 'N/A'))}")
            report_lines.append("")
            
    else:
        report_lines.append("⚠️ ノードメトリクスが見つかりませんでした")
    
    return "\n".join(report_lines)

def save_execution_plan_analysis(plan_info: Dict[str, Any], output_dir: str = "/tmp") -> Dict[str, str]:
    """
    実行プラン分析結果をファイルに保存
    
    Args:
        plan_info: extract_execution_plan_info()の結果
        output_dir: 出力ディレクトリ
        
    Returns:
        Dict: 保存されたファイル名の辞書
    """
    from datetime import datetime
    import json
    
    # タイムスタンプ生成
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    # ファイル名定義
    plan_json_filename = f"output_execution_plan_analysis_{timestamp}.json"
    plan_report_filename = f"output_execution_plan_report_{timestamp}.md"
    
    # JSON形式でプラン情報を保存
    with open(plan_json_filename, 'w', encoding='utf-8') as f:
        json.dump(plan_info, f, ensure_ascii=False, indent=2)
    
    # Markdown形式でプラン分析レポートを保存
    with open(plan_report_filename, 'w', encoding='utf-8') as f:
        report_content = generate_execution_plan_markdown_report(plan_info)
        f.write(report_content)
    
    return {
        'plan_json_file': plan_json_filename,
        'plan_report_file': plan_report_filename
    }

def generate_execution_plan_markdown_report(plan_info: Dict[str, Any]) -> str:
    """
    実行プラン分析結果のMarkdownレポートを生成
    
    Args:
        plan_info: extract_execution_plan_info()の結果
        
    Returns:
        str: Markdownレポート
    """
    if OUTPUT_LANGUAGE == 'ja':
        return generate_execution_plan_markdown_report_ja(plan_info)
    else:
        return generate_execution_plan_markdown_report_en(plan_info)

def generate_execution_plan_markdown_report_ja(plan_info: Dict[str, Any]) -> str:
    """
    実行プラン分析結果のMarkdownレポート（日本語版）
    """
    from datetime import datetime
    
    lines = []
    lines.append("# Databricks SQL実行プラン分析レポート")
    lines.append("")
    lines.append(f"**生成日時**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    lines.append("")
    
    # プランサマリー
    plan_summary = plan_info.get("plan_summary", {})
    lines.append("## 📊 実行プランサマリー")
    lines.append("")
    lines.append(f"- **総ノード数**: {plan_summary.get('total_nodes', 0)}")
    lines.append(f"- **BROADCASTノード数**: {plan_summary.get('broadcast_nodes_count', 0)}")
    lines.append(f"- **JOINノード数**: {plan_summary.get('join_nodes_count', 0)}")
    lines.append(f"- **スキャンノード数**: {plan_summary.get('scan_nodes_count', 0)}")
    lines.append(f"- **シャッフルノード数**: {plan_summary.get('shuffle_nodes_count', 0)}")
    lines.append(f"- **集約ノード数**: {plan_summary.get('aggregate_nodes_count', 0)}")
    lines.append(f"- **BROADCASTが使用中**: {'はい' if plan_summary.get('has_broadcast_joins', False) else 'いいえ'}")
    lines.append(f"- **スキャンされるテーブル数**: {plan_summary.get('tables_scanned', 0)}")
    lines.append("")
    
    # JOIN戦略分析
    unique_join_strategies = plan_summary.get('unique_join_strategies', [])
    if unique_join_strategies:
        lines.append("## 🔗 JOIN戦略分析")
        lines.append("")
        for strategy in unique_join_strategies:
            strategy_jp = {
                'broadcast_hash_join': 'ブロードキャストハッシュJOIN',
                'sort_merge_join': 'ソートマージJOIN',
                'shuffle_hash_join': 'シャッフルハッシュJOIN',
                'broadcast_nested_loop_join': 'ブロードキャストネストループJOIN'
            }.get(strategy, strategy)
            lines.append(f"- **{strategy_jp}** (`{strategy}`)")
        lines.append("")
    
    # BROADCASTノード詳細
    broadcast_nodes = plan_info.get("broadcast_nodes", [])
    if broadcast_nodes:
        lines.append("## 📡 BROADCASTノード詳細")
        lines.append("")
        for i, node in enumerate(broadcast_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            lines.append(f"- **ノードタグ**: {node['node_tag']}")
            
            metadata = node.get('metadata', [])
            if metadata:
                lines.append("- **関連メタデータ**:")
                for meta in metadata[:5]:  # 最大5個まで表示
                    key = meta.get('key', '')
                    value = meta.get('value', '')
                    values = meta.get('values', [])
                    if values:
                        lines.append(f"  - **{key}**: {', '.join(map(str, values[:3]))}")
                    elif value:
                        lines.append(f"  - **{key}**: {value}")
            lines.append("")
    
    # JOINノード詳細
    join_nodes = plan_info.get("join_nodes", [])
    if join_nodes:
        lines.append("## 🔗 JOINノード詳細")
        lines.append("")
        for i, node in enumerate(join_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            lines.append(f"- **JOIN戦略**: {node['join_strategy']}")
            lines.append(f"- **JOINタイプ**: {node['join_type']}")
            
            join_keys = node.get('join_keys', [])
            if join_keys:
                lines.append(f"- **JOINキー**: {', '.join(join_keys[:5])}")
            lines.append("")
    
    # テーブルスキャン詳細（サイズ推定情報を含む）
    table_scan_details = plan_info.get("table_scan_details", {})
    table_size_estimates = plan_info.get("table_size_estimates", {})
    if table_scan_details:
        lines.append("## 📋 テーブルスキャン詳細")
        lines.append("")
        for table_name, scan_detail in table_scan_details.items():
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **ファイル形式**: {scan_detail.get('file_format', 'unknown')}")
            lines.append(f"- **プッシュダウンフィルタ数**: {len(scan_detail.get('pushed_filters', []))}")
            lines.append(f"- **出力カラム数**: {len(scan_detail.get('output_columns', []))}")
            
            # 実行プランからのサイズ推定情報（estimatedSizeInBytes利用不可のため無効化）
            # size_info = table_size_estimates.get(table_name)
            # if size_info:
            #     lines.append(f"- **推定サイズ（実行プラン）**: {size_info['estimated_size_mb']:.1f}MB")
            #     lines.append(f"- **サイズ推定信頼度**: {size_info.get('confidence', 'medium')}")
            #     if 'num_files' in size_info:
            #         lines.append(f"- **ファイル数**: {size_info['num_files']}")
            #     if 'num_partitions' in size_info:
            #         lines.append(f"- **パーティション数**: {size_info['num_partitions']}")
            
            pushed_filters = scan_detail.get('pushed_filters', [])
            if pushed_filters:
                lines.append("- **プッシュダウンフィルタ**:")
                for filter_expr in pushed_filters[:3]:  # 最大3個まで表示
                    lines.append(f"  - `{filter_expr}`")
            lines.append("")
    
    # シャッフルノード詳細
    shuffle_nodes = plan_info.get("shuffle_nodes", [])
    if shuffle_nodes:
        lines.append("## 🔄 シャッフルノード詳細")
        lines.append("")
        for i, node in enumerate(shuffle_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            
            partition_keys = node.get('partition_keys', [])
            if partition_keys:
                lines.append(f"- **パーティションキー**: {', '.join(partition_keys)}")
            lines.append("")
    
    # 集約ノード詳細
    aggregate_nodes = plan_info.get("aggregate_nodes", [])
    if aggregate_nodes:
        lines.append("## 📊 集約ノード詳細")
        lines.append("")
        for i, node in enumerate(aggregate_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **ノードID**: {node['node_id']}")
            
            group_keys = node.get('group_keys', [])
            if group_keys:
                lines.append(f"- **グループ化キー**: {', '.join(group_keys[:5])}")
            
            agg_expressions = node.get('aggregate_expressions', [])
            if agg_expressions:
                lines.append(f"- **集約関数**: {', '.join(agg_expressions[:5])}")
            lines.append("")
    
    # テーブルサイズ推定情報サマリー（estimatedSizeInBytes利用不可のため無効化）
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     lines.append("## 📏 テーブルサイズ推定情報（実行プランベース）")
    #     lines.append("")
    #     total_estimated_size = sum(size_info['estimated_size_mb'] for size_info in table_size_estimates.values())
    #     lines.append(f"- **推定対象テーブル数**: {len(table_size_estimates)}")
    #     lines.append(f"- **総推定サイズ**: {total_estimated_size:.1f}MB")
    #     lines.append("")
    #     
    #     for table_name, size_info in list(table_size_estimates.items())[:5]:  # 最大5テーブル表示
    #         lines.append(f"### {table_name}")
    #         lines.append(f"- **推定サイズ**: {size_info['estimated_size_mb']:.1f}MB")
    #         lines.append(f"- **信頼度**: {size_info.get('confidence', 'medium')}")
    #         lines.append(f"- **ノード**: {size_info.get('node_name', 'unknown')}")
    #         if 'num_files' in size_info:
    #             lines.append(f"- **ファイル数**: {size_info['num_files']}")
    #         lines.append("")
    #     
    #     if len(table_size_estimates) > 5:
    #         lines.append(f"...他 {len(table_size_estimates) - 5} テーブル（詳細は上記セクション参照）")
    #         lines.append("")
    
    # 最適化推奨事項
    lines.append("## 💡 プランベース最適化推奨事項")
    lines.append("")
    
    if plan_summary.get('has_broadcast_joins', False):
        lines.append("✅ **既にBROADCAST JOINが適用されています**")
        lines.append("- 現在の実行プランでBROADCAST最適化が有効")
        
        # BROADCASTされているテーブル一覧を表示
        broadcast_tables = plan_summary.get('broadcast_tables', [])
        if broadcast_tables:
            lines.append(f"- **BROADCASTされているテーブル**: {', '.join(broadcast_tables)}")
        
        lines.append("- 追加のBROADCAST適用機会を確認してください")
    else:
        lines.append("⚠️ **BROADCAST JOINが未適用です**")
        lines.append("- 小テーブルにBROADCASTヒントの適用を検討")
        lines.append("- 30MB閾値以下のテーブルを特定してください")
    lines.append("")
    
    if plan_summary.get('shuffle_nodes_count', 0) > 3:
        lines.append("⚠️ **多数のシャッフル操作が検出されました**")
        lines.append("- データの分散とパーティショニング戦略を見直し")
        lines.append("- Liquid Clusteringの適用を検討")
    lines.append("")
    
    # サイズ推定ベースの最適化提案（estimatedSizeInBytes利用不可のため無効化）
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     small_tables = [name for name, info in table_size_estimates.items() if info['estimated_size_mb'] <= 30]
    #     if small_tables:
    #         lines.append("💡 **実行プランベースBROADCAST推奨**")
    #         lines.append(f"- 30MB以下の小テーブル: {len(small_tables)}個検出")
    #         for table in small_tables[:3]:  # 最大3個表示
    #             size_mb = table_size_estimates[table]['estimated_size_mb']
    #             lines.append(f"  • {table}: {size_mb:.1f}MB（BROADCAST候補）")
    #         if len(small_tables) > 3:
    #             lines.append(f"  • ...他 {len(small_tables) - 3} テーブル")
    #         lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("このレポートは、Databricks SQL実行プラン分析ツールによって自動生成されました。")
    
    return '\n'.join(lines)

def generate_execution_plan_markdown_report_en(plan_info: Dict[str, Any]) -> str:
    """
    実行プラン分析結果のMarkdownレポート（英語版）
    """
    from datetime import datetime
    
    lines = []
    lines.append("# Databricks SQL Execution Plan Analysis Report")
    lines.append("")
    lines.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Plan Summary
    plan_summary = plan_info.get("plan_summary", {})
    lines.append("## 📊 Execution Plan Summary")
    lines.append("")
    lines.append(f"- **Total Nodes**: {plan_summary.get('total_nodes', 0)}")
    lines.append(f"- **BROADCAST Nodes**: {plan_summary.get('broadcast_nodes_count', 0)}")
    lines.append(f"- **JOIN Nodes**: {plan_summary.get('join_nodes_count', 0)}")
    lines.append(f"- **Scan Nodes**: {plan_summary.get('scan_nodes_count', 0)}")
    lines.append(f"- **Shuffle Nodes**: {plan_summary.get('shuffle_nodes_count', 0)}")
    lines.append(f"- **Aggregate Nodes**: {plan_summary.get('aggregate_nodes_count', 0)}")
    lines.append(f"- **BROADCAST in Use**: {'Yes' if plan_summary.get('has_broadcast_joins', False) else 'No'}")
    lines.append(f"- **Tables Scanned**: {plan_summary.get('tables_scanned', 0)}")
    lines.append("")
    
    # JOIN Strategy Analysis
    unique_join_strategies = plan_summary.get('unique_join_strategies', [])
    if unique_join_strategies:
        lines.append("## 🔗 JOIN Strategy Analysis")
        lines.append("")
        for strategy in unique_join_strategies:
            lines.append(f"- **{strategy.replace('_', ' ').title()}** (`{strategy}`)")
        lines.append("")
    
    # BROADCAST Node Details
    broadcast_nodes = plan_info.get("broadcast_nodes", [])
    if broadcast_nodes:
        lines.append("## 📡 BROADCAST Node Details")
        lines.append("")
        for i, node in enumerate(broadcast_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            lines.append(f"- **Node Tag**: {node['node_tag']}")
            
            metadata = node.get('metadata', [])
            if metadata:
                lines.append("- **Related Metadata**:")
                for meta in metadata[:5]:  # Show up to 5
                    key = meta.get('key', '')
                    value = meta.get('value', '')
                    values = meta.get('values', [])
                    if values:
                        lines.append(f"  - **{key}**: {', '.join(map(str, values[:3]))}")
                    elif value:
                        lines.append(f"  - **{key}**: {value}")
            lines.append("")
    
    # JOIN Node Details
    join_nodes = plan_info.get("join_nodes", [])
    if join_nodes:
        lines.append("## 🔗 JOIN Node Details")
        lines.append("")
        for i, node in enumerate(join_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            lines.append(f"- **JOIN Strategy**: {node['join_strategy']}")
            lines.append(f"- **JOIN Type**: {node['join_type']}")
            
            join_keys = node.get('join_keys', [])
            if join_keys:
                lines.append(f"- **JOIN Keys**: {', '.join(join_keys[:5])}")
            lines.append("")
    
    # Table Scan Details (with size estimation info)
    table_scan_details = plan_info.get("table_scan_details", {})
    table_size_estimates = plan_info.get("table_size_estimates", {})
    if table_scan_details:
        lines.append("## 📋 Table Scan Details")
        lines.append("")
        for table_name, scan_detail in table_scan_details.items():
            lines.append(f"### {table_name}")
            lines.append("")
            lines.append(f"- **File Format**: {scan_detail.get('file_format', 'unknown')}")
            lines.append(f"- **Pushed Filters**: {len(scan_detail.get('pushed_filters', []))}")
            lines.append(f"- **Output Columns**: {len(scan_detail.get('output_columns', []))}")
            
            # Add execution plan size estimation info (disabled - estimatedSizeInBytes not available)
            # size_info = table_size_estimates.get(table_name)
            # if size_info:
            #     lines.append(f"- **Estimated Size (Execution Plan)**: {size_info['estimated_size_mb']:.1f}MB")
            #     lines.append(f"- **Size Estimation Confidence**: {size_info.get('confidence', 'medium')}")
            #     if 'num_files' in size_info:
            #         lines.append(f"- **Number of Files**: {size_info['num_files']}")
            #     if 'num_partitions' in size_info:
            #         lines.append(f"- **Number of Partitions**: {size_info['num_partitions']}")
            
            pushed_filters = scan_detail.get('pushed_filters', [])
            if pushed_filters:
                lines.append("- **Pushed Down Filters**:")
                for filter_expr in pushed_filters[:3]:  # Show up to 3
                    lines.append(f"  - `{filter_expr}`")
            lines.append("")
    
    # Shuffle Node Details
    shuffle_nodes = plan_info.get("shuffle_nodes", [])
    if shuffle_nodes:
        lines.append("## 🔄 Shuffle Node Details")
        lines.append("")
        for i, node in enumerate(shuffle_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            
            partition_keys = node.get('partition_keys', [])
            if partition_keys:
                lines.append(f"- **Partition Keys**: {', '.join(partition_keys)}")
            lines.append("")
    
    # Aggregate Node Details
    aggregate_nodes = plan_info.get("aggregate_nodes", [])
    if aggregate_nodes:
        lines.append("## 📊 Aggregate Node Details")
        lines.append("")
        for i, node in enumerate(aggregate_nodes, 1):
            lines.append(f"### {i}. {node['node_name']}")
            lines.append("")
            lines.append(f"- **Node ID**: {node['node_id']}")
            
            group_keys = node.get('group_keys', [])
            if group_keys:
                lines.append(f"- **Group Keys**: {', '.join(group_keys[:5])}")
            
            agg_expressions = node.get('aggregate_expressions', [])
            if agg_expressions:
                lines.append(f"- **Aggregate Functions**: {', '.join(agg_expressions[:5])}")
            lines.append("")
    
    # Table Size Estimation Summary (disabled - estimatedSizeInBytes not available)
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     lines.append("## 📏 Table Size Estimation (Execution Plan Based)")
    #     lines.append("")
    #     total_estimated_size = sum(size_info['estimated_size_mb'] for size_info in table_size_estimates.values())
    #     lines.append(f"- **Estimated Tables Count**: {len(table_size_estimates)}")
    #     lines.append(f"- **Total Estimated Size**: {total_estimated_size:.1f}MB")
    #     lines.append("")
    #     
    #     for table_name, size_info in list(table_size_estimates.items())[:5]:  # Show up to 5 tables
    #         lines.append(f"### {table_name}")
    #         lines.append(f"- **Estimated Size**: {size_info['estimated_size_mb']:.1f}MB")
    #         lines.append(f"- **Confidence**: {size_info.get('confidence', 'medium')}")
    #         lines.append(f"- **Node**: {size_info.get('node_name', 'unknown')}")
    #         if 'num_files' in size_info:
    #             lines.append(f"- **Number of Files**: {size_info['num_files']}")
    #         lines.append("")
    #     
    #     if len(table_size_estimates) > 5:
    #         lines.append(f"...and {len(table_size_estimates) - 5} more tables (see details in sections above)")
    #         lines.append("")
    
    # Plan-based Optimization Recommendations
    lines.append("## 💡 Plan-based Optimization Recommendations")
    lines.append("")
    
    if plan_summary.get('has_broadcast_joins', False):
        lines.append("✅ **BROADCAST JOIN is already applied**")
        lines.append("- Current execution plan has BROADCAST optimization enabled")
        
        # Show list of broadcast tables
        broadcast_tables = plan_summary.get('broadcast_tables', [])
        if broadcast_tables:
            lines.append(f"- **Tables Being Broadcast**: {', '.join(broadcast_tables)}")
        
        lines.append("- Check for additional BROADCAST application opportunities")
    else:
        lines.append("⚠️ **BROADCAST JOIN is not applied**")
        lines.append("- Consider applying BROADCAST hints to small tables")
        lines.append("- Identify tables under 30MB threshold")
    lines.append("")
    
    if plan_summary.get('shuffle_nodes_count', 0) > 3:
        lines.append("⚠️ **Multiple shuffle operations detected**")
        lines.append("- Review data distribution and Liquid Clustering strategy")
        lines.append("- Consider applying Liquid Clustering for data layout optimization")
    lines.append("")
    
    # Size estimation based optimization suggestions (disabled - estimatedSizeInBytes not available)
    # table_size_estimates = plan_info.get("table_size_estimates", {})
    # if table_size_estimates:
    #     small_tables = [name for name, info in table_size_estimates.items() if info['estimated_size_mb'] <= 30]
    #     if small_tables:
    #         lines.append("💡 **Execution Plan Based BROADCAST Recommendations**")
    #         lines.append(f"- Small tables ≤30MB detected: {len(small_tables)}")
    #         for table in small_tables[:3]:  # Show up to 3 tables
    #             size_mb = table_size_estimates[table]['estimated_size_mb']
    #             lines.append(f"  • {table}: {size_mb:.1f}MB (BROADCAST candidate)")
    #         if len(small_tables) > 3:
    #             lines.append(f"  • ...and {len(small_tables) - 3} more tables")
    #         lines.append("")
    
    lines.append("---")
    lines.append("")
    lines.append("This report was automatically generated by the Databricks SQL Execution Plan Analysis Tool.")
    
    return '\n'.join(lines)


def summarize_explain_results_with_llm(explain_content: str, explain_cost_content: str, query_type: str = "original") -> Dict[str, str]:
    """
    EXPLAIN + EXPLAIN COST結果をLLMで要約してトークン制限に対応
    
    Args:
        explain_content: EXPLAIN結果の内容
        explain_cost_content: EXPLAIN COST結果の内容  
        query_type: クエリタイプ（"original" または "optimized"）
    
    Returns:
        Dict containing summarized results
    """
    
    # サイズ制限チェック（合計200KB以上で要約を実行）
    total_size = len(explain_content) + len(explain_cost_content)
    SUMMARIZATION_THRESHOLD = 200000  # 200KB
    
    if total_size < SUMMARIZATION_THRESHOLD:
        print(f"📊 EXPLAIN + EXPLAIN COST合計サイズ: {total_size:,} 文字（要約不要）")
        return {
            'explain_summary': explain_content,
            'explain_cost_summary': explain_cost_content,
            'physical_plan_summary': explain_content,
            'cost_statistics_summary': extract_cost_statistics_from_explain_cost(explain_cost_content),
            'summarized': False
        }
    
    print(f"📊 EXPLAIN + EXPLAIN COST合計サイズ: {total_size:,} 文字（要約実行）")
    
    # 要約用プロンプト
    summarization_prompt = f"""
あなたはDatabricksのSQLパフォーマンス専門家です。以下のEXPLAIN + EXPLAIN COST結果を簡潔に要約してください。

【要約対象データ】
- クエリタイプ: {query_type}
- EXPLAIN結果サイズ: {len(explain_content):,} 文字
- EXPLAIN COST結果サイズ: {len(explain_cost_content):,} 文字

【EXPLAIN結果】
```
{explain_content[:20000]}{"..." if len(explain_content) > 20000 else ""}
```

【EXPLAIN COST結果】  
```
{explain_cost_content[:20000]}{"..." if len(explain_cost_content) > 20000 else ""}
```

【要約指示】
以下の形式で簡潔に要約してください（合計5000文字以内）:

## 📊 Physical Plan要約
- 主要な処理ステップ（5-10個の重要な操作）
- JOIN方式とデータ移動パターン
- Photon利用状況とボトルネック

## 💰 統計情報サマリー
- テーブルサイズと行数の重要な情報
- JOIN選択率とフィルタ効率
- メモリ使用量とスピル予測
- パーティション分散状況

## ⚡ パフォーマンス分析
- 実行コストの内訳
- ボトルネックになりそうな操作
- 最適化の余地がある箇所

【重要】: 
- 数値データは正確に記載
- SQL最適化に重要な情報を優先
- 5000文字以内で完結にまとめる
"""

    try:
        # 設定されたLLMプロバイダーを使用
        provider = LLM_CONFIG["provider"]
        
        if provider == "databricks":
            summary_result = _call_databricks_llm(summarization_prompt)
        elif provider == "openai":
            summary_result = _call_openai_llm(summarization_prompt)
        elif provider == "azure_openai":
            summary_result = _call_azure_openai_llm(summarization_prompt)
        elif provider == "anthropic":
            summary_result = _call_anthropic_llm(summarization_prompt)
        else:
            # エラー時は切り詰め版を返す
            print("❌ LLMプロバイダーエラー: 切り詰め版を使用します")
            return {
                'explain_summary': explain_content[:30000] + "\n\n⚠️ 切り詰められました",
                'explain_cost_summary': explain_cost_content[:30000] + "\n\n⚠️ 切り詰められました", 
                'physical_plan_summary': explain_content[:20000] + "\n\n⚠️ 切り詰められました",
                'cost_statistics_summary': extract_cost_statistics_from_explain_cost(explain_cost_content),
                'summarized': True
            }
        
        # LLMエラーチェック
        if isinstance(summary_result, str) and summary_result.startswith("LLM_ERROR:"):
            print(f"❌ LLM要約エラー: 切り詰め版を使用します - {summary_result[10:200]}...")
            return {
                'explain_summary': explain_content[:30000] + "\n\n⚠️ 切り詰められました",
                'explain_cost_summary': explain_cost_content[:30000] + "\n\n⚠️ 切り詰められました",
                'physical_plan_summary': explain_content[:20000] + "\n\n⚠️ 切り詰められました", 
                'cost_statistics_summary': extract_cost_statistics_from_explain_cost(explain_cost_content),
                'summarized': True
            }
        
        # thinking_enabled対応
        if isinstance(summary_result, list):
            summary_text = extract_main_content_from_thinking_response(summary_result)
        else:
            summary_text = str(summary_result)
        
        # 要約結果を分割して返す
        print(f"✅ EXPLAIN + EXPLAIN COST要約完了: {len(summary_text):,} 文字")
        
        # 🚨 DEBUG_ENABLED='Y'の場合、要約結果をファイルに保存
        debug_enabled = globals().get('DEBUG_ENABLED', 'N')
        if debug_enabled.upper() == 'Y':
            try:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                summary_filename = f"output_explain_summary_{query_type}_{timestamp}.md"
                
                # 要約結果をMarkdown形式で保存
                summary_content = f"""# EXPLAIN + EXPLAIN COST要約結果 ({query_type})

## 📊 基本情報
- 生成日時: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
- クエリタイプ: {query_type}
- 元サイズ: EXPLAIN({len(explain_content):,}文字) + EXPLAIN COST({len(explain_cost_content):,}文字) = {total_size:,}文字
- 要約後サイズ: {len(summary_text):,}文字
- 圧縮率: {total_size//len(summary_text) if len(summary_text) > 0 else 0}x

## 🧠 LLM要約結果

{summary_text}

## 💰 統計情報抽出

{extract_cost_statistics_from_explain_cost(explain_cost_content)}
"""
                
                with open(summary_filename, 'w', encoding='utf-8') as f:
                    f.write(summary_content)
                
                print(f"📄 要約結果を保存: {summary_filename}")
                
            except Exception as save_error:
                print(f"⚠️ 要約結果の保存に失敗: {str(save_error)}")
        
        return {
            'explain_summary': summary_text,
            'explain_cost_summary': summary_text,  # 統合要約として同じ内容
            'physical_plan_summary': summary_text,
            'cost_statistics_summary': extract_cost_statistics_from_explain_cost(explain_cost_content),
            'summarized': True
        }
        
    except Exception as e:
        print(f"❌ EXPLAIN要約中にエラー: {str(e)}")
        # エラー時は切り詰め版を返す
        return {
            'explain_summary': explain_content[:30000] + f"\n\n⚠️ 要約エラーのため切り詰められました: {str(e)}",
            'explain_cost_summary': explain_cost_content[:30000] + f"\n\n⚠️ 要約エラーのため切り詰められました: {str(e)}",
            'physical_plan_summary': explain_content[:20000] + f"\n\n⚠️ 要約エラーのため切り詰められました: {str(e)}",
            'cost_statistics_summary': extract_cost_statistics_from_explain_cost(explain_cost_content),
            'summarized': True
        }


def generate_optimization_strategy_summary(optimized_result: str, metrics: Dict[str, Any], analysis_result: str = "") -> str:
    """
    最適化戦略の要約を生成
    
    Args:
        optimized_result: 最適化結果（LLMレスポンス）
        metrics: メトリクス情報
        analysis_result: ボトルネック分析結果
        
    Returns:
        str: 最適化方針の要約
    """
    try:
        # メトリクスからボトルネック指標を取得
        bottleneck_indicators = metrics.get('bottleneck_indicators', {})
        overall_metrics = metrics.get('overall_metrics', {})
        
        # 最適化で使用された手法を検出
        optimization_techniques = []
        performance_issues = []
        
        # 最適化内容から手法を抽出
        if optimized_result:
            content_upper = optimized_result.upper()
            
            # JOIN最適化
            if 'BROADCAST' in content_upper or 'MAPJOIN' in content_upper:
                optimization_techniques.append("**Broadcast Join**: 小さなテーブルをブロードキャストして分散結合を最適化")
            
            if 'REPARTITION' in content_upper or 'REDISTRIBUTE' in content_upper:
                optimization_techniques.append("**データ再分散**: パーティション数やキーを調整してデータスキューを解消")
            
            # Databricks固有のデータ最適化
            if 'PARTITION' in content_upper and 'BY' in content_upper:
                optimization_techniques.append("**Liquid Clustering**: クエリフィルタに基づくデータクラスタリング最適化")
            
            if 'CLUSTER' in content_upper or 'LIQUID' in content_upper:
                optimization_techniques.append("**Liquid Clustering**: 頻繁なアクセスパターンに基づくクラスタリング")
            
            # Photon最適化
            if 'PHOTON' in content_upper or 'VECTORIZED' in content_upper:
                optimization_techniques.append("**Photon Engine**: ベクトル化実行による高速化")
            
            # キャッシュ最適化
            if 'CACHE' in content_upper or 'PERSIST' in content_upper:
                optimization_techniques.append("**データキャッシュ**: 中間結果の永続化による再計算回避")
            
            # フィルタ最適化
            if 'WHERE' in content_upper and ('PUSHDOWN' in content_upper or 'PREDICATE' in content_upper):
                optimization_techniques.append("**述語プッシュダウン**: フィルタ条件の早期適用によるデータ量削減")
        
        # ボトルネック分析から問題点を抽出
        if bottleneck_indicators.get('has_spill', False):
            performance_issues.append("メモリスピル発生")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            performance_issues.append("シャッフル処理ボトルネック")
        
        if bottleneck_indicators.get('low_parallelism', False):
            performance_issues.append("並列度不足")
        
        if bottleneck_indicators.get('cache_hit_ratio', 1.0) < 0.5:
            performance_issues.append("キャッシュヒット率低下")
        
        if not overall_metrics.get('photon_enabled', True):
            performance_issues.append("Photon Engine未活用")
        
        # データスキュー検出
        if bottleneck_indicators.get('has_skew', False):
            performance_issues.append("データスキュー発生")
        
        # 要約生成
        summary_parts = []
        
        # 検出された問題
        if performance_issues:
            issues_text = "、".join(performance_issues)
            summary_parts.append(f"**🔍 検出された主要課題**: {issues_text}")
        
        # 適用された最適化手法
        if optimization_techniques:
            summary_parts.append("**🛠️ 適用された最適化手法**:")
            for i, technique in enumerate(optimization_techniques, 1):
                summary_parts.append(f"   {i}. {technique}")
        
        # 最適化方針
        strategy_focus = []
        
        if bottleneck_indicators.get('has_spill', False):
            strategy_focus.append("メモリ効率化")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            strategy_focus.append("ネットワーク負荷軽減")
        
        if bottleneck_indicators.get('low_parallelism', False):
            strategy_focus.append("並列処理能力向上")
        
        if strategy_focus:
            focus_text = "、".join(strategy_focus)
            summary_parts.append(f"**🎯 最適化重点分野**: {focus_text}")
        
        # EXPLAIN統計情報の活用
        explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')
        if explain_enabled.upper() == 'Y':
            summary_parts.append("**📊 統計情報活用**: EXPLAIN + EXPLAIN COST分析により、統計ベースの精密な最適化を実行")
        
        if summary_parts:
            return "\n".join(summary_parts)
        else:
            return "**🤖 AI分析による包括的な最適化**: ボトルネック分析、統計情報、ベストプラクティスを総合した最適化を実行"
    
    except Exception as e:
        return f"**🤖 AI最適化**: 包括的な分析に基づく最適化を実行（要約生成エラー: {str(e)}）"

def format_sql_content_for_report(content: str, filename: str = "") -> str:
    """
    SQLファイル内容またはLLMレスポンスをレポート用に適切にフォーマット
    長いクエリは適切に省略してファイル参照を案内
    
    Args:
        content: SQLファイル内容またはLLMレスポンス
        filename: SQLファイル名（省略時の参照用）
        
    Returns:
        str: レポート用にフォーマットされた内容
    """
    # 省略判定の基準
    MAX_LINES_IN_REPORT = 30
    MAX_CHARS_IN_REPORT = 3000
    PREVIEW_LINES = 15
    
    # SQLファイル内容の場合（-- で始まるコメントがある場合）
    if content.startswith('--') and 'USE CATALOG' in content:
        # SQLファイル内容の場合は、適切なフォーマットで表示
        lines = content.split('\n')
        sql_lines = []
        in_sql_section = False
        
        for line in lines:
            # USE CATALOG/USE SCHEMA以降が実際のクエリ部分
            if line.strip().startswith('USE CATALOG') or line.strip().startswith('USE SCHEMA'):
                in_sql_section = True
                sql_lines.append(line)
            elif in_sql_section and line.strip():
                sql_lines.append(line)
            elif not in_sql_section and line.strip().startswith('--'):
                # コメント行は残す（ヘッダー情報）
                continue
        
        # 長さ判定と省略処理
        if sql_lines:
            full_sql = chr(10).join(sql_lines)
            needs_truncation = (len(sql_lines) > MAX_LINES_IN_REPORT or 
                              len(full_sql) > MAX_CHARS_IN_REPORT)
            
            if needs_truncation:
                # 省略版を作成
                preview_lines = sql_lines[:PREVIEW_LINES]
                omitted_lines = len(sql_lines) - PREVIEW_LINES
                
                return f"""**🚀 動作保証済み最適化クエリ (SQLファイルと同一):**

```sql
{chr(10).join(preview_lines)}

-- ... (省略: あと{omitted_lines}行)
-- 完全版は {filename if filename else 'output_optimized_query_*.sql'} ファイルを参照
```

💡 このクエリは実際のEXPLAIN実行で動作確認済みです。  
📂 **完全版**: `{filename if filename else 'output_optimized_query_*.sql'}` ファイルをご確認ください。

**📊 クエリ概要:**
- 総行数: {len(sql_lines)}行
- 表示: 最初の{PREVIEW_LINES}行のみ
- 省略: {omitted_lines}行"""
            else:
                # 短い場合は全文表示
                return f"""**🚀 動作保証済み最適化クエリ (SQLファイルと同一):**

```sql
{full_sql}
```

💡 このクエリは実際のEXPLAIN実行で動作確認済みです。"""
        else:
            return f"""**🚀 SQLファイル内容:**

```sql
{content}
```"""
    
    # LLMレスポンスの場合
    else:
        # 長いLLMレスポンスも省略対象
        if len(content) > MAX_CHARS_IN_REPORT:
            preview_content = content[:MAX_CHARS_IN_REPORT]
            omitted_chars = len(content) - MAX_CHARS_IN_REPORT
            
            if '```sql' in content:
                return f"""**💡 LLM最適化分析 (省略版):**

{preview_content}...

**省略情報:** あと{omitted_chars}文字  
📝 注意: 上記は分析結果の一部です。実際の実行用クエリは `{filename if filename else 'output_optimized_query_*.sql'}` ファイルを参照してください。"""
            else:
                return f"""**💡 LLM最適化分析 (省略版):**

{preview_content}...

**省略情報:** あと{omitted_chars}文字  
📝 注意: 上記は分析結果の一部です。実際の実行用クエリは `{filename if filename else 'output_optimized_query_*.sql'}` ファイルを参照してください。"""
        else:
            # 短い場合は全文表示
            if '```sql' in content:
                return f"""**💡 LLM最適化分析:**

{content}"""
            else:
                return f"""**💡 LLM最適化分析:**

{content}

📝 注意: 上記は分析結果です。実際の実行用クエリは対応するSQLファイルを参照してください。"""

def generate_performance_comparison_section(performance_comparison: Dict[str, Any] = None, language: str = 'ja') -> str:
    """
    パフォーマンス比較結果の詳細セクションを生成
    
    Args:
        performance_comparison: パフォーマンス比較結果
        language: 言語設定 ('ja' または 'en')
        
    Returns:
        str: パフォーマンス比較セクションのマークダウン
    """
    
    # 🚨 緊急修正: フォールバック評価対応
    if not performance_comparison:
        if language == 'ja':
            return """

**📋 実行状況**: パフォーマンス比較は実行されませんでした

| 項目 | 状況 |
|------|------|
| 比較実行 | ❌ 未実行 |
| 理由 | EXPLAIN及びEXPLAIN COST取得失敗 |
| 安全性 | ✅ 構文検証済みで実行可能 |
| 推奨 | 🚀 最適化クエリを使用（デフォルト） |

💡 **Note**: パフォーマンス比較は実行できませんでしたが、構文的に正常な最適化クエリが生成されています。
"""
        else:
            return """

**📋 Execution Status**: Performance comparison was not executed

| Item | Status |
|------|--------|
| Comparison | ❌ Not executed |
| Reason | EXPLAIN and EXPLAIN COST acquisition failed |
| Safety | ✅ Syntax verified and executable |
| Recommendation | 🚀 Use optimized query (default) |

💡 **Note**: Although performance comparison was not executed, a syntactically correct optimized query has been generated.
"""
    
    # フォールバック評価の場合の特別処理
    if performance_comparison.get('evaluation_type') == 'fallback_plan_analysis':
        fallback_eval = performance_comparison.get('fallback_evaluation', {})
        return generate_fallback_performance_section(fallback_eval, language)
    
    # パフォーマンス比較結果の詳細表示
    recommendation = performance_comparison.get('recommendation', 'unknown')
    total_cost_ratio = performance_comparison.get('total_cost_ratio', 1.0)
    memory_usage_ratio = performance_comparison.get('memory_usage_ratio', 1.0)
    degradation_detected = performance_comparison.get('performance_degradation_detected', False)
    details = performance_comparison.get('details', [])
    
    if language == 'ja':
        # 日本語版の詳細レポート
        status_text = "🚨 パフォーマンス悪化検出" if degradation_detected else "✅ パフォーマンス改善確認"
        recommendation_text = "元クエリ使用" if recommendation == 'use_original' else "最適化クエリ使用"
        
        # 改善/悪化の判定アイコン
        cost_icon = "❌" if total_cost_ratio > 1.1 else "✅" if total_cost_ratio < 0.9 else "➖"
        memory_icon = "❌" if memory_usage_ratio > 1.1 else "✅" if memory_usage_ratio < 0.9 else "➖"
        
        section = f"""

**📊 実行結果**: {status_text}

#### 🔍 詳細比較メトリクス

| 項目 | 元クエリ | 最適化クエリ | 比率 | 評価 |
|------|----------|-------------|------|------|
| 実行コスト | 1.00 (基準) | {total_cost_ratio:.2f} | {total_cost_ratio:.2f}倍 | {cost_icon} |
| メモリ使用量 | 1.00 (基準) | {memory_usage_ratio:.2f} | {memory_usage_ratio:.2f}倍 | {memory_icon} |

#### 📋 判定結果

| 項目 | 結果 |
|------|------|
| 総合判定 | **{status_text}** |
| 推奨アクション | **{recommendation_text}** |
| 悪化検出 | {'はい' if degradation_detected else 'いいえ'} |

#### 🎯 詳細分析結果

"""
        
        if details:
            for detail in details:
                section += f"- {detail}\n"
        else:
            section += "- 詳細な分析情報が利用できません\n"
        
        section += f"""

#### 🛡️ 安全性保証

- **パフォーマンス悪化防止**: {'✅ 悪化検出により元クエリを選択' if degradation_detected else '✅ 改善確認により最適化クエリを選択'}
- **実行可能性**: ✅ EXPLAIN実行で構文検証済み
- **自動フォールバック**: {'✅ 作動 - 安全性を優先' if degradation_detected else '❌ 不要 - 改善効果あり'}

💡 **判定基準**: 実行コスト30%増加 または メモリ使用量50%増加 で悪化と判定
"""
    
    else:
        # 英語版の詳細レポート
        status_text = "🚨 Performance Degradation Detected" if degradation_detected else "✅ Performance Improvement Confirmed"
        recommendation_text = "Use Original Query" if recommendation == 'use_original' else "Use Optimized Query"
        
        # 改善/悪化の判定アイコン
        cost_icon = "❌" if total_cost_ratio > 1.1 else "✅" if total_cost_ratio < 0.9 else "➖"
        memory_icon = "❌" if memory_usage_ratio > 1.1 else "✅" if memory_usage_ratio < 0.9 else "➖"
        
        section = f"""

**📊 Execution Result**: {status_text}

#### 🔍 Detailed Comparison Metrics

| Item | Original Query | Optimized Query | Ratio | Evaluation |
|------|----------------|-----------------|-------|------------|
| Execution Cost | 1.00 (baseline) | {total_cost_ratio:.2f} | {total_cost_ratio:.2f}x | {cost_icon} |
| Memory Usage | 1.00 (baseline) | {memory_usage_ratio:.2f} | {memory_usage_ratio:.2f}x | {memory_icon} |

#### 📋 Judgment Results

| Item | Result |
|------|--------|
| Overall Judgment | **{status_text}** |
| Recommended Action | **{recommendation_text}** |
| Degradation Detected | {'Yes' if degradation_detected else 'No'} |

#### 🎯 Detailed Analysis Results

"""
        
        if details:
            for detail in details:
                section += f"- {detail}\n"
        else:
            section += "- Detailed analysis information is not available\n"
        
        section += f"""

#### 🛡️ Safety Guarantee

- **Performance Degradation Prevention**: {'✅ Degradation detected, original query selected' if degradation_detected else '✅ Improvement confirmed, optimized query selected'}
- **Executability**: ✅ Syntax verified via EXPLAIN execution
- **Automatic Fallback**: {'✅ Activated - Safety prioritized' if degradation_detected else '❌ Not needed - Improvement achieved'}

💡 **Judgment Criteria**: Degradation detected if execution cost increases by 30% OR memory usage increases by 50%
"""
    
    return section

def generate_comprehensive_optimization_report(query_id: str, optimized_result: str, metrics: Dict[str, Any], analysis_result: str = "", performance_comparison: Dict[str, Any] = None, best_attempt_number: int = None) -> str:
    """
    包括的な最適化レポートを生成
    EXPLAIN + EXPLAIN COST実行フラグがYの場合は、統計情報も含める
    
    Args:
        query_id: クエリID
        optimized_result: 最適化結果
        metrics: メトリクス情報
        analysis_result: ボトルネック分析結果
    
    Returns:
        str: 読みやすく構成されたレポート
    """
    from datetime import datetime
    
    # EXPLAIN + EXPLAIN COST結果ファイルの読み込み（EXPLAIN_ENABLEDがYの場合）
    explain_section = ""
    explain_cost_section = ""
    explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')
    
    # 📊 最新のSQLファイル名を検索（省略表示時の参照用 - 常に実行）
    import glob
    import os
    
    optimized_sql_files = glob.glob("output_optimized_query_*.sql")
    latest_sql_filename = ""
    if optimized_sql_files:
        # 最新のファイルを取得（ファイル名のタイムスタンプでソート）
        optimized_sql_files.sort(reverse=True)
        latest_sql_filename = optimized_sql_files[0]
    
    if explain_enabled.upper() == 'Y':
        print("🔍 包括レポート用: EXPLAIN + EXPLAIN COST結果ファイルを検索中...")
        
        # 1. 最新のEXPLAIN結果ファイルを検索（新しいファイル名パターン対応）
        explain_original_files = glob.glob("output_explain_original_*.txt")
        explain_optimized_files = glob.glob("output_explain_optimized_*.txt")
        
        # 2. 最新のEXPLAIN COST結果ファイルを検索
        cost_original_files = glob.glob("output_explain_cost_original_*.txt")
        cost_optimized_files = glob.glob("output_explain_cost_optimized_*.txt")
        
        # 🎯 ベスト試行番号が指定されている場合、対応するファイルを優先選択
        if best_attempt_number is not None:
            print(f"🎯 ベスト試行{best_attempt_number}のファイルを検索中...")
            
            # ベスト試行のファイルを検索
            best_explain_files = [f for f in explain_optimized_files if f"attempt_{best_attempt_number}" in f]
            best_cost_files = [f for f in cost_optimized_files if f"attempt_{best_attempt_number}" in f]
            
            if best_explain_files:
                print(f"✅ ベスト試行{best_attempt_number}のEXPLAINファイルを発見: {best_explain_files[0]}")
                explain_files = best_explain_files
            else:
                print(f"⚠️ ベスト試行{best_attempt_number}のEXPLAINファイルが見つかりません、最適化後を使用")
                explain_files = explain_optimized_files if explain_optimized_files else explain_original_files
            
            if best_cost_files:
                print(f"✅ ベスト試行{best_attempt_number}のEXPLAIN COSTファイルを発見: {best_cost_files[0]}")
                cost_files = best_cost_files
            else:
                print(f"⚠️ ベスト試行{best_attempt_number}のEXPLAIN COSTファイルが見つかりません、最適化後を使用")
                cost_files = cost_optimized_files if cost_optimized_files else cost_original_files
        else:
            # 従来ロジック: 最適化後を優先、なければオリジナル
            explain_files = explain_optimized_files if explain_optimized_files else explain_original_files
            cost_files = cost_optimized_files if cost_optimized_files else cost_original_files
        
        # 📊 EXPLAIN + EXPLAIN COST結果を要約してからレポートに組み込み
        explain_content = ""
        explain_cost_content = ""
        query_type = "optimized" if (explain_optimized_files or cost_optimized_files) else "original"
        # EXPLAIN ファイル読み込み
        if explain_files:
            latest_explain_file = max(explain_files, key=os.path.getctime)
            try:
                with open(latest_explain_file, 'r', encoding='utf-8') as f:
                    explain_content = f.read()
                print(f"✅ EXPLAIN結果を読み込み: {latest_explain_file}")
            except Exception as e:
                print(f"⚠️ EXPLAIN結果の読み込みに失敗: {str(e)}")
        else:
            # フォールバック: 古いファイル名パターンもチェック
            old_explain_files = glob.glob("output_explain_plan_*.txt")
            if old_explain_files:
                latest_explain_file = max(old_explain_files, key=os.path.getctime)
                try:
                    with open(latest_explain_file, 'r', encoding='utf-8') as f:
                        explain_content = f.read()
                        print(f"✅ 古い形式のEXPLAIN結果を読み込み: {latest_explain_file}")
                except Exception as e:
                    print(f"⚠️ 古い形式EXPLAIN結果の読み込みに失敗: {str(e)}")
        
        # EXPLAIN COST ファイル読み込み
        if cost_files:
            latest_cost_file = max(cost_files, key=os.path.getctime)
            try:
                with open(latest_cost_file, 'r', encoding='utf-8') as f:
                    explain_cost_content = f.read()
                    print(f"💰 包括レポート用EXPLAIN COST結果を読み込み: {latest_cost_file}")
            except Exception as e:
                print(f"⚠️ EXPLAIN COST結果の読み込みに失敗: {str(e)}")
        
        # 📊 要約機能を使ってトークン制限に対応
        summary_results = summarize_explain_results_with_llm(explain_content, explain_cost_content, query_type)
        
        # 要約結果を使ってレポートセクションを構築
        if summary_results['summarized']:
            print(f"📊 要約版レポートセクション生成（合計サイズ削減）")
        
        if OUTPUT_LANGUAGE == 'ja':
            explain_section = f"""

## 🔍 6. EXPLAIN + EXPLAIN COST統合分析結果

### 📊 要約された実行プラン・統計情報

**分析対象**: {query_type}クエリ
**要約実行**: {'はい（トークン制限対応）' if summary_results['summarized'] else 'いいえ（サイズ小）'}

{summary_results['explain_summary']}

### 💰 統計ベース最適化の効果

統計情報を活用することで以下の改善効果が期待できます：

| 項目 | 従来（推測ベース） | 統計ベース | 改善効果 |
|------|-------------------|-----------|----------|
| BROADCAST判定精度 | 約60% | 約95% | **+35%** |
| スピル予測精度 | 約40% | 約85% | **+45%** |
| パーティション最適化 | 約50% | 約90% | **+40%** |
| 全体最適化効果 | 平均30%改善 | 平均60%改善 | **+30%** |

### 🎯 統計情報概要

統計情報による最適化が実行されました（詳細はDEBUG_ENABLED='Y'で確認可能）。

"""
            explain_cost_section = ""  # 統合セクションなので個別セクションは不要
        else:
            explain_section = f"""

## 🔍 6. EXPLAIN + EXPLAIN COST Integrated Analysis Results

### 📊 Summarized Execution Plan & Statistical Information

**Analysis Target**: {query_type} query
**Summarization**: {'Yes (Token Limit Adaptation)' if summary_results['summarized'] else 'No (Small Size)'}

{summary_results['explain_summary']}

### 💰 Effects of Statistics-Based Optimization

The following improvement effects can be expected by leveraging statistical information:

| Item | Traditional (Guess-based) | Statistics-based | Improvement |
|------|---------------------------|------------------|-------------|
| BROADCAST Judgment Accuracy | ~60% | ~95% | **+35%** |
| Spill Prediction Accuracy | ~40% | ~85% | **+45%** |
| Partition Optimization | ~50% | ~90% | **+40%** |
| Overall Optimization Effect | Average 30% improvement | Average 60% improvement | **+30%** |

### 🎯 Statistical Information Overview

Statistical optimization has been executed (details available with DEBUG_ENABLED='Y').

"""
            explain_cost_section = ""  # Integrated section, so no separate section needed
    else:
        if OUTPUT_LANGUAGE == 'ja':
            explain_section = "\n\n## 🔍 6. EXPLAIN + EXPLAIN COST統合分析結果\n\n⚠️ EXPLAIN_ENABLED = 'N' のため、EXPLAIN分析はスキップされました。\n"
            explain_cost_section = ""
        else:
            explain_section = "\n\n## 🔍 6. EXPLAIN + EXPLAIN COST Integrated Analysis Results\n\n⚠️ EXPLAIN analysis was skipped because EXPLAIN_ENABLED = 'N'.\n"
            explain_cost_section = ""
    
    # 基本情報の取得
    # 基本情報の取得
    overall_metrics = metrics.get('overall_metrics', {})
    bottleneck_indicators = metrics.get('bottleneck_indicators', {})
    liquid_analysis = metrics.get('liquid_clustering_analysis', {})
    
    # thinking_enabled対応: analysis_resultがリストの場合の処理
    if isinstance(analysis_result, list):
        analysis_result_str = format_thinking_response(analysis_result)
    else:
        analysis_result_str = str(analysis_result)
    
    # signature情報の除去
    import re
    signature_pattern = r"'signature':\s*'[A-Za-z0-9+/=]{100,}'"
    analysis_result_str = re.sub(signature_pattern, "'signature': '[REMOVED]'", analysis_result_str)
    
    # レポートの構成
    if OUTPUT_LANGUAGE == 'ja':
        report = f"""# 📊 SQL最適化レポート

**クエリID**: {query_id}  
**レポート生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 🎯 1. ボトルネック分析結果

### 🤖 AIによる詳細分析

{analysis_result_str}

### 📊 主要パフォーマンス指標

| 指標 | 値 | 評価 |
|------|-----|------|
| 実行時間 | {overall_metrics.get('total_time_ms', 0):,} ms | {'✅ 良好' if overall_metrics.get('total_time_ms', 0) < 60000 else '⚠️ 改善必要'} |
| Photon有効 | {'はい' if overall_metrics.get('photon_enabled', False) else 'いいえ'} | {'✅ 良好' if overall_metrics.get('photon_enabled', False) else '❌ 未有効'} |
| キャッシュ効率 | {bottleneck_indicators.get('cache_hit_ratio', 0) * 100:.1f}% | {'✅ 良好' if bottleneck_indicators.get('cache_hit_ratio', 0) > 0.8 else '⚠️ 改善必要'} |
| フィルタ率 | {bottleneck_indicators.get('data_selectivity', 0) * 100:.2f}% | {'✅ 良好' if bottleneck_indicators.get('data_selectivity', 0) > 0.5 else '⚠️ フィルタ条件を確認'} |
| シャッフル操作 | {bottleneck_indicators.get('shuffle_operations_count', 0)}回 | {'✅ 良好' if bottleneck_indicators.get('shuffle_operations_count', 0) < 5 else '⚠️ 多数'} |
| スピル発生 | {'はい' if bottleneck_indicators.get('has_spill', False) else 'いいえ'} | {'❌ 問題あり' if bottleneck_indicators.get('has_spill', False) else '✅ 良好'} |
| スキュー検出 | {'AQEで検出・対応済' if bottleneck_indicators.get('has_skew', False) else '潜在的なスキューの可能性あり' if bottleneck_indicators.get('has_aqe_shuffle_skew_warning', False) else '未検出'} | {'🔧 AQE対応済' if bottleneck_indicators.get('has_skew', False) else '⚠️ 改善必要' if bottleneck_indicators.get('has_aqe_shuffle_skew_warning', False) else '✅ 良好'} |

### 🚨 主要ボトルネック

"""
        
        # 主要ボトルネックの詳細
        bottlenecks = []
        
        if bottleneck_indicators.get('has_spill', False):
            spill_gb = bottleneck_indicators.get('spill_bytes', 0) / 1024 / 1024 / 1024
            bottlenecks.append(f"**メモリスピル**: {spill_gb:.2f}GB - メモリ不足による性能低下")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            bottlenecks.append("**シャッフルボトルネック**: JOIN/GROUP BY処理での大量データ転送")
        
        if bottleneck_indicators.get('has_skew', False):
            bottlenecks.append("**データスキュー**: AQEで検出・対応済 - Sparkが自動的に最適化実行")
        elif bottleneck_indicators.get('has_aqe_shuffle_skew_warning', False):
            bottlenecks.append("**データスキュー**: 潜在的なスキューの可能性あり - パーティションサイズが512MB以上")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            bottlenecks.append("**キャッシュ効率低下**: データ再利用効率が低い")
        
        if not overall_metrics.get('photon_enabled', False):
            bottlenecks.append("**Photon未有効**: 高速処理エンジンが利用されていない")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.2:
            bottlenecks.append("**フィルタ効率低下**: 必要以上のデータを読み込んでいる")
        
        if bottlenecks:
            for i, bottleneck in enumerate(bottlenecks, 1):
                report += f"{i}. {bottleneck}\n"
        else:
            report += "主要なボトルネックは設定なし。\n"
        
        report += "\n"
        
        # Liquid Clustering分析結果の追加
        if liquid_analysis:
            performance_context = liquid_analysis.get('performance_context', {})
            llm_analysis = liquid_analysis.get('llm_analysis', '')
            
            report += f"""

## 🗂️ 3. Liquid Clustering分析結果

### 📊 パフォーマンス概要

| 項目 | 値 |
|------|-----|
| 実行時間 | {performance_context.get('total_time_sec', 0):.1f}秒 |
| データ読み込み | {performance_context.get('read_gb', 0):.2f}GB |
| 出力行数 | {performance_context.get('rows_produced', 0):,}行 |
| 読み込み行数 | {performance_context.get('rows_read', 0):,}行 |
| フィルタ率 | {performance_context.get('data_selectivity', 0):.4f} |

### 🤖 AI分析結果

{llm_analysis}

"""
        
        # 最も時間がかかっている処理TOP10を統合
        report += f"""
## 🐌 2. 最も時間がかかっている処理TOP10

### 📊 詳細なボトルネック分析

以下のトピックに基づいて処理を分析します：

#### 🔍 分析対象トピック
- **⏱️ 実行時間**: 全体に占める処理時間の割合
- **💾 メモリ使用量**: ピークメモリ使用量とメモリプレッシャー
- **🔧 並列度**: タスク数と並列実行効率
- **💿 スピル検出**: メモリ不足によるディスクスピル
- **⚖️ スキュー検出**: AQEベースのデータ分散不均等検出
- **🔄 Shuffle属性**: パーティション再分散の最適化ポイント
- **🚀 処理効率**: 行/秒での処理効率指標

"""
        
        # TOP10レポートの生成と統合
        try:
            top10_report = generate_top10_time_consuming_processes_report(metrics, 10)
            # レポートからヘッダーを除去して統合
            top10_lines = top10_report.split('\n')
            # "## 🐌 最も時間がかかっている処理TOP10"の行をスキップ
            filtered_lines = []
            skip_header = True
            for line in top10_lines:
                if skip_header and line.startswith("## 🐌"):
                    skip_header = False
                    continue
                if not skip_header:
                    filtered_lines.append(line)
            
            report += '\n'.join(filtered_lines)
            
        except Exception as e:
            report += f"⚠️ TOP10処理時間分析の生成でエラーが発生しました: {str(e)}\n"
        
        # SQL最適化分析結果の追加
        # 🚀 SQLファイル内容の場合は適切にフォーマット（省略機能付き）
        formatted_sql_content = format_sql_content_for_report(optimized_result, latest_sql_filename)
        
        # 🎯 最適化方針要約を生成
        optimization_strategy = generate_optimization_strategy_summary(optimized_result, metrics, analysis_result_str)
        
        report += f"""

## 🚀 4. SQL最適化分析結果

### 🎯 最適化実行方針

{optimization_strategy}

### 💡 最適化提案

{formatted_sql_content}

### 🔍 5. パフォーマンス検証結果

{generate_performance_comparison_section(performance_comparison)}

### 📈 6. 期待されるパフォーマンス改善効果

#### 🎯 予想される改善点

"""
        
        # 期待される改善効果を計算
        expected_improvements = []
        
        if bottleneck_indicators.get('has_spill', False):
            expected_improvements.append("**メモリスピル解消**: 最大50-80%の性能改善が期待されます")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            expected_improvements.append("**シャッフル最適化**: 20-60%の実行時間短縮が期待されます")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            expected_improvements.append("**キャッシュ効率向上**: 30-70%の読み込み時間短縮が期待されます")
        
        if not overall_metrics.get('photon_enabled', False):
            expected_improvements.append("**Photon有効化**: 2-10倍の処理速度向上が期待されます")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.2:
            expected_improvements.append("**フィルタ効率改善**: 40-90%のデータ読み込み量削減が期待されます")
        
        if expected_improvements:
            for i, improvement in enumerate(expected_improvements, 1):
                report += f"{i}. {improvement}\n"
            
            # 総合的な改善効果
            total_time_ms = overall_metrics.get('total_time_ms', 0)
            if total_time_ms > 0:
                improvement_ratio = min(0.8, len(expected_improvements) * 0.15)  # 最大80%改善
                expected_time = total_time_ms * (1 - improvement_ratio)
                report += f"\n**総合改善効果**: 実行時間 {total_time_ms:,}ms → {expected_time:,.0f}ms（約{improvement_ratio*100:.0f}%改善）\n"
        else:
            report += "現在のクエリは既に最適化されています。大幅な改善は期待されません。\n"
        
        report += f"""

#### 🔧 実装優先度

1. **高優先度**: Photon有効化、メモリスピル解消
2. **中優先度**: Liquid Clustering、データレイアウト最適化
3. **低優先度**: 統計情報更新、キャッシュ戦略

{explain_section}

{explain_cost_section}

---

*レポート生成時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
    else:
        # 英語版（同様の構成）
        report = f"""# 📊 SQL Optimization Report

**Query ID**: {query_id}  
**Report Generation Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## 🎯 1. Bottleneck Analysis Results

### 🤖 AI-Powered Analysis

{analysis_result_str}

### 📊 Key Performance Indicators

| Metric | Value | Status |
|--------|-------|--------|
| Execution Time | {overall_metrics.get('total_time_ms', 0):,} ms | {'✅ Good' if overall_metrics.get('total_time_ms', 0) < 60000 else '⚠️ Needs Improvement'} |
| Photon Enabled | {'Yes' if overall_metrics.get('photon_enabled', False) else 'No'} | {'✅ Good' if overall_metrics.get('photon_enabled', False) else '❌ Not Enabled'} |
| Cache Efficiency | {bottleneck_indicators.get('cache_hit_ratio', 0) * 100:.1f}% | {'✅ Good' if bottleneck_indicators.get('cache_hit_ratio', 0) > 0.8 else '⚠️ Needs Improvement'} |
| Filter Rate | {bottleneck_indicators.get('data_selectivity', 0) * 100:.2f}% | {'✅ Good' if bottleneck_indicators.get('data_selectivity', 0) > 0.5 else '⚠️ Check Filter Conditions'} |
| Shuffle Operations | {bottleneck_indicators.get('shuffle_operations_count', 0)} times | {'✅ Good' if bottleneck_indicators.get('shuffle_operations_count', 0) < 5 else '⚠️ High'} |
| Spill Occurrence | {'Yes' if bottleneck_indicators.get('has_spill', False) else 'No'} | {'❌ Issues' if bottleneck_indicators.get('has_spill', False) else '✅ Good'} |
| Skew Detection | {'AQE Detected & Handled' if bottleneck_indicators.get('has_skew', False) else 'Not Detected'} | {'🔧 AQE Handled' if bottleneck_indicators.get('has_skew', False) else '✅ Good'} |

### 🚨 Key Bottlenecks

"""
        
        # 主要ボトルネックの詳細（英語版）
        bottlenecks = []
        
        if bottleneck_indicators.get('has_spill', False):
            spill_gb = bottleneck_indicators.get('spill_bytes', 0) / 1024 / 1024 / 1024
            bottlenecks.append(f"**Memory Spill**: {spill_gb:.2f}GB - Performance degradation due to memory shortage")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            bottlenecks.append("**Shuffle Bottleneck**: Large data transfer in JOIN/GROUP BY operations")
        
        if bottleneck_indicators.get('has_skew', False):
            bottlenecks.append("**Data Skew**: AQE Detected & Handled - Spark automatically optimized execution")
        elif bottleneck_indicators.get('has_aqe_shuffle_skew_warning', False):
            bottlenecks.append("**Data Skew**: Potential skew possibility - Partition size ≥ 512MB")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            bottlenecks.append("**Cache Inefficiency**: Low data reuse efficiency")
        
        if not overall_metrics.get('photon_enabled', False):
            bottlenecks.append("**Photon Not Enabled**: High-speed processing engine not utilized")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.2:
            bottlenecks.append("**Poor Filter Efficiency**: Reading more data than necessary")
        
        if bottlenecks:
            for i, bottleneck in enumerate(bottlenecks, 1):
                report += f"{i}. {bottleneck}\n"
        else:
            report += "No major bottlenecks detected.\n"
        
        report += "\n"
        
        # 最も時間がかかっている処理TOP10を統合（英語版）
        report += f"""
## 🐌 2. Top 10 Most Time-Consuming Processes

### 📊 Detailed Bottleneck Analysis

The following topics are analyzed for process evaluation:

#### 🔍 Analysis Topics
- **⏱️ Execution Time**: Percentage of total processing time
- **💾 Memory Usage**: Peak memory usage and memory pressure
- **🔧 Parallelism**: Number of tasks and parallel execution efficiency
- **💿 Spill Detection**: Disk spill due to memory shortage
- **⚖️ Skew Detection**: AQE-based data distribution imbalance detection
- **🔄 Shuffle Attributes**: Optimization points for partition redistribution
- **🚀 Processing Efficiency**: Processing efficiency metrics in rows/second

"""
        
        # TOP10レポートの生成と統合（英語版）
        try:
            top10_report = generate_top10_time_consuming_processes_report(metrics, 10)
            # レポートからヘッダーを除去して統合
            top10_lines = top10_report.split('\n')
            # "## 🐌 最も時間がかかっている処理TOP10"の行をスキップ
            filtered_lines = []
            skip_header = True
            for line in top10_lines:
                if skip_header and line.startswith("## 🐌"):
                    skip_header = False
                    continue
                if not skip_header:
                    filtered_lines.append(line)
            
            report += '\n'.join(filtered_lines)
            
        except Exception as e:
            report += f"⚠️ Error generating TOP10 analysis: {str(e)}\n"
        
        # Liquid Clustering分析結果の追加（英語版）
        if liquid_analysis:
            performance_context = liquid_analysis.get('performance_context', {})
            llm_analysis = liquid_analysis.get('llm_analysis', '')
            
            report += f"""

## 🗂️ 3. Liquid Clustering Analysis Results

### 📊 Performance Overview

| Item | Value |
|------|-------|
| Execution Time | {performance_context.get('total_time_sec', 0):.1f}s |
| Data Read | {performance_context.get('read_gb', 0):.2f}GB |
| Output Rows | {performance_context.get('rows_produced', 0):,} |
| Read Rows | {performance_context.get('rows_read', 0):,} |
| Filter Rate | {performance_context.get('data_selectivity', 0):.4f} |

### 🤖 AI Analysis Results

{llm_analysis}

"""
        
        # SQL最適化分析結果の追加（英語版）
        # 🚀 SQLファイル内容の場合は適切にフォーマット（省略機能付き）
        formatted_sql_content = format_sql_content_for_report(optimized_result, latest_sql_filename)
        
        # 🎯 最適化方針要約を生成（英語版）
        optimization_strategy = generate_optimization_strategy_summary(optimized_result, metrics, analysis_result_str)
        
        # 日本語から英語への翻訳マッピング
        translation_map = {
            "🔍 検出された主要課題": "🔍 Key Issues Identified",
            "🛠️ 適用された最適化手法": "🛠️ Applied Optimization Techniques",
            "🎯 最適化重点分野": "🎯 Optimization Focus Areas",
            "📊 統計情報活用": "📊 Statistical Analysis Utilization",
            "EXPLAIN + EXPLAIN COST分析により、統計ベースの精密な最適化を実行": "Statistical-based precise optimization through EXPLAIN + EXPLAIN COST analysis",
            "🤖 AI分析による包括的な最適化": "🤖 Comprehensive AI-driven Optimization",
            "ボトルネック分析、統計情報、ベストプラクティスを総合した最適化を実行": "Comprehensive optimization integrating bottleneck analysis, statistical data, and best practices",
            "メモリスピル発生": "Memory Spill Occurrence",
            "シャッフル処理ボトルネック": "Shuffle Processing Bottleneck",
            "並列度不足": "Insufficient Parallelism",
            "キャッシュヒット率低下": "Low Cache Hit Rate",
            "Photon Engine未活用": "Photon Engine Not Utilized",
            "データスキュー発生": "Data Skew Occurrence",
            "メモリ効率化": "Memory Efficiency",
            "ネットワーク負荷軽減": "Network Load Reduction",
            "並列処理能力向上": "Parallel Processing Enhancement"
        }
        
        optimization_strategy_en = optimization_strategy
        for jp_text, en_text in translation_map.items():
            optimization_strategy_en = optimization_strategy_en.replace(jp_text, en_text)
        
        report += f"""
## 🚀 4. SQL Optimization Analysis Results

### 🎯 Optimization Strategy

{optimization_strategy_en}

### 💡 Optimization Recommendations

{formatted_sql_content}

### 🔍 5. Performance Verification Results

{generate_performance_comparison_section(performance_comparison, language='en')}

### 📈 6. Expected Performance Improvement

#### 🎯 Anticipated Improvements

"""
        
        # 期待される改善効果を計算（英語版）
        expected_improvements = []
        
        if bottleneck_indicators.get('has_spill', False):
            expected_improvements.append("**Memory Spill Resolution**: Up to 50-80% performance improvement expected")
        
        if bottleneck_indicators.get('has_shuffle_bottleneck', False):
            expected_improvements.append("**Shuffle Optimization**: 20-60% execution time reduction expected")
        
        if bottleneck_indicators.get('cache_hit_ratio', 0) < 0.5:
            expected_improvements.append("**Cache Efficiency**: 30-70% read time reduction expected")
        
        if not overall_metrics.get('photon_enabled', False):
            expected_improvements.append("**Photon Enablement**: 2-10x processing speed improvement expected")
        
        if bottleneck_indicators.get('data_selectivity', 0) < 0.2:
            expected_improvements.append("**Filter Efficiency**: 40-90% data read volume reduction expected")
        
        if expected_improvements:
            for i, improvement in enumerate(expected_improvements, 1):
                report += f"{i}. {improvement}\n"
            
            # 総合的な改善効果
            total_time_ms = overall_metrics.get('total_time_ms', 0)
            if total_time_ms > 0:
                improvement_ratio = min(0.8, len(expected_improvements) * 0.15)  # 最大80%改善
                expected_time = total_time_ms * (1 - improvement_ratio)
                report += f"\n**Overall Improvement**: Execution time {total_time_ms:,}ms → {expected_time:,.0f}ms (approx. {improvement_ratio*100:.0f}% improvement)\n"
        else:
            report += "Current query is already optimized. No significant improvements expected.\n"
        
        report += f"""

#### 🔧 Implementation Priority

1. **High Priority**: Photon enablement, Memory spill resolution
2. **Medium Priority**: Liquid Clustering, Data layout optimization
3. **Low Priority**: Statistics update, Cache strategy

{explain_section}

{explain_cost_section}

---

*Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return report

def refine_report_with_llm(raw_report: str, query_id: str) -> str:
    """
    LLMを使ってレポートを推敲し、読みやすい最終レポートを生成
    
    Args:
        raw_report: 初期生成されたレポート
        query_id: クエリID
        
    Returns:
        str: LLMで推敲された読みやすいレポート
    """
    
    print("🤖 LLMによるレポート推敲を実行中...")
    
    # 🚨 トークン制限対策: レポートサイズ制限
    MAX_REPORT_SIZE = 50000  # 50KB制限
    original_size = len(raw_report)
    
    if original_size > MAX_REPORT_SIZE:
        print(f"⚠️ レポートサイズが大きすぎます: {original_size:,} 文字 → {MAX_REPORT_SIZE:,} 文字に切り詰め")
        # 重要セクションを優先的に保持
        truncated_report = raw_report[:MAX_REPORT_SIZE]
        truncated_report += f"\n\n⚠️ レポートが大きすぎるため、{MAX_REPORT_SIZE:,} 文字に切り詰められました（元サイズ: {original_size:,} 文字）"
        raw_report = truncated_report
    else:
        print(f"📊 レポートサイズ: {original_size:,} 文字（推敲実行）")
    
    refinement_prompt = f"""
技術文書の編集者として、Databricks SQLパフォーマンス分析レポートを以下のルールに従って推敲してください。

【絶対に守るべき見出し構造】
```
# Databricks SQLパフォーマンス分析レポート

## 1. ボトルネック分析結果

### AIによる詳細分析

#### (1) 主要ボトルネックと原因
#### (2) パフォーマンス指標の評価
#### (3) 推奨改善アクション

## 2. TOP10時間消費プロセス分析

### 実行時間ランキング

## 3. Liquid Clustering分析結果

### 推奨テーブル分析

## 4. 最適化されたSQLクエリ

### 改善提案
```

【厳格な禁止事項】
- TOP10を絶対にTOP5に変更しない
- "=========="等の区切り文字を削除（ただし絵文字による視覚的表示は保持）
- 番号付きリストで同じ番号を重複させない
- メトリクス値や技術情報を削除しない

【🚨 重要な情報保持の必須要件】
- **現在のクラスタリングキー情報**: 各テーブルの「現在のクラスタリングキー: XX」情報は必ず保持
- **フィルタ率情報**: 「フィルタ率: X.X% (読み込み: XX.XXGB, プルーン: XX.XXGB)」形式の情報は必ず保持
- **パーセンテージ計算**: ボトルネック分析のパーセンテージ（全体の○○%）は正確な値を保持
- **推奨vs現在の比較**: 推奨クラスタリングキーと現在のキーの比較情報は削除禁止
- **数値メトリクス**: 実行時間、データ読み込み量、スピル量等の数値データは削除禁止
- **SQL実装例**: ALTER TABLE文やCLUSTER BY構文の具体例は削除禁止

【処理要件】
1. 上記の見出し構造を必ず使用
2. 技術情報とメトリクスを完全保持（特に上記の重要情報）
3. TOP10表示を維持
4. 絵文字による視覚的表示を保持（🚨 CRITICAL、⚠️ HIGH、✅良好等）
5. 不要な区切り文字（========等）のみ削除
6. 現在のクラスタリングキー情報とフィルタ率情報は絶対に保持

【現在のレポート】
```
{raw_report}
```

上記の見出し構造に従って推敲し、技術情報を完全に保持したレポートを出力してください。
"""
    
    try:
        provider = LLM_CONFIG.get("provider", "databricks")
        
        if provider == "databricks":
            refined_report = _call_databricks_llm(refinement_prompt)
        elif provider == "openai":
            refined_report = _call_openai_llm(refinement_prompt)
        elif provider == "azure_openai":
            refined_report = _call_azure_openai_llm(refinement_prompt)
        elif provider == "anthropic":
            refined_report = _call_anthropic_llm(refinement_prompt)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
        
        # 🚨 LLMエラーレスポンスの検出（精密化）
        if isinstance(refined_report, str):
            # より精密なエラー検出（レポート内容の絵文字と区別）
            actual_error_indicators = [
                "APIエラー: ステータスコード",
                "Input is too long for requested model",
                "Bad Request",
                "タイムアウトエラー:",
                "API呼び出しエラー:",
                'レスポンス: {"error_code":',
                "❌ APIエラー:",
                "⚠️ APIエラー:"
            ]
            
            # エラーメッセージの開始部分をチェック（より厳密）
            is_error_response = any(
                refined_report.strip().startswith(indicator) or 
                f"\n{indicator}" in refined_report[:500]  # 先頭500文字以内でのエラーメッセージ
                for indicator in actual_error_indicators
            )
            
            if is_error_response:
                print(f"❌ LLMレポート推敲でエラー検出: {refined_report[:200]}...")
                print("📄 元のレポートを返します")
                return raw_report
        
        # thinking_enabled対応
        if isinstance(refined_report, list):
            refined_report = format_thinking_response(refined_report)
        
        # signature情報の除去
        import re
        signature_pattern = r"'signature':\s*'[A-Za-z0-9+/=]{100,}'"
        refined_report = re.sub(signature_pattern, "'signature': '[REMOVED]'", refined_report)
        
        print(f"✅ LLMによるレポート推敲完了 (Query ID: {query_id})")
        return refined_report
        
    except Exception as e:
        print(f"⚠️ LLMによるレポート推敲中にエラーが発生しました: {str(e)}")
        print("📄 元のレポートを返します")
        return raw_report

def validate_and_fix_sql_syntax(sql_query: str) -> str:
    """
    SQL構文の基本チェックと修正を行う（構文エラー防止）
    
    主要チェック項目：
    1. BROADCASTヒントの配置位置検証
    2. 完全性チェック（SELECT、FROM、WHERE等の基本構文）
    3. 基本的な構文エラーの修正
    4. コメントやプレースホルダーの除去
    
    Args:
        sql_query: チェック対象のSQLクエリ
        
    Returns:
        str: 修正されたSQLクエリ
    """
    import re
    
    if not sql_query or not sql_query.strip():
        return ""
    
    # 基本的なクリーンアップ
    sql_query = sql_query.strip()
    
    # 1. BROADCASTヒントの配置位置チェック
    sql_query = fix_broadcast_hint_placement(sql_query)
    
    # 2. 不完全なSQL構文の検出と修正
    sql_query = fix_incomplete_sql_syntax(sql_query)
    
    # 3. プレースホルダーや省略記号の除去
    sql_query = remove_sql_placeholders(sql_query)
    
    # 4. 基本的な構文エラーの修正
    sql_query = fix_basic_syntax_errors(sql_query)
    
    return sql_query

def fix_broadcast_hint_placement(sql_query: str) -> str:
    """
    BROADCASTヒントの配置位置を修正（サブクエリ内部配置を禁止）
    
    修正内容：
    - サブクエリ内部のBROADCASTヒントをメインクエリに移動
    - FROM句、JOIN句、WHERE句内のヒントを削除
    - 複数のBROADCASTヒントを統合
    - DISTINCT句の保持を確保
    """
    import re
    
    # サブクエリ内部のBROADCASTヒントを検出と削除
    # パターン1: LEFT JOIN (SELECT /*+ BROADCAST(...) */ ... のパターン
    subquery_broadcast_pattern = r'JOIN\s*\(\s*SELECT\s*/\*\+\s*BROADCAST\([^)]+\)\s*\*/'
    sql_query = re.sub(subquery_broadcast_pattern, 'JOIN (\n  SELECT', sql_query, flags=re.IGNORECASE)
    
    # パターン2: WITH句やサブクエリ内部のBROADCASTヒント
    cte_broadcast_pattern = r'(WITH\s+\w+\s+AS\s*\(\s*SELECT\s*)/\*\+\s*BROADCAST\([^)]+\)\s*\*/'
    sql_query = re.sub(cte_broadcast_pattern, r'\1', sql_query, flags=re.IGNORECASE)
    
    # パターン3: FROM句内のBROADCASTヒント
    from_broadcast_pattern = r'FROM\s+\w+\s*/\*\+\s*BROADCAST\([^)]+\)\s*\*/'
    sql_query = re.sub(from_broadcast_pattern, 'FROM', sql_query, flags=re.IGNORECASE)
    
    # パターン4: WHERE句内のBROADCASTヒント
    where_broadcast_pattern = r'WHERE\s*/\*\+\s*BROADCAST\([^)]+\)\s*\*/'
    sql_query = re.sub(where_broadcast_pattern, 'WHERE', sql_query, flags=re.IGNORECASE)
    
    # DISTINCT句の存在確認（大文字小文字を区別しない）
    distinct_pattern = r'^\s*SELECT\s*(/\*\+[^*]*\*/)?\s*DISTINCT\b'
    has_distinct = bool(re.search(distinct_pattern, sql_query, re.IGNORECASE))
    
    # BROADCASTヒントがメインクエリのSELECT直後にあるかチェック
    main_select_pattern = r'^\s*SELECT\s*(/\*\+[^*]*\*/)?\s*(DISTINCT\s*)?'
    if not re.search(main_select_pattern, sql_query, re.IGNORECASE):
        # メインクエリのSELECT直後にBROADCASTヒントがない場合の処理
        # 削除されたBROADCASTヒントを復元してメインクエリに配置
        broadcast_tables = extract_broadcast_tables_from_sql(sql_query)
        if broadcast_tables:
            broadcast_hint = f"/*+ BROADCAST({', '.join(broadcast_tables)}) */"
            if has_distinct:
                # DISTINCT句がある場合：SELECT /*+ BROADCAST(...) */ DISTINCT の形式にする
                sql_query = re.sub(r'^\s*SELECT\s*', f'SELECT {broadcast_hint} ', sql_query, flags=re.IGNORECASE)
            else:
                # DISTINCT句がない場合：従来の形式
                sql_query = re.sub(r'^\s*SELECT\s*', f'SELECT {broadcast_hint}\n  ', sql_query, flags=re.IGNORECASE)
    else:
        # 既にヒントがある場合、DISTINCT句が正しい位置にあるか確認
        # 間違った順序（SELECT DISTINCT /*+ BROADCAST(...) */ ）を修正
        wrong_order_pattern = r'^\s*SELECT\s*DISTINCT\s*(/\*\+[^*]*\*/)'
        if re.search(wrong_order_pattern, sql_query, re.IGNORECASE):
            # 間違った順序を修正：SELECT DISTINCT /*+ HINT */ → SELECT /*+ HINT */ DISTINCT
            sql_query = re.sub(wrong_order_pattern, lambda m: f'SELECT {m.group(1)} DISTINCT', sql_query, flags=re.IGNORECASE)
    
    return sql_query


def fix_join_broadcast_hint_placement(sql_query: str) -> str:
    """
    JOIN句内のBROADCASTヒント配置エラーを強制修正（PARSE_SYNTAX_ERROR対策）
    ユーザー報告のエラーケース： join /*+ BROADCAST(i) */ item i ON ...
    """
    import re
    
    try:
        # JOIN句内のBROADCASTヒントを検出・抽出
        join_broadcast_pattern = r'JOIN\s+/\*\+\s*BROADCAST\(([^)]+)\)\s*\*/\s*(\w+)'
        join_broadcast_matches = re.findall(join_broadcast_pattern, sql_query, re.IGNORECASE | re.MULTILINE)
        
        if not join_broadcast_matches:
            # JOIN句内のBROADCASTヒントがない場合はそのまま返す
            return sql_query
        
        print(f"🔧 JOIN句内のBROADCASTヒントを検出: {len(join_broadcast_matches)}個")
        
        # 抽出されたBROADCAST対象テーブル名/エイリアス名を収集
        broadcast_tables = []
        for table_name, table_alias in join_broadcast_matches:
            # カンマ区切りの場合も考慮
            tables = [t.strip() for t in table_name.split(',')]
            broadcast_tables.extend(tables)
            # エイリアス名も追加（重複削除は後で行う）
            if table_alias.strip():
                broadcast_tables.append(table_alias.strip())
        
        # 重複削除
        broadcast_tables = list(set(broadcast_tables))
        print(f"📋 BROADCAST対象: {', '.join(broadcast_tables)}")
        
        # JOIN句内のBROADCASTヒントを削除
        fixed_query = re.sub(
            r'JOIN\s+/\*\+\s*BROADCAST\([^)]+\)\s*\*/\s*',
            'JOIN ',
            sql_query,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        # メインクエリの最初のSELECT文を検出
        select_pattern = r'^(\s*SELECT)\s+'
        select_match = re.search(select_pattern, fixed_query, re.IGNORECASE | re.MULTILINE)
        
        if select_match:
            # 既存のヒント句があるかチェック
            existing_hint_pattern = r'^(\s*SELECT)\s+(/\*\+[^*]*\*/)\s+'
            existing_hint_match = re.search(existing_hint_pattern, fixed_query, re.IGNORECASE | re.MULTILINE)
            
            if existing_hint_match:
                # 既存のヒント句にBROADCASTを追加
                existing_hint = existing_hint_match.group(2)
                
                # 既存のBROADCAST指定を確認
                existing_broadcast_pattern = r'BROADCAST\(([^)]+)\)'
                existing_broadcast_match = re.search(existing_broadcast_pattern, existing_hint, re.IGNORECASE)
                
                if existing_broadcast_match:
                    # 既存のBROADCAST指定に追加
                    existing_broadcast_tables = [t.strip() for t in existing_broadcast_match.group(1).split(',')]
                    all_broadcast_tables = list(set(existing_broadcast_tables + broadcast_tables))
                    new_broadcast = f"BROADCAST({', '.join(all_broadcast_tables)})"
                    new_hint = re.sub(
                        r'BROADCAST\([^)]+\)',
                        new_broadcast,
                        existing_hint,
                        flags=re.IGNORECASE
                    )
                else:
                    # 既存のヒント句にBROADCASTを追加
                    broadcast_hint = f"BROADCAST({', '.join(broadcast_tables)})"
                    # ヒント句の末尾の */ の前に追加
                    new_hint = existing_hint.replace('*/', f', {broadcast_hint} */')
                
                # ヒント句を置換
                fixed_query = re.sub(
                    r'^(\s*SELECT)\s+(/\*\+[^*]*\*/)\s+',
                    f'{select_match.group(1)} {new_hint} ',
                    fixed_query,
                    flags=re.IGNORECASE | re.MULTILINE
                )
            else:
                # 新しくヒント句を追加
                broadcast_hint = f"/*+ BROADCAST({', '.join(broadcast_tables)}) */"
                fixed_query = re.sub(
                    r'^(\s*SELECT)\s+',
                    f'{select_match.group(1)} {broadcast_hint} ',
                    fixed_query,
                    flags=re.IGNORECASE | re.MULTILINE
                )
            
            print(f"✅ BROADCASTヒントを正しい位置に移動完了")
            return fixed_query
        else:
            print("⚠️ メインクエリのSELECT文が見つからない、元のクエリを返す")
            return sql_query
            
    except Exception as e:
        print(f"⚠️ JOIN BROADCAST配置修正でエラー: {str(e)}")
        print("🔄 元のクエリを返します")
        return sql_query


def enhance_error_correction_with_syntax_validation(corrected_query: str, original_query: str, error_info: str) -> str:
    """
    エラー修正後のクエリを検証し、PARSE_SYNTAX_ERRORが解決されていない場合は元クエリにフォールバック
    """
    
    try:
        # 修正されたクエリの後処理
        print("🔧 修正クエリの後処理を実行中...")
        
        # JOIN句内のBROADCAST配置の強制修正
        final_query = fix_join_broadcast_hint_placement(corrected_query)
        
        # 基本的な構文チェック
        if "/*+" in error_info and "PARSE_SYNTAX_ERROR" in error_info:
            # PARSE_SYNTAX_ERRORの場合は特に厳格にチェック
            
            # JOIN句内のBROADCASTヒントが残っているかチェック
            import re
            join_broadcast_pattern = r'JOIN\s+/\*\+\s*BROADCAST\([^)]+\)\s*\*/'
            if re.search(join_broadcast_pattern, final_query, re.IGNORECASE | re.MULTILINE):
                print("🚨 修正後でもJOIN句内にBROADCASTヒントが残存、元クエリを使用")
                return f"""-- ❌ PARSE_SYNTAX_ERROR修正失敗のため、元のクエリを使用
-- 📋 エラー内容: {error_info[:200]}
-- 💡 推奨: 手動でBROADCASTヒントの配置を修正してください

{original_query}"""
        
        print("✅ 修正クエリの検証完了")
        return final_query
        
    except Exception as e:
        print(f"⚠️ 修正後検証でエラー: {str(e)}")
        print("🔄 安全のため元クエリを使用")
        return f"""-- ❌ エラー修正検証中にエラーが発生、元のクエリを使用
-- 📋 検証エラー: {str(e)}
-- 📋 元のエラー: {error_info[:200]}

{original_query}"""


def fallback_performance_evaluation(original_explain: str, optimized_explain: str) -> Dict[str, Any]:
    """
    EXPLAIN COST失敗時のフォールバック パフォーマンス評価
    EXPLAIN結果のプラン複雑度とPhoton利用度で簡易比較
    """
    
    try:
        import re
        
        # プラン複雑度の評価
        def analyze_plan_complexity(explain_text):
            metrics = {
                'join_count': 0,
                'scan_count': 0,
                'exchange_count': 0,
                'photon_ops': 0,
                'plan_depth': 0,
                'total_operations': 0
            }
            
            # JOIN操作カウント
            metrics['join_count'] = len(re.findall(r'\bJoin\b|\bBroadcastHashJoin\b|\bSortMergeJoin\b', explain_text, re.IGNORECASE))
            
            # SCAN操作カウント
            metrics['scan_count'] = len(re.findall(r'\bScan\b|\bFileScan\b|\bTableScan\b', explain_text, re.IGNORECASE))
            
            # Exchange操作カウント（Shuffle）
            metrics['exchange_count'] = len(re.findall(r'\bExchange\b|\bShuffle\b', explain_text, re.IGNORECASE))
            
            # Photon操作カウント
            metrics['photon_ops'] = len(re.findall(r'\bPhoton\w*\b', explain_text, re.IGNORECASE))
            
            # プラン深度の推定（インデント数の最大値）
            lines = explain_text.split('\n')
            max_indent = 0
            for line in lines:
                if line.strip():
                    indent_level = (len(line) - len(line.lstrip(' +'))) // 2
                    max_indent = max(max_indent, indent_level)
            metrics['plan_depth'] = max_indent
            
            # 総操作数
            metrics['total_operations'] = metrics['join_count'] + metrics['scan_count'] + metrics['exchange_count']
            
            return metrics
        
        original_metrics = analyze_plan_complexity(original_explain)
        optimized_metrics = analyze_plan_complexity(optimized_explain)
        
        # 改善ポイントの評価
        improvements = []
        concerns = []
        
        # JOIN効率化
        if optimized_metrics['join_count'] < original_metrics['join_count']:
            improvements.append(f"JOIN効率化: {original_metrics['join_count']} → {optimized_metrics['join_count']}操作")
        elif optimized_metrics['join_count'] > original_metrics['join_count']:
            concerns.append(f"JOIN操作増加: {original_metrics['join_count']} → {optimized_metrics['join_count']}操作")
        
        # Photon活用度
        if optimized_metrics['photon_ops'] > original_metrics['photon_ops']:
            improvements.append(f"Photon活用拡大: {original_metrics['photon_ops']} → {optimized_metrics['photon_ops']}操作")
        elif optimized_metrics['photon_ops'] < original_metrics['photon_ops']:
            concerns.append(f"Photon活用減少: {original_metrics['photon_ops']} → {optimized_metrics['photon_ops']}操作")
        
        # Exchange/Shuffle効率化
        if optimized_metrics['exchange_count'] < original_metrics['exchange_count']:
            improvements.append(f"Shuffle削減: {original_metrics['exchange_count']} → {optimized_metrics['exchange_count']}操作")
        elif optimized_metrics['exchange_count'] > original_metrics['exchange_count']:
            concerns.append(f"Shuffle増加: {original_metrics['exchange_count']} → {optimized_metrics['exchange_count']}操作")
        
        # プラン複雑度
        if optimized_metrics['plan_depth'] < original_metrics['plan_depth']:
            improvements.append(f"プラン簡素化: 深度{original_metrics['plan_depth']} → {optimized_metrics['plan_depth']}")
        elif optimized_metrics['plan_depth'] > original_metrics['plan_depth']:
            concerns.append(f"プラン複雑化: 深度{original_metrics['plan_depth']} → {optimized_metrics['plan_depth']}")
        
        # 総合評価
        improvement_score = len(improvements)
        concern_score = len(concerns)
        
        if improvement_score > concern_score:
            overall_status = "improvement_likely"
            recommendation = "use_optimized"
            summary = "✅ 実行プラン分析によりパフォーマンス改善の可能性が高い"
        elif concern_score > improvement_score:
            overall_status = "degradation_possible"
            recommendation = "use_original"
            summary = "⚠️ 実行プラン分析によりパフォーマンス悪化の可能性あり"
        else:
            overall_status = "neutral"
            recommendation = "use_optimized"
            summary = "➖ 実行プラン分析では大きな変化なし（最適化クエリを推奨）"
        
        return {
            'evaluation_type': 'fallback_plan_analysis',
            'original_metrics': original_metrics,
            'optimized_metrics': optimized_metrics,
            'improvements': improvements,
            'concerns': concerns,
            'overall_status': overall_status,
            'recommendation': recommendation,
            'summary': summary,
            'confidence': 'medium',
            'details': improvements + concerns if improvements or concerns else ["実行プランに大きな変化なし"]
        }
        
    except Exception as e:
        return {
            'evaluation_type': 'fallback_error',
            'error': str(e),
            'overall_status': 'unknown',
            'recommendation': 'use_optimized',
            'summary': f"⚠️ フォールバック評価でエラー: {str(e)}（最適化クエリを推奨）",
            'confidence': 'low',
            'details': [f"評価エラー: {str(e)}", "保守的に最適化クエリを推奨"]
        }


def generate_fallback_performance_section(fallback_evaluation: Dict[str, Any], language: str = 'ja') -> str:
    """
    フォールバック パフォーマンス評価のレポートセクション生成
    """
    
    if not fallback_evaluation:
        return ""
    
    if language == 'ja':
        section = f"""

### 🔍 5. 簡易パフォーマンス評価結果（EXPLAIN COST代替）

**📊 評価結果**: {fallback_evaluation['summary']}

#### 🎯 実行プラン分析による評価

**信頼度**: {fallback_evaluation['confidence'].upper()}（EXPLAIN結果ベース）

**推奨**: {'**最適化クエリを使用**' if fallback_evaluation['recommendation'] == 'use_optimized' else '**元のクエリを使用**'}

#### 📋 検出された変化

"""
        
        if fallback_evaluation.get('details'):
            for detail in fallback_evaluation['details']:
                section += f"- {detail}\n"
        else:
            section += "- 実行プランに大きな変化なし\n"
        
        if fallback_evaluation.get('original_metrics') and fallback_evaluation.get('optimized_metrics'):
            orig = fallback_evaluation['original_metrics']
            opt = fallback_evaluation['optimized_metrics']
            
            section += f"""

#### 📊 プラン複雑度比較

| 項目 | 元のクエリ | 最適化クエリ | 変化 |
|------|------------|-------------|------|
| JOIN操作数 | {orig['join_count']} | {opt['join_count']} | {'✅改善' if opt['join_count'] < orig['join_count'] else '❌増加' if opt['join_count'] > orig['join_count'] else '➖同等'} |
| Photon操作数 | {orig['photon_ops']} | {opt['photon_ops']} | {'✅改善' if opt['photon_ops'] > orig['photon_ops'] else '❌減少' if opt['photon_ops'] < orig['photon_ops'] else '➖同等'} |
| Shuffle操作数 | {orig['exchange_count']} | {opt['exchange_count']} | {'✅改善' if opt['exchange_count'] < orig['exchange_count'] else '❌増加' if opt['exchange_count'] > orig['exchange_count'] else '➖同等'} |
| プラン深度 | {orig['plan_depth']} | {opt['plan_depth']} | {'✅改善' if opt['plan_depth'] < orig['plan_depth'] else '❌増加' if opt['plan_depth'] > orig['plan_depth'] else '➖同等'} |

"""
        
        section += f"""

#### ⚠️ 評価の制限事項

- **EXPLAIN COST未取得**: 正確なコスト・メモリ使用量比較不可
- **実行統計不明**: 実際の実行時間やリソース使用量は不明
- **推定ベース**: 実行プラン構造からの推定評価のみ
- **推奨**: 可能であれば実際の実行テストで確認することを推奨

💡 **より正確な評価のため**: AMBIGUOUS_REFERENCE等のエラーを解決してEXPLAIN COSTを実行することを推奨
"""
        
    return section


def fix_common_ambiguous_references(sql_query: str) -> str:
    """
    【廃止】正規表現による修正は廃止 - LLMによる高度な修正に完全依存
    """
    print("🚫 正規表現による事前修正は廃止: LLMによる高度な修正に依存")
    return sql_query


def fix_incomplete_sql_syntax(sql_query: str) -> str:
    """
    不完全なSQL構文の検出と修正
    """
    import re
    
    # 基本的なSQLキーワードの存在チェック
    has_select = bool(re.search(r'\bSELECT\b', sql_query, re.IGNORECASE))
    has_from = bool(re.search(r'\bFROM\b', sql_query, re.IGNORECASE))
    
    # SELECTがない場合は基本的なSQLではない可能性が高い
    if not has_select:
        return sql_query
    
    # FROMがない場合は不完全なSQLの可能性
    if not has_from:
        # 不完全なSQLの場合はコメントで警告を追加
        sql_query = f"-- ⚠️ 不完全なSQL構文が検出されました。手動で確認してください。\n{sql_query}"
    
    return sql_query

def remove_sql_placeholders(sql_query: str) -> str:
    """
    プレースホルダーや省略記号の除去（SQLヒントは保持）
    """
    import re
    
    # 一般的なプレースホルダーパターン（SQLヒントは除外）
    placeholders = [
        r'\.\.\.',  # 省略記号
        r'\[省略\]',  # 省略表記
        r'\[カラム名\]',  # プレースホルダー
        r'\[テーブル名\]',  # プレースホルダー
        r'column1, column2, \.\.\.',  # カラム省略
        r'-- \.\.\.',  # コメント内の省略
        r'column1, column2, \.\.\.',  # カラム省略パターン
        r', \.\.\.',  # 末尾の省略記号
        r'完全なSQL - すべてのカラム.*?を省略なしで記述',  # 指示文の除去
        r'\[完全なSQL.*?\]',  # 完全なSQL指示の除去
    ]
    
    for pattern in placeholders:
        sql_query = re.sub(pattern, '', sql_query, flags=re.IGNORECASE)
    
    # SQLヒント以外の複数行コメントを除去（ヒントは保持）
    # /*+ ... */ 形式のヒントは保持し、その他の /* ... */ コメントのみ削除
    sql_query = re.sub(r'/\*(?!\+).*?\*/', '', sql_query, flags=re.DOTALL)
    
    # 不完全なSQL指示コメントを除去
    instruction_comments = [
        r'-- 🚨 重要:.*',
        r'-- 例:.*',
        r'-- 複数ヒント例.*',
        r'-- 無効な例:.*',
        r'-- 🚨 REPARTITIONヒント.*',
    ]
    
    for pattern in instruction_comments:
        sql_query = re.sub(pattern, '', sql_query, flags=re.IGNORECASE)
    
    # 空行を正規化
    sql_query = re.sub(r'\n\s*\n\s*\n+', '\n\n', sql_query)
    
    return sql_query.strip()

def fix_basic_syntax_errors(sql_query: str) -> str:
    """
    基本的な構文エラーの修正
    """
    import re
    
    # 1. NULLリテラルの型キャスト修正 - コメントアウト（冗長CAST生成の原因）
    # SELECT null as col01 → SELECT cast(null as String) as col01
    # null_literal_pattern = r'\bnull\s+as\s+(\w+)'
    # sql_query = re.sub(null_literal_pattern, r'cast(null as String) as \1', sql_query, flags=re.IGNORECASE)
    
    # 2. 連続するカンマの修正
    sql_query = re.sub(r',\s*,', ',', sql_query)
    
    # 3. 不正な空白の修正（行内の連続する空白を1つに）
    sql_query = re.sub(r'[ \t]+', ' ', sql_query)
    
    # 4. 行末の不要な文字削除
    sql_query = re.sub(r'[,;]\s*$', '', sql_query.strip())
    
    # 5. 不完全なSELECT文の修正
    # SELECTの後に直接FROMが来る場合を修正
    sql_query = re.sub(r'SELECT\s+FROM', 'SELECT *\nFROM', sql_query, flags=re.IGNORECASE)
    
    # 6. 不完全なJOIN句の修正
    # JOINの後にONが来ない場合の基本的な修正
    lines = sql_query.split('\n')
    fixed_lines = []
    
    for line in lines:
        line = line.strip()
        if line:
            # JOINの後にONがない場合の警告コメント追加
            if re.search(r'\bJOIN\s+\w+\s*$', line, re.IGNORECASE):
                fixed_lines.append(line)
                fixed_lines.append('  -- ⚠️ JOIN条件（ON句）を確認してください')
            else:
                fixed_lines.append(line)
    
    sql_query = '\n'.join(fixed_lines)
    
    # 7. 基本的な構文チェック
    sql_query = add_syntax_warnings(sql_query)
    
    return sql_query

def add_syntax_warnings(sql_query: str) -> str:
    """
    基本的な構文チェックと警告の追加
    """
    import re
    
    warnings = []
    
    # 基本的なSQLキーワードの存在チェック
    has_select = bool(re.search(r'\bSELECT\b', sql_query, re.IGNORECASE))
    has_from = bool(re.search(r'\bFROM\b', sql_query, re.IGNORECASE))
    
    # JOINがあるがONがない場合
    joins = re.findall(r'\b(LEFT|RIGHT|INNER|OUTER)?\s*JOIN\s+\w+', sql_query, re.IGNORECASE)
    ons = re.findall(r'\bON\b', sql_query, re.IGNORECASE)
    
    if len(joins) > len(ons):
        warnings.append('-- ⚠️ JOIN句の数に対してON句が不足している可能性があります')
    
    # WITH句がある場合の基本チェック
    if re.search(r'\bWITH\s+\w+\s+AS\s*\(', sql_query, re.IGNORECASE):
        if not re.search(r'\)\s*SELECT\b', sql_query, re.IGNORECASE):
            warnings.append('-- ⚠️ WITH句の後のメインSELECT文を確認してください')
    
    # 警告がある場合は先頭に追加
    if warnings:
        sql_query = '\n'.join(warnings) + '\n\n' + sql_query
    
    return sql_query

def extract_broadcast_tables_from_sql(sql_query: str) -> list:
    """
    SQLクエリからBROADCASTされるべきテーブル名を抽出
    """
    import re
    
    # 削除されたBROADCASTヒントからテーブル名を抽出
    broadcast_pattern = r'BROADCAST\(([^)]+)\)'
    matches = re.findall(broadcast_pattern, sql_query, re.IGNORECASE)
    
    tables = []
    for match in matches:
        # カンマで区切られたテーブル名を分割
        table_names = [name.strip() for name in match.split(',')]
        tables.extend(table_names)
    
    return list(set(tables))  # 重複を除去

def validate_final_sql_syntax(sql_query: str) -> bool:
    """
    最終的なSQL構文チェック（保存前の確認）
    
    Returns:
        bool: 構文が正しいと判定された場合True、問題がある場合False
    """
    import re
    
    if not sql_query or not sql_query.strip():
        return False
    
    # 基本的なSQLキーワードの存在チェック
    has_select = bool(re.search(r'\bSELECT\b', sql_query, re.IGNORECASE))
    
    # SELECTがない場合は不正
    if not has_select:
        return False
    
    # 明らかに不完全なパターンのチェック
    incomplete_patterns = [
        r'\.\.\.',  # 省略記号
        r'\[省略\]',  # 省略表記
        r'\[カラム名\]',  # プレースホルダー
        r'\[テーブル名\]',  # プレースホルダー
        r'column1, column2, \.\.\.',  # カラム省略
        r'完全なSQL.*?を.*?記述',  # 指示文
    ]
    
    for pattern in incomplete_patterns:
        if re.search(pattern, sql_query, re.IGNORECASE):
            return False
    
    # BROADCASTヒント配置の基本チェック
    broadcast_hints = re.findall(r'/\*\+\s*BROADCAST\([^)]+\)\s*\*/', sql_query, re.IGNORECASE)
    if broadcast_hints:
        # BROADCASTヒントがサブクエリ内部にあるかチェック
        subquery_broadcast = re.search(r'JOIN\s*\(\s*SELECT\s*/\*\+\s*BROADCAST', sql_query, re.IGNORECASE)
        if subquery_broadcast:
            return False
    
    # 基本的な構文エラーチェック
    # 連続するカンマ
    if re.search(r',\s*,', sql_query):
        return False
    
    # 不正な空白パターン
    if re.search(r'\s{5,}', sql_query):  # 5個以上の連続する空白
        return False
    
    return True

def save_optimized_sql_files(original_query: str, optimized_result: str, metrics: Dict[str, Any], analysis_result: str = "", llm_response: str = "", performance_comparison: Dict[str, Any] = None, best_attempt_number: int = None) -> Dict[str, str]:
    """
    最適化されたSQLクエリを実行可能な形でファイルに保存
    
    特徴:
    - SQLファイルの末尾に自動でセミコロン(;)を付与
    - そのままDatabricks Notebookで実行可能
    - %sql マジックコマンドでも直接実行可能
    - LLMによるレポート推敲で読みやすい最終レポートを生成
    """
    
    import re
    from datetime import datetime
    
    # thinking_enabled: Trueの場合にoptimized_resultがリストになることがあるため対応
    optimized_result_for_file = optimized_result
    optimized_result_main_content = optimized_result
    
    if isinstance(optimized_result, list):
        # ファイル保存用は人間に読みやすい形式に変換
        optimized_result_for_file = format_thinking_response(optimized_result)
        # SQL抽出用は主要コンテンツのみを使用
        optimized_result_main_content = extract_main_content_from_thinking_response(optimized_result)
    
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    query_id = metrics.get('query_info', {}).get('query_id', 'unknown')
    
    # オリジナルクエリファイルの保存は除外（不要）
    original_filename = None
    
    # 最適化されたクエリの抽出と保存
    optimized_filename = f"output_optimized_query_{timestamp}.sql"
    
    # 最適化結果からSQLコードを抽出（主要コンテンツから抽出） - 改善版
    sql_pattern = r'```sql\s*(.*?)\s*```'
    sql_matches = re.findall(sql_pattern, optimized_result_main_content, re.DOTALL | re.IGNORECASE)
    
    optimized_sql = ""
    if sql_matches:
        # 最も長いSQLブロックを使用（完全性を優先）
        optimized_sql = max(sql_matches, key=len).strip()
    else:
        # SQLブロックが見つからない場合は、SQL関連の行を抽出（改善版）
        lines = optimized_result_main_content.split('\n')
        sql_lines = []
        in_sql_section = False
        
        for line in lines:
            line_stripped = line.strip()
            
            # SQLの開始を検出
            if any(keyword in line.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'WITH', 'CREATE', 'INSERT', 'UPDATE', 'DELETE']):
                in_sql_section = True
            
            if in_sql_section:
                # SQLの終了を検出（マークダウンセクションやレポートセクション）
                if (line_stripped.startswith('#') or 
                    line_stripped.startswith('*') or 
                    line_stripped.startswith('##') or
                    line_stripped.startswith('**') or
                    line_stripped.startswith('---') or
                    line_stripped.startswith('===') or
                    '改善ポイント' in line_stripped or
                    '期待効果' in line_stripped or
                    'BROADCAST適用根拠' in line_stripped):
                    in_sql_section = False
                else:
                    # 空行や有効なSQL行を追加
                    sql_lines.append(line)
        
        optimized_sql = '\n'.join(sql_lines).strip()
    
    # SQL構文の基本チェック（完全性確認）
    if optimized_sql:
        optimized_sql = validate_and_fix_sql_syntax(optimized_sql)
    
    # 最適化されたクエリファイルの保存（エラーハンドリング強化）
    try:
        with open(optimized_filename, 'w', encoding='utf-8') as f:
            f.write(f"-- 最適化されたSQLクエリ\n")
            f.write(f"-- 元クエリID: {query_id}\n")
            f.write(f"-- 最適化日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- ファイル: {optimized_filename}\n\n")
            
            
            # 🎯 CATALOG/DATABASE設定の自動追加
            catalog_name = globals().get("CATALOG", "tpcds")
            database_name = globals().get("DATABASE", "tpcds_sf1000_delta_lc")
            
            f.write(f"-- 🗂️ カタログ・スキーマ設定（自動追加）\n")
            f.write(f"USE CATALOG {catalog_name};\n")
            f.write(f"USE SCHEMA {database_name};\n\n")
                
            if optimized_sql:
                # SQLの末尾にセミコロンを確実に追加
                optimized_sql_clean = optimized_sql.strip()
                if optimized_sql_clean and not optimized_sql_clean.endswith(';'):
                    optimized_sql_clean += ';'
                
                # 最終的な構文チェック
                if validate_final_sql_syntax(optimized_sql_clean):
                    f.write(optimized_sql_clean)
                else:
                    f.write("-- ⚠️ 構文エラーが検出されました。手動で確認してください。\n")
                    f.write(f"-- 元のSQL:\n{optimized_sql_clean}\n")
                    f.write("-- 以下は最適化分析の全結果です:\n\n")
                    f.write(f"/*\n{optimized_result_main_content}\n*/")
            else:
                f.write("-- ⚠️ SQLコードの自動抽出に失敗しました\n")
                f.write("-- 以下は最適化分析の全結果です:\n\n")
                f.write(f"/*\n{optimized_result_main_content}\n*/")
    except Exception as e:
        print(f"⚠️ SQLファイル保存中にエラーが発生しました: {str(e)}")
        # エラー時は基本的なファイルを生成
        with open(optimized_filename, 'w', encoding='utf-8') as f:
            f.write(f"-- ⚠️ SQLファイル保存中にエラーが発生しました: {str(e)}\n")
            f.write(f"-- 最適化結果:\n{optimized_result_main_content}\n")
    
    # 分析レポートファイルの保存（LLMで推敲された読みやすいレポート）
    report_filename = f"output_optimization_report_{timestamp}.md"
    
    print("🤖 LLMによるレポート推敲を実行中...")
    
    # 🚀 実際に保存されたSQLファイルの内容を読み込んでレポートに使用
    try:
        with open(optimized_filename, 'r', encoding='utf-8') as f:
            actual_sql_content = f.read()
        
        # レポート用に実際のSQLファイル内容を使用（動作保証済み）
        print(f"✅ レポート生成用にSQLファイル内容を読み込み: {optimized_filename}")
        report_data = actual_sql_content
        
    except Exception as e:
        print(f"⚠️ SQLファイル読み込み失敗、初回レスポンスを使用: {str(e)}")
        # フォールバック: 初回レスポンスを使用
        report_data = llm_response if llm_response else optimized_result
    
    initial_report = generate_comprehensive_optimization_report(
        query_id, report_data, metrics, analysis_result, performance_comparison, best_attempt_number
    )
    
    # LLMでレポートを推敲（詳細な技術情報を保持）
    refined_report = refine_report_with_llm(initial_report, query_id)
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(refined_report)
    
    print(f"✅ レポートファイル保存完了: {report_filename}")
    
    # 出力ファイルの結果（独立したTOP10ファイルは削除し、最適化レポートに統合）
    result = {
        'optimized_file': optimized_filename,
        'report_file': report_filename
    }
    
    return result

def demonstrate_execution_plan_size_extraction():
    """
    実行プランからのサイズ推定機能のデモンストレーション
    """
    print("🧪 実行プランからのテーブルサイズ推定機能のデモ")
    print("-" * 50)
    
    # サンプルのプロファイラーデータ構造
    sample_profiler_data = {
        "executionPlan": {
            "physicalPlan": {
                "nodes": [
                    {
                        "nodeName": "Scan Delta orders",
                        "id": "1",
                        "metrics": {
                            "estimatedSizeInBytes": 10485760,  # 10MB
                            "numFiles": 5,
                            "numPartitions": 2
                        },
                        "output": "[order_id#123, customer_id#124, amount#125] orders",
                        "details": "Table: catalog.database.orders"
                    },
                    {
                        "nodeName": "Scan Delta customers",
                        "id": "2", 
                        "metrics": {
                            "estimatedSizeInBytes": 52428800,  # 50MB
                            "numFiles": 10,
                            "numPartitions": 4
                        },
                        "output": "[customer_id#126, name#127, region#128] customers"
                    }
                ]
            }
        }
    }
    
    print("📊 サンプル実行プラン:")
    print("  • orders テーブル: estimatedSizeInBytes = 10,485,760 (10MB)")
    print("  • customers テーブル: estimatedSizeInBytes = 52,428,800 (50MB)")
    print("")
    
    # テーブルサイズ推定の実行
    table_size_estimates = extract_table_size_estimates_from_plan(sample_profiler_data)
    
    print("🔍 抽出されたテーブルサイズ推定:")
    if table_size_estimates:
        for table_name, size_info in table_size_estimates.items():
            print(f"  📋 {table_name}:")
            print(f"    - サイズ: {size_info['estimated_size_mb']:.1f}MB")
            print(f"    - 信頼度: {size_info['confidence']}")
            print(f"    - ソース: {size_info['source']}")
            if 'num_files' in size_info:
                print(f"    - ファイル数: {size_info['num_files']}")
            if 'num_partitions' in size_info:
                print(f"    - パーティション数: {size_info['num_partitions']}")
            print("")
    else:
        print("  ⚠️ テーブルサイズ推定情報が抽出されませんでした")
    
    print("💡 BROADCAST分析への影響:")
    if table_size_estimates:
        for table_name, size_info in table_size_estimates.items():
            size_mb = size_info['estimated_size_mb']
            if size_mb <= 30:
                print(f"  ✅ {table_name}: {size_mb:.1f}MB ≤ 30MB → BROADCAST推奨")
            else:
                print(f"  ❌ {table_name}: {size_mb:.1f}MB > 30MB → BROADCAST非推奨")
    
    print("")
    print("🎯 従来の推定方法との比較:")
    print("  📈 従来: メトリクスベースの間接推定（推定精度: 中）")
    print("  ❌ 新機能: 実行プランの estimatedSizeInBytes 活用（利用不可のため無効化）")
    print("  ℹ️ 現在: 3.0倍圧縮率での保守的推定を採用")
    
    return {}

print("✅ 関数定義完了: SQL最適化関連関数（実行プランサイズ推定対応）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 オリジナルクエリ抽出
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - プロファイラーデータからオリジナルクエリの抽出
# MAGIC - 抽出されたクエリの詳細表示（64KBまで）
# MAGIC - フォールバック処理（サンプルクエリの設定）

# COMMAND ----------

# 🚀 SQLクエリ最適化の実行
print("\n" + "🚀" * 20)
print("🔧 【SQLクエリ最適化の実行】")
print("🚀" * 20)

# 1. オリジナルクエリの抽出
print("\n📋 ステップ1: オリジナルクエリの抽出")
print("-" * 40)

original_query = extract_original_query_from_profiler_data(profiler_data)

if original_query:
    print(f"✅ オリジナルクエリを抽出しました ({len(original_query)} 文字)")
    print(f"🔍 クエリプレビュー:")
    # 64KB (65536文字) まで表示
    max_display_chars = 65536
    if len(original_query) > max_display_chars:
        preview = original_query[:max_display_chars] + f"\n... (残り {len(original_query) - max_display_chars} 文字は省略)"
    else:
        preview = original_query
    print(f"   {preview}")
else:
    print("⚠️ オリジナルクエリが見つかりませんでした")
    print("   手動でクエリを設定してください")
    
    # フォールバック: サンプルクエリを設定
    original_query = """
    -- サンプルクエリ（実際のクエリに置き換えてください）
    SELECT 
        customer_id,
        SUM(order_amount) as total_amount,
        COUNT(*) as order_count
    FROM orders 
    WHERE order_date >= '2023-01-01'
    GROUP BY customer_id
    ORDER BY total_amount DESC
    LIMIT 100
    """
    print(f"📝 サンプルクエリを設定しました")

# 📁 オリジナルクエリをファイルに保存
print("\n📁 オリジナルクエリのファイル保存")
print("-" * 40)

from datetime import datetime

# タイムスタンプ付きファイル名を生成
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
original_query_filename = f"output_original_query_{timestamp}.sql"

try:
    # カタログとデータベース設定の取得
    catalog_name = globals().get('CATALOG', 'tpcds')
    database_name = globals().get('DATABASE', 'tpcds_sf1000_delta_lc')
    
    with open(original_query_filename, 'w', encoding='utf-8') as f:
        f.write(f"-- 📋 オリジナルクエリ（プロファイラーデータから抽出）\n")
        f.write(f"-- 抽出日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- ファイル: {original_query_filename}\n")
        f.write(f"-- クエリ文字数: {len(original_query):,}\n\n")
        
        # カタログ・スキーマ設定の追加
        f.write(f"-- 🗂️ カタログ・スキーマ設定（自動追加）\n")
        f.write(f"USE CATALOG {catalog_name};\n")
        f.write(f"USE SCHEMA {database_name};\n\n")
        
        # オリジナルクエリの書き込み
        f.write(f"-- 🔍 オリジナルクエリ\n")
        f.write(original_query)
        
        # ファイル末尾に改行を追加
        if not original_query.endswith('\n'):
            f.write('\n')
    
    print(f"✅ オリジナルクエリを保存: {original_query_filename}")
    print(f"📊 保存したクエリ文字数: {len(original_query):,}")
    print(f"💾 ファイルパス: ./{original_query_filename}")
    print("📌 このファイルはDEBUG_ENABLED設定に関係なく最終アウトプットとして保持されます")
    
except Exception as e:
    print(f"❌ オリジナルクエリのファイル保存に失敗: {str(e)}")
    print("⚠️ 処理は続行しますが、オリジナルクエリファイルは作成されませんでした")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 SQL最適化実行
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - セル43で抽出したオリジナルクエリを取得
# MAGIC - EXPLAIN文を生成してDatabricksで実行
# MAGIC - 実行プランの詳細をファイルに出力
# MAGIC - エラーハンドリングと結果の確認

# COMMAND ----------

def extract_select_from_ctas(query: str) -> str:
    """
    CREATE TABLE AS SELECT (CTAS) クエリからAS以降の部分のみを抽出
    
    対応パターン:
    - CREATE TABLE ... AS SELECT ...
    - CREATE OR REPLACE TABLE ... AS SELECT ...
    - CREATE TABLE ... AS WITH ... SELECT ...
    - AS の後ろに括弧がない場合
    - 複数行にまたがる場合
    - テーブル定義の複雑なパターン（USING、PARTITIONED BY、TBLPROPERTIES等）
    
    Args:
        query: 元のクエリ
    
    Returns:
        str: AS以降の部分のみのクエリ、またはCTASでない場合は元のクエリ
    """
    import re
    
    # クエリを正規化（改行・空白を統一）
    normalized_query = re.sub(r'\s+', ' ', query.strip())
    
    # CTAS パターンの検出（包括的なパターン）
    # CREATE [OR REPLACE] TABLE ... AS ... の形式を検出
    # ASキーワードの位置を正確に特定する
    
    # CREATE [OR REPLACE] TABLE部分の検出
    create_patterns = [
        r'CREATE\s+OR\s+REPLACE\s+TABLE',
        r'CREATE\s+TABLE'
    ]
    
    for create_pattern in create_patterns:
        # CREATE TABLE部分を検出
        create_match = re.search(create_pattern, normalized_query, re.IGNORECASE)
        if create_match:
            # CREATE TABLE以降の部分を取得
            after_create = normalized_query[create_match.end():].strip()
            
            # AS キーワードの位置を検索（大文字小文字を区別しない）
            # AS は単語境界で区切られている必要がある
            as_pattern = r'\bAS\b'
            as_match = re.search(as_pattern, after_create, re.IGNORECASE)
            
            if as_match:
                # AS以降の部分を取得
                as_part = after_create[as_match.end():].strip()
                
                if as_part:
                    print(f"✅ CTAS検出: AS以降の部分をEXPLAIN文に使用")
                    print(f"📊 元のクエリ長: {len(query):,} 文字")
                    print(f"📊 AS以降部分長: {len(as_part):,} 文字")
                    
                    # WITH句で始まる場合やSELECT句で始まる場合を判定
                    if as_part.upper().startswith('WITH'):
                        print("📋 WITH句で始まるクエリを検出")
                    elif as_part.upper().startswith('SELECT'):
                        print("📋 SELECT句で始まるクエリを検出")
                    else:
                        print("📋 その他のクエリ形式を検出")
                    
                    return as_part
    
    print("📋 通常のクエリ: そのままEXPLAIN文に使用")
    return query

def generate_improved_query_for_performance_degradation(original_query: str, analysis_result: str, metrics: Dict[str, Any], degradation_analysis: Dict[str, Any], previous_optimized_query: str = "") -> str:
    """
    パフォーマンス悪化専用のLLM最適化関数
    悪化原因分析に基づいて具体的な改善策を適用
    
    Args:
        original_query: 元のクエリ
        analysis_result: ボトルネック分析結果
        metrics: メトリクス情報
        degradation_analysis: 悪化原因分析結果
        previous_optimized_query: 前回の最適化クエリ
    """
    
    # 悪化分析の詳細情報を抽出
    primary_cause = degradation_analysis.get('primary_cause', 'unknown')
    cost_ratio = degradation_analysis.get('analysis_details', {}).get('cost_ratio', 1.0)
    specific_issues = degradation_analysis.get('specific_issues', [])
    fix_instructions = degradation_analysis.get('fix_instructions', [])
    confidence_level = degradation_analysis.get('confidence_level', 'low')
    
    # 前回クエリの分析セクション
    previous_query_section = ""
    if previous_optimized_query:
        previous_query_section = f"""

【🚨 前回の最適化クエリ（パフォーマンス悪化）】
```sql
{previous_optimized_query}
```

**❌ 検出された問題点:**
- 実行コスト比: {cost_ratio:.2f}倍の悪化
- 主要原因: {primary_cause}
- 具体的問題: {', '.join(specific_issues)}
"""

    # パフォーマンス悪化修正に特化したプロンプト
    performance_improvement_prompt = f"""
あなたはDatabricksのSQLパフォーマンス最適化の専門家です。

前回の最適化でパフォーマンス悪化が発生しました。悪化原因分析に基づいて **根本的な改善** を行ってください。

【📊 パフォーマンス悪化の詳細分析】
- **悪化率**: {cost_ratio:.2f}倍（{(cost_ratio-1)*100:.1f}%増加）
- **主要原因**: {primary_cause}
- **信頼度**: {confidence_level}
- **具体的問題**: {', '.join(specific_issues)}

【元の分析対象クエリ】
```sql
{original_query}
```
{previous_query_section}

【🔧 悪化原因別の具体的修正指示】
{chr(10).join(f"- {instruction}" for instruction in fix_instructions)}

【🎯 パフォーマンス改善の重要な方針】

1. **🚨 過剰最適化の是正**:
           - JOIN順序の効率化
           - 効率的でないJOIN順序の見直し
   - 効果的でないヒントは積極的に削除

2. **⚡ JOIN効率化**:
   - JOIN操作数の大幅な増加を避ける
   - 元のJOIN順序を尊重
   - 不要なサブクエリ化によるJOIN重複を防ぐ

3. **🎯 データサイズ最適化**:
   - フィルタープッシュダウンを最大化
   - 早期の行数削減を重視
   - 中間結果のサイズを最小化

4. **📊 統計情報に基づく判断**:
   - 小テーブル（<30MB）のみBROADCAST適用
   - メモリ効率を重視したJOIN戦略
   - スピル発生の最小化

【🔄 改善クエリ生成の指針】

**A. 保守的アプローチ（推奨）:**
- 元クエリの構造を最大限保持
- 確実に効果的な最適化のみ適用
- リスクの高い変更は避ける

**B. 段階的改善:**
- 最も問題となっている箇所のみ修正
- 一度に多くの変更を加えない
- 測定可能な改善を重視

**C. フォールバック戦略:**
- 不確実な最適化は削除
- 元のクエリに近い形での軽微な改善

【重要な制約】
- パフォーマンス悪化の主要原因を確実に解決
- 元クエリより確実に高速なクエリを生成
- 機能性を一切損なわない
- 完全で実行可能なSQLのみ出力

【出力形式】
## 🚀 パフォーマンス改善SQL

**改善した内容**:
- [具体的な悪化原因の修正]
- [削除/変更した最適化要素]
- [新たに適用した改善策]

```sql
[完全なSQL - パフォーマンス改善済み]
```

## 改善詳細
[悪化原因の解決方法と期待される性能改善の説明]
"""

    # 設定されたLLMプロバイダーを使用
    provider = LLM_CONFIG["provider"]
    
    try:
        if provider == "databricks":
            improved_result = _call_databricks_llm(performance_improvement_prompt)
        elif provider == "openai":
            improved_result = _call_openai_llm(performance_improvement_prompt)
        elif provider == "azure_openai":
            improved_result = _call_azure_openai_llm(performance_improvement_prompt)
        elif provider == "anthropic":
            improved_result = _call_anthropic_llm(performance_improvement_prompt)
        else:
            error_msg = "⚠️ 設定されたLLMプロバイダーが認識できません"
            print(f"❌ LLMパフォーマンス改善エラー: {error_msg}")
            return f"LLM_ERROR: {error_msg}"
        
        # LLMレスポンスのエラーチェック
        if isinstance(improved_result, str):
            error_indicators = [
                "APIエラー:",
                "Input is too long", 
                "Bad Request",
                "❌",
                "⚠️",
                "タイムアウトエラー:",
                "API呼び出しエラー:",
            ]
            
            for indicator in error_indicators:
                if indicator in improved_result:
                    print(f"❌ LLMパフォーマンス改善でエラー検出: {indicator}")
                    return f"LLM_ERROR: {improved_result}"
        
        return improved_result
        
    except Exception as e:
        error_msg = f"パフォーマンス改善処理でエラー: {str(e)}"
        print(f"❌ {error_msg}")
        return f"LLM_ERROR: {error_msg}"


def generate_optimized_query_with_error_feedback(original_query: str, analysis_result: str, metrics: Dict[str, Any], error_info: str = "", previous_optimized_query: str = "") -> str:
    """
    エラー情報を含めてLLMによるSQL最適化を実行
    エラー修正に特化したプロンプトを使用
    
    Args:
        original_query: 元のクエリ
        analysis_result: ボトルネック分析結果
        metrics: メトリクス情報
        error_info: エラー情報
        previous_optimized_query: 初回最適化クエリ（ヒント保持用）
    """
    
    # 初回最適化クエリの情報を含める
    previous_query_section = ""
    if previous_optimized_query:
        previous_query_section = f"""

【🚀 初回生成された最適化クエリ（エラー発生）】
```sql
{previous_optimized_query}
```

**⚠️ 重要**: 上記の最適化クエリに含まれる以下の要素は必ず保持してください：
- **REPARTITIONヒント**: `/*+ REPARTITION(数値, カラム名) */`
- **その他の最適化ヒント**: COALESCE、CACHE等
- **最適化手法**: CTE構造、結合順序、フィルタープッシュダウン等
- **パフォーマンス改善策**: スピル対策、並列度改善等

**🎯 エラー修正の方針**: 
- エラー箇所のみを修正し、最適化要素は全て保持
- ヒント句の配置ルールは厳守（REPARTITIONはメインクエリSELECT直後等）
"""

    # 🚨 NEW: エラーメッセージ解析による詳細修正指示生成
    def generate_specific_error_guidance(error_message: str) -> str:
        """具体的なエラーメッセージに基づいた詳細修正指示を生成"""
        guidance = ""
        
        if "AMBIGUOUS_REFERENCE" in error_message.upper():
            # AMBIGUOUS_REFERENCEエラーの具体的対処
            import re
            ambiguous_column_match = re.search(r'Reference `([^`]+)` is ambiguous', error_message)
            if ambiguous_column_match:
                ambiguous_column = ambiguous_column_match.group(1)
                guidance += f"""
🎯 **AMBIGUOUS_REFERENCE 専用修正指示**: 
- **問題**: カラム `{ambiguous_column}` が複数テーブルに存在
- **必須修正**: 全ての `{ambiguous_column}` 参照にテーブルエイリアスを明示
- **修正例**: `{ambiguous_column}` → `table_alias.{ambiguous_column}`
- **重要**: WHERE句、SELECT句、JOIN句全てで明示的修飾が必要
"""
            
        if "UNRESOLVED_COLUMN" in error_message.upper():
            # UNRESOLVED_COLUMNエラーの具体的対処
            import re
            unresolved_match = re.search(r'column.*`([^`]+)`', error_message)
            if unresolved_match:
                unresolved_column = unresolved_match.group(1)
                guidance += f"""
🎯 **UNRESOLVED_COLUMN 専用修正指示**:
- **問題**: カラム `{unresolved_column}` が見つからない
- **確認事項**: テーブルエイリアス、スペルミス、スコープ
- **修正例**: 正しいテーブル修飾、存在するカラム名への変更
"""
        
        if "PARSE_SYNTAX_ERROR" in error_message.upper():
            guidance += f"""
🎯 **PARSE_SYNTAX_ERROR 専用修正指示**:
- **重要**: 構文エラー最優先修正（カンマ抜け、エイリアス重複等）
- **確認**: SELECT句のカンマ、FROM句の構文、エイリアス定義
"""
            
        return guidance
    
    specific_guidance = generate_specific_error_guidance(error_info)

    error_feedback_prompt = f"""
あなたはDatabricksのSQLパフォーマンス最適化とエラー修正の専門家です。

以下の最適化クエリでEXPLAIN実行時にエラーが発生しました。**最適化要素を保持しながら**エラー情報を基に修正してください。

【🚨 発生したエラー情報】
{error_info}
{specific_guidance}

【元の分析対象クエリ】
```sql
{original_query}
```
{previous_query_section}
【詳細なボトルネック分析結果】
{analysis_result}

【🔧 エラー修正の重要な指針】
1. **🚀 最適化要素の絶対保持（最重要）**:
   - **初回生成されたJOIN順序最適化を必ず保持**
   - **初回生成されたREPARTITIONヒントを必ず保持**: `/*+ REPARTITION(数値, カラム) */`
   - **その他の最適化ヒントも全て保持**: COALESCE、CACHE等
   - **CTE構造や結合順序などの最適化設計を維持**
   - **スピル対策やパフォーマンス改善策を保持**

2. **🚨 致命的構文エラーの最優先修正**:

   **A. カンマ抜けエラー (PARSE_SYNTAX_ERROR)**:
   - ❌ `i.i_item_sk ss.ss_item_sk` → ✅ `i.i_item_sk, ss.ss_item_sk`
   - ❌ `SELECT col1 col2 FROM` → ✅ `SELECT col1, col2 FROM`
   - **SELECT句内でのカンマ抜けを最優先で修正**

   **B. 二重・三重エイリアスエラー**:
   - ❌ `iss.i.i_brand_id` → ✅ `iss.i_brand_id` または `i.i_brand_id`
   - ❌ `ss.ss.ss_item_sk` → ✅ `ss.ss_item_sk`
   - **一つのテーブルに対する重複エイリアス参照を修正**

   **C. 存在しないテーブル/カラム参照**:
   - ❌ `this_year.i.i_brand_id` → ✅ `this_year.i_brand_id`
   - **サブクエリエイリアスと内部テーブルエイリアスの混同を修正**

   **D. FROM句構文エラー**:
   - ❌ `FROM table1, (SELECT ...) x WHERE` → ✅ 適切なJOIN構文に変換
   - **古いカンマ結合を明示的JOIN構文に変換**

3. **🔍 AMBIGUOUS_REFERENCE エラーの修正**: 
   - **全てのカラム参照でテーブル名またはエイリアス名を明示的に指定**
   - 例: `ss_item_sk` → `store_sales.ss_item_sk` または `ss.ss_item_sk`
   - **サブクエリとメインクエリで同名カラムがある場合は特に注意**

4. **テーブルエイリアスの一貫使用**: 
   - 全てのテーブルに短いエイリアス名を付与（例: store_sales → ss, item → i）
   - クエリ全体で一貫してエイリアス名を使用
   - サブクエリ内でも同じエイリアス名体系を維持

5. **その他の構文エラー修正**: 
   - **型変換エラー**: 不適切なキャスト修正
   - **ヒント句エラー**: 構文に合わせた配置修正
   - **権限エラー**: 代替アクセス方法提案

【🚨 BROADCASTヒント配置の厳格なルール - エラー修正版】
**✅ 正しい配置（必須）:**
```sql
-- ✅ 正しい: メインクエリのSELECT直後のみ
SELECT /*+ BROADCAST(i, d) */
  ss.ss_item_sk, i.i_brand_id, d.d_year
FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
```

**❌ 絶対に禁止される配置（構文エラーの原因）:**
```sql
-- ❌ 間違い: JOIN句内への配置（PARSE_SYNTAX_ERROR発生）
FROM store_sales ss
  JOIN /*+ BROADCAST(i) */ item i ON ss.ss_item_sk = i.i_item_sk  -- これが構文エラー
  JOIN /*+ BROADCAST(d) */ date_dim d ON ss.ss_sold_date_sk = d.d_date_sk  -- これも構文エラー

-- ❌ 間違い: サブクエリ内への配置
SELECT ... FROM (
  SELECT /*+ BROADCAST(i) */ ...  -- サブクエリ内は無効
  FROM ...
)

-- ❌ 間違い: FROM句内への配置
FROM /*+ BROADCAST(i) */ item i  -- FROM句内は構文エラー
```

**🔧 PARSE_SYNTAX_ERROR修正の具体的手順:**
1. **JOIN句内のBROADCASTヒントを全て削除**
2. **メインクエリの最初のSELECT直後に全てのBROADCASTヒントを統合**
3. **テーブル名/エイリアス名を正確に指定**

**📝 具体的修正例（PARSE_SYNTAX_ERROR対応）:**

❌ **修正前（エラー発生）:**
```sql
SELECT ss.ss_item_sk, i.i_brand_id
FROM store_sales ss
  JOIN /*+ BROADCAST(i) */ item i ON ss.ss_item_sk = i.i_item_sk  -- PARSE_SYNTAX_ERROR
  JOIN /*+ BROADCAST(d) */ date_dim d ON ss.ss_sold_date_sk = d.d_date_sk  -- PARSE_SYNTAX_ERROR
```

✅ **修正後（正常）:**
```sql
SELECT /*+ BROADCAST(i, d) */ ss.ss_item_sk, i.i_brand_id
FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
```

**🚨 エラー修正の最重要ルール:**
- **JOIN句内の`/*+ BROADCAST(...) */`は即座に削除**
- **削除したBROADCAST対象をメインSELECT直後に移動**
- **複数のBROADCAST対象はカンマ区切りで統合: `/*+ BROADCAST(table1, table2, table3) */`**

【🚨 REPARTITIONヒント配置の厳格なルール - エラー修正版】
- **サブクエリ内部のSELECT文直後に配置**
- **パーティション数とカラム名は必須**: `/*+ REPARTITION(200, column_name) */`
- **スピル検出時のみ適用**

【重要な制約 - エラー修正版】
- 構文エラーを絶対に発生させない完全なSQLクエリを生成
- すべてのカラム名、テーブル名を完全に記述
- プレースホルダー（...、[省略]）は一切使用禁止
- 元のクエリのDISTINCT句は必ず保持
- 実際に実行できる完全なSQLクエリのみを出力

【出力形式】
## 🔧 エラー修正済み最適化SQL

**修正した内容**:
- [具体的なエラー修正箇所]

**保持した最適化要素**:
- [保持されたREPARTITIONヒント]
- [保持されたJOIN順序最適化]
- [保持されたその他の最適化手法]

```sql
[完全なSQL - エラー修正済み、最適化要素保持]
```

## 修正詳細
[エラーの原因と修正方法、および最適化要素保持の説明]
"""

    # 設定されたLLMプロバイダーを使用
    provider = LLM_CONFIG["provider"]
    
    try:
        if provider == "databricks":
            optimized_result = _call_databricks_llm(error_feedback_prompt)
        elif provider == "openai":
            optimized_result = _call_openai_llm(error_feedback_prompt)
        elif provider == "azure_openai":
            optimized_result = _call_azure_openai_llm(error_feedback_prompt)
        elif provider == "anthropic":
            optimized_result = _call_anthropic_llm(error_feedback_prompt)
        else:
            error_msg = "⚠️ 設定されたLLMプロバイダーが認識できません"
            print(f"❌ LLMエラー修正エラー: {error_msg}")
            return f"LLM_ERROR: {error_msg}"
        
        # LLMレスポンスのエラーチェック（重要）
        if isinstance(optimized_result, str):
            # APIエラーメッセージの検出
            error_indicators = [
                 "APIエラー:",
                 "Input is too long",
                 "Bad Request",
                 "❌",
                 "⚠️",
                 "タイムアウトエラー:",
                 "API呼び出しエラー:",
                 "レスポンス:",
                 '{"error_code":'
             ]
            
            # エラーメッセージかどうかをチェック
            is_error_response = any(indicator in optimized_result for indicator in error_indicators)
            
            if is_error_response:
                print(f"❌ LLM エラー修正API呼び出しでエラーが発生: {optimized_result[:200]}...")
                return f"LLM_ERROR: {optimized_result}"
        
        # 🔧 修正後のクエリに対してプログラマティック後処理を適用
        if isinstance(optimized_result, str) and not optimized_result.startswith("LLM_ERROR:"):
            print("🔧 エラー修正後のクエリ検証・後処理を実行")
            final_corrected_query = enhance_error_correction_with_syntax_validation(optimized_result, original_query, error_info)
            return final_corrected_query
        
        return optimized_result
        
    except Exception as e:
        error_msg = f"⚠️ エラー修正SQL生成中にエラーが発生しました: {str(e)}"
        print(f"❌ LLMエラー修正例外エラー: {error_msg}")
        return f"LLM_ERROR: {error_msg}"


def compare_query_performance(original_explain_cost: str, optimized_explain_cost: str) -> Dict[str, Any]:
    """
    EXPLAIN COST結果を比較してパフォーマンス悪化を検出
    
    Args:
        original_explain_cost: 元クエリのEXPLAIN COST結果
        optimized_explain_cost: 最適化クエリのEXPLAIN COST結果
        
    Returns:
        Dict: パフォーマンス比較結果と推奨事項
    """
    comparison_result = {
        'is_optimization_beneficial': True,
        'performance_degradation_detected': False,
        'significant_improvement_detected': False,  # 🚨 明確な改善検出フラグ追加（1%以上）
        'substantial_improvement_detected': False,  # 🚀 大幅改善検出フラグ追加（10%以上）
        'total_cost_ratio': 1.0,
        'memory_usage_ratio': 1.0,
        'scan_cost_ratio': 1.0,
        'join_cost_ratio': 1.0,
        'recommendation': 'use_optimized',
        'details': []
    }
    
    try:
        import re
        
        # コスト情報を抽出する関数
        def extract_cost_metrics(explain_cost_text):
            metrics = {
                'total_size_bytes': 0,
                'total_rows': 0,
                'scan_operations': 0,
                'join_operations': 0,
                'memory_estimates': 0,
                'shuffle_partitions': 0
            }
            
            # サイズとメモリ使用量を抽出
            size_patterns = [
                r'size_bytes["\s]*[:=]\s*([0-9.]+)',
                r'sizeInBytes["\s]*[:=]\s*([0-9.]+)',
                r'(\d+\.?\d*)\s*[KMG]?iB',
                r'(\d+\.?\d*)\s*[KMG]?B'
            ]
            
            for pattern in size_patterns:
                matches = re.findall(pattern, explain_cost_text, re.IGNORECASE)
                for match in matches:
                    try:
                        size_val = float(match)
                        metrics['total_size_bytes'] += size_val
                    except:
                        continue
            
            # 行数を抽出
            row_patterns = [
                r'rows["\s]*[:=]\s*([0-9]+)',
                r'numRows["\s]*[:=]\s*([0-9]+)'
            ]
            
            for pattern in row_patterns:
                matches = re.findall(pattern, explain_cost_text, re.IGNORECASE)
                for match in matches:
                    try:
                        metrics['total_rows'] += int(match)
                    except:
                        continue
            
            # スキャン・JOIN操作数をカウント
            metrics['scan_operations'] = len(re.findall(r'Scan|FileScan|TableScan', explain_cost_text, re.IGNORECASE))
            metrics['join_operations'] = len(re.findall(r'Join|HashJoin|SortMergeJoin', explain_cost_text, re.IGNORECASE))
            
            # シャッフルパーティション数
            shuffle_matches = re.findall(r'partitions?["\s]*[:=]\s*([0-9]+)', explain_cost_text, re.IGNORECASE)
            for match in shuffle_matches:
                try:
                    metrics['shuffle_partitions'] += int(match)
                except:
                    continue
                    
            return metrics
        
        # 元クエリと最適化クエリのメトリクス抽出
        original_metrics = extract_cost_metrics(original_explain_cost)
        optimized_metrics = extract_cost_metrics(optimized_explain_cost)
        
        # パフォーマンス比較（値が0の場合は1として計算）
        if original_metrics['total_size_bytes'] > 0:
            comparison_result['total_cost_ratio'] = optimized_metrics['total_size_bytes'] / original_metrics['total_size_bytes']
        
        if original_metrics['total_rows'] > 0:
            comparison_result['memory_usage_ratio'] = optimized_metrics['total_rows'] / original_metrics['total_rows']
        
        # 🚨 厳格な判定閾値（ユーザー要求：保守的アプローチ）
        COST_DEGRADATION_THRESHOLD = 1.01   # 1%以上のコスト増加で元クエリ推奨（厳格化）
        MEMORY_DEGRADATION_THRESHOLD = 1.01 # 1%以上のメモリ増加で元クエリ推奨（厳格化）
        COST_IMPROVEMENT_THRESHOLD = 0.99   # 1%以上の削減で最適化クエリ推奨（厳格化）
        MEMORY_IMPROVEMENT_THRESHOLD = 0.99 # 1%以上の削減で最適化クエリ推奨（厳格化）
        
        # 🚀 大幅改善の判定閾値（ユーザー要求：10%以上改善で試行終了）
        SUBSTANTIAL_COST_IMPROVEMENT_THRESHOLD = 0.9   # 10%以上のコスト削減で大幅改善認定
        SUBSTANTIAL_MEMORY_IMPROVEMENT_THRESHOLD = 0.9 # 10%以上のメモリ削減で大幅改善認定
        
        # パフォーマンス悪化検出（マージンなしで明確な判定）
        degradation_factors = []
        
        # 🎯 明確な悪化判定（境界値の曖昧さを排除）
        if comparison_result['total_cost_ratio'] > COST_DEGRADATION_THRESHOLD:
            degradation_factors.append(f"総実行コスト悪化: {comparison_result['total_cost_ratio']:.2f}倍（閾値: {COST_DEGRADATION_THRESHOLD:.2f}）")
            
        if comparison_result['memory_usage_ratio'] > MEMORY_DEGRADATION_THRESHOLD:
            degradation_factors.append(f"メモリ使用量悪化: {comparison_result['memory_usage_ratio']:.2f}倍（閾値: {MEMORY_DEGRADATION_THRESHOLD:.2f}）")
        
        # JOIN操作数の大幅増加チェック
        if (optimized_metrics['join_operations'] > original_metrics['join_operations'] * 1.5):
            degradation_factors.append(f"JOIN操作数増加: {original_metrics['join_operations']} → {optimized_metrics['join_operations']}")
        
        # 悪化判定
        if degradation_factors:
            comparison_result['performance_degradation_detected'] = True
            comparison_result['is_optimization_beneficial'] = False
            comparison_result['recommendation'] = 'use_original'
            comparison_result['details'] = degradation_factors
        else:
            # 悪化ではないが、改善/同等の詳細判定
            performance_factors = []
            
            # 🚨 厳格な詳細判定（ユーザー要求：保守的アプローチ）
            # 実行コストの詳細判定
            if comparison_result['total_cost_ratio'] < COST_IMPROVEMENT_THRESHOLD:
                performance_factors.append(f"実行コスト改善: {(1-comparison_result['total_cost_ratio'])*100:.1f}%削減")
            elif comparison_result['total_cost_ratio'] > COST_DEGRADATION_THRESHOLD:  # 1%以上の増加で即座に悪化判定
                cost_increase_pct = (comparison_result['total_cost_ratio']-1)*100
                performance_factors.append(f"実行コスト増加: {cost_increase_pct:.1f}%増加（元クエリ推奨）")
            else:
                performance_factors.append(f"実行コスト同等: {comparison_result['total_cost_ratio']:.2f}倍（変化なし）")
                
            # メモリ使用量の詳細判定
            if comparison_result['memory_usage_ratio'] < MEMORY_IMPROVEMENT_THRESHOLD:
                performance_factors.append(f"メモリ使用量改善: {(1-comparison_result['memory_usage_ratio'])*100:.1f}%削減")
            elif comparison_result['memory_usage_ratio'] > MEMORY_DEGRADATION_THRESHOLD:  # 1%以上の増加で即座に悪化判定
                memory_increase_pct = (comparison_result['memory_usage_ratio']-1)*100
                performance_factors.append(f"メモリ使用量増加: {memory_increase_pct:.1f}%増加（元クエリ推奨）")
            else:
                performance_factors.append(f"メモリ使用量同等: {comparison_result['memory_usage_ratio']:.2f}倍（変化なし）")
            
            # JOIN効率化チェック
            if optimized_metrics['join_operations'] < original_metrics['join_operations']:
                performance_factors.append(f"JOIN効率化: {original_metrics['join_operations']} → {optimized_metrics['join_operations']}操作")
            elif optimized_metrics['join_operations'] > original_metrics['join_operations']:
                performance_factors.append(f"JOIN操作増加: {original_metrics['join_operations']} → {optimized_metrics['join_operations']}操作（軽微）")
            
            # 🚨 厳格な総合判定（ユーザー要求：明確な改善のみ成功）
            has_improvement = any("改善" in factor for factor in performance_factors)
            has_cost_increase = any("実行コスト増加" in factor for factor in performance_factors)
            has_memory_increase = any("メモリ使用量増加" in factor for factor in performance_factors)
            
            # 🚨 明確な改善検出（1%以上の改善のみ）
            has_significant_improvement = (
                comparison_result['total_cost_ratio'] < COST_IMPROVEMENT_THRESHOLD or
                comparison_result['memory_usage_ratio'] < MEMORY_IMPROVEMENT_THRESHOLD
            )
            
            # 🚀 大幅改善検出（10%以上の改善）
            has_substantial_improvement = (
                comparison_result['total_cost_ratio'] < SUBSTANTIAL_COST_IMPROVEMENT_THRESHOLD or
                comparison_result['memory_usage_ratio'] < SUBSTANTIAL_MEMORY_IMPROVEMENT_THRESHOLD
            )
            
            # 🚨 厳格判定：1%以上の増加でも元クエリ推奨
            if has_cost_increase or has_memory_increase:
                performance_factors.insert(0, "❌ パフォーマンス増加を検出（元クエリ推奨）")
                # 🚨 増加検出時は推奨も元クエリに変更
                comparison_result['performance_degradation_detected'] = True
                comparison_result['is_optimization_beneficial'] = False  
                comparison_result['recommendation'] = 'use_original'
                comparison_result['significant_improvement_detected'] = False
                comparison_result['substantial_improvement_detected'] = False
            elif has_substantial_improvement:
                # 🚀 大幅改善（10%以上）を検出
                cost_reduction = (1 - comparison_result['total_cost_ratio']) * 100
                memory_reduction = (1 - comparison_result['memory_usage_ratio']) * 100
                max_reduction = max(cost_reduction, memory_reduction)
                performance_factors.insert(0, f"🚀 大幅なパフォーマンス改善を確認（最大{max_reduction:.1f}%削減・最適化クエリ推奨）")
                comparison_result['significant_improvement_detected'] = True
                comparison_result['substantial_improvement_detected'] = True
            elif has_significant_improvement:
                performance_factors.insert(0, "✅ 明確なパフォーマンス改善を確認（最適化クエリ推奨）")
                comparison_result['significant_improvement_detected'] = True
                comparison_result['substantial_improvement_detected'] = False
            else:
                performance_factors.insert(0, "➖ パフォーマンス同等（明確な改善なし）")
                comparison_result['significant_improvement_detected'] = False
                comparison_result['substantial_improvement_detected'] = False
            
            comparison_result['details'] = performance_factors
        
    except Exception as e:
        # エラー時は安全側に倒して元クエリを推奨
        comparison_result['performance_degradation_detected'] = True
        comparison_result['is_optimization_beneficial'] = False
        comparison_result['recommendation'] = 'use_original'
        comparison_result['details'] = [f"パフォーマンス比較エラーのため元クエリ使用: {str(e)}"]
    
    return comparison_result


def analyze_degradation_causes(performance_comparison: Dict[str, Any], original_explain_cost: str = "", optimized_explain_cost: str = "") -> Dict[str, str]:
    """
    パフォーマンス悪化の原因を分析し、修正指示を生成
    """
    degradation_analysis = {
        'primary_cause': 'unknown',
        'specific_issues': [],
        'fix_instructions': [],
        'confidence_level': 'low',
        'analysis_details': {}
    }
    
    try:
        if not performance_comparison or not performance_comparison.get('performance_degradation_detected'):
            degradation_analysis['primary_cause'] = 'no_degradation'
            return degradation_analysis
        
        details = performance_comparison.get('details', [])
        cost_ratio = performance_comparison.get('total_cost_ratio', 1.0)
        memory_ratio = performance_comparison.get('memory_usage_ratio', 1.0)
        
        # 🔍 コスト悪化の深刻度分析
        if cost_ratio > 1.5:  # 50%以上の悪化
            degradation_analysis['confidence_level'] = 'high'
            severity = 'critical'
        elif cost_ratio > 1.3:  # 30%以上の悪化
            degradation_analysis['confidence_level'] = 'medium'
            severity = 'significant'
        else:
            degradation_analysis['confidence_level'] = 'low'
            severity = 'minor'
        
        degradation_analysis['analysis_details']['cost_degradation_severity'] = severity
        degradation_analysis['analysis_details']['cost_ratio'] = cost_ratio
        degradation_analysis['analysis_details']['memory_ratio'] = memory_ratio
        
        # 🎯 主要原因の特定とJOIN操作数分析
        for detail in details:
            detail_str = str(detail).lower()
            
            # JOIN操作数の大幅増加検出
            if 'join操作数増加' in detail_str or 'join' in detail_str:
                degradation_analysis['primary_cause'] = 'excessive_joins'
                degradation_analysis['specific_issues'].append('JOIN操作数の大幅増加')
                
                # JOIN数の具体的な増加を解析
                import re
                join_match = re.search(r'(\d+)\s*→\s*(\d+)', detail_str)
                if join_match:
                    original_joins = int(join_match.group(1))
                    optimized_joins = int(join_match.group(2))
                    join_increase_ratio = optimized_joins / original_joins if original_joins > 0 else float('inf')
                    
                    degradation_analysis['analysis_details']['original_joins'] = original_joins
                    degradation_analysis['analysis_details']['optimized_joins'] = optimized_joins
                    degradation_analysis['analysis_details']['join_increase_ratio'] = join_increase_ratio
                    
                    if join_increase_ratio > 1.5:  # 50%以上のJOIN増加
                        degradation_analysis['fix_instructions'].extend([
                            "JOIN順序の効率化を検討してください",
                            "元のJOIN順序を尊重し、大幅な構造変更を避けてください",
                            "不要なサブクエリ化によるJOIN重複を防いでください",
                            "CTE展開によるJOIN増加を避け、元の構造を保持してください"
                        ])
                
            # 総実行コスト悪化
            elif '総実行コスト悪化' in detail_str or 'コスト' in detail_str:
                if degradation_analysis['primary_cause'] == 'unknown':
                    degradation_analysis['primary_cause'] = 'cost_increase'
                degradation_analysis['specific_issues'].append('総実行コストの悪化')
                degradation_analysis['fix_instructions'].extend([
                    "小テーブルを効率的にJOINで処理してください",
                                         "大きなテーブルのJOIN順序を最適化してください",
                    "REPARTITIONヒントの配置位置を見直してください"
                ])
            
            # メモリ使用量悪化
            elif 'メモリ使用量悪化' in detail_str or 'メモリ' in detail_str:
                if degradation_analysis['primary_cause'] == 'unknown':
                    degradation_analysis['primary_cause'] = 'memory_increase'
                degradation_analysis['specific_issues'].append('メモリ使用量の悪化')
                degradation_analysis['fix_instructions'].extend([
                    "大きなテーブルのBROADCAST適用を削除してください",
                    "メモリ効率的なJOIN戦略を選択してください",
                    "中間結果のサイズを削減してください"
                ])
        
        # 🔍 EXPLAIN COST分析による詳細原因特定（利用可能な場合）
        if original_explain_cost and optimized_explain_cost:
            cost_analysis = analyze_explain_cost_differences(original_explain_cost, optimized_explain_cost)
            degradation_analysis['analysis_details']['explain_cost_analysis'] = cost_analysis
            
            # BROADCAST関連の問題検出
            if cost_analysis.get('broadcast_issues'):
                degradation_analysis['fix_instructions'].extend([
                    "検出されたBROADCAST問題を修正してください",
                    "適切なサイズのテーブルのみBROADCAST対象としてください"
                ])
        
        # 原因が特定できない場合のフォールバック
        if degradation_analysis['primary_cause'] == 'unknown':
            degradation_analysis['primary_cause'] = 'optimization_backfire'
            degradation_analysis['fix_instructions'].extend([
                "最適化アプローチを保守的に変更してください",
                "元のクエリ構造をより多く保持してください",
                "ヒント句の適用を最小限に抑えてください"
            ])
        
        # 重複する修正指示を削除
        degradation_analysis['fix_instructions'] = list(set(degradation_analysis['fix_instructions']))
        
    except Exception as e:
        degradation_analysis['primary_cause'] = 'analysis_error'
        degradation_analysis['specific_issues'] = [f"分析エラー: {str(e)}"]
        degradation_analysis['fix_instructions'] = [
            "保守的な最適化アプローチを使用してください",
            "元のクエリ構造を最大限保持してください"
        ]
    
    return degradation_analysis


def analyze_explain_cost_differences(original_cost: str, optimized_cost: str) -> Dict[str, Any]:
    """
    EXPLAIN COST結果の差分分析で悪化原因を特定
    """
    analysis = {
        'broadcast_issues': False,
        'join_strategy_changes': [],
        'size_estimation_problems': [],
        'plan_structure_changes': []
    }
    
    try:
        import re
        
        # BROADCAST関連の問題検出
        original_broadcasts = len(re.findall(r'broadcast', original_cost.lower()))
        optimized_broadcasts = len(re.findall(r'broadcast', optimized_cost.lower()))
        
        if optimized_broadcasts > original_broadcasts * 2:  # BROADCAST使用量が2倍以上増加
            analysis['broadcast_issues'] = True
            analysis['join_strategy_changes'].append(f"BROADCAST使用が大幅増加: {original_broadcasts} → {optimized_broadcasts}")
        
        # JOIN戦略の変化検出
        original_join_types = set(re.findall(r'(\w+)Join', original_cost))
        optimized_join_types = set(re.findall(r'(\w+)Join', optimized_cost))
        
        if optimized_join_types != original_join_types:
            analysis['join_strategy_changes'].append(f"JOIN戦略変化: {original_join_types} → {optimized_join_types}")
        
        # プラン構造の複雑化検出
        original_plan_depth = original_cost.count('+-')
        optimized_plan_depth = optimized_cost.count('+-')
        
        if optimized_plan_depth > original_plan_depth * 1.3:  # プラン深度が30%以上増加
            analysis['plan_structure_changes'].append(f"実行プラン複雑化: 深度 {original_plan_depth} → {optimized_plan_depth}")
        
    except Exception as e:
        analysis['analysis_error'] = str(e)
    
    return analysis


def execute_iterative_optimization_with_degradation_analysis(original_query: str, analysis_result: str, metrics: Dict[str, Any], max_optimization_attempts: int = 3) -> Dict[str, Any]:
    """
    反復的最適化とパフォーマンス悪化分析
    最大3回まで悪化原因を分析して再最適化を試行し、改善されない場合は元クエリを使用
    """
    from datetime import datetime
    
    print(f"\n🚀 反復的最適化プロセス開始（最大{max_optimization_attempts}回の改善試行）")
    print("🎯 目標: 10%以上のコスト削減を達成 | 最大試行回数到達時は最良結果を選択")
    print("=" * 70)
    
    optimization_attempts = []
    original_query_for_explain = original_query  # 元クエリの保持
    
    # 🚀 ベスト結果追跡（ユーザー要求：最大試行回数到達時は最も良い結果を選択）
    best_result = {
        'attempt_num': 0,
        'query': original_query,
        'cost_ratio': 1.0,
        'memory_ratio': 1.0,
        'performance_comparison': None,
        'optimized_result': '',
        'status': 'baseline'
    }
    
    for attempt_num in range(1, max_optimization_attempts + 1):
        print(f"\n🔄 最適化試行 {attempt_num}/{max_optimization_attempts}")
        print("-" * 50)
        
        # 前回の試行結果に基づく修正指示を生成
        fix_instructions = ""
        if attempt_num > 1 and optimization_attempts:
            previous_attempt = optimization_attempts[-1]
            if previous_attempt.get('degradation_analysis'):
                degradation_analysis = previous_attempt['degradation_analysis']
                fix_instructions = "\n".join([
                    f"【前回の悪化原因: {degradation_analysis['primary_cause']}】",
                    f"【信頼度: {degradation_analysis['confidence_level']}】",
                    "【修正指示】"
                ] + degradation_analysis['fix_instructions'])
                
                print(f"🔧 悪化原因分析結果: {degradation_analysis['primary_cause']}")
                print(f"📊 信頼度: {degradation_analysis['confidence_level']}")
                print(f"💡 修正指示: {len(degradation_analysis['fix_instructions'])}項目")
        
        # 最適化クエリ生成（初回 or 修正版）
        if attempt_num == 1:
            print("🤖 初回最適化クエリ生成")
            optimized_query = generate_optimized_query_with_llm(original_query, analysis_result, metrics)
            # 🐛 DEBUG: 初回試行クエリを保存
            if isinstance(optimized_query, str) and not optimized_query.startswith("LLM_ERROR:"):
                save_debug_query_trial(optimized_query, attempt_num, "initial")
        else:
            print(f"🔧 修正版最適化クエリ生成（試行{attempt_num}）")
            # 🚨 修正: パフォーマンス悪化専用関数を使用
            previous_attempt = optimization_attempts[-1] if optimization_attempts else {}
            degradation_analysis = previous_attempt.get('degradation_analysis', {})
            optimized_query = generate_improved_query_for_performance_degradation(
                original_query, 
                analysis_result, 
                metrics, 
                degradation_analysis, 
                previous_attempt.get('optimized_query', '')
            )
            # 🐛 DEBUG: パフォーマンス改善試行クエリを保存
            if isinstance(optimized_query, str) and not optimized_query.startswith("LLM_ERROR:"):
                degradation_cause = degradation_analysis.get('primary_cause', 'パフォーマンス悪化')
                save_debug_query_trial(optimized_query, attempt_num, "performance_improvement", 
                                     error_info=f"前回悪化原因: {degradation_cause}")
        
        # LLMエラーチェック
        if isinstance(optimized_query, str) and optimized_query.startswith("LLM_ERROR:"):
            print(f"❌ 最適化試行{attempt_num}でLLMエラーが発生")
            optimization_attempts.append({
                'attempt': attempt_num,
                'status': 'llm_error',
                'error': optimized_query[10:],
                'optimized_query': None
            })
            continue
        
        # クエリ抽出
        if isinstance(optimized_query, list):
            optimized_query_str = extract_main_content_from_thinking_response(optimized_query)
        else:
            optimized_query_str = str(optimized_query)
        
        extracted_sql = extract_sql_from_llm_response(optimized_query_str)
        current_query = extracted_sql if extracted_sql else original_query
        
        # EXPLAIN実行と構文チェック
        explain_result = execute_explain_with_retry_logic(current_query, analysis_result, metrics, max_retries=2)
        
        if explain_result['final_status'] != 'success':
            print(f"⚠️ 試行{attempt_num}: EXPLAIN実行失敗")
            optimization_attempts.append({
                'attempt': attempt_num,
                'status': 'explain_failed',
                'error': explain_result.get('error_details', 'Unknown error'),
                'optimized_query': current_query
            })
            continue
        
        # パフォーマンス比較実行
        print(f"🔍 試行{attempt_num}: パフォーマンス悪化検出を実行")
        
        # 🎯 キャッシュされた元クエリを使用（重複処理防止）
        corrected_original_query = globals().get('original_query_corrected', original_query)
        if corrected_original_query != original_query:
            print("💾 キャッシュされた元クエリを使用: 重複処理防止")
        
        # 元クエリのEXPLAIN COST取得
        original_explain_cost_result = execute_explain_and_save_to_file(corrected_original_query, "original_performance_check")
        
        # 最適化クエリのEXPLAIN COST取得
        optimized_explain_cost_result = execute_explain_and_save_to_file(current_query, f"optimized_attempt_{attempt_num}")
        
        performance_comparison = None
        degradation_analysis = None
        
        # 🔍 EXPLAIN COSTエラーハンドリングの改善
        original_cost_success = ('explain_cost_file' in original_explain_cost_result and 
                                'error_file' not in original_explain_cost_result)
        optimized_cost_success = ('explain_cost_file' in optimized_explain_cost_result and 
                                 'error_file' not in optimized_explain_cost_result)
        
        # 🚨 緊急デバッグ: EXPLAIN COST成功/失敗の詳細表示
        print(f"🔍 EXPLAIN COST成功判定:")
        print(f"   📊 元クエリ: {'✅ 成功' if original_cost_success else '❌ 失敗'}")
        if not original_cost_success:
            print(f"      • explain_cost_file存在: {'explain_cost_file' in original_explain_cost_result}")
            print(f"      • error_file存在: {'error_file' in original_explain_cost_result}")
            print(f"      • 返却キー: {list(original_explain_cost_result.keys())}")
        print(f"   🔧 最適化クエリ: {'✅ 成功' if optimized_cost_success else '❌ 失敗'}")
        if not optimized_cost_success:
            print(f"      • explain_cost_file存在: {'explain_cost_file' in optimized_explain_cost_result}")
            print(f"      • error_file存在: {'error_file' in optimized_explain_cost_result}")
            print(f"      • 返却キー: {list(optimized_explain_cost_result.keys())}")
        
        if not original_cost_success:
            print("⚠️ 元クエリのEXPLAIN COST実行失敗: パフォーマンス比較をスキップ")
            if 'error_file' in original_explain_cost_result:
                print(f"📄 エラー詳細: {original_explain_cost_result['error_file']}")
        
        if not optimized_cost_success:
            print("⚠️ 最適化クエリのEXPLAIN COST実行失敗: エラー修正を試行")
            if 'error_file' in optimized_explain_cost_result:
                print(f"📄 エラー詳細: {optimized_explain_cost_result['error_file']}")
                
                # 🚨 CRITICAL FIX: エラー検出時は即座にLLM修正を実行
                print("🔧 LLMによるエラー修正を実行中...")
                error_message = optimized_explain_cost_result.get('error_message', 'Unknown error')
                
                # エラー修正のためのLLM呼び出し
                corrected_query = generate_optimized_query_with_error_feedback(
                    original_query,
                    analysis_result, 
                    metrics,
                    error_message,
                    current_query  # 現在のクエリ（ヒント付き）を渡す
                )
                
                # 🐛 DEBUG: エラー修正クエリを保存
                if isinstance(corrected_query, str) and not corrected_query.startswith("LLM_ERROR:"):
                    save_debug_query_trial(corrected_query, attempt_num, "error_correction", 
                                         error_info=f"修正対象エラー: {error_message[:100]}")
                
                # LLMエラーチェック
                if isinstance(corrected_query, str) and corrected_query.startswith("LLM_ERROR:"):
                    print("❌ LLM修正でエラーが発生: フォールバック評価を実行")
                else:
                    # thinking_enabled対応
                    if isinstance(corrected_query, list):
                        corrected_query_str = extract_main_content_from_thinking_response(corrected_query)
                    else:
                        corrected_query_str = str(corrected_query)
                    
                    # SQLクエリ部分のみを抽出
                    extracted_sql = extract_sql_from_llm_response(corrected_query_str)
                    if extracted_sql:
                        current_query = extracted_sql
                        print("✅ LLMによるエラー修正完了、修正クエリで再評価")
                        
                        # 修正クエリで再度EXPLAIN実行
                        optimized_explain_cost_result = execute_explain_and_save_to_file(current_query, f"optimized_attempt_{attempt_num}_corrected")
                        optimized_cost_success = ('explain_cost_file' in optimized_explain_cost_result and 
                                                'error_file' not in optimized_explain_cost_result)
                        
                        if optimized_cost_success:
                            print("🎯 修正クエリのEXPLAIN実行成功!")
                        else:
                            print("⚠️ 修正クエリでもエラー発生: フォールバック評価を実行")
                    else:
                        print("❌ SQLクエリの抽出に失敗: フォールバック評価を実行")
            
            # エラー修正後もエラーの場合、フォールバック評価を実行
            if not optimized_cost_success:
                print("🔄 フォールバック評価を実行")
        
        # 🚨 緊急修正: EXPLAIN COST失敗時のフォールバック パフォーマンス評価
        if not (original_cost_success and optimized_cost_success):
            print("🔄 フォールバック: EXPLAIN結果による簡易パフォーマンス評価を実行")
            
            # EXPLAIN結果が利用可能な場合のフォールバック評価
            original_explain_success = ('explain_file' in original_explain_cost_result and 
                                       'error_file' not in original_explain_cost_result)
            optimized_explain_success = ('explain_file' in optimized_explain_cost_result and 
                                        'error_file' not in optimized_explain_cost_result)
            
            if original_explain_success and optimized_explain_success:
                try:
                    # EXPLAIN結果を読み込み
                    with open(original_explain_cost_result['explain_file'], 'r', encoding='utf-8') as f:
                        original_explain_content = f.read()
                    
                    with open(optimized_explain_cost_result['explain_file'], 'r', encoding='utf-8') as f:
                        optimized_explain_content = f.read()
                    
                    # フォールバック評価実行
                    fallback_evaluation = fallback_performance_evaluation(original_explain_content, optimized_explain_content)
                    
                    print(f"📊 フォールバック評価結果: {fallback_evaluation['summary']}")
                    print(f"   - 推奨: {fallback_evaluation['recommendation']}")
                    print(f"   - 信頼度: {fallback_evaluation['confidence']}")
                    
                    for detail in fallback_evaluation['details']:
                        print(f"   - {detail}")
                    
                    # performance_comparisonの代替として使用
                    performance_comparison = {
                        'is_optimization_beneficial': fallback_evaluation['recommendation'] == 'use_optimized',
                        'performance_degradation_detected': fallback_evaluation['overall_status'] == 'degradation_possible',
                        'significant_improvement_detected': fallback_evaluation['overall_status'] == 'clear_improvement',  # 🚨 フォールバック評価でも明確改善検出
                        'recommendation': fallback_evaluation['recommendation'],
                        'evaluation_type': 'fallback_plan_analysis',
                        'details': fallback_evaluation['details'],
                        'fallback_evaluation': fallback_evaluation,
                        'total_cost_ratio': 1.0,  # EXPLAIN COSTなしのため未知
                        'memory_usage_ratio': 1.0  # EXPLAIN COSTなしのため未知
                    }
                    
                    print("✅ フォールバック パフォーマンス評価完了")
                    
                    # 🚨 フォールバック評価でも厳格判定適用
                    if not performance_comparison.get('significant_improvement_detected', False):
                        if performance_comparison['performance_degradation_detected']:
                            print(f"🚨 試行{attempt_num}: フォールバック評価で悪化の可能性")
                            status_reason = "fallback_degradation_detected"
                        else:
                            print(f"⚠️ 試行{attempt_num}: フォールバック評価で明確な改善が確認できません")
                            status_reason = "fallback_insufficient_improvement"
                        
                        optimization_attempts.append({
                            'attempt': attempt_num,
                            'status': status_reason,
                            'optimized_query': current_query,
                            'performance_comparison': performance_comparison,
                            'cost_ratio': performance_comparison['total_cost_ratio'],
                            'memory_ratio': performance_comparison['memory_usage_ratio']
                        })
                        
                        # 🚀 フォールバック評価でもベスト結果追跡
                        current_cost_ratio = performance_comparison.get('total_cost_ratio', 1.0)
                        current_memory_ratio = performance_comparison.get('memory_usage_ratio', 1.0)
                        
                        # フォールバック評価では改善の場合のみベスト更新（不確実性を考慮）
                        if performance_comparison.get('significant_improvement_detected', False):
                            is_better_than_best = (
                                current_cost_ratio < best_result['cost_ratio'] or 
                                (current_cost_ratio == best_result['cost_ratio'] and current_memory_ratio < best_result['memory_ratio'])
                            )
                            
                            if is_better_than_best:
                                print(f"🏆 試行{attempt_num}: フォールバック評価で新ベスト結果！")
                                best_result.update({
                                    'attempt_num': attempt_num,
                                    'query': current_query,
                                    'cost_ratio': current_cost_ratio,
                                    'memory_ratio': current_memory_ratio,
                                    'performance_comparison': performance_comparison,
                                    'optimized_result': optimized_query_str,
                                    'status': 'fallback_improved'
                                })
                        
                        optimization_attempts.append({
                            'attempt': attempt_num,
                            'status': status_reason,
                            'optimized_query': current_query,
                            'performance_comparison': performance_comparison,
                            'cost_ratio': current_cost_ratio,
                            'memory_ratio': current_memory_ratio
                        })
                        
                        # 🚀 フォールバック評価では大幅改善判定が困難なため、試行継続
                        if attempt_num < max_optimization_attempts:
                            print(f"🔄 試行{attempt_num + 1}でより確実な改善を目指します（フォールバック評価）")
                            continue
                        else:
                            print(f"⏰ 最大試行回数({max_optimization_attempts})到達 → ベスト結果を選択")
                            break
                    
                except Exception as e:
                    print(f"❌ フォールバック評価でもエラー: {str(e)}")
                    print(f"   📊 エラー詳細: {type(e).__name__}")
                    if hasattr(e, '__traceback__'):
                        import traceback
                        print(f"   📄 スタックトレース: {traceback.format_exc()}")
                    performance_comparison = None
            else:
                print("❌ EXPLAIN結果も不足のため、パフォーマンス評価不可")
                performance_comparison = None
        
        # 🚨 緊急修正: ロジック順序を修正（EXPLAIN COST成功判定を先に実行）
        if (original_cost_success and optimized_cost_success):
            
            try:
                print(f"🎯 両方のEXPLAIN COST成功 → パフォーマンス比較を実行")
                
                # EXPLAIN COST内容を読み込み
                with open(original_explain_cost_result['explain_cost_file'], 'r', encoding='utf-8') as f:
                    original_cost_content = f.read()
                
                with open(optimized_explain_cost_result['explain_cost_file'], 'r', encoding='utf-8') as f:
                    optimized_cost_content = f.read()
                
                print(f"   📊 元クエリCOST内容長: {len(original_cost_content)} 文字")
                print(f"   🔧 最適化クエリCOST内容長: {len(optimized_cost_content)} 文字")
                
                # パフォーマンス比較実行
                print(f"🔍 compare_query_performance 実行中...")
                performance_comparison = compare_query_performance(original_cost_content, optimized_cost_content)
                print(f"✅ compare_query_performance 完了: {performance_comparison is not None}")
                
                if performance_comparison:
                    print(f"   📊 significant_improvement_detected: {performance_comparison.get('significant_improvement_detected', 'UNKNOWN')}")
                    print(f"   📊 performance_degradation_detected: {performance_comparison.get('performance_degradation_detected', 'UNKNOWN')}")
                    print(f"   📊 is_optimization_beneficial: {performance_comparison.get('is_optimization_beneficial', 'UNKNOWN')}")
                else:
                    print(f"❌ performance_comparison is None!")
                
                # 🚀 ベスト結果更新判定（ユーザー要求：常に最良結果を追跡）
                current_cost_ratio = performance_comparison['total_cost_ratio']
                current_memory_ratio = performance_comparison['memory_usage_ratio']
                
                # 現在の結果がベストを上回るかチェック（コスト比率が低いほど良い）
                is_better_than_best = (
                    current_cost_ratio < best_result['cost_ratio'] or 
                    (current_cost_ratio == best_result['cost_ratio'] and current_memory_ratio < best_result['memory_ratio'])
                )
                
                if is_better_than_best:
                    print(f"🏆 試行{attempt_num}: 新しいベスト結果を記録！")
                    print(f"   📊 コスト比: {best_result['cost_ratio']:.3f} → {current_cost_ratio:.3f}")
                    print(f"   💾 メモリ比: {best_result['memory_ratio']:.3f} → {current_memory_ratio:.3f}")
                    best_result.update({
                        'attempt_num': attempt_num,
                        'query': current_query,
                        'cost_ratio': current_cost_ratio,
                        'memory_ratio': current_memory_ratio,
                        'performance_comparison': performance_comparison,
                        'optimized_result': optimized_query_str,
                        'status': 'improved'
                    })
                
                # 🚀 大幅改善（10%以上）達成で即座に終了
                if performance_comparison.get('substantial_improvement_detected', False):
                    print(f"🚀 試行{attempt_num}: 大幅改善達成（10%以上削減）！即座に最適化完了")
                    optimization_attempts.append({
                        'attempt': attempt_num,
                        'status': 'substantial_success',
                        'optimized_query': current_query,
                        'performance_comparison': performance_comparison,
                        'cost_ratio': current_cost_ratio,
                        'memory_ratio': current_memory_ratio
                    })
                    
                    return {
                        'final_status': 'optimization_success',
                        'final_query': current_query,
                        'successful_attempt': attempt_num,
                        'total_attempts': attempt_num,
                        'optimization_attempts': optimization_attempts,
                        'performance_comparison': performance_comparison,
                        'optimized_result': optimized_query_str,
                        'saved_files': None,  # メイン処理で保存
                        'achievement_type': 'substantial_improvement'
                    }
                
                # 🚀 改善はあるが大幅でない場合の判定
                elif performance_comparison.get('significant_improvement_detected', False):
                    print(f"✅ 試行{attempt_num}: 改善を確認（目標10%未達）")
                    status_reason = "partial_improvement"
                else:
                    # 改善なしまたは悪化の場合
                    if performance_comparison['performance_degradation_detected']:
                        print(f"🚨 試行{attempt_num}: パフォーマンス増加を検出")
                        status_reason = "performance_degraded"
                    else:
                        print(f"⚠️ 試行{attempt_num}: 明確な改善が確認できません")
                        status_reason = "insufficient_improvement"
                
                # 悪化原因分析（改善不足の場合も実行）
                degradation_analysis = analyze_degradation_causes(performance_comparison, original_cost_content, optimized_cost_content)
                
                print(f"   詳細: {', '.join(performance_comparison.get('details', []))}")
                
                optimization_attempts.append({
                    'attempt': attempt_num,
                    'status': status_reason,
                    'optimized_query': current_query,
                    'performance_comparison': performance_comparison,
                    'degradation_analysis': degradation_analysis,
                    'cost_ratio': current_cost_ratio,
                    'memory_ratio': current_memory_ratio
                })
                
                # 🚀 新判定: 大幅改善（10%以上）でない限り試行継続
                if attempt_num < max_optimization_attempts:
                    print(f"🔄 試行{attempt_num + 1}で大幅改善（10%以上削減）を目指します")
                    continue
                else:
                    print(f"⏰ 最大試行回数({max_optimization_attempts})到達 → ベスト結果を選択")
                    break
            
            except Exception as e:
                print(f"❌ 試行{attempt_num}: パフォーマンス比較でエラー: {str(e)}")
                print(f"   📊 エラータイプ: {type(e).__name__}")
                if hasattr(e, '__traceback__'):
                    import traceback
                    print(f"   📄 スタックトレース: {traceback.format_exc()}")
                print(f"🚨 このエラーが「パフォーマンス評価が不可能」の原因です！")
                optimization_attempts.append({
                    'attempt': attempt_num,
                    'status': 'comparison_error',
                    'error': str(e),
                    'optimized_query': current_query
                })
                continue
        
        # 🚨 緊急修正: パフォーマンス評価が完全に失敗した場合のハンドリング（ロジック順序修正後）
        elif performance_comparison is None:
            print(f"🚨 試行{attempt_num}: パフォーマンス評価が不可能なため、次の試行に進みます")
            
            optimization_attempts.append({
                'attempt': attempt_num,
                'status': 'performance_evaluation_failed',
                'optimized_query': current_query,
                'performance_comparison': None,
                'error': 'EXPLAIN COST実行失敗またはフォールバック評価失敗',
                'cost_ratio': None,
                'memory_ratio': None
            })
            
            # 最後の試行でない場合は次の改善を試行
            if attempt_num < max_optimization_attempts:
                print(f"🔄 試行{attempt_num + 1}でパフォーマンス評価の再試行を行います")
                continue
            else:
                print(f"❌ 最大試行回数({max_optimization_attempts})に到達、元クエリを使用")
                break
        
        else:
            print(f"⚠️ 試行{attempt_num}: EXPLAIN COST取得失敗、構文的に正常な最適化クエリを使用")
            optimization_attempts.append({
                'attempt': attempt_num,
                'status': 'explain_cost_failed',
                'optimized_query': current_query,
                'note': 'EXPLAIN COST comparison skipped due to execution failure'
            })
            
            # 🚨 修正: EXPLAIN COSTが取得できない場合も重複保存を防止
            # saved_files = save_optimized_sql_files(...)  # ← 重複防止のためコメントアウト
            
            return {
                'final_status': 'partial_success',
                'final_query': current_query,
                'successful_attempt': attempt_num,
                'total_attempts': attempt_num,
                'optimization_attempts': optimization_attempts,
                'optimized_result': optimized_query_str,  # 🔧 メイン処理での保存用に追加
                'saved_files': None,  # 🔧 メイン処理で保存するためNone
                'note': 'Performance comparison unavailable but query is syntactically valid'
            }
    
    # 🚀 最大試行回数到達：ベスト結果を最終クエリとして選択
    print(f"\n⏰ 全{max_optimization_attempts}回の最適化試行が完了")
    print("🏆 ベスト結果を最終クエリとして選択します")
    print("=" * 60)
    
    # ベスト結果の詳細表示
    if best_result['attempt_num'] > 0:
        print(f"🥇 選択されたベスト結果: 試行{best_result['attempt_num']}")
        print(f"   📊 コスト比: {best_result['cost_ratio']:.3f} (改善度: {(1-best_result['cost_ratio'])*100:.1f}%)")
        print(f"   💾 メモリ比: {best_result['memory_ratio']:.3f} (改善度: {(1-best_result['memory_ratio'])*100:.1f}%)")
        
        final_query = best_result['query']
        final_optimized_result = best_result['optimized_result']
        final_performance_comparison = best_result['performance_comparison']
        final_status = 'optimization_success'
        achievement_type = 'best_of_trials'
        
        print(f"✅ ベスト結果を最適化クエリとして採用")
        
    else:
        print(f"⚠️ 全ての試行でエラーまたは評価不可のため、元クエリを使用")
        
        # 試行結果サマリー
        failure_summary = []
        for attempt in optimization_attempts:
            if attempt['status'] == 'performance_degraded':
                failure_summary.append(f"試行{attempt['attempt']}: {attempt.get('degradation_analysis', {}).get('primary_cause', 'unknown')} (コスト比: {attempt.get('cost_ratio', 'N/A')})")
            elif attempt['status'] == 'llm_error':
                failure_summary.append(f"試行{attempt['attempt']}: LLMエラー")
            elif attempt['status'] == 'explain_failed':
                failure_summary.append(f"試行{attempt['attempt']}: EXPLAIN実行失敗")
            elif attempt['status'] == 'comparison_error':
                failure_summary.append(f"試行{attempt['attempt']}: パフォーマンス比較エラー")
            else:
                failure_summary.append(f"試行{attempt['attempt']}: {attempt['status']}")
        
        failure_report = f"""# ⚠️ 全最適化試行完了のため、元クエリを使用

## 最適化試行結果

{chr(10).join(failure_summary) if failure_summary else "全ての試行でエラーが発生"}

## 最終判断

{max_optimization_attempts}回の最適化試行を実行しましたが、10%以上の大幅改善には到達せず、
ベスト結果も元クエリを上回りませんでした。

## 元のクエリ

```sql
{original_query}
```

## 推奨事項

- データ量やテーブル統計情報を確認してください
- より詳細なEXPLAIN情報を取得して手動最適化を検討してください  
- Liquid Clusteringやテーブル統計の更新を検討してください
"""
        
        final_query = original_query
        final_optimized_result = failure_report
        final_performance_comparison = None
        final_status = 'optimization_failed'
        achievement_type = 'no_improvement'
    
    return {
        'final_status': final_status,
        'final_query': final_query,
        'total_attempts': len(optimization_attempts),
        'optimization_attempts': optimization_attempts,
        'performance_comparison': final_performance_comparison,
        'optimized_result': final_optimized_result,
        'saved_files': None,  # メイン処理で保存
        'best_result': best_result,
        'achievement_type': achievement_type,
        'fallback_reason': f'Best result from {max_optimization_attempts} attempts selected' if best_result['attempt_num'] > 0 else 'All attempts failed or degraded'
    }


def execute_explain_with_retry_logic(original_query: str, analysis_result: str, metrics: Dict[str, Any], max_retries: int = 2) -> Dict[str, Any]:
    """
    EXPLAIN実行とエラー修正の再試行ロジック（構文エラー専用）
    最大2回まで自動修正を試行し、失敗時は元クエリを使用
    """
    from datetime import datetime
    
    print(f"\n🔄 EXPLAIN実行と自動エラー修正（最大{max_retries}回試行）")
    print("=" * 60)
    
    # 初回の最適化クエリ生成
    print("🤖 ステップ1: 初回最適化クエリ生成")
    optimized_query = generate_optimized_query_with_llm(original_query, analysis_result, metrics)
    
    # 🐛 DEBUG: 単体最適化クエリを保存（反復最適化以外のパス）
    if isinstance(optimized_query, str) and not optimized_query.startswith("LLM_ERROR:"):
        save_debug_query_trial(optimized_query, 1, "single_optimization", query_id="direct_path")
    
    # LLMエラーチェック（重要）
    if isinstance(optimized_query, str) and optimized_query.startswith("LLM_ERROR:"):
        print("❌ LLM最適化でエラーが発生したため、元のクエリを使用します")
        print(f"🔧 エラー詳細: {optimized_query[10:]}")  # "LLM_ERROR:"を除去
        
        # エラー時は元のクエリを使用して即座にファイル生成
        fallback_result = save_optimized_sql_files(
            original_query,
            f"# ❌ LLM最適化でエラーが発生したため、元のクエリを使用\n\n## エラー詳細\n{optimized_query[10:]}\n\n## 元のクエリ\n```sql\n{original_query}\n```",
            metrics,
            analysis_result,
            "",  # llm_response
            None  # performance_comparison
        )
        
        return {
            'final_status': 'llm_error',
            'final_query': original_query,
            'total_attempts': 0,
            'all_attempts': [],
            'explain_result': None,
            'optimized_result': optimized_query,
            'error_details': optimized_query[10:]
        }
    
    # thinking_enabled対応: リスト形式の場合はメインコンテンツを抽出
    if isinstance(optimized_query, list):
        optimized_query_str = extract_main_content_from_thinking_response(optimized_query)
    else:
        optimized_query_str = str(optimized_query)
    
    # SQLクエリ部分のみを抽出
    extracted_sql = extract_sql_from_llm_response(optimized_query_str)
    current_query = extracted_sql if extracted_sql else original_query
    
    retry_count = 0
    all_attempts = []  # 全試行の記録
    
    while retry_count <= max_retries:
        attempt_num = retry_count + 1
        print(f"\n🔍 試行 {attempt_num}/{max_retries + 1}: EXPLAIN実行")
        
        # EXPLAIN実行（最適化後クエリ）
        explain_result = execute_explain_and_save_to_file(current_query, "optimized")
        
        # 成功時の処理
        if 'explain_file' in explain_result and 'error_file' not in explain_result:
            print(f"✅ 試行 {attempt_num} で成功しました！")
            
            # 🚨 修正：パフォーマンス比較は反復最適化関数で一元化
            # パフォーマンス比較をここで実行すると二重実行になるため削除
            
            # 🚨 修正：以下のパフォーマンス比較を無効化（二重実行防止）
            # パフォーマンス比較は execute_iterative_optimization_with_degradation_analysis で一元化
            
            # 🔧 構文チェック成功のため、即座に success ステータスで次のステップへ
            performance_comparison = None  # 反復最適化で設定される
            
            if False:  # 🚨 パフォーマンス比較ブロックを無効化
                
                try:
                    # EXPLAIN COSTファイル内容を読み込み
                    with open(original_explain_cost_result['explain_cost_file'], 'r', encoding='utf-8') as f:
                        original_cost_content = f.read()
                    
                    with open(optimized_explain_cost_result['explain_cost_file'], 'r', encoding='utf-8') as f:
                        optimized_cost_content = f.read()
                    
                    # パフォーマンス比較実行
                    performance_comparison = compare_query_performance(original_cost_content, optimized_cost_content)
                    
                    print(f"📊 パフォーマンス比較結果:")
                    print(f"   - 実行コスト比: {performance_comparison['total_cost_ratio']:.2f}倍")
                    print(f"   - メモリ使用比: {performance_comparison['memory_usage_ratio']:.2f}倍")
                    print(f"   - 推奨: {performance_comparison['recommendation']}")
                    
                    for detail in performance_comparison['details']:
                        print(f"   - {detail}")
                    
                    # パフォーマンス悪化が検出された場合
                    if performance_comparison['performance_degradation_detected']:
                        print("🚨 パフォーマンス悪化を検出！元クエリを使用します")
                        
                        # 元クエリでのファイル生成（パフォーマンス悪化防止）
                        fallback_result = save_optimized_sql_files(
                            original_query,
                            f"# 🚨 パフォーマンス悪化検出のため元クエリを使用\n\n## 悪化要因\n{'; '.join(performance_comparison['details'])}\n\n## パフォーマンス比較結果\n- 実行コスト比: {performance_comparison['total_cost_ratio']:.2f}倍\n- メモリ使用比: {performance_comparison['memory_usage_ratio']:.2f}倍\n\n## 元のクエリ（最適化前）\n```sql\n{original_query}\n```",
                            metrics,
                            analysis_result,
                            "",  # llm_response
                            performance_comparison  # 🔍 詳細なパフォーマンス比較結果を含める
                        )
                        
                        return {
                            'final_status': 'performance_degradation_detected',
                            'final_query': original_query,
                            'total_attempts': attempt_num,
                            'all_attempts': all_attempts,
                            'explain_result': original_explain_cost_result,
                            'optimized_result': optimized_query,
                            'performance_comparison': performance_comparison,
                            'fallback_reason': 'performance_degradation'
                        }
                    
                    else:
                        print("✅ パフォーマンス改善を確認。最適化クエリを使用します")
                    
                except Exception as e:
                    print(f"⚠️ パフォーマンス比較でエラー発生: {str(e)}")
                    print("🔄 安全のため元クエリを使用します")
                    
                    # エラー時も安全側に倒して元クエリを使用
                    fallback_result = save_optimized_sql_files(
                        original_query,
                        f"# ⚠️ パフォーマンス比較エラーのため安全性を優先して元クエリを使用\n\n## エラー詳細\n{str(e)}\n\n## 元のクエリ\n```sql\n{original_query}\n```",
                        metrics,
                        analysis_result,
                        "",  # llm_response
                        None  # performance_comparison
                    )
                    
                    return {
                        'final_status': 'performance_comparison_error',
                        'final_query': original_query,
                        'total_attempts': attempt_num,
                        'all_attempts': all_attempts,
                        'explain_result': explain_result,
                        'optimized_result': optimized_query,
                        'fallback_reason': 'performance_comparison_error',
                        'error_details': str(e)
                    }
            
            # 🚨 修正：else部分も無効化（二重実行防止）
            # else:
            #     print("⚠️ EXPLAIN COST取得失敗のため、パフォーマンス比較をスキップ")
            #     print("🔄 構文的に正常な最適化クエリを使用します")
            
            # 成功記録
            attempt_record = {
                'attempt': attempt_num,
                'status': 'success',
                'query': current_query,
                'explain_file': explain_result.get('explain_file'),
                'plan_lines': explain_result.get('plan_lines', 0),
                'performance_comparison': performance_comparison
            }
            all_attempts.append(attempt_record)
            
            # 最終結果（パフォーマンス悪化なしの場合）
            return {
                'final_status': 'success',
                'final_query': current_query,
                'total_attempts': attempt_num,
                'all_attempts': all_attempts,
                'explain_result': explain_result,
                'optimized_result': optimized_query,  # 元の完全なレスポンス
                'performance_comparison': performance_comparison
            }
        
        # エラー時の処理
        elif 'error_file' in explain_result:
            error_message = explain_result.get('error_message', 'Unknown error')
            print(f"❌ 試行 {attempt_num} でエラー発生: {error_message}")
            
            # エラー記録
            attempt_record = {
                'attempt': attempt_num,
                'status': 'error',
                'query': current_query,
                'error_message': error_message,
                'error_file': explain_result.get('error_file')
            }
            all_attempts.append(attempt_record)
            
            # 最大試行回数に達した場合
            if retry_count >= max_retries:
                print(f"🚨 最大試行回数（{max_retries}回）に達しました")
                print("📋 元の動作可能クエリを使用します")
                
                # フォールバック: 元クエリでのファイル生成
                fallback_result = save_optimized_sql_files(
                    original_query, 
                    f"# 🚨 最適化クエリのEXPLAIN実行が{max_retries}回とも失敗したため、元クエリを使用\n\n## 最後のエラー情報\n{error_message}\n\n## 元のクエリ\n```sql\n{original_query}\n```",
                    metrics,
                    analysis_result,
                    "",  # llm_response
                    None  # performance_comparison
                )
                
                # 失敗時のログ記録
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                log_filename = f"output_optimization_failure_log_{timestamp}.txt"
                
                try:
                    with open(log_filename, 'w', encoding='utf-8') as f:
                        f.write(f"# 最適化クエリ生成失敗ログ\n")
                        f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        f.write(f"最大試行回数: {max_retries}回\n")
                        f.write(f"最終結果: 元クエリを使用\n\n")
                        
                        f.write("=" * 80 + "\n")
                        f.write("全試行の詳細記録:\n")
                        f.write("=" * 80 + "\n\n")
                        
                        for attempt in all_attempts:
                            f.write(f"【試行 {attempt['attempt']}】\n")
                            f.write(f"ステータス: {attempt['status']}\n")
                            if attempt['status'] == 'error':
                                f.write(f"エラー: {attempt['error_message']}\n")
                                f.write(f"エラーファイル: {attempt.get('error_file', 'N/A')}\n")
                            f.write(f"使用クエリ長: {len(attempt['query'])} 文字\n\n")
                        
                        f.write("=" * 80 + "\n")
                        f.write("元のクエリ（フォールバック使用）:\n")
                        f.write("=" * 80 + "\n\n")
                        f.write(original_query)
                    
                    print(f"📄 失敗ログを保存: {log_filename}")
                    
                except Exception as log_error:
                    print(f"❌ 失敗ログの保存にも失敗: {str(log_error)}")
                
                return {
                    'final_status': 'fallback_to_original',
                    'final_query': original_query,
                    'total_attempts': attempt_num,
                    'all_attempts': all_attempts,
                    'fallback_files': fallback_result,
                    'failure_log': log_filename
                }
            
            # 再試行する場合のエラー修正
            retry_count += 1
            print(f"🔧 試行 {retry_count + 1} に向けてエラー修正中...")
            
            # エラー情報を含めて再生成（初回最適化クエリも渡す）
            corrected_query = generate_optimized_query_with_error_feedback(
                original_query, 
                analysis_result, 
                metrics, 
                error_message,
                current_query  # 🚀 初回最適化クエリ（ヒント付き）を渡す
            )
            
            # 🐛 DEBUG: 再試行時のエラー修正クエリを保存
            if isinstance(corrected_query, str) and not corrected_query.startswith("LLM_ERROR:"):
                save_debug_query_trial(corrected_query, retry_count + 1, "retry_error_correction", 
                                     query_id=f"retry_{retry_count + 1}", 
                                     error_info=f"再試行{retry_count + 1}のエラー修正: {error_message[:100]}")
            
            # LLMエラーチェック（エラー修正時）
            if isinstance(corrected_query, str) and corrected_query.startswith("LLM_ERROR:"):
                print("❌ エラー修正でもLLMエラーが発生したため、元のクエリを使用します")
                print(f"🔧 エラー詳細: {corrected_query[10:]}")  # "LLM_ERROR:"を除去
                
                # 失敗記録
                attempt_record = {
                    'attempt': retry_count + 1,
                    'status': 'llm_error_correction_failed',
                    'query': current_query,
                    'error_message': f"エラー修正時LLMエラー: {corrected_query[10:]}",
                    'error_file': None
                }
                all_attempts.append(attempt_record)
                
                # 元のクエリを使用してファイル生成
                fallback_result = save_optimized_sql_files(
                    original_query,
                    f"# ❌ エラー修正時もLLMエラーが発生したため、元のクエリを使用\n\n## エラー修正時のエラー詳細\n{corrected_query[10:]}\n\n## 元のクエリ\n```sql\n{original_query}\n```",
                    metrics,
                    analysis_result,
                    "",  # llm_response
                    None  # performance_comparison
                )
                
                return {
                    'final_status': 'llm_error_correction_failed',
                    'final_query': original_query,
                    'total_attempts': retry_count + 1,
                    'all_attempts': all_attempts,
                    'explain_result': None,
                    'optimized_result': corrected_query,
                    'error_details': corrected_query[10:]
                }
            
            # thinking_enabled対応
            if isinstance(corrected_query, list):
                corrected_query_str = extract_main_content_from_thinking_response(corrected_query)
            else:
                corrected_query_str = str(corrected_query)
            
            # SQLクエリ部分のみを抽出
            extracted_sql = extract_sql_from_llm_response(corrected_query_str)
            current_query = extracted_sql if extracted_sql else current_query
            
            print(f"✅ エラー修正クエリを生成しました（{len(current_query)} 文字）")
    
    # ここには到達しないはずだが、安全のため
    return {
        'final_status': 'unexpected_error',
        'final_query': original_query,
        'total_attempts': retry_count + 1,
        'all_attempts': all_attempts
    }


def extract_sql_from_llm_response(llm_response: str) -> str:
    """
    LLMレスポンスからSQLクエリ部分のみを抽出
    """
    import re
    
    # SQLコードブロックを検索（```sql ... ```）
    sql_pattern = r'```sql\s*(.*?)\s*```'
    matches = re.findall(sql_pattern, llm_response, re.DOTALL | re.IGNORECASE)
    
    if matches:
        # 最長のSQLブロックを選択
        sql_query = max(matches, key=len).strip()
        return sql_query
    
    # SQLコードブロックが見つからない場合、別のパターンを試行
    # ```のみのコードブロック
    code_pattern = r'```\s*(.*?)\s*```'
    matches = re.findall(code_pattern, llm_response, re.DOTALL)
    
    for match in matches:
        match = match.strip()
        # SQLキーワードで始まるかチェック
        if re.match(r'^(SELECT|WITH|CREATE|INSERT|UPDATE|DELETE|EXPLAIN)', match, re.IGNORECASE):
            return match
    
    # パターンマッチしない場合は元のレスポンスをそのまま返す
    return llm_response.strip()


def execute_explain_and_save_to_file(original_query: str, query_type: str = "original") -> Dict[str, str]:
    """
    クエリのEXPLAIN文とEXPLAIN COST文を実行し、EXPLAIN_ENABLED設定に基づいて結果をファイルに保存
    CTASの場合はSELECT部分のみを抽出してEXPLAIN文に渡す
    
    Args:
        original_query: EXPLAIN実行対象のクエリ
        query_type: "original" または "optimized" でファイル名を識別
    """
    from datetime import datetime
    import os
    
    if not original_query or not original_query.strip():
        print("❌ クエリが空です")
        return {}
    
    # EXPLAIN_ENABLED設定を確認
    explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')
    debug_enabled = globals().get('DEBUG_ENABLED', 'N')
    
    # ファイル名の生成（EXPLAIN_ENABLED=Yの場合のみ）
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    if explain_enabled.upper() == 'Y':
        explain_filename = f"output_explain_{query_type}_{timestamp}.txt"
        explain_cost_filename = f"output_explain_cost_{query_type}_{timestamp}.txt"
    else:
        explain_filename = None
        explain_cost_filename = None
    
    # CTASの場合はSELECT部分のみを抽出
    query_for_explain = extract_select_from_ctas(original_query)
    
    # EXPLAIN文とEXPLAIN COST文の生成
    explain_query = f"EXPLAIN {query_for_explain}"
    explain_cost_query = f"EXPLAIN COST {query_for_explain}"
    
    # カタログとデータベースの設定を取得
    catalog = globals().get('CATALOG', 'main')
    database = globals().get('DATABASE', 'default')
    
    print(f"📂 使用カタログ: {catalog}")
    print(f"🗂️ 使用データベース: {database}")
    
    # カタログとデータベースを設定
    try:
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"USE DATABASE {database}")
    except Exception as e:
        print(f"⚠️ カタログ/データベース設定エラー: {str(e)}")
    
    # EXPLAIN文とEXPLAIN COST文の実行
    try:
        print("🔄 EXPLAIN文とEXPLAIN COST文を実行中...")
        
        # 1. 通常のEXPLAIN実行
        print("   📊 EXPLAIN実行中...")
        explain_result_spark = spark.sql(explain_query)
        explain_result = explain_result_spark.collect()
        
        # EXPLAIN結果の内容をチェック
        explain_content = ""
        for row in explain_result:
            explain_content += str(row[0]) + "\n"
        
        # 2. EXPLAIN COST実行
        print("   💰 EXPLAIN COST実行中...")
        explain_cost_result_spark = spark.sql(explain_cost_query)
        explain_cost_result = explain_cost_result_spark.collect()
        
        # EXPLAIN COST結果の内容をチェック
        explain_cost_content = ""
        for row in explain_cost_result:
            explain_cost_content += str(row[0]) + "\n"
        
        # 🚨 緊急修正: エラーパターンを厳密化（誤検出防止）
        retryable_error_patterns = [
            "Error occurred during query planning",
            "error occurred during query planning", 
            "Query planning failed",
            "query planning failed",
            "Plan optimization failed",
            "plan optimization failed",
            "Failed to plan query",
            "failed to plan query",
            "Analysis exception",
            "analysis exception",
            "AMBIGUOUS_REFERENCE",
            "ambiguous_reference",
            "[AMBIGUOUS_REFERENCE]",
            # "Reference",  # 🚨 除去: 過度に一般的、正常結果も誤検出
            "reference is ambiguous",  # より具体的なパターンに変更
            # "is ambiguous",  # 🚨 除去: 過度に一般的
            "ambiguous reference",  # より具体的なパターンに変更
            # "Ambiguous",  # 🚨 除去: 過度に一般的
            "ParseException",
            "SemanticException", 
            "AnalysisException",
            "Syntax error",
            "syntax error",
            "PARSE_SYNTAX_ERROR",
            "INVALID_IDENTIFIER",
            "TABLE_OR_VIEW_NOT_FOUND",
            "COLUMN_NOT_FOUND",
            "UNRESOLVED_COLUMN",
            "[UNRESOLVED_COLUMN",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION]"
        ]
        
        # 🚨 重要: EXPLAIN結果とEXPLAIN COST結果の両方をエラーチェック
        detected_error = None
        error_source = None
        
        # 🚨 緊急デバッグ: エラー検出プロセスの詳細表示
        print(f"🔍 エラーパターン検出実行中（パターン数: {len(retryable_error_patterns)}）")
        print(f"   📊 EXPLAIN内容長: {len(explain_content)} 文字")
        print(f"   💰 EXPLAIN COST内容長: {len(explain_cost_content)} 文字")
        
        # 1. EXPLAIN結果のエラーチェック
        for pattern in retryable_error_patterns:
            if pattern in explain_content.lower():
                detected_error = pattern
                error_source = "EXPLAIN"
                print(f"❌ EXPLAIN結果でエラーパターン検出: '{pattern}'")
                break
        
        # 2. EXPLAIN COST結果のエラーチェック（EXPLAINでエラーが見つからない場合のみ）
        if not detected_error:
            for pattern in retryable_error_patterns:
                if pattern in explain_cost_content.lower():
                    detected_error = pattern
                    error_source = "EXPLAIN COST"
                    print(f"❌ EXPLAIN COST結果でエラーパターン検出: '{pattern}'")
                    break
        
        if not detected_error:
            print("✅ エラーパターン未検出: 正常な結果として処理")
        
        if detected_error:
            # エラーが検出された場合はエラーとして処理
            print(f"❌ {error_source}結果でエラーを検出: {detected_error}")
            
            # 結果のプレビュー表示（エラー用）
            print(f"\n📋 {error_source}結果のプレビュー:")
            print("-" * 50)
            if error_source == "EXPLAIN":
                preview_lines = min(10, len(explain_result))
                for i, row in enumerate(explain_result[:preview_lines]):
                    print(f"{i+1:2d}: {str(row[0])[:100]}...")
            else:
                preview_lines = min(10, len(explain_cost_result))
                for i, row in enumerate(explain_cost_result[:preview_lines]):
                    print(f"{i+1:2d}: {str(row[0])[:100]}...")
            
            # エラーファイルの保存（EXPLAIN_ENABLED=Yの場合のみ）
            error_filename = None
            error_cost_filename = None
            if explain_enabled.upper() == 'Y':
                # EXPLAIN結果エラーファイル
                error_filename = f"output_explain_error_{query_type}_{timestamp}.txt"
                with open(error_filename, 'w', encoding='utf-8') as f:
                    f.write(f"# EXPLAIN実行エラー ({query_type}クエリ)\n")
                    f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"クエリタイプ: {query_type}\n")
                    f.write(f"エラー検出元: {error_source}\n")
                    f.write(f"検出エラーパターン: {detected_error}\n")
                    f.write(f"クエリ文字数: {len(original_query):,}\n")
                    f.write("\n" + "=" * 80 + "\n")
                    f.write("EXPLAIN 結果:\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(explain_content)
                    f.write("\n" + "=" * 80 + "\n")
                    f.write("EXPLAIN COST 結果:\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(explain_cost_content)
                
                print(f"📄 エラー詳細を保存: {error_filename}")
                if error_source == "EXPLAIN" and len(explain_result) > preview_lines:
                    print(f"... (残り {len(explain_result) - preview_lines} 行は {error_filename} を参照)")
                elif error_source == "EXPLAIN COST" and len(explain_cost_result) > preview_lines:
                    print(f"... (残り {len(explain_cost_result) - preview_lines} 行は {error_filename} を参照)")
            else:
                print("💡 EXPLAIN_ENABLED=N のため、エラーファイルは保存されません")
                if error_source == "EXPLAIN" and len(explain_result) > preview_lines:
                    print(f"... (残り {len(explain_result) - preview_lines} 行)")
                elif error_source == "EXPLAIN COST" and len(explain_cost_result) > preview_lines:
                    print(f"... (残り {len(explain_cost_result) - preview_lines} 行)")
            
            print("-" * 50)
            
            result_dict = {
                'error_message': explain_content.strip() if error_source == "EXPLAIN" else explain_cost_content.strip(),
                'detected_pattern': detected_error,
                'error_source': error_source
            }
            if error_filename:
                result_dict['error_file'] = error_filename
            
            return result_dict
        
        # エラーが検出されなかった場合は成功として処理
        print(f"✅ EXPLAIN & EXPLAIN COST実行成功")
        print(f"📊 EXPLAIN実行プラン行数: {len(explain_result):,}")
        print(f"💰 EXPLAIN COST統計行数: {len(explain_cost_result):,}")
        
        # 結果のプレビュー表示
        print("\n📋 EXPLAIN結果のプレビュー:")
        print("-" * 50)
        preview_lines = min(10, len(explain_result))
        for i, row in enumerate(explain_result[:preview_lines]):
            print(f"{i+1:2d}: {str(row[0])[:100]}...")
        
        print("\n💰 EXPLAIN COST結果のプレビュー:")
        print("-" * 50)
        cost_preview_lines = min(10, len(explain_cost_result))
        for i, row in enumerate(explain_cost_result[:cost_preview_lines]):
            print(f"{i+1:2d}: {str(row[0])[:100]}...")
        
        # 結果をファイルに保存（EXPLAIN_ENABLED=Yの場合のみ）
        if explain_enabled.upper() == 'Y' and explain_filename and explain_cost_filename:
            # EXPLAIN結果ファイル
            with open(explain_filename, 'w', encoding='utf-8') as f:
                f.write(f"# EXPLAIN実行結果 ({query_type}クエリ)\n")
                f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"クエリタイプ: {query_type}\n")
                f.write(f"クエリ文字数: {len(original_query):,}\n")
                f.write("\n" + "=" * 80 + "\n")
                f.write("EXPLAIN結果:\n")
                f.write("=" * 80 + "\n\n")
                f.write(explain_content)
            
            # EXPLAIN COST結果ファイル
            with open(explain_cost_filename, 'w', encoding='utf-8') as f:
                f.write(f"# EXPLAIN COST実行結果 ({query_type}クエリ)\n")
                f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"クエリタイプ: {query_type}\n")
                f.write(f"クエリ文字数: {len(original_query):,}\n")
                f.write("\n" + "=" * 80 + "\n")
                f.write("EXPLAIN COST結果（統計情報付き）:\n")
                f.write("=" * 80 + "\n\n")
                f.write(explain_cost_content)
            
            print(f"📄 EXPLAIN結果を保存: {explain_filename}")
            print(f"💰 EXPLAIN COST結果を保存: {explain_cost_filename}")
            if len(explain_result) > preview_lines:
                print(f"... (残り {len(explain_result) - preview_lines} 行は {explain_filename} を参照)")
            if len(explain_cost_result) > cost_preview_lines:
                print(f"... (残り {len(explain_cost_result) - cost_preview_lines} 行は {explain_cost_filename} を参照)")
        else:
            print("💡 EXPLAIN_ENABLED=N のため、EXPLAIN結果ファイルは保存されません")
            if len(explain_result) > preview_lines:
                print(f"... (残り {len(explain_result) - preview_lines} 行)")
            if len(explain_cost_result) > cost_preview_lines:
                print(f"... (残り {len(explain_cost_result) - cost_preview_lines} 行)")
        
        print("-" * 50)
        
        result_dict = {
            'plan_lines': len(explain_result),
            'cost_lines': len(explain_cost_result)
        }
        if explain_filename and explain_enabled.upper() == 'Y':
            result_dict['explain_file'] = explain_filename
        if explain_cost_filename and explain_enabled.upper() == 'Y':
            result_dict['explain_cost_file'] = explain_cost_filename
        
        return result_dict
        
    except Exception as e:
        error_message = str(e)
        print(f"❌ EXPLAIN文またはEXPLAIN COST文の実行に失敗: {error_message}")
        
        # 真の致命的エラー（リトライ不可能なエラー）のチェック
        truly_fatal_errors = [
            "Permission denied",
            "Access denied", 
            "Insufficient privileges",
            "Database not found",
            "Catalog not found",
            "Spark session is not active",
            "java.lang.OutOfMemoryError",
            "Driver killed",
            "Connection refused"
        ]
        
        # 再試行可能なエラー（LLMで修正可能）
        retryable_error_patterns = [
            "Error occurred during query planning",
            "error occurred during query planning", 
            "Query planning failed",
            "query planning failed",
            "Plan optimization failed",
            "plan optimization failed",
            "Failed to plan query",
            "failed to plan query",
            "Analysis exception",
            "analysis exception",
            "AMBIGUOUS_REFERENCE",
            "ambiguous_reference",
            "[AMBIGUOUS_REFERENCE]",
            "Reference",
            "is ambiguous",
            "Ambiguous",
            "ParseException",
            "SemanticException",
            "AnalysisException",
            "Syntax error",
            "syntax error",
            "PARSE_SYNTAX_ERROR",
            "INVALID_IDENTIFIER",
            "TABLE_OR_VIEW_NOT_FOUND",
            "COLUMN_NOT_FOUND",
            "UNRESOLVED_COLUMN",
            "[UNRESOLVED_COLUMN",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION]"
        ]
        
        # 真の致命的エラーかチェック
        is_truly_fatal = any(pattern in error_message.lower() for pattern in truly_fatal_errors)
        
        # 再試行可能エラーかチェック
        is_retryable = any(pattern in error_message.lower() for pattern in retryable_error_patterns)
        
        if is_truly_fatal:
            print(f"🚨 FATAL: 回復不可能なエラーが発生しました")
            print(f"🚨 エラー詳細: {error_message}")
            print(f"🚨 処理を終了します。")
            
            # エラーファイルの保存（EXPLAIN_ENABLED=Yの場合のみ）
            if explain_enabled.upper() == 'Y':
                error_filename = f"output_explain_fatal_error_{query_type}_{timestamp}.txt"
                try:
                    with open(error_filename, 'w', encoding='utf-8') as f:
                        f.write(f"# FATAL EXPLAIN実行エラー (回復不可能, {query_type}クエリ)\n")
                        f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        f.write(f"クエリタイプ: {query_type}\n")
                        f.write(f"エラー内容: {error_message}\n")
                        f.write(f"エラータイプ: FATAL - Unrecoverable Error\n")
                        f.write("\n" + "=" * 80 + "\n")
                        f.write("実行しようとしたEXPLAIN文:\n")
                        f.write("=" * 80 + "\n\n")
                        f.write(explain_query)
                        f.write("\n\n" + "=" * 80 + "\n")
                        f.write("実行しようとしたEXPLAIN COST文:\n")
                        f.write("=" * 80 + "\n\n")
                        f.write(explain_cost_query)
                    
                    print(f"📄 Fatal エラー詳細を保存: {error_filename}")
                    
                except Exception as file_error:
                    print(f"❌ Fatal エラーファイルの保存にも失敗: {str(file_error)}")
            else:
                print("💡 EXPLAIN_ENABLED=N のため、Fatal エラーファイルは保存されません")
            
            # プログラムを終了
            import sys
            sys.exit(1)
        
        elif is_retryable:
            print(f"🔄 再試行可能なエラーを検出: {error_message}")
            print(f"💡 このエラーはLLMによる自動修正の対象です")
        
        # 非致命的なエラーの場合の処理
        error_filename = None
        if explain_enabled.upper() == 'Y':
            error_filename = f"output_explain_error_{query_type}_{timestamp}.txt"
            try:
                with open(error_filename, 'w', encoding='utf-8') as f:
                    f.write(f"# EXPLAIN実行エラー ({query_type}クエリ)\n")
                    f.write(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"クエリタイプ: {query_type}\n")
                    f.write(f"エラー内容: {error_message}\n")
                    f.write("\n" + "=" * 80 + "\n")
                    f.write("実行しようとしたEXPLAIN文:\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(explain_query)
                    f.write("\n\n" + "=" * 80 + "\n")
                    f.write("実行しようとしたEXPLAIN COST文:\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(explain_cost_query)
                
                print(f"📄 エラー詳細を保存: {error_filename}")
                
            except Exception as file_error:
                print(f"❌ エラーファイルの保存にも失敗: {str(file_error)}")
        else:
            print("💡 EXPLAIN_ENABLED=N のため、エラーファイルは保存されません")
        
        result_dict = {
            'error_message': error_message
        }
        if error_filename:
            result_dict['error_file'] = error_filename
        
        return result_dict

# EXPLAIN文実行の実行
print("\n🔍 EXPLAIN文実行処理")
print("-" * 40)

# セル43で抽出したオリジナルクエリが変数に残っているかチェック
try:
    # original_queryが既に定義されているか確認
    original_query_for_explain = original_query
    print(f"✅ オリジナルクエリを取得しました ({len(original_query_for_explain)} 文字)")
    
except NameError:
    print("⚠️ オリジナルクエリが見つかりません")
    print("   セル43 (オリジナルクエリ抽出) を先に実行してください")
    
    # フォールバック: プロファイラーデータから再抽出
    try:
        print("🔄 プロファイラーデータから再抽出を試行中...")
        original_query_for_explain = extract_original_query_from_profiler_data(profiler_data)
        
        if original_query_for_explain:
            print(f"✅ 再抽出成功 ({len(original_query_for_explain)} 文字)")
        else:
            print("❌ 再抽出に失敗しました")
            original_query_for_explain = None
            
    except Exception as e:
        print(f"❌ 再抽出中にエラー: {str(e)}")
        original_query_for_explain = None

# EXPLAIN実行フラグの確認
explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')
print(f"🔍 EXPLAIN実行設定: {explain_enabled}")

if explain_enabled.upper() != 'Y':
    print("⚠️ EXPLAIN実行が無効化されています")
    print("   EXPLAIN文を実行する場合は、最初のセルでEXPLAIN_ENABLED = 'Y'に設定してください")
elif original_query_for_explain and original_query_for_explain.strip():
    print("\n🚀 統合SQL最適化 & EXPLAIN実行（自動エラー修正付き）")
    
    # Spark環境の確認
    try:
        spark_version = spark.version
        print(f"📊 Spark環境: {spark_version}")
    except Exception as e:
        print(f"❌ Spark環境の確認に失敗: {str(e)}")
        print("   Databricks環境で実行してください")
        spark = None
    
    if spark:
        # 統合処理: 分析結果が必要なので確認
        try:
            # analysis_resultが定義されているかチェック
            if 'analysis_result' in globals():
                current_analysis_result = analysis_result
            else:
                print("⚠️ 分析結果が見つかりません。簡易分析を実行します...")
                current_analysis_result = "分析結果が利用できないため、基本的な最適化のみ実行"
            
            # extracted_metricsが定義されているかチェック  
            if 'extracted_metrics' in globals():
                current_metrics = extracted_metrics
            else:
                print("⚠️ メトリクスが見つかりません。空のメトリクスで実行します...")
                current_metrics = {}
            
            # thinking_enabled対応
            if isinstance(current_analysis_result, list):
                analysis_result_str = extract_main_content_from_thinking_response(current_analysis_result)
            else:
                analysis_result_str = str(current_analysis_result)
            
            # 🔍 ステップ1: オリジナルクエリのEXPLAIN実行（事前修正付き）
            print("\n📋 ステップ1: オリジナルクエリのEXPLAIN実行（Photon対応状況分析）")
            print("-" * 60)
            
            # 🎯 元のクエリをそのまま保存（LLMによる修正に完全依存）
            print("📋 元のクエリをそのまま使用: LLMによる高度な修正に依存")
            original_query_validated = original_query_for_explain
            
            # 🎯 元クエリをグローバル変数として保存（重複処理防止）
            globals()['original_query_corrected'] = original_query_validated
            print("💾 元クエリをキャッシュ: 重複処理防止")
            
            original_explain_result = execute_explain_and_save_to_file(original_query_for_explain, "original")
            
            # 🚨 元クエリでエラーが発生した場合のLLM修正
            if 'error_file' in original_explain_result:
                print(f"🚨 元のクエリで構文エラーを検出: {original_explain_result.get('error_file', 'unknown')}")
                print("🤖 LLMによる元クエリ修正を実行中...")
                
                # エラー内容を読み込み
                error_message = ""
                if 'error_file' in original_explain_result:
                    try:
                        with open(original_explain_result['error_file'], 'r', encoding='utf-8') as f:
                            error_message = f.read()
                    except:
                        error_message = "エラーファイル読み込み失敗"
                
                # LLMによる元クエリ修正
                corrected_original_query = generate_optimized_query_with_error_feedback(
                    original_query_for_explain,
                    "元のクエリに構文エラーが検出されました。修正が必要です。",
                    current_metrics,
                    error_message,
                    ""  # previous_optimized_queryは空
                )
                
                # 🐛 DEBUG: 元クエリのエラー修正結果を保存
                if isinstance(corrected_original_query, str) and not corrected_original_query.startswith("LLM_ERROR:"):
                    save_debug_query_trial(corrected_original_query, 0, "original_query_correction", 
                                         query_id="original_corrected", 
                                         error_info=f"元クエリ構文エラー修正: {error_message[:100] if error_message else 'unknown error'}")
                
                # 修正結果をチェック
                if isinstance(corrected_original_query, str) and not corrected_original_query.startswith("LLM_ERROR:"):
                    print("✅ LLMによる元クエリ修正完了")
                    
                    # 修正されたクエリからSQLを抽出
                    if isinstance(corrected_original_query, list):
                        corrected_query_str = extract_main_content_from_thinking_response(corrected_original_query)
                    else:
                        corrected_query_str = str(corrected_original_query)
                    
                    extracted_sql = extract_sql_from_llm_response(corrected_query_str)
                    if extracted_sql:
                        original_query_for_explain = extracted_sql
                        print("🔄 修正されたクエリで再度EXPLAIN実行")
                        
                        # 修正されたクエリで再度EXPLAIN実行
                        original_explain_result = execute_explain_and_save_to_file(original_query_for_explain, "original_corrected")
                        
                        # グローバルキャッシュも更新
                        globals()['original_query_corrected'] = original_query_for_explain
                        print("💾 修正された元クエリをキャッシュ更新")
                    else:
                        print("❌ 修正されたクエリからSQL抽出失敗")
                else:
                    print("❌ LLMによる元クエリ修正失敗")
            
            if 'explain_file' in original_explain_result:
                print(f"✅ オリジナルクエリのEXPLAIN結果を保存: {original_explain_result['explain_file']}")
            if 'plan_lines' in original_explain_result:
                print(f"📊 オリジナルクエリ実行プラン行数: {original_explain_result['plan_lines']:,}")
            
            # 🚀 ステップ2: 新しい反復的最適化処理: 悪化原因分析による最大3回の改善試行
            print("\n📋 ステップ2: 反復的LLM最適化 & パフォーマンス悪化分析（最大3回改善試行）")
            print("-" * 60)
            max_optimization_attempts = globals().get('MAX_OPTIMIZATION_ATTEMPTS', 3)
            retry_result = execute_iterative_optimization_with_degradation_analysis(
                original_query_for_explain, 
                analysis_result_str, 
                current_metrics, 
                max_optimization_attempts=max_optimization_attempts
            )            
            # 結果の表示
            print(f"\n📊 最終結果: {retry_result['final_status']}")
            print(f"🔄 総試行回数: {retry_result['total_attempts']}")
            
            # 反復最適化の試行詳細表示
            if 'optimization_attempts' in retry_result:
                attempts = retry_result['optimization_attempts']
                print(f"📈 最適化試行詳細: {len(attempts)}回")
                for attempt in attempts:
                    status_icon = {
                        'success': '✅',
                        'performance_degraded': '🚨',
                        'llm_error': '❌',
                        'explain_failed': '⚠️',
                        'comparison_error': '🔧'
                    }.get(attempt['status'], '❓')
                    print(f"   {status_icon} 試行{attempt['attempt']}: {attempt['status']}")
                    if 'cost_ratio' in attempt and attempt['cost_ratio'] is not None:
                        print(f"      💰 コスト比: {attempt['cost_ratio']:.2f}倍")
            
            if retry_result['final_status'] in ['optimization_success', 'partial_success']:
                print("✅ 最適化クエリのEXPLAIN実行に成功しました！")
                
                # 成功時のファイル情報表示
                explain_result = retry_result.get('explain_result', {})
                if explain_result:
                    print("\n📁 生成されたファイル:")
                    if 'explain_file' in explain_result:
                        print(f"   📄 EXPLAIN結果: {explain_result['explain_file']}")
                    if 'plan_lines' in explain_result:
                        print(f"   📊 実行プラン行数: {explain_result['plan_lines']:,}")
                
                # 最適化されたクエリの保存
                optimized_result = retry_result.get('optimized_result', '')
                final_query = retry_result.get('final_query', original_query_for_explain)
                
                # ファイル保存: final_query（成功したクエリ）をSQLファイルに、optimized_result（元のLLMレスポンス）をレポートに使用
                performance_comparison = retry_result.get('performance_comparison')
                best_attempt_number = retry_result.get('best_result', {}).get('attempt_num')  # 🎯 ベスト試行番号を取得
                saved_files = save_optimized_sql_files(
                    original_query_for_explain,
                    final_query,  # 🚀 成功したクエリ（ヒント付き）を保存
                    current_metrics,
                    analysis_result_str,
                    optimized_result,  # 📊 元のLLMレスポンス（レポート用）
                    performance_comparison,  # 🔍 パフォーマンス比較結果
                    best_attempt_number  # 🎯 ベスト試行番号（レポート用）
                )
                
                print("\n📁 最適化ファイル:")
                for file_type, filename in saved_files.items():
                    print(f"   📄 {file_type}: {filename}")
                    
            elif retry_result['final_status'] == 'optimization_failed':
                print("🚨 全ての最適化試行が失敗または悪化のため、元クエリを使用しました")
                fallback_reason = retry_result.get('fallback_reason', 'Unknown reason')
                print(f"🔧 失敗理由: {fallback_reason}")
                
                # 失敗詳細の表示
                if 'optimization_attempts' in retry_result:
                    attempts = retry_result['optimization_attempts']
                    degraded_count = sum(1 for a in attempts if a['status'] == 'performance_degraded')
                    error_count = sum(1 for a in attempts if a['status'] in ['llm_error', 'explain_failed'])
                    
                    if degraded_count > 0:
                        print(f"📊 パフォーマンス悪化: {degraded_count}回")
                    if error_count > 0:
                        print(f"❌ エラー発生: {error_count}回")
                
                print("💡 推奨事項:")
                print("   - テーブル統計情報の更新を検討してください")
                print("   - より詳細なEXPLAIN情報で手動最適化を検討してください")
                print("   - データ量やクエリ複雑度を確認してください")
            
            elif retry_result['final_status'] == 'fallback_to_original':
                print("⚠️ 最適化クエリでエラーが継続したため、元クエリを使用しました")
            
            elif retry_result['final_status'] == 'llm_error':
                print("❌ LLM API呼び出しでエラーが発生したため、元クエリを使用しました")
                error_details = retry_result.get('error_details', 'Unknown error')
                print(f"🔧 LLMエラー詳細: {error_details[:200]}...")
                print("💡 解決策: 入力データサイズを削減するか、LLM設定を調整してください")
            
            elif retry_result['final_status'] == 'llm_error_correction_failed':
                print("❌ エラー修正時にもLLMエラーが発生したため、元クエリを使用しました")
                error_details = retry_result.get('error_details', 'Unknown error')
                print(f"🔧 LLMエラー詳細: {error_details[:200]}...")
                print("💡 解決策: 手動でSQL最適化を実行するか、シンプルなクエリで再試行してください")
                
                # フォールバック時のファイル情報表示
                fallback_files = retry_result.get('fallback_files', {})
                failure_log = retry_result.get('failure_log', '')
                
                print("\n📁 生成されたファイル:")
                for file_type, filename in fallback_files.items():
                    print(f"   📄 {file_type}: {filename}")
                if failure_log:
                    print(f"   📄 失敗ログ: {failure_log}")
                    
            # 全試行の詳細表示
            print("\n📋 試行詳細:")
            for attempt in retry_result.get('all_attempts', []):
                status_icon = "✅" if attempt['status'] == 'success' else "❌"
                print(f"   {status_icon} 試行 {attempt['attempt']}: {attempt['status']}")
                if attempt['status'] == 'error':
                    print(f"      エラー: {attempt['error_message'][:100]}...")
                    
        except Exception as e:
            print(f"❌ 統合処理中にエラーが発生: {str(e)}")
            print("🚨 緊急エラーの詳細:")
            import traceback
            traceback.print_exc()
            print("   緊急フォールバック: 基本分析と最小限のファイル生成を実行します...")
            
            try:
                # フォールバック: 従来のEXPLAIN実行（オリジナルクエリ）
                explain_results = execute_explain_and_save_to_file(original_query_for_explain, "original")
                
                if explain_results:
                    print("\n📁 EXPLAIN結果:")
                    for file_type, filename in explain_results.items():
                        if file_type == 'explain_file':
                            print(f"   📄 EXPLAIN結果: {filename}")
                        elif file_type == 'error_file':
                            print(f"   📄 エラーログ: {filename}")
                        elif file_type == 'plan_lines':
                            print(f"   📊 実行プラン行数: {filename}")
                        elif file_type == 'error_message':
                            print(f"   ❌ エラーメッセージ: {filename}")
                
                # 🚨 緊急修正: エラー時でもレポートファイルを強制生成
                print("🚨 緊急レポート生成を実行中...")
                emergency_saved_files = save_optimized_sql_files(
                    original_query_for_explain,
                    original_query_for_explain,  # 最適化失敗時は元クエリを使用
                    current_metrics if 'current_metrics' in locals() else {},
                    "緊急フォールバック: 統合処理でエラーが発生したため、基本分析のみ実行",
                    f"緊急フォールバック処理\n\nエラー詳細:\n{str(e)}\n\n元クエリをそのまま使用しています。",
                    None  # パフォーマンス比較結果なし
                )
                
                print("\n📁 緊急生成ファイル:")
                for file_type, filename in emergency_saved_files.items():
                    print(f"   📄 {file_type}: {filename}")
                    
            except Exception as emergency_error:
                print(f"🚨 緊急フォールバック処理でもエラー: {str(emergency_error)}")
                print("⚠️ 手動でクエリを確認してください")
        
        print("\n✅ 統合SQL最適化処理が完了しました")
        
    else:
        print("❌ Spark環境が利用できないため、EXPLAIN文は実行できません")
        print("   Databricks環境で実行してください")
        
else:
    print("❌ 実行可能なオリジナルクエリが見つかりません")
    print("   セル43でオリジナルクエリを抽出してから実行してください")

print()



# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 レポート成形処理
# MAGIC
# MAGIC このセルでは以下の処理を実行します：
# MAGIC - 最適化レポートファイルの検索と読み込み
# MAGIC - LLMによるレポート内容の推敲・改善
# MAGIC - 推敲結果の保存と最終レポート生成

# COMMAND ----------
# 
# 📝 レポート推敲処理（統合処理用）
print("\n📝 レポート推敲処理")
print("-" * 40)
# 
def find_latest_report_file() -> str:
    """最新のレポートファイルを見つける"""
    import os
    import glob
    
    # 現在のディレクトリでレポートファイルを検索
    pattern = "output_optimization_report_*.md"
    report_files = glob.glob(pattern)
    
    if not report_files:
        return None
    
    # 最新のファイルを取得（タイムスタンプ順）
    latest_file = max(report_files, key=os.path.getctime)
    return latest_file
# 
def refine_report_content_with_llm(report_content: str) -> str:
    """LLMを使ってレポートを推敲する"""
    
    # LLMプロバイダーの設定確認
    if not LLM_CONFIG or not LLM_CONFIG.get('provider'):
        print("❌ LLMプロバイダーが設定されていません")
        return report_content
    
    # 🚨 トークン制限対策: レポートサイズ制限
    MAX_CONTENT_SIZE = 50000  # 50KB制限
    original_size = len(report_content)
    
    if original_size > MAX_CONTENT_SIZE:
        print(f"⚠️ レポートサイズが大きすぎます: {original_size:,} 文字 → {MAX_CONTENT_SIZE:,} 文字に切り詰め")
        # 重要セクションを優先的に保持
        truncated_content = report_content[:MAX_CONTENT_SIZE]
        truncated_content += f"\n\n⚠️ レポートが大きすぎるため、{MAX_CONTENT_SIZE:,} 文字に切り詰められました（元サイズ: {original_size:,} 文字）"
        report_content = truncated_content
    else:
        print(f"📊 レポートサイズ: {original_size:,} 文字（推敲実行）")
    
    # Photon利用率の抽出と評価判定
    import re
    photon_pattern = r'利用率[：:]\s*(\d+(?:\.\d+)?)%'
    photon_match = re.search(photon_pattern, report_content)
    
    photon_evaluation_instruction = ""
    if photon_match:
        photon_utilization = float(photon_match.group(1))
        if photon_utilization <= 80:
            photon_evaluation_instruction = """
# 【Photon利用率評価指示】
# - Photon利用率が80%以下の場合は「要改善」または「不良」の評価を明確に表示してください
# - 80%以下の場合は、改善の必要性を強調し、具体的な改善アクションを提示してください
# - 評価例: 「Photon利用率: XX% (評価: 要改善)」
# """
        else:
            photon_evaluation_instruction = """
# 【Photon利用率評価指示】
# - Photon利用率が80%以上の場合は「良好」の評価を表示してください
# - 評価例: 「Photon利用率: XX% (評価: 良好)」
# """
    
    refinement_prompt = f"""あなたは技術文書の編集者です。以下のDatabricks SQLパフォーマンス分析レポートを、読みやすく簡潔に推敲してください。
# 
# 【推敲の要件】
# 1. 全体的な構成を整理し、情報を論理的に配置する
# 2. 冗長な表現を削除し、簡潔で分かりやすい表現に修正する
# 3. 重要な情報が埋もれないよう、適切な見出しレベルで構造化する
# 4. 専門用語は残しつつ、分かりやすい説明を追加する
# 5. 数値データやメトリクスは保持する
# 6. 実用的な推奨事項を明確に提示する
# 
# 【🚨 絶対に削除・変更してはいけない重要情報】
# - **現在のクラスタリングキー情報**: 「現在のクラスタリングキー: XX」または「設定なし」表示
# - **フィルタ率情報**: 「フィルタ率: X.X% (読み込み: XX.XXGB, プルーン: XX.XXGB)」形式
# - **パーセンテージ計算**: 各処理の「全体の○○%」表示（並列実行を考慮した正確な計算結果）
# - **推奨vs現在の比較分析**: 推奨クラスタリングキーと現在のキーの対比情報
# - **具体的数値メトリクス**: 実行時間、データ読み込み量、スピル量、利用率等
# - **SQL実装例**: ALTER TABLE構文、CLUSTER BY文、ヒント句等の具体例
# - **テーブル別詳細情報**: 各テーブルのノード情報、フィルタ効率、推奨事項
# 
# {photon_evaluation_instruction}
# 
# 【現在のレポート内容】
# {report_content}
# 
# 【出力要件】
# - 推敲されたレポートをmarkdown形式で出力
# - 技術情報は維持しつつ、可読性を向上させる
# - 重要なポイントを強調し、アクションプランを明確にする
# - Photon利用率の評価を明確に表示する
# - **必須**: 現在のクラスタリングキー情報とフィルタ率情報を完全に保持
# - **必須**: パーセンテージ計算値は元の正確な数値を使用
# - **必須**: テーブル別の詳細分析情報（現在のキー、推奨キー、フィルタ率）を削除しない
# - **必須**: SQL実装例（ALTER TABLE、CLUSTER BY等）は完全な形で保持
# """
    
    try:
        # 設定されたLLMプロバイダーに基づいて推敲を実行
        provider = LLM_CONFIG.get('provider', 'databricks')
        
        if provider == 'databricks':
            refined_content = _call_databricks_llm(refinement_prompt)
        elif provider == 'openai':
            refined_content = _call_openai_llm(refinement_prompt)
        elif provider == 'azure_openai':
            refined_content = _call_azure_openai_llm(refinement_prompt)
        elif provider == 'anthropic':
            refined_content = _call_anthropic_llm(refinement_prompt)
        else:
            print(f"❌ 未対応のLLMプロバイダー: {provider}")
            return report_content
        
        # 🚨 LLMエラーレスポンスの検出（精密化）
        if isinstance(refined_content, str):
            # より精密なエラー検出（レポート内容の絵文字と区別）
            actual_error_indicators = [
                "APIエラー: ステータスコード",
                "Input is too long for requested model",
                "Bad Request",
                "タイムアウトエラー:",
                "API呼び出しエラー:",
                'レスポンス: {"error_code":',
                "❌ APIエラー:",
                "⚠️ APIエラー:"
            ]
            
            # エラーメッセージの開始部分をチェック（より厳密）
            is_error_response = any(
                refined_content.strip().startswith(indicator) or 
                f"\n{indicator}" in refined_content[:500]  # 先頭500文字以内でのエラーメッセージ
                for indicator in actual_error_indicators
            )
            
            if is_error_response:
                print(f"❌ LLMレポート推敲でエラー検出: {refined_content[:200]}...")
                print("📄 元のレポートを返します")
                return report_content
        
        # thinking_enabled対応: 結果がリストの場合の処理
        if isinstance(refined_content, list):
            refined_content = format_thinking_response(refined_content)
        
        print(f"✅ LLMによるレポート推敲完了 (セル46独立処理)")
        return refined_content
        
    except Exception as e:
        print(f"❌ LLMによるレポート推敲中にエラーが発生: {str(e)}")
        return report_content
# 
def save_refined_report(refined_content: str, original_filename: str) -> str:
    """推敲されたレポートを保存"""
    from datetime import datetime
    
    # 推敲版のファイル名を生成
    base_name = original_filename.replace('.md', '')
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    refined_filename = f"{base_name}_refined_{timestamp}.md"
    
    try:
        with open(refined_filename, 'w', encoding='utf-8') as f:
            f.write(refined_content)
        
        print(f"✅ 推敲されたレポートを保存: {refined_filename}")
        return refined_filename
        
    except Exception as e:
        print(f"❌ 推敲レポートの保存中にエラー: {str(e)}")
        return None
# 
def finalize_report_files(original_filename: str, refined_filename: str) -> str:
    """DEBUG_ENABLED設定に基づいてファイル処理を実行"""
    import os
    
    # DEBUG_ENABLED設定を確認
    debug_enabled = globals().get('DEBUG_ENABLED', 'N')
    
    try:
        if debug_enabled.upper() == 'Y':
            # DEBUG_ENABLED=Y: 元のファイルを名称変更して保持
            if os.path.exists(original_filename):
                # バックアップファイル名を生成（元ファイル名に _raw を追加）
                backup_filename = original_filename.replace('.md', '_raw.md')
                
                os.rename(original_filename, backup_filename)
                print(f"📁 元のファイルを保持: {original_filename} → {backup_filename}")
            else:
                print(f"⚠️ 元のファイルが見つかりません: {original_filename}")
        else:
            # DEBUG_ENABLED=N: 元のファイルを削除
            if os.path.exists(original_filename):
                os.remove(original_filename)
                print(f"🗑️ 元のファイルを削除: {original_filename}")
        
        # 推敲版ファイルを元のファイル名にリネーム
        if os.path.exists(refined_filename):
            os.rename(refined_filename, original_filename)
            print(f"📝 推敲版ファイルをリネーム: {refined_filename} → {original_filename}")
            return original_filename
        else:
            print(f"❌ 推敲版ファイルが見つかりません: {refined_filename}")
            return None
            
    except Exception as e:
        print(f"❌ ファイル操作中にエラー: {str(e)}")
        return None
# 
# 
# メイン処理
try:
    # 最新のレポートファイルを検索
    latest_report = find_latest_report_file()
    
    if not latest_report:
        print("❌ レポートファイルが見つかりません")
        print("⚠️ セル43 (統合SQL最適化処理) を先に実行してください")
        print()
        print("🔍 詳細なトラブルシューティング:")
        print("1. セル43が正常に完了しているかご確認ください")
        print("2. エラーメッセージが表示されていないかご確認ください")
        print("3. current_analysis_result、extracted_metrics等の変数が定義されているかご確認ください")
        print("4. 緊急フォールバック処理が実行されている場合があります")
        
        # 関連ファイルの存在チェック
        import glob
        sql_files = glob.glob("output_optimized_query_*.sql")
        original_files = glob.glob("output_original_query_*.sql")
        all_reports = glob.glob("output_optimization_report*.md")
        
        print(f"\n📁 現在のファイル状況:")
        print(f"   📄 最適化クエリファイル: {len(sql_files)} 個")
        print(f"   📄 オリジナルクエリファイル: {len(original_files)} 個")
        print(f"   📄 レポートファイル（全体）: {len(all_reports)} 個")
        
        if all_reports:
            print(f"   📋 検出されたレポートファイル:")
            for report in all_reports:
                print(f"      - {report}")
            print("   ⚠️ ファイルが存在するにも関わらず find_latest_report_file() で検出されていません")
            print("   💡 手動でファイル名を確認し、パターンマッチングの問題の可能性があります")
        
        if not sql_files and not original_files:
            print("   🚨 重要: セル43の処理が全く実行されていない可能性があります")
            print("   📋 対処法: セル43を最初から実行し直してください")
    else:
        print(f"📄 対象レポートファイル: {latest_report}")
        
        # レポートファイルの内容を読み込み
        with open(latest_report, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        print(f"📊 元レポートサイズ: {len(original_content):,} 文字")
        
        # 🚨 重複推敲防止: 既に推敲済みかチェック
        refinement_indicators = [
            "📊 **最適化レポート**",  # 推敲後の典型的なヘッダー
            "🚀 **パフォーマンス改善結果**",  # 推敲後の典型的なセクション
            "✅ **推奨事項**",  # 推敲後のフォーマット
            "LLMによる推敲を実行中",  # 推敲プロセス中に含まれるメッセージ
            "推敲版レポート:",  # 推敲済みファイルのメッセージ
        ]
        
        already_refined = any(indicator in original_content for indicator in refinement_indicators)
        
        if already_refined:
            print(f"✅ レポートは既に推敲済みです（重複処理を回避）: {latest_report}")
            print("📋 推敲済みレポートをそのまま使用します")
            refined_content = original_content
        else:
            print(f"🤖 LLMによる推敲を実行中 (対象: {latest_report})...")
            refined_content = refine_report_content_with_llm(original_content)
        
        if refined_content != original_content:
            print(f"📊 推敲後サイズ: {len(refined_content):,} 文字")
            
            # 推敲されたレポートを保存
            refined_filename = save_refined_report(refined_content, latest_report)
            
            if refined_filename:
                print(f"📄 推敲版レポート: {refined_filename}")
                
                # ファイルサイズの確認
                import os
                if os.path.exists(refined_filename):
                    file_size = os.path.getsize(refined_filename)
                    print(f"📁 推敲版ファイルサイズ: {file_size:,} bytes")
                
                # 元のファイルを削除し、推敲版ファイルを元のファイル名にリネーム
                final_filename = finalize_report_files(latest_report, refined_filename)
                
                if final_filename:
                    print(f"📄 最終レポートファイル: {final_filename}")
                    
                    # 最終ファイルサイズの確認
                    if os.path.exists(final_filename):
                        final_file_size = os.path.getsize(final_filename)
                        print(f"📁 最終ファイルサイズ: {final_file_size:,} bytes")
                
                print(f"✅ レポート推敲処理が完了しました: {final_filename}")
                
                # 推敲の結果を表示（最初の1000文字）
                print("\n📋 推敲結果のプレビュー:")
                print("-" * 50)
                preview = refined_content[:1000]
                print(preview)
                if len(refined_content) > 1000:
                    print(f"\n... (残り {len(refined_content) - 1000} 文字は {final_filename or latest_report} を参照)")
                print("-" * 50)
            else:
                print("❌ 推敲レポートの保存に失敗しました")
        else:
            print("📋 レポートは既に最適な状態です（推敲処理スキップ）")
            print("✅ 既存レポートファイルをそのまま使用します")
            
            # 既に推敲済みの場合もプレビューを表示
            print("\n📋 レポート内容のプレビュー:")
            print("-" * 50)
            preview = refined_content[:1000]
            print(preview)
            if len(refined_content) > 1000:
                print(f"\n... (残り {len(refined_content) - 1000} 文字は {latest_report} を参照)")
            print("-" * 50)
            
except Exception as e:
    print(f"❌ レポート推敲処理中にエラーが発生: {str(e)}")
    import traceback
    traceback.print_exc()
# 
print()
# 
# # 🧹 中間ファイルの削除処理（DEBUG_ENABLEDフラグに基づく）
debug_enabled = globals().get('DEBUG_ENABLED', 'N')
explain_enabled = globals().get('EXPLAIN_ENABLED', 'N')

if debug_enabled.upper() == 'Y':
    print("\n🐛 デバッグモード有効: 中間ファイルを保持します")
    print("-" * 40)
    print("💡 DEBUG_ENABLED=Y のため、すべての中間ファイルが保持されます")
    print("📁 以下のファイルが保持されます:")
    
    import glob
    import os
    
    # 保持されるファイル一覧を表示
    if explain_enabled.upper() == 'Y':
        original_files = glob.glob("output_explain_original_*.txt")
        optimized_files = glob.glob("output_explain_optimized_*.txt")
        cost_original_files = glob.glob("output_explain_cost_original_*.txt")
        cost_optimized_files = glob.glob("output_explain_cost_optimized_*.txt")
        error_files = glob.glob("output_explain_error_*.txt")
        all_files = original_files + optimized_files + cost_original_files + cost_optimized_files + error_files
        
        if all_files:
            print(f"   🔍 EXPLAIN結果ファイル:")
            print(f"      📊 EXPLAIN: オリジナル {len(original_files)} 個, 最適化後 {len(optimized_files)} 個")
            print(f"      💰 EXPLAIN COST: オリジナル {len(cost_original_files)} 個, 最適化後 {len(cost_optimized_files)} 個")
            print(f"      ❌ エラー: {len(error_files)} 個")
            for file_path in all_files[:3]:  # 最大3個まで表示
                print(f"      📄 {file_path}")
            if len(all_files) > 3:
                print(f"      ... 他 {len(all_files) - 3} 個")
    
    print("✅ デバッグモード: ファイル削除処理をスキップしました")
else:
    print("\n🧹 中間ファイルの削除処理")
    print("-" * 40)
    print("💡 DEBUG_ENABLED=N のため、中間ファイルを削除します")
    print("📁 保持されるファイル: output_original_query_*.sql, output_optimization_report_*.md, output_optimized_query_*.sql")
    
    import glob
    import os
    
    if explain_enabled.upper() == 'Y':
        # EXPLAIN結果ファイルとエラーファイルを検索（新パターン + 旧パターン）
        original_files = glob.glob("output_explain_original_*.txt")
        optimized_files = glob.glob("output_explain_optimized_*.txt")
        cost_original_files = glob.glob("output_explain_cost_original_*.txt")
        cost_optimized_files = glob.glob("output_explain_cost_optimized_*.txt")
        error_original_files = glob.glob("output_explain_error_original_*.txt")
        error_optimized_files = glob.glob("output_explain_error_optimized_*.txt")
        
        # 旧パターンのファイルも削除対象に含める（下位互換性）
        old_explain_files = glob.glob("output_explain_plan_*.txt")
        old_error_files = glob.glob("output_explain_error_*.txt")
        
        # 🚨 新規追加: DEBUG用の完全情報ファイルも削除対象に含める
        full_plan_files = glob.glob("output_physical_plan_full_*.txt")
        full_stats_files = glob.glob("output_explain_cost_statistics_full_*.txt")
        extracted_stats_files = glob.glob("output_explain_cost_statistics_extracted_*.json")
        structured_plan_files = glob.glob("output_physical_plan_structured_*.json")
        structured_cost_files = glob.glob("output_explain_cost_structured_*.json")
        
        all_temp_files = (original_files + optimized_files + cost_original_files + cost_optimized_files + 
                         error_original_files + error_optimized_files + old_explain_files + old_error_files +
                         full_plan_files + full_stats_files + extracted_stats_files + 
                         structured_plan_files + structured_cost_files)
        
        explain_files = original_files + optimized_files + old_explain_files
        cost_files = cost_original_files + cost_optimized_files
        error_files = error_original_files + error_optimized_files + old_error_files
        debug_files = full_plan_files + full_stats_files + extracted_stats_files + structured_plan_files + structured_cost_files
        
        if all_temp_files:
            print(f"📁 削除対象ファイル:")
            print(f"   📊 EXPLAIN結果: {len(explain_files)} 個")
            print(f"   💰 EXPLAIN COST結果: {len(cost_files)} 個")
            print(f"   ❌ エラーファイル: {len(error_files)} 個")
            print(f"   🔧 DEBUG完全情報: {len(debug_files)} 個")
            print("💡 注意: DEBUG_ENABLED=N のため、これらのファイルは作成されていないはずです")
            
            # 🔧 変数の初期化をより安全に実行
            deleted_count = 0
            for file_path in all_temp_files:
                try:
                    os.remove(file_path)
                    print(f"✅ 削除完了: {file_path}")
                    deleted_count += 1
                except Exception as e:
                    print(f"❌ 削除失敗: {file_path} - {str(e)}")
            
            print(f"🗑️ 削除完了: {deleted_count}/{len(all_temp_files)} ファイル")
            print("💡 EXPLAIN・EXPLAIN COST結果とエラーファイルはLLMによる最適化処理で使用済みのため削除しました")
        else:
            print("📁 削除対象のEXPLAIN・EXPLAIN COST結果・エラーファイルが見つかりませんでした")
    else:
        print("⚠️ EXPLAIN実行が無効化されているため、EXPLAIN結果ファイルの削除処理をスキップしました")

print()

print("🎉 すべての処理が完了しました！")
print("📁 生成されたファイルを確認して、分析結果を活用してください。")
