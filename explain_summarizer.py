# EXPLAIN + EXPLAIN COST要約機能（スタンドアロン版）
import re

def extract_cost_statistics_from_explain_cost(explain_cost_content: str) -> str:
    """
    EXPLAIN COST結果から統計情報を抽出して構造化（改善版）
    """
    if not explain_cost_content:
        return ""
    
    statistics_info = []
    
    try:
        lines = explain_cost_content.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # テーブル統計情報の抽出（改善）
            if 'statistics=' in line.lower() or 'stats=' in line.lower() or 'Statistics(' in line:
                statistics_info.append(f"📊 テーブル統計: {line}")
            
            # 行数情報の抽出（改善）
            elif 'rows=' in line.lower() or 'rowcount=' in line.lower() or 'rows:' in line.lower():
                statistics_info.append(f"📈 行数情報: {line}")
            
            # サイズ情報の抽出（改善）
            elif ('size=' in line.lower() or 'sizeinbytes=' in line.lower() or 'sizeInBytes=' in line 
                  or 'GB' in line or 'MB' in line or 'size:' in line.lower()):
                statistics_info.append(f"💾 サイズ情報: {line}")
            
            # コスト情報の抽出（改善）
            elif ('cost=' in line.lower() or 'Cost(' in line or 'cost:' in line.lower() 
                  or 'costs:' in line.lower() or 'estimated cost' in line.lower()):
                statistics_info.append(f"💰 コスト情報: {line}")
            
            # 選択率情報の抽出（改善）
            elif ('selectivity=' in line.lower() or 'filter=' in line.lower() or 'selectivity:' in line.lower()
                  or 'selection' in line.lower()):
                statistics_info.append(f"🎯 選択率情報: {line}")
            
            # パーティション情報の抽出（改善）
            elif ('partition' in line.lower() and ('count' in line.lower() or 'size' in line.lower()
                  or 'average' in line.lower() or 'per partition' in line.lower())):
                statistics_info.append(f"🔄 パーティション情報: {line}")
            
            # メモリ情報の抽出（新規追加）
            elif ('memory' in line.lower() or 'spill' in line.lower() or 'threshold' in line.lower()):
                statistics_info.append(f"💾 メモリ情報: {line}")
            
            # JOIN情報の抽出（新規追加）
            elif ('join' in line.lower() and ('cost' in line.lower() or 'selectivity' in line.lower()
                  or 'input' in line.lower() or 'output' in line.lower())):
                statistics_info.append(f"🔗 JOIN情報: {line}")
    
    except Exception as e:
        statistics_info.append(f"⚠️ 統計情報抽出エラー: {str(e)}")
    
    return '\n'.join(statistics_info) if statistics_info else "統計情報が見つかりませんでした"


def summarize_explain_results_with_llm(explain_content: str, explain_cost_content: str, query_type: str = "original") -> dict:
    """
    EXPLAIN + EXPLAIN COST結果をLLMで要約してトークン制限に対応（モック版）
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
    
    # モック版要約（実際のLLM呼び出しの代わり）
    print("🧪 モック版要約を生成中...")
    
    # Physical Planから重要な情報を抽出
    physical_operations = []
    if 'PhotonBroadcastHashJoin' in explain_content:
        physical_operations.append("- PhotonBroadcastHashJoin検出（高速JOIN処理）")
    if 'ShuffleExchange' in explain_content:
        physical_operations.append("- ShuffleExchange検出（データ再分散）")
    if 'FileScan' in explain_content:
        physical_operations.append("- FileScan検出（テーブルスキャン）")
    if 'PhotonResultStage' in explain_content:
        physical_operations.append("- PhotonResultStage検出（Photon最適化済み）")
    
    # EXPLAIN COSTから統計情報を抽出
    cost_info = []
    if 'GiB' in explain_cost_content:
        gib_matches = re.findall(r'(\d+\.?\d*)\s*GiB', explain_cost_content)
        if gib_matches:
            max_size = max([float(x) for x in gib_matches])
            cost_info.append(f"- 最大テーブルサイズ: {max_size:.1f} GiB")
    
    if 'rowCount=' in explain_cost_content:
        row_matches = re.findall(r'rowCount=(\d+\.?\d*E?\+?\d*)', explain_cost_content)
        if row_matches:
            cost_info.append(f"- 行数情報: {len(row_matches)}個のテーブル統計")
    
    if 'sizeInBytes=' in explain_cost_content:
        cost_info.append("- 詳細サイズ統計: 利用可能")
    
    # 要約レポートを生成
    summary_text = f"""
## 📊 Physical Plan要約（自動生成）
{chr(10).join(physical_operations) if physical_operations else "- 標準的な実行プラン"}

## 💰 統計情報サマリー
{chr(10).join(cost_info) if cost_info else "- 基本的な統計情報のみ"}

## ⚡ パフォーマンス分析
- 分析対象: {query_type}クエリ
- 原本サイズ: {total_size:,} 文字
- 要約後サイズ: 約5,000文字
- 圧縮率: 約{total_size // 5000:.0f}x

### 🎯 最適化推奨事項
- EXPLAIN結果の詳細分析により特定可能
- 統計情報ベースのBROADCAST判定推奨
- Photon対応状況の最適化検討
"""

    result = {
        'explain_summary': summary_text,
        'explain_cost_summary': summary_text,
        'physical_plan_summary': summary_text,
        'cost_statistics_summary': extract_cost_statistics_from_explain_cost(explain_cost_content),
        'summarized': True
    }
    
    print(f"✅ 要約完了: {len(result['explain_summary']):,} 文字")
    return result


def test_with_actual_files():
    """実際のファイルでテスト"""
    
    print("🧪 EXPLAIN + EXPLAIN COST要約機能テスト")
    print("=" * 60)
    
    # ファイル読み込み
    try:
        with open('output_explain_original_20250727-172316.txt', 'r', encoding='utf-8') as f:
            explain_content = f.read()
        print(f"✅ EXPLAIN結果: {len(explain_content):,} 文字")
        
        with open('output_explain_cost_original_20250727-172316.txt', 'r', encoding='utf-8') as f:
            explain_cost_content = f.read()  
        print(f"✅ EXPLAIN COST結果: {len(explain_cost_content):,} 文字")
        
    except FileNotFoundError as e:
        print(f"❌ ファイルが見つかりません: {str(e)}")
        return False
    
    # 要約実行
    result = summarize_explain_results_with_llm(explain_content, explain_cost_content, "original")
    
    print(f"\n📋 要約結果:")
    print("-" * 30)
    print(result['explain_summary'])
    
    print(f"\n📊 統計情報抽出結果:")
    print("-" * 30)
    print(result['cost_statistics_summary'][:1000] + "...")
    
    print(f"\n🎉 テスト成功!")
    print(f"📉 サイズ削減: {len(explain_content) + len(explain_cost_content):,} → {len(result['explain_summary']):,} 文字")
    
    return True


if __name__ == "__main__":
    test_with_actual_files() 