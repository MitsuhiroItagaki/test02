# レポート推敲処理のトークン制限対策テスト
import sys
sys.path.append('.')

# 必要な関数をインポート（モック版）
def format_thinking_response(content):
    return str(content)

# モック関数
def _call_databricks_llm(prompt):
    # プロンプトサイズチェック
    if len(prompt) > 100000:  # 100KB以上でエラーシミュレーション
        return 'APIエラー: ステータスコード 400\nレスポンス: {"error_code":"BAD_REQUEST","message":"{\\"message\\":\\"Input is too long for requested model.\\"}"}'
    return "推敲されたレポート内容（モック）"

# LLM設定（モック）
LLM_CONFIG = {
    "provider": "databricks"
}

def refine_report_with_llm_test(raw_report: str, query_id: str) -> str:
    """
    修正版のrefine_report_with_llm関数（テスト用）
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
    
    refinement_prompt = f"""技術文書の編集者として、Databricks SQLパフォーマンス分析レポートを推敲してください。

【現在のレポート】
```
{raw_report}
```

上記のレポートを推敲し、技術情報を完全に保持したレポートを出力してください。
"""
    
    try:
        provider = LLM_CONFIG.get("provider", "databricks")
        
        if provider == "databricks":
            refined_report = _call_databricks_llm(refinement_prompt)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
        
        # 🚨 LLMエラーレスポンスの検出
        if isinstance(refined_report, str):
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
            
            is_error_response = any(indicator in refined_report for indicator in error_indicators)
            
            if is_error_response:
                print(f"❌ LLMレポート推敲でエラー検出: {refined_report[:200]}...")
                print("📄 元のレポートを返します")
                return raw_report
        
        print("✅ LLMによるレポート推敲完了")
        return refined_report
        
    except Exception as e:
        print(f"⚠️ LLMによるレポート推敲中にエラーが発生しました: {str(e)}")
        print("📄 元のレポートを返します")
        return raw_report


def test_report_refinement():
    """レポート推敲処理のテスト"""
    
    print("🧪 レポート推敲処理のトークン制限対策テスト")
    print("=" * 60)
    
    # テストケース1: 小サイズレポート（正常処理）
    print("\n📊 テストケース1: 小サイズレポート")
    small_report = "# SQL最適化レポート\n\n## ボトルネック分析\nテスト内容" * 100  # 約5KB
    result1 = refine_report_with_llm_test(small_report, "test1")
    
    print(f"入力サイズ: {len(small_report):,} 文字")
    print(f"出力サイズ: {len(result1):,} 文字")
    print(f"結果: {'✅ 成功' if '推敲されたレポート内容' in result1 else '❌ 失敗'}")
    
    # テストケース2: 大サイズレポート（切り詰め処理）
    print("\n📊 テストケース2: 大サイズレポート（切り詰め）")
    large_report = "# SQL最適化レポート\n\n## ボトルネック分析\n詳細なテスト内容" * 2000  # 約100KB
    result2 = refine_report_with_llm_test(large_report, "test2")
    
    print(f"入力サイズ: {len(large_report):,} 文字")
    print(f"出力サイズ: {len(result2):,} 文字")
    print(f"結果: {'✅ 成功' if '推敲されたレポート内容' in result2 else '❌ 失敗'}")
    
    # テストケース3: 超大サイズレポート（エラー処理）
    print("\n📊 テストケース3: 超大サイズレポート（LLMエラー処理）")
    huge_report = "# SQL最適化レポート\n\n## ボトルネック分析\n超詳細なテスト内容" * 5000  # 約250KB
    result3 = refine_report_with_llm_test(huge_report, "test3")
    
    print(f"入力サイズ: {len(huge_report):,} 文字")
    print(f"出力サイズ: {len(result3):,} 文字")
    expected_result = huge_report[:50000] + f"\n\n⚠️ レポートが大きすぎるため、50,000 文字に切り詰められました（元サイズ: {len(huge_report):,} 文字）"
    success = result3 == expected_result
    print(f"結果: {'✅ 成功（元レポート返却）' if success else '❌ 失敗'}")
    
    print(f"\n🎉 テスト完了!")
    print(f"💡 効果: 大容量レポートでもLLMトークン制限エラーが回避されます")
    
    return True


if __name__ == "__main__":
    test_report_refinement() 