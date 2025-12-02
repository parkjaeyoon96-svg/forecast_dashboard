"""
JSON 파일을 읽어 AI 분석을 생성하는 스크립트

사용법:
    python scripts/generate_ai_insights.py --date 20251124 --brand MLB
    python scripts/generate_ai_insights.py --date 20251124 --all-brands

환경 변수:
    OPENAI_API_KEY: OpenAI API 키 (선택사항, 없으면 로컬 분석만 수행)
"""

import os
import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# 프로젝트 루트 디렉토리를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    # Windows 콘솔 인코딩 문제 방지
    try:
        print("Warning: OpenAI package not installed. Using local analysis only.")
    except:
        pass

# 브랜드 코드 매핑
BRAND_CODE_MAP = {
    'MLB': 'M',
    'MLB_KIDS': 'I',
    'DISCOVERY': 'X',
    'DUVETICA': 'V',
    'SERGIO': 'ST',
    'SUPRA': 'W'
}

BRAND_NAME_MAP = {v: k for k, v in BRAND_CODE_MAP.items()}

class AIInsightGenerator:
    """AI 인사이트 생성기"""
    
    def __init__(self, api_key: Optional[str] = None, use_local: bool = False):
        self.use_openai = OPENAI_AVAILABLE and api_key and not use_local
        if self.use_openai:
            self.client = OpenAI(api_key=api_key)
            self.model = "gpt-4"
        else:
            self.client = None
            print("[INFO] OpenAI API를 사용하지 않습니다. 로컬 분석만 수행합니다.")
    
    def generate_insight(self, data: Dict, context: str, analysis_type: str) -> str:
        """AI 인사이트 생성"""
        
        if self.use_openai:
            return self._generate_with_openai(data, context, analysis_type)
        else:
            return self._generate_local_analysis(data, context, analysis_type)
    
    def _generate_with_openai(self, data: Dict, context: str, analysis_type: str) -> str:
        """OpenAI API를 사용한 분석"""
        try:
            prompt = self._build_prompt(data, context, analysis_type)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 매출 데이터 분석 전문가입니다. 다음 데이터를 분석하여 실용적이고 실행 가능한 인사이트를 제공해주세요. 한국어로 응답해주세요."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.7,
                max_tokens=2000
            )
            
            return response.choices[0].message.content
        except Exception as e:
            print(f"❌ OpenAI API 오류: {e}")
            return self._generate_local_analysis(data, context, analysis_type)
    
    def _generate_local_analysis(self, data: Dict, context: str, analysis_type: str) -> str:
        """로컬 분석 (규칙 기반)"""
        if analysis_type == "pl":
            return self._analyze_pl_local(data)
        elif analysis_type == "treemap":
            return self._analyze_treemap_local(data)
        elif analysis_type == "radar":
            return self._analyze_radar_local(data)
        elif analysis_type == "channel_pl":
            return self._analyze_channel_pl_local(data)
        elif analysis_type == "weekly":
            return self._analyze_weekly_local(data)
        elif analysis_type == "inventory":
            return self._analyze_inventory_local(data)
        elif analysis_type == "sale_rate":
            return self._analyze_sale_rate_local(data)
        elif analysis_type == "overview":
            return self._analyze_overview_local(data)
        else:
            return "분석 유형을 인식할 수 없습니다."
    
    def _build_prompt(self, data: Dict, context: str, analysis_type: str) -> str:
        """프롬프트 생성"""
        prompts = {
            "pl": f"""
다음 손익계산서 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 매출 목표 대비 달성률 및 전년 대비 성장률
2. 할인율 관리 상태
3. 직접비 효율성
4. 영업이익 달성률
5. 개선이 필요한 영역 및 제안사항
""",
            "treemap": f"""
다음 채널별/아이템별 매출구성 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 채널별 매출 비중 및 집중도
2. 아이템별 매출 비중 및 다양성
3. 주요 채널/아이템의 성과
4. 개선이 필요한 영역
""",
            "radar": f"""
다음 매출 계획/전년비 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 채널별 목표 대비 달성률
2. 전년 대비 성장률
3. 우수 성과 채널 및 개선 필요 채널
4. 성장 동력 분석
""",
            "channel_pl": f"""
다음 주요 채널별 손익 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 채널별 매출 및 수익성
2. 매출총이익률 분석
3. 고수익 채널 및 저수익 채널
4. 수익성 개선 방안
""",
            "weekly": f"""
다음 주차별 매출 추세 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 주차별 매출 추세
2. 전년 대비 성장률
3. 최근 추세 변화
4. 예상 전망
""",
            "inventory": f"""
다음 재고주수 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 재고주수 높은 상품
2. 전년 대비 재고 변화
3. 재고 관리 개선 필요 상품
4. 재고 최적화 방안
""",
            "sale_rate": f"""
다음 판매율 데이터를 분석하여 인사이트를 도출해주세요:

브랜드: {context}
데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 평균 판매율
2. 판매율 높은/낮은 상품
3. 전년 대비 판매율 변화
4. 판매율 개선 방안
""",
            "overview": f"""
다음 전체 현황 데이터를 분석하여 인사이트를 도출해주세요:

데이터:
{json.dumps(data, indent=2, ensure_ascii=False)}

다음 관점에서 분석해주세요:
1. 전체 매출 목표 대비 달성률 및 전년 대비 성장률
2. 브랜드별 기여도 및 성과
3. 직접이익 및 영업이익 달성률
4. 주요 리스크 요소 및 개선 방안
5. 월말 예상 전망
"""
        }
        return prompts.get(analysis_type, "분석할 데이터를 제공해주세요.")
    
    def _analyze_pl_local(self, data: Dict) -> str:
        """손익계산서 로컬 분석"""
        insight = "<strong>📊 손익계산서 분석</strong><br>"
        
        revenue = data.get("revenue", {})
        if revenue:
            forecast = revenue.get("forecast", 0) / 100
            target = revenue.get("target", 0) / 100
            achievement = revenue.get("achievement", 0)
            yoy = revenue.get("yoy", 0)
            
            insight += f"• 실판매액 <strong>{forecast:.2f}억원</strong>으로 목표 대비 <strong>{achievement}%</strong> 달성률을 보이고 있습니다.<br>"
            if yoy > 0:
                insight += f"• 전년 대비 <strong>{yoy}%</strong> 성장을 기록하여 {'안정적인' if yoy >= 110 else '소폭' if yoy >= 100 else '감소하는'} 성장세를 유지하고 있습니다.<br>"
        
        discount_rate = data.get("discountRate", {})
        if discount_rate:
            forecast_discount = discount_rate.get("forecast", 0)
            target_discount = discount_rate.get("target", 0)
            if forecast_discount > target_discount:
                insight += f"• 할인율 <strong>{forecast_discount}%</strong>는 목표 <strong>{target_discount}%</strong> 대비 높은 수준으로 할인율 관리 개선이 필요하며,<br>"
            else:
                insight += f"• 할인율 <strong>{forecast_discount}%</strong>는 목표 <strong>{target_discount}%</strong> 대비 양호한 수준입니다.<br>"
        
        direct_profit = data.get("directProfit", {})
        if direct_profit:
            achievement = direct_profit.get("achievement", 0)
            insight += f"• 직접비 효율이 목표 대비 <strong>{achievement}%</strong>로 {'양호한' if achievement >= 95 else '개선이 필요한'} 비용 관리를 보이고 있습니다.<br>"
        
        op_profit = data.get("opProfit", {})
        if op_profit:
            forecast = op_profit.get("forecast", 0) / 100
            achievement = op_profit.get("achievement", 0)
            insight += f"• 영업이익은 <strong>{forecast:.2f}억원</strong>으로 목표 대비 <strong>{achievement}%</strong> 달성률을 기록했습니다.<br>"
        
        return insight
    
    def _analyze_treemap_local(self, data: Dict) -> str:
        """트리맵 로컬 분석 (브랜드별 데이터 기반) - 인사이트 및 액션 포함"""
        insight = ""
        
        # 브랜드별 데이터 구조 확인
        brand_data = None
        if "byBrand" in data.get("channelTreemapData", {}):
            by_brand = data["channelTreemapData"]["byBrand"]
            if by_brand:
                brand_code = list(by_brand.keys())[0] if isinstance(by_brand, dict) else None
                if brand_code and brand_code in by_brand:
                    brand_data = by_brand[brand_code]
        elif "channel" in data:
            brand_data = data
        
        channel_insights = []
        if brand_data and "channel" in brand_data:
            channels = brand_data["channel"].get("channels", {})
            if channels:
                channel_totals = {}
                channel_discounts = {}
                for channel, channel_data in channels.items():
                    if channel_data and isinstance(channel_data, dict) and "sales" in channel_data:
                        channel_totals[channel] = channel_data["sales"] / 100000000
                        channel_discounts[channel] = channel_data.get("discountRate", 0)
            
            if channel_totals:
                sorted_channels = sorted(channel_totals.items(), key=lambda x: x[1], reverse=True)
                total = sum(channel_totals.values())
                
                if sorted_channels:
                    top_channel = sorted_channels[0]
                    percentage = (top_channel[1] / total * 100) if total > 0 else 0
                    top_discount = channel_discounts.get(top_channel[0], 0)
                    
                    # 채널별 인사이트 생성
                    channel_name = top_channel[0]
                    if channel_name in ["면세점", "백화점"]:
                        insight_text = f"• <strong>{channel_name}</strong>이(가) 전체 매출의 <strong>{percentage:.1f}%</strong>를 차지하며 프리미엄 채널로서 브랜드 포지셔닝에 기여하고 있습니다. ({top_channel[1]:.1f}억원)"
                        if top_discount < 25:
                            insight_text += f" 할인율 <strong>{top_discount:.1f}%</strong>로 프리미엄 이미지 유지에 유리합니다."
                        insight_text += "<br>"
                        channel_insights.append(insight_text)
                    elif channel_name in ["대리점", "제휴몰"]:
                        insight_text = f"• <strong>{channel_name}</strong>이(가) 전체 매출의 <strong>{percentage:.1f}%</strong>를 차지하며 주요 유통 채널로 자리잡고 있습니다. ({top_channel[1]:.1f}억원)"
                        if top_discount > 30:
                            insight_text += f" 다만 할인율 <strong>{top_discount:.1f}%</strong>로 수익성 관리가 필요합니다."
                        insight_text += "<br>"
                        channel_insights.append(insight_text)
                    else:
                        insight_text = f"• <strong>{channel_name}</strong>이(가) 전체 매출의 <strong>{percentage:.1f}%</strong>를 차지하며 주요 채널로 성장하고 있습니다. ({top_channel[1]:.1f}억원)<br>"
                        channel_insights.append(insight_text)
                    
                    if len(sorted_channels) > 1:
                        second_channel = sorted_channels[1]
                        second_percentage = (second_channel[1] / total * 100) if total > 0 else 0
                        channel_insights.append(f"• <strong>{second_channel[0]}</strong>이(가) <strong>{second_percentage:.1f}%</strong>로 두 번째로 높은 비중을 차지합니다. ({second_channel[1]:.1f}억원)<br>")
                    
                    # 채널 집중도 분석 및 액션
                    if len(sorted_channels) >= 3:
                        top3_total = sum([ch[1] for ch in sorted_channels[:3]])
                        top3_share = (top3_total / total * 100) if total > 0 else 0
                        if top3_share > 70:
                            channel_insights.append(f"• 상위 3개 채널이 전체의 <strong>{top3_share:.1f}%</strong>를 차지하여 채널 집중도가 높습니다. 채널 다양화를 통해 리스크 분산이 필요합니다.<br>")
                    
                    # 액션 아이템 추가
                    if percentage > 40:
                        channel_insights.append(f"<strong>💡 액션:</strong> {channel_name}에 과도하게 의존하고 있어 채널 다각화 전략 수립이 필요합니다. 백화점, 대리점 등 다른 채널의 성장을 위한 마케팅 및 입점 확대를 검토하세요.<br>")
                    elif len(sorted_channels) > 3:
                        low_channels = [ch[0] for ch in sorted_channels[3:] if (ch[1] / total * 100) < 5]
                        if low_channels:
                            channel_insights.append(f"<strong>💡 액션:</strong> {', '.join(low_channels[:2])} 채널의 매출 비중이 낮습니다. 해당 채널의 성장 잠재력을 재평가하고 마케팅 지원을 강화하거나 효율성 검토가 필요합니다.<br>")
        
        insight += "".join(channel_insights) if channel_insights else "• 채널별 매출 데이터를 분석 중입니다.<br>"
        
        insight += "<br>"
        
        item_insights = []
        if brand_data and "item" in brand_data:
            items = brand_data["item"].get("items", {})
            if items:
                item_totals = {}
                for item, item_data in items.items():
                    if item_data and isinstance(item_data, dict) and "sales" in item_data:
                        item_totals[item] = item_data["sales"] / 100000000
                
                if item_totals:
                    sorted_items = sorted(item_totals.items(), key=lambda x: x[1], reverse=True)
                    total = sum(item_totals.values())
                    
                    if sorted_items:
                        top_item = sorted_items[0]
                        percentage = (top_item[1] / total * 100) if total > 0 else 0
                        item_insights.append(f"• <strong>{top_item[0]}</strong>이(가) 전체 매출의 <strong>{percentage:.1f}%</strong>를 차지하며 핵심 아이템으로 자리잡고 있습니다. ({top_item[1]:.1f}억원)<br>")
                        
                        if len(sorted_items) > 1:
                            second_item = sorted_items[1]
                            second_percentage = (second_item[1] / total * 100) if total > 0 else 0
                            item_insights.append(f"• <strong>{second_item[0]}</strong>이(가) <strong>{second_percentage:.1f}%</strong>로 두 번째로 높은 비중을 차지합니다. ({second_item[1]:.1f}억원)<br>")
                        
                        # 아이템 집중도 분석 및 액션
                        if percentage > 50:
                            item_insights.append(f"<strong>💡 액션:</strong> {top_item[0]}에 과도하게 의존하고 있어 아이템 다각화가 필요합니다. 신제품 개발 및 다른 카테고리 확대를 통해 포트폴리오를 강화하세요.<br>")
                        elif len(sorted_items) >= 3:
                            top3_items = sorted_items[:3]
                            top3_total = sum([it[1] for it in top3_items])
                            top3_share = (top3_total / total * 100) if total > 0 else 0
                            if top3_share > 80:
                                item_insights.append(f"• 상위 3개 아이템이 전체의 <strong>{top3_share:.1f}%</strong>를 차지하여 아이템 집중도가 높습니다. 신제품 라인업 확대를 검토하세요.<br>")
        
        insight += "".join(item_insights) if item_insights else "• 아이템별 매출 데이터를 분석 중입니다.<br>"
        
        return insight
    
    def _analyze_radar_local(self, data: Dict) -> str:
        """레이더 차트 로컬 분석 - 인사이트 및 액션 포함"""
        insight = ""
        
        channel_plan = data.get("channelPlan", {})
        channel_yoy = data.get("channelYoy", {})
        channel_current = data.get("channelCurrent", {})
        
        insights_list = []
        actions_list = []
        
        if channel_plan and channel_current:
            channel_analysis = []
            for brand_code in channel_plan.keys():
                if isinstance(channel_plan[brand_code], dict):
                    for channel in channel_plan[brand_code].keys():
                        plan = channel_plan[brand_code].get(channel, 0)
                        current = channel_current.get(brand_code, {}).get(channel, 0) if isinstance(channel_current.get(brand_code), dict) else 0
                        yoy = channel_yoy.get(brand_code, {}).get(channel, 0) if isinstance(channel_yoy.get(brand_code), dict) else 0
                if plan > 0:
                    achievement = (current / plan * 100) if plan > 0 else 0
                    channel_analysis.append({
                        "channel": channel,
                        "achievement": achievement,
                                "yoy": yoy,
                                "current": current / 100000000,
                                "plan": plan / 100000000
                    })
            
            if channel_analysis:
                channel_analysis.sort(key=lambda x: x["achievement"], reverse=True)
                
                # 우수 성과 채널
                top = channel_analysis[0]
                if top["achievement"] >= 100:
                    insights_list.append(f"• <strong>{top['channel']}</strong>이(가) 목표 대비 <strong>{top['achievement']:.1f}%</strong>로 목표를 초과 달성했습니다. ({top['current']:.1f}억원 / 목표 {top['plan']:.1f}억원)<br>")
                    if top["yoy"] > 110:
                        insights_list.append(f"  전년 대비 <strong>{top['yoy']:.1f}%</strong> 성장하여 강한 성장 모멘텀을 보이고 있습니다.<br>")
                else:
                    insights_list.append(f"• <strong>{top['channel']}</strong>이(가) 목표 대비 <strong>{top['achievement']:.1f}%</strong>로 가장 높은 달성률을 보이고 있습니다. ({top['current']:.1f}억원 / 목표 {top['plan']:.1f}억원)<br>")
                
                # 미달성 채널 분석
                under_achievers = [ch for ch in channel_analysis if ch["achievement"] < 90]
                if under_achievers:
                    worst = min(under_achievers, key=lambda x: x["achievement"])
                    gap = worst["plan"] - worst["current"]
                    actions_list.append(f"<strong>💡 액션:</strong> {worst['channel']} 채널이 목표 대비 <strong>{worst['achievement']:.1f}%</strong>로 미달성하고 있습니다. (부족분: {gap:.1f}억원) 마케팅 지원 강화 및 프로모션 전략 수립이 필요합니다.<br>")
                
                # 전년 대비 하락 채널
                declining = [ch for ch in channel_analysis if ch["yoy"] < 90]
                if declining:
                    worst_declining = min(declining, key=lambda x: x["yoy"])
                    actions_list.append(f"<strong>💡 액션:</strong> {worst_declining['channel']} 채널이 전년 대비 <strong>{worst_declining['yoy']:.1f}%</strong>로 하락했습니다. 채널별 성과 분석 및 개선 방안 마련이 시급합니다.<br>")
        
        insight += "".join(insights_list) if insights_list else "• 채널별 목표 대비 달성률을 분석 중입니다.<br>"
        if actions_list:
            insight += "<br>" + "".join(actions_list)
        
        return insight
    
    def _analyze_channel_pl_local(self, data: Dict) -> str:
        """채널별 손익 로컬 분석 - 인사이트 및 액션 포함"""
        insight = ""
        
        insights_list = []
        actions_list = []
        
        if isinstance(data, dict):
            channel_analysis = []
            for channel, channel_data in data.items():
                if isinstance(channel_data, dict) and "revenue" in channel_data:
                    revenue = channel_data["revenue"] / 100000000
                    gross_profit_rate = channel_data.get("grossProfitRate", 0)
                    operating_profit = channel_data.get("operatingProfit", 0) / 100000000
                    operating_profit_rate = channel_data.get("operatingProfitRate", 0)
                    channel_analysis.append({
                        "channel": channel,
                        "revenue": revenue,
                        "gross_profit_rate": gross_profit_rate,
                        "operating_profit": operating_profit,
                        "operating_profit_rate": operating_profit_rate
                    })
            
            if channel_analysis:
                channel_analysis.sort(key=lambda x: x["revenue"], reverse=True)
                
                # 최고 매출 채널
                top = channel_analysis[0]
                insights_list.append(f"• <strong>{top['channel']}</strong>이(가) 매출 <strong>{top['revenue']:.1f}억원</strong>으로 가장 큰 비중을 차지하며, 매출총이익률은 <strong>{top['gross_profit_rate']:.1f}%</strong>입니다.<br>")
                
                if top["gross_profit_rate"] >= 75:
                    insights_list.append(f"  높은 매출총이익률로 수익성이 우수한 채널입니다. 해당 채널의 성장을 지속적으로 지원하세요.<br>")
                elif top["gross_profit_rate"] < 60:
                    actions_list.append(f"<strong>💡 액션:</strong> {top['channel']} 채널의 매출총이익률 <strong>{top['gross_profit_rate']:.1f}%</strong>가 낮습니다. 할인율 관리 및 원가 최적화를 통해 수익성을 개선해야 합니다.<br>")
                
                # 수익성 우수 채널
                high_profit = [ch for ch in channel_analysis if ch["gross_profit_rate"] >= 80 and ch["revenue"] > 0]
                if high_profit:
                    best_profit = max(high_profit, key=lambda x: x["gross_profit_rate"])
                    if best_profit["channel"] != top["channel"]:
                        insights_list.append(f"• <strong>{best_profit['channel']}</strong> 채널이 매출총이익률 <strong>{best_profit['gross_profit_rate']:.1f}%</strong>로 가장 높은 수익성을 보이고 있습니다. ({best_profit['revenue']:.1f}억원)<br>")
                
                # 적자 채널
                loss_channels = [ch for ch in channel_analysis if ch["operating_profit"] < 0]
                if loss_channels:
                    worst = min(loss_channels, key=lambda x: x["operating_profit"])
                    actions_list.append(f"<strong>💡 액션:</strong> {worst['channel']} 채널이 영업이익 <strong>{worst['operating_profit']:.1f}억원</strong>으로 적자 상태입니다. 채널별 비용 구조 재검토 및 수익성 개선 방안 수립이 필요합니다.<br>")
        
        insight += "".join(insights_list) if insights_list else "• 채널별 손익 데이터를 분석 중입니다.<br>"
        if actions_list:
            insight += "<br>" + "".join(actions_list)
        
        return insight
    
    def _analyze_weekly_local(self, data: Dict) -> str:
        """주차별 매출추세 로컬 분석 - 인사이트 및 액션 포함"""
        insight = ""
        
        insights_list = []
        actions_list = []
        
        weekly_data = data.get("weeklySalesTrend", {})
        if weekly_data:
            total_current = 0
            total_previous = 0
            weekly_trends = []
            
            # 브랜드별 데이터 처리
            if isinstance(weekly_data, dict):
                for brand_code, brand_weekly in weekly_data.items():
                    if isinstance(brand_weekly, dict):
                        for channel, channel_data in brand_weekly.items():
                            if isinstance(channel_data, dict):
                                current = channel_data.get("current", [])
                                previous = channel_data.get("previous", [])
                                
                                if current and isinstance(current, list):
                                    current_sum = sum(w.get("value", 0) for w in current if isinstance(w, dict))
                                    total_current += current_sum
                                    
                                    # 최근 4주 추세 분석
                                    if len(current) >= 4:
                                        recent_4weeks = [w.get("value", 0) for w in current[-4:] if isinstance(w, dict)]
                                        if len(recent_4weeks) >= 2:
                                            recent_avg = sum(recent_4weeks) / len(recent_4weeks)
                                            earlier_avg = sum(recent_4weeks[:2]) / 2 if len(recent_4weeks) >= 2 else recent_avg
                                            if earlier_avg > 0:
                                                trend = ((recent_avg - earlier_avg) / earlier_avg * 100)
                                                weekly_trends.append({
                                                    "channel": channel,
                                                    "trend": trend,
                                                    "recent_avg": recent_avg
                                                })
                                
                                if previous and isinstance(previous, list):
                                    total_previous += sum(w.get("value", 0) for w in previous if isinstance(w, dict))
            
            if total_previous > 0:
                current_billion = total_current / 100000000
                previous_billion = total_previous / 100000000
                yoy = ((current_billion / previous_billion - 1) * 100) if previous_billion > 0 else 0
                
                if yoy > 110:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy:+.1f}%</strong> 성장하여 강한 성장세를 보이고 있습니다.<br>")
                elif yoy > 100:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy:+.1f}%</strong> 성장했습니다.<br>")
                elif yoy < 95:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy:+.1f}%</strong> 감소했습니다.<br>")
                    actions_list.append(f"<strong>💡 액션:</strong> 전년 대비 매출이 하락하고 있습니다. 마케팅 강화 및 프로모션 전략 수립을 통해 매출 회복이 필요합니다.<br>")
                else:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy:+.1f}%</strong> 수준을 유지하고 있습니다.<br>")
            
            # 최근 추세 분석
            if weekly_trends:
                declining = [t for t in weekly_trends if t["trend"] < -10]
                if declining:
                    worst = min(declining, key=lambda x: x["trend"])
                    actions_list.append(f"<strong>💡 액션:</strong> {worst['channel']} 채널의 최근 4주 매출이 <strong>{worst['trend']:.1f}%</strong> 하락 추세입니다. 즉각적인 마케팅 개입이 필요합니다.<br>")
                
                growing = [t for t in weekly_trends if t["trend"] > 10]
                if growing:
                    best = max(growing, key=lambda x: x["trend"])
                    insights_list.append(f"• <strong>{best['channel']}</strong> 채널이 최근 4주간 <strong>{best['trend']:.1f}%</strong> 성장하여 긍정적인 추세를 보이고 있습니다.<br>")
        
        insight += "".join(insights_list) if insights_list else "• 주차별 매출 추세를 분석 중입니다.<br>"
        if actions_list:
            insight += "<br>" + "".join(actions_list)
        
        return insight
    
    def _analyze_inventory_local(self, data: Dict) -> str:
        """재고주수 로컬 분석 - 인사이트 및 액션 포함"""
        insight = ""
        
        insights_list = []
        actions_list = []
        
        stock_data = data.get("clothingBrandStatus", [])
        if isinstance(stock_data, list):
            high_stock = []
            very_high_stock = []
            
            for item in stock_data:
                if isinstance(item, dict):
                    stock_weeks = item.get("stockWeeks", 0) or item.get("재고주수", 0)
                    item_name = item.get("아이템명") or item.get("itemName") or "상품"
                    stock_qty = item.get("재고") or item.get("stock", 0)
                    
                    if stock_weeks > 52:  # 1년 이상
                        very_high_stock.append({
                            "name": item_name,
                            "weeks": stock_weeks,
                            "qty": stock_qty
                        })
                    elif stock_weeks > 40:  # 40주 이상
                        high_stock.append({
                            "name": item_name,
                            "weeks": stock_weeks,
                            "qty": stock_qty
                        })
            
            if very_high_stock:
                worst = max(very_high_stock, key=lambda x: x["weeks"])
                actions_list.append(f"<strong>💡 액션:</strong> {worst['name']} 상품의 재고주수가 <strong>{worst['weeks']:.0f}주</strong>로 매우 높습니다. (재고: {worst['qty']:,}개) 즉각적인 재고 처리 전략(대폭 할인, 아울렛 이동 등)이 필요합니다.<br>")
            
            if high_stock:
                total_high_stock = len(high_stock) + len(very_high_stock)
                insights_list.append(f"• 재고주수 <strong>40주 이상</strong>인 상품이 <strong>{total_high_stock}개</strong>로 재고 관리가 필요합니다.<br>")
                
                if len(high_stock) > 0:
                    avg_weeks = sum([h["weeks"] for h in high_stock]) / len(high_stock)
                    actions_list.append(f"<strong>💡 액션:</strong> 평균 재고주수 <strong>{avg_weeks:.0f}주</strong>인 상품들의 재고 처리를 위해 프로모션 계획 수립 및 아울렛 채널 활용을 검토하세요.<br>")
            
            # 적정 재고 상품
            optimal_stock = [item for item in stock_data if isinstance(item, dict) and 10 <= (item.get("stockWeeks", 0) or item.get("재고주수", 0)) <= 30]
            if optimal_stock:
                insights_list.append(f"• 재고주수 <strong>10~30주</strong> 범위의 적정 재고 상품이 <strong>{len(optimal_stock)}개</strong>로 재고 관리가 양호합니다.<br>")
            
            # 저재고 상품
            low_stock = [item for item in stock_data if isinstance(item, dict) and (item.get("stockWeeks", 0) or item.get("재고주수", 0)) < 5]
            if low_stock:
                insights_list.append(f"• 재고주수 <strong>5주 미만</strong>인 상품이 <strong>{len(low_stock)}개</strong>로 재고 보충이 필요할 수 있습니다.<br>")
        
        insight += "".join(insights_list) if insights_list else "• 재고주수 데이터를 분석 중입니다.<br>"
        if actions_list:
            insight += "<br>" + "".join(actions_list)
        
        return insight
    
    def _analyze_sale_rate_local(self, data: Dict) -> str:
        """판매율 로컬 분석 - 인사이트 및 액션 포함"""
        insight = ""
        
        insights_list = []
        actions_list = []
        
        stock_data = data.get("clothingBrandStatus", [])
        if isinstance(stock_data, list):
            sales_rates = []
            low_sale_items = []
            
            for item in stock_data:
                if isinstance(item, dict):
                    rate = item.get("cumSalesRate", 0)
                    if rate:
                        sales_rates.append(rate)
                        item_name = item.get("아이템명") or item.get("itemName") or "상품"
                        if rate < 20:  # 판매율 20% 미만
                            low_sale_items.append({
                                "name": item_name,
                                "rate": rate
                            })
            
            if sales_rates:
                avg_rate = sum(sales_rates) / len(sales_rates)
                
                if avg_rate >= 40:
                    insights_list.append(f"• 평균 누적 판매율은 <strong>{avg_rate:.1f}%</strong>로 양호한 수준입니다. 상품 기획 및 마케팅 전략이 효과적입니다.<br>")
                elif avg_rate >= 25:
                    insights_list.append(f"• 평균 누적 판매율은 <strong>{avg_rate:.1f}%</strong>입니다. 목표 달성을 위해 판매 촉진 활동이 필요합니다.<br>")
                else:
                    insights_list.append(f"• 평균 누적 판매율은 <strong>{avg_rate:.1f}%</strong>로 낮은 수준입니다.<br>")
                    actions_list.append(f"<strong>💡 액션:</strong> 전반적인 판매율이 낮습니다. 프로모션 강화, 가격 조정, 마케팅 캠페인 등을 통해 판매를 촉진해야 합니다.<br>")
                
                # 저판매율 상품 분석
                if low_sale_items:
                    worst = min(low_sale_items, key=lambda x: x["rate"])
                    actions_list.append(f"<strong>💡 액션:</strong> {worst['name']} 상품의 판매율이 <strong>{worst['rate']:.1f}%</strong>로 매우 낮습니다. 재고 처리 전략 수립 또는 추가 마케팅 지원이 필요합니다.<br>")
                
                # 고판매율 상품
                high_sale_items = [item for item in stock_data if isinstance(item, dict) and (item.get("cumSalesRate") or 0) > 50]
                if high_sale_items:
                    insights_list.append(f"• 판매율 <strong>50% 이상</strong>인 인기 상품이 <strong>{len(high_sale_items)}개</strong>로 성과가 우수합니다. 해당 상품의 성공 요인을 분석하여 다른 상품에 적용하세요.<br>")
        
        insight += "".join(insights_list) if insights_list else "• 판매율 데이터를 분석 중입니다.<br>"
        if actions_list:
            insight += "<br>" + "".join(actions_list)
        
        return insight
    
    def _analyze_overview_local(self, data: Dict) -> str:
        """전체 현황 로컬 분석"""
        insight = ""
        
        # KPI 데이터 분석
        kpi_data = data.get("kpi", {})
        if kpi_data:
            revenue_forecast = kpi_data.get("revenueForecast", 0) / 100000000
            revenue_plan = kpi_data.get("revenuePlan", 0) / 100000000
            revenue_vs_plan = kpi_data.get("revenueVsPlan", 0)
            revenue_vs_previous = kpi_data.get("revenueVsPrevious", 0)
            
            if revenue_plan > 0:
                achievement = (revenue_forecast / revenue_plan * 100) if revenue_plan > 0 else 0
                insight += f"• 전체 실판매액은 <strong>{revenue_forecast:.1f}억원</strong>으로 목표 대비 <strong>{achievement:.1f}%</strong> 달성률을 보이고 있습니다.<br>"
                if revenue_vs_plan < 0:
                    insight += f"• 목표 대비 <strong>{abs(revenue_vs_plan):.1f}%</strong> 부족하여 목표 달성을 위한 추가 노력이 필요합니다.<br>"
            
            if revenue_vs_previous:
                insight += f"• 전년 대비 <strong>{revenue_vs_previous:+.1f}%</strong> {'성장' if revenue_vs_previous > 0 else '감소'}했습니다.<br>"
            
            direct_profit_rate = kpi_data.get("directProfitRateForecast", 0)
            if direct_profit_rate:
                insight += f"• 직접이익률은 <strong>{direct_profit_rate:.1f}%</strong>로 {'양호한' if direct_profit_rate >= 30 else '개선이 필요한'} 수준입니다.<br>"
            
            op_profit_rate = kpi_data.get("operatingProfitRateForecast", 0)
            if op_profit_rate:
                insight += f"• 영업이익률은 <strong>{op_profit_rate:.1f}%</strong>로 {'양호한' if op_profit_rate >= 15 else '개선이 필요한'} 수준입니다.<br>"
        
        # PL 데이터 분석
        pl_data = data.get("pl", {})
        if pl_data:
            revenue = pl_data.get("revenue", {})
            if revenue:
                forecast = revenue.get("forecast", 0)
                target = revenue.get("target", 0)
                achievement = revenue.get("achievement", 0)
                if achievement < 95:
                    insight += f"• 매출 달성률이 <strong>{achievement}%</strong>로 목표 미달 위험이 있습니다.<br>"
        
        # 브랜드별 기여도 분석
        by_brand = data.get("by_brand", {})
        if by_brand:
            brand_contributions = []
            
            # 딕셔너리인 경우
            if isinstance(by_brand, dict):
                for brand, brand_data in by_brand.items():
                    if isinstance(brand_data, dict):
                        revenue = brand_data.get("revenue", 0) or brand_data.get("forecast", 0) or brand_data.get("SALES", 0)
                        if revenue:
                            brand_contributions.append({
                                "brand": brand,
                                "revenue": revenue
                            })
            # 리스트인 경우
            elif isinstance(by_brand, list):
                for brand_info in by_brand:
                    if isinstance(brand_info, dict):
                        brand = brand_info.get("BRAND") or brand_info.get("brand") or brand_info.get("name", "")
                        revenue = brand_info.get("SALES", 0) or brand_info.get("revenue", 0) or brand_info.get("forecast", 0)
                        if revenue and brand:
                            brand_contributions.append({
                                "brand": brand,
                                "revenue": revenue
                            })
            
            if brand_contributions:
                brand_contributions.sort(key=lambda x: x["revenue"], reverse=True)
                total_revenue = sum(b["revenue"] for b in brand_contributions)
                
                if brand_contributions and total_revenue > 0:
                    top_brand = brand_contributions[0]
                    share = (top_brand["revenue"] / total_revenue * 100)
                    insight += f"• <strong>{top_brand['brand']}</strong> 브랜드가 전체 매출의 <strong>{share:.1f}%</strong>를 차지하며 가장 큰 기여를 하고 있습니다.<br>"
        
        return insight


def load_json_file(file_path: Path) -> Optional[Dict]:
    """JSON 파일 로드"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[WARNING] 파일을 찾을 수 없습니다: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON 파싱 오류: {file_path} - {e}")
        return None


def generate_insights_for_overview(date_str: str, generator: AIInsightGenerator, output_dir: Path):
    """전체 현황에 대한 모든 인사이트 생성"""
    base_dir = project_root / "public" / "data" / date_str
    
    insights = {}
    overview_data = {}
    
    # 1. 전체 KPI 분석
    kpi_file = base_dir / "overview_kpi.json"
    if kpi_file.exists():
        print("[ANALYZING] 전체 KPI 분석 중...")
        kpi_data = load_json_file(kpi_file)
        if kpi_data and "OVERVIEW" in kpi_data:
            overview_data["kpi"] = kpi_data["OVERVIEW"]
    
    # 2. 전체 손익계산서 분석
    pl_file = base_dir / "overview_pl.json"
    if pl_file.exists():
        print("[ANALYZING] 전체 손익계산서 분석 중...")
        pl_data = load_json_file(pl_file)
        if pl_data:
            overview_data["pl"] = pl_data
    
    # 3. 브랜드별 기여도 분석
    by_brand_file = base_dir / "overview_by_brand.json"
    if by_brand_file.exists():
        print("[ANALYZING] 브랜드별 기여도 분석 중...")
        by_brand_data = load_json_file(by_brand_file)
        if by_brand_data:
            overview_data["by_brand"] = by_brand_data
    
    # 4. 월중누적매출추이 분석
    trend_file = base_dir / "overview_trend.json"
    if trend_file.exists():
        print("[ANALYZING] 월중누적매출추이 분석 중...")
        trend_data = load_json_file(trend_file)
        if trend_data:
            overview_data["trend"] = trend_data
    
    # 5. 전체 재고 분석
    stock_file = base_dir / "overview_stock_analysis.json"
    if stock_file.exists():
        print("[ANALYZING] 전체 재고 분석 중...")
        stock_data = load_json_file(stock_file)
        if stock_data:
            overview_data["stock"] = stock_data
    
    # 전체 현황 통합 분석
    if overview_data:
        overview_insight = generator.generate_insight(overview_data, "전체 현황", "overview")
        insights["overview"] = overview_insight
        
        # keyPoints 생성 (주요 포인트 추출)
        key_points = []
        kpi_data = overview_data.get("kpi", {})
        by_brand_data = overview_data.get("by_brand", {})
        
        if kpi_data and isinstance(kpi_data, dict) and "OVERVIEW" in kpi_data:
            kpi = kpi_data["OVERVIEW"]
            revenue_forecast = kpi.get("revenueForecast", 0) / 100000000
            revenue_plan = kpi.get("revenuePlan", 0) / 100000000
            revenue_vs_plan = kpi.get("revenueVsPlan", 0)
            revenue_vs_previous = kpi.get("revenueVsPrevious", 0)
            
            key_points.append(f"• 총 실판매액: {revenue_forecast:.1f}억원 (목표 대비 {100 + revenue_vs_plan:.1f}%, 전년 대비 {100 + revenue_vs_previous:.1f}%)")
            
            op_profit = kpi.get("operatingProfitForecast", 0) / 100000000
            if op_profit:
                key_points.append(f"• 영업이익: {op_profit:.1f}억원")
        
        # 브랜드별 기여도 요약
        if by_brand_data:
            brand_contributions = []
            
            # 딕셔너리인 경우
            if isinstance(by_brand_data, dict):
                for brand, brand_info in by_brand_data.items():
                    if isinstance(brand_info, dict):
                        revenue = brand_info.get("revenue", 0) or brand_info.get("forecast", 0) or brand_info.get("SALES", 0)
                        if revenue:
                            brand_contributions.append({
                                "brand": brand,
                                "revenue": revenue
                            })
            # 리스트인 경우
            elif isinstance(by_brand_data, list):
                for brand_info in by_brand_data:
                    if isinstance(brand_info, dict):
                        brand = brand_info.get("BRAND") or brand_info.get("brand") or brand_info.get("name", "")
                        revenue = brand_info.get("SALES", 0) or brand_info.get("revenue", 0) or brand_info.get("forecast", 0)
                        if revenue and brand:
                            brand_contributions.append({
                                "brand": brand,
                                "revenue": revenue
                            })
            
            if brand_contributions:
                brand_contributions.sort(key=lambda x: x["revenue"], reverse=True)
                total_revenue = sum(b["revenue"] for b in brand_contributions)
                if brand_contributions and total_revenue > 0:
                    top_brand = brand_contributions[0]
                    share = (top_brand["revenue"] / total_revenue * 100)
                    key_points.append(f"• {top_brand['brand']}: {top_brand['revenue']:.1f}억원으로 {share:.1f}% 기여, {'목표 초과' if share > 20 else '주요 브랜드'}")
    else:
        overview_insight = ""
        key_points = []
    
    # HTML insightsData 형식에 맞게 변환
    overview_data_format = {
        "overview": {
            "content": overview_insight,
            "keyPoints": " ".join(key_points) if key_points else ""
        }
    }
    
    # 결과 저장 (원본 형식)
    output_file = output_dir / f"ai_insights_overview_{date_str}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            "type": "overview",
            "date": date_str,
            "generated_at": datetime.now().isoformat(),
            "insights": insights
        }, f, indent=2, ensure_ascii=False)
    
    # HTML 호환 형식으로도 저장
    output_file_html = output_dir / f"insights_data_overview_{date_str}.json"
    with open(output_file_html, 'w', encoding='utf-8') as f:
        json.dump(overview_data_format, f, indent=2, ensure_ascii=False)
    
    print(f"[SUCCESS] 전체 현황 인사이트 생성 완료: {output_file}")
    return overview_data_format


def generate_insights_for_brand(date_str: str, brand: str, generator: AIInsightGenerator, output_dir: Path):
    """특정 브랜드에 대한 모든 인사이트 생성"""
    base_dir = project_root / "public" / "data" / date_str
    brand_code = BRAND_CODE_MAP.get(brand, brand)
    
    insights = {}
    
    # 1. 손익계산서 분석
    pl_file = base_dir / "brand_pl.json"
    if pl_file.exists():
        print(f"[ANALYZING] 손익계산서 분석 중... ({brand})")
        pl_data = load_json_file(pl_file)
        if pl_data and brand in pl_data:
            insights["pl"] = generator.generate_insight(pl_data[brand], brand, "pl")
    
    # 2. 트리맵 분석
    treemap_file = base_dir / "treemap.json"
    if treemap_file.exists():
        print(f"[ANALYZING] 트리맵 분석 중... ({brand})")
        treemap_data = load_json_file(treemap_file)
        if treemap_data:
            # 브랜드별 데이터 필터링
            brand_treemap_data = {}
            if "channelTreemapData" in treemap_data:
                channel_treemap = treemap_data["channelTreemapData"]
                if "byBrand" in channel_treemap and brand_code in channel_treemap["byBrand"]:
                    brand_treemap_data["channelTreemapData"] = {
                        "byBrand": {
                            brand_code: channel_treemap["byBrand"][brand_code]
                        }
                    }
            
            if "itemTreemapData" in treemap_data:
                item_treemap = treemap_data["itemTreemapData"]
                if "byBrand" in item_treemap and brand_code in item_treemap["byBrand"]:
                    if "channelTreemapData" not in brand_treemap_data:
                        brand_treemap_data["channelTreemapData"] = {}
                    brand_treemap_data["itemTreemapData"] = {
                        "byBrand": {
                            brand_code: item_treemap["byBrand"][brand_code]
                        }
                    }
            
            if brand_treemap_data:
                insights["treemap"] = generator.generate_insight(brand_treemap_data, brand, "treemap")
    
    # 3. 레이더 차트 분석
    radar_file = base_dir / "radar_chart.json"
    if radar_file.exists():
        print(f"[ANALYZING] 레이더 차트 분석 중... ({brand})")
        radar_data = load_json_file(radar_file)
        if radar_data:
            # 브랜드별로 필터링
            brand_radar_data = {}
            if "channelPlan" in radar_data and brand_code in radar_data["channelPlan"]:
                brand_radar_data["channelPlan"] = {brand_code: radar_data["channelPlan"][brand_code]}
            if "channelCurrent" in radar_data and brand_code in radar_data["channelCurrent"]:
                brand_radar_data["channelCurrent"] = {brand_code: radar_data["channelCurrent"][brand_code]}
            if "channelYoy" in radar_data and brand_code in radar_data["channelYoy"]:
                brand_radar_data["channelYoy"] = {brand_code: radar_data["channelYoy"][brand_code]}
            
            if brand_radar_data:
                insights["radar"] = generator.generate_insight(brand_radar_data, brand, "radar")
    
    # 4. 채널별 손익 분석
    channel_pl_file = base_dir / "channel_pl.json"
    if channel_pl_file.exists():
        print(f"[ANALYZING] 채널별 손익 분석 중... ({brand})")
        channel_pl_data = load_json_file(channel_pl_file)
        if channel_pl_data and brand_code in channel_pl_data:
            insights["channelPl"] = generator.generate_insight(channel_pl_data[brand_code], brand, "channel_pl")
    
    # 5. 주차별 매출추세 분석
    weekly_file = base_dir / "weekly_trend.json"
    if weekly_file.exists():
        print(f"[ANALYZING] 주차별 매출추세 분석 중... ({brand})")
        weekly_data = load_json_file(weekly_file)
        if weekly_data:
            insights["weekly"] = generator.generate_insight(weekly_data, brand, "weekly")
    
    # 6. 재고주수 분석
    stock_file = base_dir / "stock_analysis.json"
    if stock_file.exists():
        print(f"[ANALYZING] 재고주수 분석 중... ({brand})")
        stock_data = load_json_file(stock_file)
        if stock_data and brand_code in stock_data.get("clothingBrandStatus", {}):
            brand_stock = {"clothingBrandStatus": stock_data["clothingBrandStatus"][brand_code]}
            insights["inventory"] = generator.generate_insight(brand_stock, brand, "inventory")
            insights["saleRate"] = generator.generate_insight(brand_stock, brand, "sale_rate")
    
    # 브랜드별 주요 내용(content)과 핵심인사이트(keyPoints) 생성
    brand_kpi_file = base_dir / "brand_kpi.json"
    content = ""
    key_points = []
    
    if brand_kpi_file.exists():
        kpi_data = load_json_file(brand_kpi_file)
        if kpi_data and brand_code in kpi_data:
            brand_kpi = kpi_data[brand_code]
            
            # KPI 데이터 구조: 평면 구조 (revenueForecast, revenuePlan 등이 직접 키)
            revenue_forecast = brand_kpi.get("revenueForecast", 0) / 100000000
            revenue_plan = brand_kpi.get("revenuePlan", 0) / 100000000
            revenue_previous = brand_kpi.get("revenuePrevious", 0) / 100000000
            revenue_vs_plan = brand_kpi.get("revenueVsPlan", 0)
            revenue_vs_previous = brand_kpi.get("revenueVsPrevious", 0)
            
            op_profit_forecast = brand_kpi.get("operatingProfitForecast", 0) / 100000000
            op_profit_plan = brand_kpi.get("operatingProfitPlan", 0) / 100000000 if brand_kpi.get("operatingProfitPlan") else 0
            op_profit_previous = brand_kpi.get("operatingProfitPrevious", 0) / 100000000
            op_profit_vs_plan = brand_kpi.get("profitVsPlan", 0) if brand_kpi.get("profitVsPlan") else 0
            op_profit_vs_previous = brand_kpi.get("profitVsPrevious", 0) if brand_kpi.get("profitVsPrevious") else 0
            
            direct_profit_forecast = brand_kpi.get("directProfitForecast", 0) / 100000000
            direct_profit_plan = brand_kpi.get("directProfitPlan", 0) / 100000000
            direct_profit_previous = brand_kpi.get("directProfitPrevious", 0) / 100000000
            
            discount_rate_forecast = brand_kpi.get("discountRateForecast", 0)
            discount_rate_plan = brand_kpi.get("discountRatePlan", 0) if brand_kpi.get("discountRatePlan") else 0
            
            # 달성률 계산
            revenue_achievement = (revenue_forecast / revenue_plan * 100) if revenue_plan > 0 else 0
            op_achievement = (op_profit_forecast / op_profit_plan * 100) if op_profit_plan > 0 else 0
            direct_achievement = (direct_profit_forecast / direct_profit_plan * 100) if direct_profit_plan > 0 else 0
            
            # 주요 내용 생성
            if revenue_forecast > 0:
                content += f"{brand}는 {revenue_forecast:.2f}억원의 실판매액으로 목표({revenue_plan:.2f}억원) 대비 {revenue_achievement:.0f}% 달성, "
                if revenue_previous > 0:
                    content += f"전년({revenue_previous:.2f}억원) 대비 {100 + revenue_vs_previous:.0f}%를 기록하며 "
                    if revenue_vs_previous > 110:
                        content += "안정적인 성장세를 보이고 있습니다. "
                    elif revenue_vs_previous > 100:
                        content += "전년 대비 양호한 성장세를 보이고 있습니다. "
                    else:
                        content += "전년 대비 감소하는 성장세를 유지하고 있습니다. "
                else:
                    content += "목표 달성률을 보이고 있습니다. "
            
            if op_profit_forecast != 0:
                content += f"영업이익은 {op_profit_forecast:.2f}억원으로 "
                if op_profit_plan > 0:
                    content += f"목표({op_profit_plan:.2f}억원) 대비 {op_achievement:.0f}%, "
                if op_profit_previous > 0:
                    content += f"전년({op_profit_previous:.2f}억원) 대비 {100 + op_profit_vs_previous:.0f}%를 달성하여 "
                    if op_achievement >= 100:
                        content += "수익성 개선을 이루었습니다. "
                    else:
                        content += "수익성 개선이 필요합니다. "
                else:
                    content += "달성률을 기록했습니다. "
            
            if direct_profit_forecast > 0:
                content += f"직접이익은 {direct_profit_forecast:.2f}억원으로 목표({direct_profit_plan:.2f}억원) 대비 {direct_achievement:.0f}%, "
                if direct_profit_previous > 0:
                    content += f"전년({direct_profit_previous:.2f}억원) 대비 {((direct_profit_forecast / direct_profit_previous - 1) * 100):.0f}%를 기록하며 "
                content += "안정적인 수익 구조를 유지하고 있습니다. "
            
            if discount_rate_forecast > 0:
                if discount_rate_plan > 0 and discount_rate_forecast > discount_rate_plan:
                    content += f"할인율은 {discount_rate_forecast:.1f}%로 목표({discount_rate_plan:.1f}%) 대비 높은 수준이며, "
                elif discount_rate_plan > 0:
                    content += f"할인율은 {discount_rate_forecast:.1f}%로 목표({discount_rate_plan:.1f}%) 대비 양호한 수준입니다. "
                else:
                    content += f"할인율은 {discount_rate_forecast:.1f}%입니다. "
            
            # 직접비 효율 계산 (목표 대비)
            if direct_profit_plan > 0:
                direct_efficiency = (direct_profit_forecast / direct_profit_plan * 100)
                content += f"직접비 효율이 목표 대비 {direct_efficiency:.0f}%로 {'양호한' if direct_efficiency >= 95 else '개선이 필요한'} 비용 관리를 보이고 있습니다."
            
            # 핵심인사이트 생성
            if revenue_forecast > 0:
                # 목표 미달성 시
                if revenue_achievement < 95:
                    if discount_rate_plan > 0:
                        key_points.append(f"• 목표 달성을 위해 할인율 관리 강화 (현재 {discount_rate_forecast:.1f}% → 목표 {discount_rate_plan:.1f}%)")
                
                # 전년 대비 하락 시
                if revenue_vs_previous < 0:
                    key_points.append(f"• 전년 대비 {100 + revenue_vs_previous:.1f}%로 하락, 매출 회복 전략 수립 필요")
                
                # 할인율 관리 필요 시
                if discount_rate_plan > 0 and discount_rate_forecast > discount_rate_plan:
                    key_points.append(f"• 할인율 관리 강화 (현재 {discount_rate_forecast:.1f}% → 목표 {discount_rate_plan:.1f}%)")
                
                # 직접비 효율 개선 필요 시
                if direct_profit_plan > 0:
                    direct_efficiency = (direct_profit_forecast / direct_profit_plan * 100)
                    if direct_efficiency < 95:
                        key_points.append(f"• 직접비 효율 유지 및 인건비, 물류운송비 최적화 지속")
                
                # 목표 미달성 시
                if revenue_achievement < 100:
                    gap = revenue_plan - revenue_forecast
                    key_points.append(f"• 목표 대비 {100 - revenue_achievement:.0f}% 부족분 회복을 위한 프로모션 전략 조정")
                
                # 성공 사례 인사이트 (목표 초과 달성 및 전년 대비 성장)
                if revenue_achievement >= 100 and revenue_vs_previous > 0:
                    if revenue_achievement >= 105:
                        key_points.append(f"• 목표 대비 {revenue_achievement:.0f}% 초과 달성, 성장 모멘텀 지속을 위한 신제품 라인업 확대 및 마케팅 강화")
                    else:
                        key_points.append(f"• 목표 달성 및 전년 대비 {100 + revenue_vs_previous:.1f}% 성장, 성공 모델 분석하여 타 브랜드 적용 방안 검토")
                
                # 전년 대비 높은 성장 시
                if revenue_vs_previous > 50:
                    key_points.append(f"• 전년 대비 {100 + revenue_vs_previous:.1f}% 폭발적 성장, 성장 모멘텀 지속을 위한 신제품 라인업 확대 및 마케팅 강화")
                
                # 수익성 우수 시
                if direct_profit_plan > 0:
                    direct_efficiency = (direct_profit_forecast / direct_profit_plan * 100)
                    if direct_efficiency >= 120:
                        key_points.append(f"• 직접비 효율 목표 대비 {direct_efficiency:.0f}%로 매우 우수, 수익성 최적화 지속")
    
    # HTML insightsData 형식에 맞게 변환
    insights_data_format = {
        brand: {
            "content": content,
            "keyPoints": " ".join(key_points) if key_points else "",
            "treemapInsight": insights.get("treemap", ""),
            "radarInsight": insights.get("radar", ""),
            "channelPlInsight": insights.get("channelPl", ""),
            "weeklyInsight": insights.get("weekly", ""),
            "saleRateInsight": insights.get("saleRate", ""),
            "inventoryInsight": insights.get("inventory", ""),
            "part1": insights.get("pl", "")  # 손익계산서는 part1으로 저장
        }
    }
    
    # 결과 저장 (원본 형식)
    output_file = output_dir / f"ai_insights_{brand}_{date_str}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            "brand": brand,
            "date": date_str,
            "generated_at": datetime.now().isoformat(),
            "insights": insights
        }, f, indent=2, ensure_ascii=False)
    
    # HTML 호환 형식으로도 저장
    output_file_html = output_dir / f"insights_data_{brand}_{date_str}.json"
    with open(output_file_html, 'w', encoding='utf-8') as f:
        json.dump(insights_data_format, f, indent=2, ensure_ascii=False)
    
    print(f"[SUCCESS] {brand} 브랜드 인사이트 생성 완료: {output_file}")
    return insights_data_format


def main():
    parser = argparse.ArgumentParser(description="JSON 파일을 읽어 AI 분석 생성")
    parser.add_argument("--date", type=str, required=True, help="날짜 (YYYYMMDD 형식)")
    parser.add_argument("--brand", type=str, help="브랜드명 (MLB, MLB_KIDS, DISCOVERY, DUVETICA, SERGIO, SUPRA)")
    parser.add_argument("--all-brands", action="store_true", help="모든 브랜드에 대해 분석 수행")
    parser.add_argument("--overview", action="store_true", help="전체 현황에 대해 분석 수행")
    parser.add_argument("--api-key", type=str, help="OpenAI API 키 (없으면 환경변수 OPENAI_API_KEY 사용)")
    parser.add_argument("--use-local", action="store_true", help="로컬 분석만 사용 (OpenAI API 사용 안 함)")
    parser.add_argument("--output-dir", type=str, help="출력 디렉토리 (기본값: public/data/{date}/ai_insights)")
    
    args = parser.parse_args()
    
    # API 키 설정
    api_key = args.api_key or os.getenv("OPENAI_API_KEY")
    
    # 출력 디렉토리 설정
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = project_root / "public" / "data" / args.date / "ai_insights"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # AI 생성기 초기화
    generator = AIInsightGenerator(api_key=api_key, use_local=args.use_local)
    
    all_insights = {}
    
    # 전체 현황 분석
    if args.overview:
        overview_insights = generate_insights_for_overview(args.date, generator, output_dir)
        all_insights["overview"] = overview_insights
    
    # 브랜드별 분석
    if args.all_brands:
        brands = list(BRAND_CODE_MAP.keys())
        for brand in brands:
            insights = generate_insights_for_brand(args.date, brand, generator, output_dir)
            all_insights[brand] = insights
    elif args.brand:
        brands = [args.brand]
        for brand in brands:
            insights = generate_insights_for_brand(args.date, brand, generator, output_dir)
            all_insights[brand] = insights
    elif not args.overview:
        print("[ERROR] --brand, --all-brands, 또는 --overview 옵션 중 하나를 지정해주세요.")
        return
    
    # 통합 결과 저장 (원본 형식)
    summary_file = output_dir / f"ai_insights_summary_{args.date}.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump({
            "date": args.date,
            "generated_at": datetime.now().isoformat(),
            "insights": all_insights
        }, f, indent=2, ensure_ascii=False)
    
    # HTML insightsData 호환 형식으로 통합 저장
    insights_data_combined = {}
    
    # 전체 현황 데이터 병합
    if "overview" in all_insights:
        overview_data = all_insights["overview"]
        if isinstance(overview_data, dict) and "overview" in overview_data:
            insights_data_combined["overview"] = overview_data["overview"]
    
    # 브랜드별 데이터 병합
    for brand in BRAND_CODE_MAP.keys():
        if brand in all_insights:
            brand_data = all_insights[brand]
            # insights_data_format 형식인 경우 (브랜드 키로 감싸져 있음)
            if isinstance(brand_data, dict) and brand in brand_data:
                insights_data_combined[brand] = brand_data[brand]
            # 이미 평면 구조인 경우
            elif isinstance(brand_data, dict):
                insights_data_combined[brand] = brand_data
    
    # HTML에서 바로 사용할 수 있는 형식으로 저장
    insights_data_file = output_dir / f"insights_data_{args.date}.json"
    with open(insights_data_file, 'w', encoding='utf-8') as f:
        json.dump(insights_data_combined, f, indent=2, ensure_ascii=False)
    
    print(f"\n[SUCCESS] 모든 인사이트 생성 완료!")
    print(f"[INFO] 출력 디렉토리: {output_dir}")
    print(f"[INFO] HTML 호환 파일: {insights_data_file}")
    print(f"\n[INFO] HTML에서 사용하려면:")
    print(f"   const insightsData = await fetch('/public/data/{args.date}/ai_insights/insights_data_{args.date}.json').then(r => r.json());")


if __name__ == "__main__":
    main()

