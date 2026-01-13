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
        
        # overview_trend.json 구조: weekly_current, weekly_prev, cumulative_current, cumulative_prev
        weekly_current = data.get("weekly_current", [])
        weekly_prev = data.get("weekly_prev", [])
        cumulative_current = data.get("cumulative_current", [])
        cumulative_prev = data.get("cumulative_prev", [])
        
        # 누적 매출 분석 (전년 대비)
        # 데이터는 백만원 단위이므로 100으로 나눠서 억원 단위로 변환
        if cumulative_current and cumulative_prev and len(cumulative_current) > 0 and len(cumulative_prev) > 0:
            total_current = cumulative_current[-1] if cumulative_current else 0
            total_previous = cumulative_prev[-1] if cumulative_prev else 0
            
            if total_previous > 0:
                current_billion = total_current / 100  # 백만원 -> 억원
                previous_billion = total_previous / 100  # 백만원 -> 억원
                yoy_pct = ((current_billion / previous_billion - 1) * 100) if previous_billion > 0 else 0
                
                if yoy_pct > 10:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy_pct:+.1f}%</strong> 성장하여 강한 성장세를 보이고 있습니다.<br>")
                elif yoy_pct > 0:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy_pct:+.1f}%</strong> 성장했습니다.<br>")
                elif yoy_pct < -5:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy_pct:+.1f}%</strong> 감소했습니다.<br>")
                    actions_list.append(f"<strong>💡 액션:</strong> 전년 대비 매출이 하락하고 있습니다. 마케팅 강화 및 프로모션 전략 수립을 통해 매출 회복이 필요합니다.<br>")
                else:
                    insights_list.append(f"• 현재까지 누적 매출은 <strong>{current_billion:.1f}억원</strong>으로 전년 대비 <strong>{yoy_pct:+.1f}%</strong> 수준을 유지하고 있습니다.<br>")
        
        # 최근 주차별 추세 분석
        if weekly_current and weekly_prev and len(weekly_current) >= 4 and len(weekly_prev) >= 4:
            # 최근 4주 평균 vs 이전 4주 평균 비교
            recent_4weeks_current = weekly_current[-4:] if len(weekly_current) >= 4 else weekly_current
            recent_4weeks_prev = weekly_prev[-4:] if len(weekly_prev) >= 4 else weekly_prev
            
            if len(recent_4weeks_current) >= 2 and len(recent_4weeks_prev) >= 2:
                recent_avg_current = sum(recent_4weeks_current) / len(recent_4weeks_current)
                recent_avg_prev = sum(recent_4weeks_prev) / len(recent_4weeks_prev)
                
                # 최근 2주 vs 그 이전 2주 비교 (추세 분석)
                if len(recent_4weeks_current) >= 4:
                    latest_2weeks = sum(recent_4weeks_current[-2:]) / 2
                    earlier_2weeks = sum(recent_4weeks_current[:2]) / 2
                    if earlier_2weeks > 0:
                        trend = ((latest_2weeks - earlier_2weeks) / earlier_2weeks * 100)
                        
                        if trend < -10:
                            actions_list.append(f"<strong>💡 액션:</strong> 최근 2주 매출이 이전 2주 대비 <strong>{trend:.1f}%</strong> 하락 추세입니다. 즉각적인 마케팅 개입이 필요합니다.<br>")
                        elif trend > 10:
                            insights_list.append(f"• 최근 2주 매출이 이전 2주 대비 <strong>{trend:+.1f}%</strong> 성장하여 긍정적인 추세를 보이고 있습니다.<br>")
                
                # 전년 동기 대비 최근 4주 평균 비교
                if recent_avg_prev > 0:
                    recent_yoy = ((recent_avg_current / recent_avg_prev - 1) * 100)
                    if recent_yoy > 10:
                        insights_list.append(f"• 최근 4주 평균 매출은 전년 동기 대비 <strong>{recent_yoy:+.1f}%</strong>로 강한 성장세를 보이고 있습니다.<br>")
                    elif recent_yoy < -5:
                        actions_list.append(f"<strong>💡 액션:</strong> 최근 4주 평균 매출이 전년 동기 대비 <strong>{recent_yoy:+.1f}%</strong>로 하락하고 있습니다. 프로모션 강화가 필요합니다.<br>")
        
        # 채널별 추세 분석 추가
        channel_trends = data.get("channel_trends", [])
        if channel_trends:
            # 성장률 기준으로 정렬
            sorted_channels = sorted(channel_trends, key=lambda x: x.get("growth_rate", 0), reverse=True)
            
            # 상위 성장 채널
            growing_channels = [ch for ch in sorted_channels if ch.get("growth_rate", 0) > 10]
            if growing_channels:
                top_growing = growing_channels[0]
                insights_list.append(f"• <strong>{top_growing['channel']}</strong> 채널이 최근 4주간 <strong>{top_growing['growth_rate']:.1f}%</strong> 성장하여 긍정적인 추세를 보이고 있습니다.<br>")
            
            # 하락 채널
            declining_channels = [ch for ch in sorted_channels if ch.get("growth_rate", 0) < -10]
            if declining_channels:
                worst_declining = declining_channels[-1]
                actions_list.append(f"<strong>💡 액션:</strong> <strong>{worst_declining['channel']}</strong> 채널의 최근 4주 매출이 <strong>{worst_declining['growth_rate']:.1f}%</strong> 하락 추세입니다. 즉각적인 마케팅 개입이 필요합니다.<br>")
        
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
        """전체 현황 로컬 분석 - 새로운 형식"""
        insight = ""
        parts = []
        
        # KPI 데이터 분석
        kpi_data = data.get("kpi", {})
        pl_data = data.get("pl", {})
        by_brand = data.get("by_brand", [])
        stock_data = data.get("stock", {})
        
        # 1. 실판매출 목표대비, 전년대비 (월말예상실판매출/목표OR전년 실판매출)
        if kpi_data:
            revenue_forecast = kpi_data.get("revenueForecast", 0) / 100000000
            revenue_plan = kpi_data.get("revenuePlan", 0) / 100000000
            revenue_previous = kpi_data.get("revenuePrevious", 0) / 100000000
            revenue_vs_plan_pct = (revenue_forecast / revenue_plan * 100) if revenue_plan > 0 else 0
            revenue_vs_prev_pct = (revenue_forecast / revenue_previous * 100) if revenue_previous > 0 else 0
            
            if revenue_plan > 0:
                parts.append(f"전체 실판매액은 <strong>{revenue_forecast:.0f}억원</strong>으로 목표 대비 <strong>{revenue_vs_plan_pct:.0f}%</strong>, 전년 대비 <strong>{revenue_vs_prev_pct:.0f}%</strong>를 예상합니다.")
        
        # 2. 할인율 목표대비, 전년대비 (할인율 1-실판매액/TAG매출)
        if kpi_data and pl_data:
            # 할인율 계산: 1 - 실판매액/TAG매출
            tag_revenue = pl_data.get("tagRevenue", {})
            revenue = pl_data.get("revenue", {})
            discount_rate_forecast = 0
            discount_rate_plan = 0
            discount_rate_previous = 0
            
            if tag_revenue and revenue:
                tag_forecast = tag_revenue.get("forecast", 0)
                revenue_forecast_amt = revenue.get("forecast", 0)
                tag_target = tag_revenue.get("target", 0)
                tag_prev = tag_revenue.get("prev", 0)
                revenue_target = revenue.get("target", 0)
                revenue_prev = revenue.get("prev", 0)
                
                # 현재 할인율 계산
                if tag_forecast > 0:
                    discount_rate_forecast = (1 - revenue_forecast_amt / tag_forecast) * 100
                
                # 목표 할인율 계산
                if tag_target > 0 and revenue_target > 0:
                    discount_rate_plan = (1 - revenue_target / tag_target) * 100
                
                # 전년 할인율 계산
                if tag_prev > 0 and revenue_prev > 0:
                    discount_rate_previous = (1 - revenue_prev / tag_prev) * 100
            
            discount_vs_plan_pct = discount_rate_forecast - discount_rate_plan
            discount_vs_prev_pct = discount_rate_forecast - discount_rate_previous
            
            if discount_rate_forecast > 0:
                parts.append(f"할인율은 <strong>{discount_rate_forecast:.1f}%</strong>로 전년대비 <strong>{discount_vs_prev_pct:+.1f}%p</strong> 목표대비 <strong>{discount_vs_plan_pct:+.1f}%p</strong> 입니다.")
        
        # 3. 직접이익 목표대비, 전년대비 (월말예상직접이익/목표OR전년 직접이익)
        if pl_data:
            direct_profit = pl_data.get("directProfit", {})
            revenue = pl_data.get("revenue", {})
            if direct_profit and revenue:
                direct_forecast = direct_profit.get("forecast", 0)
                direct_target = direct_profit.get("target", 0)
                direct_prev = direct_profit.get("prev", 0)
                forecast_revenue = revenue.get("forecast", 0)
                
                # 직접이익율 = 직접이익/실판매액*1.1
                direct_rate = (direct_forecast / forecast_revenue * 100 * 1.1) if forecast_revenue > 0 else 0
                
                # 목표대비, 전년대비 계산
                direct_vs_plan_pct = (direct_forecast / direct_target * 100) if direct_target > 0 else 0
                direct_vs_prev_pct = (direct_forecast / direct_prev * 100) if direct_prev > 0 else 0
                
                parts.append(f"직접이익은 <strong>{direct_forecast:.0f}억원</strong>(직접이익율 {direct_rate:.1f}%)으로 목표 대비 <strong>{direct_vs_plan_pct:.0f}%</strong>, 전년 대비 <strong>{direct_vs_prev_pct:.0f}%</strong>를 달성했습니다.")
        
        # 4. 직접비 매출 비중: 인건비, 임차관리비, 물류운송비 항목만 (직접비/실판매출*1.1)
        if pl_data:
            direct_cost_detail = pl_data.get("directCostDetail", {})
            revenue = pl_data.get("revenue", {})
            if direct_cost_detail and revenue:
                forecast_revenue = revenue.get("forecast", 0)
                
                # 인건비, 임차관리비, 물류운송비만 추출
                labor_cost = direct_cost_detail.get("인건비", {}).get("forecast", 0)
                rent_cost = direct_cost_detail.get("임차관리비", {}).get("forecast", 0)
                logistics_cost = direct_cost_detail.get("물류운송비", {}).get("forecast", 0)
                
                total_selected_cost = labor_cost + rent_cost + logistics_cost
                
                if forecast_revenue > 0:
                    # 직접비 매출 비중 = (직접비/실판매출)*1.1
                    cost_ratio = (total_selected_cost / forecast_revenue * 100 * 1.1) if forecast_revenue > 0 else 0
                    parts.append(f"직접비 매출 비중: 인건비, 임차관리비, 물류운송비 항목만 <strong>{cost_ratio:.1f}%</strong>입니다.")
        
        # 5. 영업이익 목표대비, 전년대비 (월말예상영업이익/목표OR전년 영업이익)
        if pl_data:
            op_profit = pl_data.get("opProfit", {})
            revenue = pl_data.get("revenue", {})
            if op_profit and revenue:
                op_forecast = op_profit.get("forecast", 0)
                op_target = op_profit.get("target", 0)
                op_prev = op_profit.get("prev", 0)
                forecast_revenue = revenue.get("forecast", 0)
                op_rate = (op_forecast / forecast_revenue * 100) if forecast_revenue > 0 else 0
                
                # 목표대비, 전년대비 계산
                op_vs_plan_pct = (op_forecast / op_target * 100) if op_target > 0 else 0
                op_vs_prev_pct = (op_forecast / op_prev * 100) if op_prev > 0 else 0
                
                parts.append(f"영업이익은 <strong>{op_forecast:.0f}억원</strong>(영업이익율 {op_rate:.1f}%)으로 목표대비 <strong>{op_vs_plan_pct:.0f}%</strong>, 전년대비 <strong>{op_vs_prev_pct:.0f}%</strong>입니다.")
        
        # 6. 직접이익 진척율
        if kpi_data:
            progress_rate = kpi_data.get("progressRateForecast", 0)
            if progress_rate > 0:
                parts.append(f"직접이익 진척율은 <strong>{progress_rate:.0f}%</strong>로 월말 목표 달성을 위해 지속적 모니터링이 필요합니다.")
        
        # 인사이트 통합
        insight = " ".join(parts) if parts else "전체 현황 데이터를 분석 중입니다."
        
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
        
        # keyPoints 생성 (핵심인사이트) - 새로운 형식
        key_points = []
        kpi_data = overview_data.get("kpi", {})
        pl_data = overview_data.get("pl", {})
        by_brand_data = overview_data.get("by_brand", {})
        stock_data = overview_data.get("stock", {})
        
        # 1. 전년대비 매출이 높은 브랜드, 낮은 브랜드
        if by_brand_data:
            brand_analysis = []
            
            # 리스트인 경우
            if isinstance(by_brand_data, list):
                for brand_info in by_brand_data:
                    if isinstance(brand_info, dict):
                        brand = brand_info.get("BRAND") or brand_info.get("brand", "")
                        yoy_sales = brand_info.get("YOY_SALES", 0)
                        # SUPRA 제외
                        if brand and brand.upper() != "SUPRA" and yoy_sales > 0:
                            brand_analysis.append({
                                "brand": brand,
                                "yoy": yoy_sales
                            })
            
            if brand_analysis:
                highest_yoy = max(brand_analysis, key=lambda x: x["yoy"])
                lowest_yoy = min(brand_analysis, key=lambda x: x["yoy"])
                key_points.append(f"전년대비 매출증가가 가장 높은 브랜드는 <strong>{highest_yoy['brand']}</strong>({highest_yoy['yoy']}%)이며, 가장 낮은 브랜드는 <strong>{lowest_yoy['brand']}</strong>({lowest_yoy['yoy']}%)입니다.")
        
        # 2. 브랜드별 영업이익 비중이 가장 높은 브랜드, 낮은 브랜드
        if by_brand_data:
            brand_op_profit = []
            total_op_profit = 0
            
            # 리스트인 경우
            if isinstance(by_brand_data, list):
                for brand_info in by_brand_data:
                    if isinstance(brand_info, dict):
                        brand = brand_info.get("BRAND") or brand_info.get("brand", "")
                        op_profit = brand_info.get("OPERATING_PROFIT", 0)
                        # SUPRA 제외
                        if brand and brand.upper() != "SUPRA":
                            brand_op_profit.append({
                                "brand": brand,
                                "op_profit": op_profit
                            })
                            total_op_profit += op_profit if op_profit > 0 else 0
            
            if brand_op_profit and total_op_profit > 0:
                # 영업이익이 양수인 것만 필터링
                positive_brands = [b for b in brand_op_profit if b["op_profit"] > 0]
                if positive_brands:
                    highest_op = max(positive_brands, key=lambda x: x["op_profit"])
                    highest_share = (highest_op["op_profit"] / total_op_profit * 100) if total_op_profit > 0 else 0
                    
                    # 영업이익이 가장 낮은 브랜드 (양수 중)
                    if len(positive_brands) > 1:
                        lowest_op = min(positive_brands, key=lambda x: x["op_profit"])
                        lowest_share = (lowest_op["op_profit"] / total_op_profit * 100) if total_op_profit > 0 else 0
                        key_points.append(f"영업이익 비중이 가장 높은 브랜드는 <strong>{highest_op['brand']}</strong>({highest_op['op_profit']:.0f}억원) 전체비중 <strong>{highest_share:.1f}%</strong>이며, 영업이익이 가장 낮은 브랜드는 <strong>{lowest_op['brand']}</strong>({lowest_op['op_profit']:.0f}억원) 전체비중 <strong>{lowest_share:.1f}%</strong>입니다.")
                    else:
                        key_points.append(f"영업이익 비중이 가장 높은 브랜드는 <strong>{highest_op['brand']}</strong>({highest_op['op_profit']:.0f}억원) 전체비중 <strong>{highest_share:.1f}%</strong>입니다.")
        
        # 3. 판매율이 가장 높은 것, 낮은 것
        if stock_data:
            clothing_data = stock_data.get("clothingBrandStatus", {})
            if clothing_data and isinstance(clothing_data, dict):
                all_items = []
                for brand_code, items in clothing_data.items():
                    # SUPRA 제외 (브랜드 코드 'W')
                    if brand_code == 'W' or brand_code == 'SUPRA':
                        continue
                    
                    if isinstance(items, list):
                        brand_name = BRAND_NAME_MAP.get(brand_code, brand_code)
                        for item in items:
                            if isinstance(item, dict):
                                sales_rate = item.get("cumSalesRate")
                                sales_tag = item.get("cumSalesTag", 0) or item.get("orderTag", 0)
                                item_name = item.get("itemName") or item.get("아이템명", "")
                                if sales_rate is not None and isinstance(sales_rate, (int, float)) and sales_rate > 0 and item_name:
                                    all_items.append({
                                        "name": item_name,
                                        "brand": brand_name,
                                        "rate": sales_rate,
                                        "sales": sales_tag
                                    })
                
                if all_items:
                    highest = max(all_items, key=lambda x: x["rate"])
                    valid_low = [i for i in all_items if i["sales"] > 0]
                    if valid_low:
                        lowest = min(valid_low, key=lambda x: x["rate"])
                        key_points.append(f"판매율이 가장 높은 것은 <strong>{highest['brand']}</strong> <strong>{highest['name']}</strong> ({highest['rate']*100:.1f}%)이며, 낮은 것은 <strong>{lowest['brand']}</strong> <strong>{lowest['name']}</strong>({lowest['rate']*100:.0f}%)입니다.")
        
        # 4. 재고주수 적극발주인 곳 중에 재고주수 가장 적은 곳, 재고주수 긴급조치 중 재고주수 가장 긴 것 중에 매출 1억 이상인 것
        if stock_data:
            acc_data = stock_data.get("accStockAnalysis", {})
            if acc_data and isinstance(acc_data, dict):
                active_order_items = []  # 적극발주
                urgent_action_items = []  # 긴급조치
                
                for brand_code, items in acc_data.items():
                    # SUPRA 제외 (브랜드 코드 'W')
                    if brand_code == 'W' or brand_code == 'SUPRA':
                        continue
                    
                    if isinstance(items, list):
                        brand_name = BRAND_NAME_MAP.get(brand_code, brand_code)
                        for item in items:
                            if isinstance(item, dict):
                                stock_weeks = item.get("stockWeeks")
                                yoy_rate = item.get("yoyRate")
                                sale_amt = item.get("saleAmt", 0)
                                item_name = item.get("itemName") or item.get("아이템명", "")
                                
                                if stock_weeks is not None and isinstance(stock_weeks, (int, float)) and item_name and sale_amt > 0:
                                    # 전년 대비 비율 파싱
                                    yoy_value = None
                                    if yoy_rate:
                                        if isinstance(yoy_rate, (int, float)):
                                            yoy_value = yoy_rate
                                        elif isinstance(yoy_rate, str):
                                            try:
                                                yoy_value = float(yoy_rate.replace('%', '').strip())
                                            except:
                                                pass
                                    
                                    # 적극발주: 재고주수 < 30주 && 전년 대비 >= 120%
                                    if stock_weeks < 30 and yoy_value is not None and yoy_value >= 120:
                                        active_order_items.append({
                                            "name": item_name,
                                            "brand": brand_name,
                                            "sales": sale_amt,
                                            "weeks": stock_weeks
                                        })
                                    
                                    # 긴급조치: 재고주수 >= 50주 && 전년 대비 < 100% && 매출 >= 1억
                                    if stock_weeks >= 50 and yoy_value is not None and yoy_value < 100 and sale_amt >= 100000000:
                                        urgent_action_items.append({
                                            "name": item_name,
                                            "brand": brand_name,
                                            "sales": sale_amt,
                                            "weeks": stock_weeks
                                        })
                
                # 적극발주 중 재고주수가 가장 적은 곳
                inventory_text = ""
                if active_order_items:
                    shortest_active = min(active_order_items, key=lambda x: x["weeks"])
                    sales_millions = shortest_active['sales'] / 1000000
                    inventory_text = f"<strong>{shortest_active['brand']}</strong> <strong>{shortest_active['name']}</strong>(매출: {sales_millions:.0f}백만원) {shortest_active['weeks']:.1f}주로 적극 발주가 필요하며"
                
                # 긴급조치 중 재고주수가 가장 긴 것 중에 매출 1억 이상인 것
                if urgent_action_items:
                    longest_urgent = max(urgent_action_items, key=lambda x: x["weeks"])
                    sales_millions = longest_urgent['sales'] / 1000000
                    if inventory_text:
                        inventory_text += f", <strong>{longest_urgent['brand']}</strong> <strong>{longest_urgent['name']}</strong>(매출: {sales_millions:.0f}백만원) {longest_urgent['weeks']:.1f}주로 긴급 조치가 필요합니다"
                    else:
                        inventory_text = f"<strong>{longest_urgent['brand']}</strong> <strong>{longest_urgent['name']}</strong>(매출: {sales_millions:.0f}백만원) {longest_urgent['weeks']:.1f}주로 긴급 조치가 필요합니다"
                
                if inventory_text:
                    key_points.append(f"아이템 중 {inventory_text}.")
    else:
        overview_insight = ""
        key_points = []
    
    # 전체 현황 그래프별 인사이트 생성
    # 1. 손익계산서 분석
    pl_insight = ""
    if pl_data:
        pl_insight = generator.generate_insight(pl_data, "전체 현황", "pl")
    
    # 2. 트리맵 분석 (브랜드별 기여도)
    treemap_insight = ""
    treemap_file = base_dir / "treemap.json"
    if treemap_file.exists():
        print("[ANALYZING] 전체 현황 트리맵 분석 중...")
        treemap_data = load_json_file(treemap_file)
        if treemap_data:
            # 전체 브랜드 데이터를 하나로 합침
            all_brand_treemap = {}
            if "channelTreemapData" in treemap_data and "byBrand" in treemap_data["channelTreemapData"]:
                all_brand_treemap["channelTreemapData"] = {"byBrand": treemap_data["channelTreemapData"]["byBrand"]}
            if "itemTreemapData" in treemap_data and "byBrand" in treemap_data["itemTreemapData"]:
                all_brand_treemap["itemTreemapData"] = {"byBrand": treemap_data["itemTreemapData"]["byBrand"]}
            if all_brand_treemap:
                treemap_insight = generator.generate_insight(all_brand_treemap, "전체 현황", "treemap")
    
    # 3. 레이더 차트 분석
    radar_insight = ""
    radar_file = base_dir / "radar_chart.json"
    if radar_file.exists():
        print("[ANALYZING] 전체 현황 레이더 차트 분석 중...")
        radar_data = load_json_file(radar_file)
        if radar_data:
            radar_insight = generator.generate_insight(radar_data, "전체 현황", "radar")
    
    # 4. 주차별 매출추세 분석
    weekly_insight = ""
    if trend_data:
        weekly_insight = generator.generate_insight(trend_data, "전체 현황", "weekly")
    
    # 5. 재고주수 분석
    inventory_insight = ""
    sale_rate_insight = ""
    if stock_data:
        if "clothingBrandStatus" in stock_data:
            inventory_insight = generator.generate_insight(stock_data, "전체 현황", "inventory")
            sale_rate_insight = generator.generate_insight(stock_data, "전체 현황", "sale_rate")
    
    # HTML insightsData 형식에 맞게 변환
    # keyPoints는 줄바꿈을 <br>로 변환하여 HTML에서 표시되도록 함
    overview_data_format = {
        "overview": {
            "content": overview_insight,
            "keyPoints": "<br>".join(key_points) if key_points else "",
            "plInsight": pl_insight,
            "treemapInsight": treemap_insight,
            "radarInsight": radar_insight,
            "weeklyInsight": weekly_insight,
            "inventoryInsight": inventory_insight,
            "saleRateInsight": sale_rate_insight
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
            from collections import defaultdict
            
            brand_weekly_data = {}
            brand_weekly_raw = None
            
            # 구조 1: weeklySalesTrend 구조 (20251201 이후)
            if "weeklySalesTrend" in weekly_data and brand_code in weekly_data["weeklySalesTrend"]:
                brand_channels = weekly_data["weeklySalesTrend"][brand_code]
                
                # 주차별로 current와 previous 값을 합산
                weekly_current_dict = defaultdict(float)
                weekly_prev_dict = defaultdict(float)
                
                for channel_name, channel_data in brand_channels.items():
                    if "current" in channel_data:
                        for week_data in channel_data["current"]:
                            week = week_data.get("week", "")
                            value = week_data.get("value", 0) or 0
                            weekly_current_dict[week] += value
                    
                    if "previous" in channel_data:
                        for week_data in channel_data["previous"]:
                            week = week_data.get("week", "")
                            value = week_data.get("value", 0) or 0
                            weekly_prev_dict[week] += value
                
                # 주차 순서대로 정렬하여 배열로 변환
                sorted_weeks = sorted(set(list(weekly_current_dict.keys()) + list(weekly_prev_dict.keys())))
                weekly_current = [weekly_current_dict.get(week, 0) for week in sorted_weeks]
                weekly_prev = [weekly_prev_dict.get(week, 0) for week in sorted_weeks]
                
                # 누적 매출 계산 (원 단위 -> 백만원 단위로 변환)
                cumulative_current = []
                cumulative_prev = []
                cumsum_current = 0
                cumsum_prev = 0
                
                for curr, prev in zip(weekly_current, weekly_prev):
                    cumsum_current += curr / 1000000
                    cumsum_prev += prev / 1000000
                    cumulative_current.append(cumsum_current)
                    cumulative_prev.append(cumsum_prev)
                
                weekly_current_million = [val / 1000000 for val in weekly_current]
                weekly_prev_million = [val / 1000000 for val in weekly_prev]
                
                # 채널별 추세 분석
                channel_trends = []
                for channel_name, channel_data in brand_channels.items():
                    if "current" in channel_data and "previous" in channel_data:
                        current_values = [item.get("value", 0) or 0 for item in channel_data["current"][-4:]]
                        prev_values = [item.get("value", 0) or 0 for item in channel_data["previous"][-4:]]
                        
                        if len(current_values) >= 4 and len(prev_values) >= 4:
                            current_sum = sum(current_values)
                            prev_sum = sum(prev_values)
                            
                            if prev_sum > 0:
                                growth_rate = ((current_sum - prev_sum) / prev_sum) * 100
                                channel_trends.append({
                                    "channel": channel_name,
                                    "growth_rate": growth_rate,
                                    "current_sum": current_sum / 100000000
                                })
                
                brand_weekly_data = {
                    "weekly_current": weekly_current_million,
                    "weekly_prev": weekly_prev_million,
                    "cumulative_current": cumulative_current,
                    "cumulative_prev": cumulative_prev,
                    "channel_trends": channel_trends
                }
                
            # 구조 2: summary.byBrand 구조 (20251117, 20251124)
            elif "summary" in weekly_data and "byBrand" in weekly_data["summary"]:
                if brand_code in weekly_data["summary"]["byBrand"]:
                    brand_weekly_raw = weekly_data["summary"]["byBrand"][brand_code]
                    weekly_dict = brand_weekly_raw.get("weekly", {})
                    
                    # 주차별 데이터 추출 및 정렬
                    weeks_list = list(weekly_dict.keys())
                    # 주차 순서 정렬 (9/21, 10/5 등)
                    def sort_weeks(week_str):
                        parts = week_str.split('/')
                        return (int(parts[0]), int(parts[1]))
                    weeks_list_sorted = sorted(weeks_list, key=sort_weeks)
                    
                    weekly_current = []
                    weekly_prev = []
                    
                    for week_key in weeks_list_sorted:
                        week_data = weekly_dict.get(week_key, {})
                        current_val = week_data.get("당년", 0) or 0
                        prev_val = week_data.get("전년", 0) or 0
                        weekly_current.append(current_val)
                        weekly_prev.append(prev_val)
                    
                    # 누적 매출 계산 (원 단위 -> 백만원 단위)
                    cumulative_current = []
                    cumulative_prev = []
                    cumsum_current = 0
                    cumsum_prev = 0
                    
                    for curr, prev in zip(weekly_current, weekly_prev):
                        cumsum_current += curr / 1000000
                        cumsum_prev += prev / 1000000
                        cumulative_current.append(cumsum_current)
                        cumulative_prev.append(cumsum_prev)
                    
                    weekly_current_million = [val / 1000000 for val in weekly_current]
                    weekly_prev_million = [val / 1000000 for val in weekly_prev]
                    
                    # 채널별 추세 분석 - 최근 4주간 계산
                    channel_trends = []
                    
                    # rawData에서 최근 4주간 채널별 데이터 추출
                    if "rawData" in weekly_data and isinstance(weekly_data.get("rawData"), list):
                        # rawData에서 해당 브랜드의 모든 종료일 추출 및 정렬
                        brand_dates = set()
                        for row in weekly_data["rawData"]:
                            if row.get("브랜드") == brand_code:
                                end_date = row.get("종료일")
                                if end_date:
                                    brand_dates.add(end_date)
                        
                        # 종료일을 정렬하여 최근 4주 선택
                        sorted_dates = sorted(brand_dates)
                        if len(sorted_dates) >= 4:
                            recent_4weeks_dates = sorted_dates[-4:]
                        else:
                            recent_4weeks_dates = sorted_dates
                        
                        # 채널별로 최근 4주간 데이터 집계
                        channel_sums = {}
                        for row in weekly_data["rawData"]:
                            if (row.get("브랜드") == brand_code and 
                                row.get("종료일") in recent_4weeks_dates):
                                channel_name = row.get("채널명", "")
                                if not channel_name:
                                    continue
                                
                                if channel_name not in channel_sums:
                                    channel_sums[channel_name] = {"current": 0, "previous": 0}
                                
                                구분 = row.get("구분", "")
                                실판매출 = row.get("실판매출", 0) or 0
                                
                                if 구분 == "당년":
                                    channel_sums[channel_name]["current"] += 실판매출
                                elif 구분 == "전년":
                                    channel_sums[channel_name]["previous"] += 실판매출
                        
                        # 성장률 계산
                        for ch_name, sums in channel_sums.items():
                            prev_sum = sums["previous"]
                            curr_sum = sums["current"]
                            
                            if prev_sum > 0:
                                growth_rate = ((curr_sum - prev_sum) / prev_sum) * 100
                                channel_trends.append({
                                    "channel": ch_name,
                                    "growth_rate": growth_rate,
                                    "current_sum": curr_sum / 100000000
                                })
                    
                    # rawData가 없으면 기존 방식 사용 (전체 기간)
                    if not channel_trends:
                        channels_dict = brand_weekly_raw.get("channels", {})
                        for channel_name, channel_data in channels_dict.items():
                            if isinstance(channel_data, dict):
                                current_val = channel_data.get("당년", 0) or 0
                                prev_val = channel_data.get("전년", 0) or 0
                                
                                if prev_val > 0:
                                    growth_rate = ((current_val - prev_val) / prev_val) * 100
                                    channel_trends.append({
                                        "channel": channel_name,
                                        "growth_rate": growth_rate,
                                        "current_sum": current_val / 100000000
                                    })
                    
                    brand_weekly_data = {
                        "weekly_current": weekly_current_million,
                        "weekly_prev": weekly_prev_million,
                        "cumulative_current": cumulative_current,
                        "cumulative_prev": cumulative_prev,
                        "channel_trends": channel_trends
                    }
            
            if brand_weekly_data:
                insights["weekly"] = generator.generate_insight(brand_weekly_data, brand, "weekly")
    
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
            
            # 핵심인사이트 생성 (새로운 형식)
            # 1. 현재 시점 기준 판매매출 가장 높은 채널과 아이템
            treemap_file = base_dir / "treemap.json"
            if treemap_file.exists():
                treemap_data = load_json_file(treemap_file)
                if treemap_data:
                    # treemap 구조: channelTreemapData.byBrand.M.channel.channels
                    brand_channel_data = treemap_data.get("channelTreemapData", {}).get("byBrand", {}).get(brand_code, {})
                    brand_item_data = treemap_data.get("itemTreemapData", {}).get("byBrand", {}).get(brand_code, {})
                    
                    channel_treemap = brand_channel_data.get("channel", {}) if isinstance(brand_channel_data, dict) else {}
                    item_treemap = brand_item_data.get("item", {}) if isinstance(brand_item_data, dict) else {}
                    
                    # 채널별 매출 분석
                    channels_data = channel_treemap.get("channels", {}) if isinstance(channel_treemap, dict) else {}
                    if channels_data and isinstance(channels_data, dict):
                        channels = []
                        total_sales = channel_treemap.get("total", {}).get("sales", 0) if isinstance(channel_treemap, dict) else 0
                        for ch_name, ch_data in channels_data.items():
                            if isinstance(ch_data, dict):
                                sales = ch_data.get("sales", 0)
                                share = ch_data.get("share", 0)
                                if sales > 0:
                                    channels.append({
                                        "name": ch_name,
                                        "sales": sales,
                                        "share": share
                                    })
                        
                        if channels and total_sales > 0:
                            # 매출이 가장 높은 채널
                            top_channel = max(channels, key=lambda x: x["sales"])
                            top_channel_sales_billion = top_channel["sales"] / 100000000
                            top_channel_share = (top_channel["sales"] / total_sales * 100) if total_sales > 0 else top_channel["share"]
                            
                            # 현재시점 기준 날짜 계산 (업데이트 일자 -1일, 분석월 넘어가면 월말)
                            from datetime import timedelta
                            try:
                                update_date = datetime.strptime(date_str, "%Y%m%d")
                                current_date = update_date - timedelta(days=1)
                                # 분석월 계산 (YYYYMM)
                                analysis_month = date_str[:6]
                                month_end = datetime.strptime(analysis_month + "01", "%Y%m%d").replace(day=28) + timedelta(days=4)
                                month_end = month_end - timedelta(days=month_end.day)
                                
                                if current_date > month_end:
                                    current_date_str = month_end.strftime("%Y-%m-%d")
                                else:
                                    current_date_str = current_date.strftime("%Y-%m-%d")
                            except:
                                current_date_str = date_str[:4] + "-" + date_str[4:6] + "-" + date_str[6:8]
                            
                            # 아이템별 매출 분석
                            top_item_name = ""
                            top_item_sales = 0
                            top_item_share = 0
                            items_data = item_treemap.get("items", {}) if isinstance(item_treemap, dict) else {}
                            if items_data and isinstance(items_data, dict):
                                items = []
                                total_item_sales = item_treemap.get("total", {}).get("sales", 0) if isinstance(item_treemap, dict) else 0
                                for item_name, item_data in items_data.items():
                                    if isinstance(item_data, dict):
                                        item_sales = item_data.get("sales", 0)
                                        if item_sales > 0:
                                            items.append({
                                                "name": item_name,
                                                "sales": item_sales
                                            })
                                
                                if items and total_item_sales > 0:
                                    top_item = max(items, key=lambda x: x["sales"])
                                    top_item_name = top_item["name"]
                                    top_item_sales = top_item["sales"]
                                    top_item_share = (top_item_sales / total_item_sales * 100) if total_item_sales > 0 else 0
                            
                            if top_item_name:
                                key_points.append(f"- 현재시점기준({current_date_str}) 판매 비중이 가장 높은 채널은 <strong>{top_channel['name']}</strong>({top_channel_sales_billion:.0f}억원)으로 전체 비중 {top_channel_share:.0f}%, 아이템 판매비중이 가장 높은 곳은 <strong>{top_item_name}</strong>({top_item_sales/100000000:.0f}억원)으로 전체비중 {top_item_share:.0f}%입니다.")
                            else:
                                key_points.append(f"- 현재시점기준({current_date_str}) 판매 비중이 가장 높은 채널은 <strong>{top_channel['name']}</strong>({top_channel_sales_billion:.0f}억원)으로 전체 비중 {top_channel_share:.0f}%입니다.")
            
            # 2. 채널 중 직접이익이 가장 높은 곳과 낮은 곳
            channel_pl_file = base_dir / "channel_pl.json"
            if channel_pl_file.exists():
                channel_pl_data = load_json_file(channel_pl_file)
                if channel_pl_data and brand_code in channel_pl_data:
                    brand_channel_pl = channel_pl_data[brand_code]
                    if isinstance(brand_channel_pl, dict):
                        channels_profit = []
                        for ch_name, ch_data in brand_channel_pl.items():
                            if isinstance(ch_data, dict):
                                revenue = ch_data.get("revenue", 0)
                                gross_profit = ch_data.get("grossProfit", 0)
                                direct_cost = ch_data.get("directCost", 0) if ch_data.get("directCost") else 0
                                # 직접이익 = 매출총이익 - 직접비 (또는 직접이익 필드가 있으면 사용)
                                direct_profit = ch_data.get("directProfit", 0) if ch_data.get("directProfit") else (gross_profit - direct_cost)
                                direct_profit_rate = (direct_profit / revenue * 100) if revenue > 0 else 0
                                
                                if revenue > 0:
                                    channels_profit.append({
                                        "name": ch_name,
                                        "direct_profit": direct_profit,
                                        "direct_profit_rate": direct_profit_rate
                                    })
                        
                        if channels_profit:
                            highest_profit = max(channels_profit, key=lambda x: x["direct_profit"])
                            lowest_profit = min(channels_profit, key=lambda x: x["direct_profit"])
                            
                            highest_profit_billion = highest_profit["direct_profit"] / 100000000
                            lowest_profit_billion = lowest_profit["direct_profit"] / 100000000
                            
                            if highest_profit["name"] != lowest_profit["name"]:
                                key_points.append(f"- 월말 예상 직접이익이 가장 높은 채널은 <strong>{highest_profit['name']}</strong>으로 {highest_profit_billion:.1f}억원(직접이익율 {highest_profit['direct_profit_rate']:.0f}%), 가장 낮은 채널은 <strong>{lowest_profit['name']}</strong>으로 {lowest_profit_billion:.1f}억원(직접이익율 {lowest_profit['direct_profit_rate']:.0f}%)입니다.")
                            else:
                                key_points.append(f"- 월말 예상 직접이익이 가장 높은 채널은 <strong>{highest_profit['name']}</strong>으로 {highest_profit_billion:.1f}억원(직접이익율 {highest_profit['direct_profit_rate']:.0f}%)입니다.")
            
            # 3. 최근 4주간 매출추세가 가장 좋은 채널, 나쁜 채널
            weekly_file = base_dir / "weekly_trend.json"
            if weekly_file.exists():
                weekly_data = load_json_file(weekly_file)
                if weekly_data:
                    # 새로운 구조: weeklySalesTrend.byBrand.M
                    if "weeklySalesTrend" in weekly_data and brand_code in weekly_data["weeklySalesTrend"]:
                        brand_channels = weekly_data["weeklySalesTrend"][brand_code]
                        channel_trends = []
                        
                        for channel_name, channel_data in brand_channels.items():
                            if isinstance(channel_data, dict) and "current" in channel_data and "previous" in channel_data:
                                current_values = [item.get("value", 0) or 0 for item in channel_data["current"][-4:]]
                                prev_values = [item.get("value", 0) or 0 for item in channel_data["previous"][-4:]]
                                
                                if len(current_values) >= 4 and len(prev_values) >= 4:
                                    current_sum = sum(current_values)
                                    prev_sum = sum(prev_values)
                                    
                                    if prev_sum > 0:
                                        growth_rate = ((current_sum - prev_sum) / prev_sum) * 100
                                        channel_trends.append({
                                            "name": channel_name,
                                            "trend": growth_rate,
                                            "current_sum": current_sum
                                        })
                        
                        if channel_trends:
                            best_channel = max(channel_trends, key=lambda x: x["trend"])
                            worst_channel = min(channel_trends, key=lambda x: x["trend"])
                            
                            if best_channel["name"] != worst_channel["name"]:
                                key_points.append(f"- 최근 4주간 <strong>{best_channel['name']}</strong> 채널이 {best_channel['trend']:+.1f}% 성장하여 긍정적 추세를 보이는 반면, <strong>{worst_channel['name']}</strong> 채널의 매출이 {worst_channel['trend']:+.1f}%로 하락 추세입니다.")
                            else:
                                key_points.append(f"- 최근 4주간 <strong>{best_channel['name']}</strong> 채널이 {best_channel['trend']:+.1f}% 성장하여 긍정적 추세를 보이고 있습니다.")
                    # 기존 구조: summary.byBrand.M (weekly_trend.json)
                    elif "summary" in weekly_data and "byBrand" in weekly_data["summary"] and brand_code in weekly_data["summary"]["byBrand"]:
                        # rawData에서 최근 4주간 채널별 데이터 추출
                        channel_trends = []
                        
                        # rawData가 있는 경우 주차별 채널 데이터 사용
                        if "rawData" in weekly_data and isinstance(weekly_data["rawData"], list):
                            # rawData에서 해당 브랜드의 모든 종료일 추출 및 정렬
                            brand_dates = set()
                            for row in weekly_data["rawData"]:
                                if row.get("브랜드") == brand_code:
                                    end_date = row.get("종료일")
                                    if end_date:
                                        brand_dates.add(end_date)
                            
                            # 종료일을 정렬하여 최근 4주 선택
                            sorted_dates = sorted(brand_dates)
                            if len(sorted_dates) >= 4:
                                recent_4weeks_dates = sorted_dates[-4:]
                            else:
                                recent_4weeks_dates = sorted_dates
                                
                                # 채널별로 최근 4주간 데이터 집계
                                channel_sums = {}
                                for row in weekly_data["rawData"]:
                                    if (row.get("브랜드") == brand_code and 
                                        row.get("종료일") in recent_4weeks_dates):
                                        channel_name = row.get("채널명", "")
                                        if not channel_name:
                                            continue
                                        
                                        if channel_name not in channel_sums:
                                            channel_sums[channel_name] = {"current": 0, "previous": 0}
                                        
                                        구분 = row.get("구분", "")
                                        실판매출 = row.get("실판매출", 0) or 0
                                        
                                        if 구분 == "당년":
                                            channel_sums[channel_name]["current"] += 실판매출
                                        elif 구분 == "전년":
                                            channel_sums[channel_name]["previous"] += 실판매출
                                
                                # 성장률 계산
                                for ch_name, sums in channel_sums.items():
                                    prev_sum = sums["previous"]
                                    curr_sum = sums["current"]
                                    
                                    if prev_sum > 0:
                                        growth_rate = ((curr_sum - prev_sum) / prev_sum) * 100
                                        channel_trends.append({
                                            "name": ch_name,
                                            "trend": growth_rate
                                        })
                        
                        # rawData가 없으면 기존 방식 사용 (전체 기간)
                        if not channel_trends:
                            brand_weekly = weekly_data["summary"]["byBrand"][brand_code]
                            channels_weekly = brand_weekly.get("channels", {})
                            
                            if channels_weekly and isinstance(channels_weekly, dict):
                                for ch_name, ch_data in channels_weekly.items():
                                    if isinstance(ch_data, dict) and "YOY" in ch_data:
                                        yoy = ch_data.get("YOY", 0)
                                        if isinstance(yoy, (int, float)) and yoy != 0:
                                            channel_trends.append({
                                                "name": ch_name,
                                                "trend": yoy
                                            })
                        
                        if channel_trends:
                            best_channel = max(channel_trends, key=lambda x: x["trend"])
                            worst_channel = min(channel_trends, key=lambda x: x["trend"])
                            
                            if best_channel["name"] != worst_channel["name"] and worst_channel["trend"] < 0:
                                key_points.append(f"- 최근 4주간 <strong>{best_channel['name']}</strong> 채널이 {best_channel['trend']:+.1f}% 성장하여 긍정적 추세를 보이는 반면, <strong>{worst_channel['name']}</strong> 채널의 매출이 {worst_channel['trend']:+.1f}%로 하락 추세입니다.")
                            elif best_channel["trend"] > 0:
                                key_points.append(f"- 최근 4주간 <strong>{best_channel['name']}</strong> 채널이 {best_channel['trend']:+.1f}% 성장하여 긍정적 추세를 보이고 있습니다.")
                    # 더 이상 사용되지 않는 구조: byBrand.M (직접)
                    elif "byBrand" in weekly_data and brand_code in weekly_data["byBrand"]:
                        brand_weekly = weekly_data["byBrand"][brand_code]
                        weekly_data_brand = brand_weekly.get("weekly", {})
                        channels_weekly = brand_weekly.get("channels", {})
                        
                        # 최근 4주간 추세 계산 (weekly 데이터에서)
                        if weekly_data_brand and isinstance(weekly_data_brand, dict):
                            # 주차별 데이터를 날짜 순으로 정렬
                            weeks_list = list(weekly_data_brand.keys())
                            def sort_weeks(week_str):
                                parts = week_str.split('/')
                                return (int(parts[0]), int(parts[1]))
                            weeks_list_sorted = sorted(weeks_list, key=sort_weeks)
                            
                            # 최근 4주 추출
                            if len(weeks_list_sorted) >= 4:
                                recent_4weeks = weeks_list_sorted[-4:]
                                recent_current_sum = 0
                                recent_prev_sum = 0
                                
                                for week_key in recent_4weeks:
                                    week_data = weekly_data_brand.get(week_key, {})
                                    if isinstance(week_data, dict):
                                        recent_current_sum += week_data.get("당년", 0) or 0
                                        recent_prev_sum += week_data.get("전년", 0) or 0
                                
                                # 전체 기간 합계 (비교용)
                                total_current_sum = 0
                                total_prev_sum = 0
                                for week_key in weeks_list_sorted:
                                    week_data = weekly_data_brand.get(week_key, {})
                                    if isinstance(week_data, dict):
                                        total_current_sum += week_data.get("당년", 0) or 0
                                        total_prev_sum += week_data.get("전년", 0) or 0
                                
                                if recent_prev_sum > 0:
                                    recent_trend = ((recent_current_sum - recent_prev_sum) / recent_prev_sum) * 100
                                    
                                    # 채널별 추세는 channels 데이터에서 가져오기
                                    if channels_weekly and isinstance(channels_weekly, dict):
                                        channel_trends = []
                                        for ch_name, ch_data in channels_weekly.items():
                                            if isinstance(ch_data, dict) and "YOY" in ch_data:
                                                yoy = ch_data.get("YOY", 0)
                                                if isinstance(yoy, (int, float)) and yoy != 0:
                                                    channel_trends.append({
                                                        "name": ch_name,
                                                        "trend": yoy
                                                    })
                                        
                                        if channel_trends:
                                            best_channel = max(channel_trends, key=lambda x: x["trend"])
                                            worst_channel = min(channel_trends, key=lambda x: x["trend"])
                                            
                                            if best_channel["name"] != worst_channel["name"] and worst_channel["trend"] < 0:
                                                key_points.append(f"- 최근 4주간 <strong>{best_channel['name']}</strong> 채널이 {best_channel['trend']:+.1f}% 성장하여 긍정적 추세를 보이는 반면, <strong>{worst_channel['name']}</strong> 채널의 매출이 {worst_channel['trend']:+.1f}%로 하락 추세입니다.")
                                            elif best_channel["trend"] > 0:
                                                key_points.append(f"- 최근 4주간 <strong>{best_channel['name']}</strong> 채널이 {best_channel['trend']:+.1f}% 성장하여 긍정적 추세를 보이고 있습니다.")
            
            # 4. 누적판매매출 높은거 2개, 누적판매매출이 0원인곳 제외 상위 30%중 판매율 차이가 가장 작은곳
            stock_file = base_dir / "stock_analysis.json"
            if stock_file.exists():
                stock_data = load_json_file(stock_file)
                if stock_data:
                    clothing_status = stock_data.get("clothingBrandStatus", {})
                    if brand_code in clothing_status:
                        brand_clothing = clothing_status[brand_code]
                        if isinstance(brand_clothing, list):
                            # 누적판매매출이 0원인곳 제외
                            valid_items = [item for item in brand_clothing if isinstance(item, dict) and (item.get("cumSalesTag") or 0) > 0]
                            
                            if valid_items:
                                # 누적판매매출 기준 정렬
                                sorted_by_sales = sorted(valid_items, key=lambda x: (x.get("cumSalesTag") or 0), reverse=True)
                                
                                # 상위 2개
                                top2_items = sorted_by_sales[:2]
                                
                                # 상위 30% 계산
                                top30_count = max(1, int(len(sorted_by_sales) * 0.3))
                                top30_items = sorted_by_sales[:top30_count]
                                
                                # 판매율 차이(cumSalesRateDiff)가 가장 작은 것 (절대값 기준)
                                if top30_items:
                                    min_diff_item = min(top30_items, key=lambda x: abs(x.get("cumSalesRateDiff", 999)) if x.get("cumSalesRateDiff") is not None else 999)
                                    
                                    # 상위 2개 아이템 정보
                                    if len(top2_items) >= 2:
                                        item1 = top2_items[0]
                                        item2 = top2_items[1]
                                        item1_name = item1.get("itemName", "")
                                        item1_rate = item1.get("cumSalesRate", 0) * 100 if item1.get("cumSalesRate") else 0
                                        item1_diff = item1.get("cumSalesRateDiff", 0) * 100 if item1.get("cumSalesRateDiff") is not None else 0
                                        item2_name = item2.get("itemName", "")
                                        item2_rate = item2.get("cumSalesRate", 0) * 100 if item2.get("cumSalesRate") else 0
                                        item2_diff = item2.get("cumSalesRateDiff", 0) * 100 if item2.get("cumSalesRateDiff") is not None else 0
                                        
                                        # 판매율 차이가 가장 작은 것 (절대값 기준, 0에 가까운 것)
                                        min_diff_item = min(top30_items, key=lambda x: abs(x.get("cumSalesRateDiff", 999)) if x.get("cumSalesRateDiff") is not None else 999)
                                        min_diff_name = min_diff_item.get("itemName", "")
                                        min_diff_rate = min_diff_item.get("cumSalesRate", 0) * 100 if min_diff_item.get("cumSalesRate") else 0
                                        min_diff_value = min_diff_item.get("cumSalesRateDiff", 0) * 100 if min_diff_item.get("cumSalesRateDiff") is not None else 0
                                        
                                        # 1위, 2위와 min_diff_item이 다른 경우만 추가
                                        if min_diff_name != item1_name and min_diff_name != item2_name:
                                            key_points.append(f"- 의류 누적 매출 1위: <strong>{item1_name}</strong>로 판매율 {item1_rate:.1f}%(전년대비 {item1_diff:+.1f}%p), 2위: <strong>{item2_name}</strong> 판매율 {item2_rate:.1f}%(전년대비 {item2_diff:+.1f}%p), 반면 <strong>{min_diff_name}</strong>는 누적판매율 전년대비 {min_diff_value:+.1f}%p로 조치 필요합니다.")
                                        else:
                                            key_points.append(f"- 의류 누적 매출 1위: <strong>{item1_name}</strong>로 판매율 {item1_rate:.1f}%(전년대비 {item1_diff:+.1f}%p), 2위: <strong>{item2_name}</strong> 판매율 {item2_rate:.1f}%(전년대비 {item2_diff:+.1f}%p)입니다.")
                                    elif len(top2_items) >= 1:
                                        item1 = top2_items[0]
                                        item1_name = item1.get("itemName", "")
                                        item1_rate = item1.get("cumSalesRate", 0) * 100 if item1.get("cumSalesRate") else 0
                                        item1_diff = item1.get("cumSalesRateDiff", 0) * 100 if item1.get("cumSalesRateDiff") is not None else 0
                                        min_diff_name = min_diff_item.get("itemName", "")
                                        min_diff_value = min_diff_item.get("cumSalesRateDiff", 0) * 100 if min_diff_item.get("cumSalesRateDiff") is not None else 0
                                        
                                        key_points.append(f"- 의류 누적 매출 1위: <strong>{item1_name}</strong>로 판매율 {item1_rate:.1f}%(전년대비 {item1_diff:+.1f}%p), 반면 <strong>{min_diff_name}</strong>는 누적판매율 전년대비 {min_diff_value:+.1f}%p로 조치 필요합니다.")
            
            # 5. 재고주수 판매매출 높은거 2개, 판매매출이 0원인곳 제외 상위 30%중 재고주수가 가장 높은곳
            if stock_file.exists():
                stock_data = load_json_file(stock_file)
                if stock_data:
                    acc_stock = stock_data.get("accStockAnalysis", {})
                    if brand_code in acc_stock:
                        brand_acc = acc_stock[brand_code]
                        if isinstance(brand_acc, list):
                            # 판매매출이 0원인곳 제외
                            valid_acc_items = [item for item in brand_acc if isinstance(item, dict) and (item.get("saleAmt") or 0) > 0]
                            
                            if valid_acc_items:
                                # 판매매출 기준 정렬
                                sorted_by_sales = sorted(valid_acc_items, key=lambda x: (x.get("saleAmt") or 0), reverse=True)
                                
                                # 상위 2개
                                top2_acc = sorted_by_sales[:2]
                                
                                # 상위 30% 계산
                                top30_count = max(1, int(len(sorted_by_sales) * 0.3))
                                top30_acc = sorted_by_sales[:top30_count]
                                
                                # 재고주수가 가장 높은 것
                                if top30_acc:
                                    max_stock_item = max(top30_acc, key=lambda x: x.get("stockWeeks", 0) if x.get("stockWeeks") is not None else 0)
                                    
                                    # 상위 2개 아이템 정보
                                    if len(top2_acc) >= 2:
                                        acc1 = top2_acc[0]
                                        acc2 = top2_acc[1]
                                        acc1_name = acc1.get("itemName", "")
                                        acc1_weeks = acc1.get("stockWeeks", 0) if acc1.get("stockWeeks") is not None else 0
                                        acc1_diff = acc1.get("stockWeeksDiff", 0) if acc1.get("stockWeeksDiff") is not None else 0
                                        acc2_name = acc2.get("itemName", "")
                                        acc2_weeks = acc2.get("stockWeeks", 0) if acc2.get("stockWeeks") is not None else 0
                                        acc2_diff = acc2.get("stockWeeksDiff", 0) if acc2.get("stockWeeksDiff") is not None else 0
                                        
                                        max_stock_name = max_stock_item.get("itemName", "")
                                        max_stock_weeks = max_stock_item.get("stockWeeks", 0) if max_stock_item.get("stockWeeks") is not None else 0
                                        max_stock_diff = max_stock_item.get("stockWeeksDiff", 0) if max_stock_item.get("stockWeeksDiff") is not None else 0
                                        
                                        key_points.append(f"- 아이템 누적판매매출 1위: <strong>{acc1_name}</strong> 재고주수 {acc1_weeks:.1f}주(전년대비 {acc1_diff:+.1f}주) 2위: <strong>{acc2_name}</strong> 재고주수 {acc2_weeks:.1f}주(전년대비 {acc2_diff:+.1f}주), 반면 <strong>{max_stock_name}</strong>는 재고주수 {max_stock_weeks:.1f}주(전년대비 {max_stock_diff:+.1f}주)로 관리필요합니다.")
                                    elif len(top2_acc) >= 1:
                                        acc1 = top2_acc[0]
                                        acc1_name = acc1.get("itemName", "")
                                        acc1_weeks = acc1.get("stockWeeks", 0) if acc1.get("stockWeeks") is not None else 0
                                        acc1_diff = acc1.get("stockWeeksDiff", 0) if acc1.get("stockWeeksDiff") is not None else 0
                                        
                                        max_stock_name = max_stock_item.get("itemName", "")
                                        max_stock_weeks = max_stock_item.get("stockWeeks", 0) if max_stock_item.get("stockWeeks") is not None else 0
                                        max_stock_diff = max_stock_item.get("stockWeeksDiff", 0) if max_stock_item.get("stockWeeksDiff") is not None else 0
                                        
                                        key_points.append(f"- 아이템 누적판매매출 1위: <strong>{acc1_name}</strong> 재고주수 {acc1_weeks:.1f}주(전년대비 {acc1_diff:+.1f}주), 반면 <strong>{max_stock_name}</strong>는 재고주수 {max_stock_weeks:.1f}주(전년대비 {max_stock_diff:+.1f}주)로 관리필요합니다.")
            
            # 6. 직접비 실판대비 비율 (인건비, 임차관리비, 물류운송비)
            pl_file = base_dir / "brand_pl.json"
            if pl_file.exists():
                pl_data = load_json_file(pl_file)
                if pl_data and brand in pl_data:
                    brand_pl = pl_data[brand]
                    if isinstance(brand_pl, dict):
                        revenue = brand_pl.get("revenue", {})
                        direct_cost_detail = brand_pl.get("directCostDetail", {})
                        
                        if revenue and direct_cost_detail:
                            forecast_revenue = revenue.get("forecast", 0)
                            
                            # 인건비, 임차관리비, 물류운송비만 추출
                            labor_cost = direct_cost_detail.get("인건비", {}).get("forecast", 0) if isinstance(direct_cost_detail.get("인건비"), dict) else 0
                            rent_cost = direct_cost_detail.get("임차관리비", {}).get("forecast", 0) if isinstance(direct_cost_detail.get("임차관리비"), dict) else 0
                            logistics_cost = direct_cost_detail.get("물류운송비", {}).get("forecast", 0) if isinstance(direct_cost_detail.get("물류운송비"), dict) else 0
                            
                            total_selected_cost = labor_cost + rent_cost + logistics_cost
                            
                            if forecast_revenue > 0:
                                # 직접비 매출 비중 = (직접비/실판매출)*1.1
                                labor_ratio = (labor_cost / forecast_revenue * 100 * 1.1) if forecast_revenue > 0 else 0
                                rent_ratio = (rent_cost / forecast_revenue * 100 * 1.1) if forecast_revenue > 0 else 0
                                logistics_ratio = (logistics_cost / forecast_revenue * 100 * 1.1) if forecast_revenue > 0 else 0
                                
                                if labor_ratio > 0 or rent_ratio > 0 or logistics_ratio > 0:
                                    key_points.append(f"- 직접비는 실판대비 인건비 {labor_ratio:.1f}%, 임차관리비 {rent_ratio:.1f}%, 물류운송비 {logistics_ratio:.1f}%입니다.")
    
    # HTML insightsData 형식에 맞게 변환
    insights_data_format = {
        brand: {
            "content": content,
            "keyPoints": "<br>".join(key_points) if key_points else "",
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
    
    # 전체 현황 분석 (--overview 옵션이 있거나 --all-brands 옵션이 있을 때)
    if args.overview or args.all_brands:
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

