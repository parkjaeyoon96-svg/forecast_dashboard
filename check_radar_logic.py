"""
레이더 차트 로직 확인 스크립트
"""

print('=' * 80)
print('레이더 차트 로직 확인')
print('=' * 80)
print()

print('요구사항:')
print('1. 당년: 채널별집계파일에서 브랜드별, 채널별 실판매출금액')
print('2. 전년: 채널별집계파일에서 브랜드별, 채널별 실판매출금액')
print('3. 계획: 계획전처리파일에서 브랜드별, 채널별 실판매출금액 (내수합계 제외)')
print()

print('현재 구현:')
print('1. 당년: brandData[brand].channels에서 forecast 값 사용')
print('   - create_treemap_data.py에서 ke30_전처리완료.csv 파일 읽음')
print('   - 이건 현시점 데이터인지 forecast 데이터인지 확인 필요')
print()
print('2. 전년: brandKPI[brand].channelYoy 사용 ✓')
print('   - previous_rawdata_YYYYMM_Shop.csv 파일에서 추출')
print('   - 실매출액(부가세 포함) 사용 ✓')
print()
print('3. 계획: brandKPI[brand].channelPlan 사용 ✓')
print('   - plan_YYYYMM_전처리완료.csv 파일에서 추출')
print('   - 내수합계 제외 처리됨 ✓')
print()

print('=' * 80)
print('확인 필요 사항:')
print('=' * 80)
print('1. 당년 데이터가 forecast 파일에서 오는지 확인')
print('   - forecast_YYYYMMDD_YYYYMM_Shop.csv 또는')
print('   - forecast_YYYYMMDD_YYYYMM_Shop_item.csv')
print()
print('2. brandData.channels의 forecast 값이 어디서 오는지 확인')
print('   - create_treemap_data.py가 어떤 파일을 읽는지')
print('   - ke30 파일인지 forecast 파일인지')



