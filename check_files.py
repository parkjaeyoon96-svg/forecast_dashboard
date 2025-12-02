from scripts.path_utils import get_plan_file_path, get_previous_year_file_path, extract_year_month_from_date

date_str = '20251124'
year_month = extract_year_month_from_date(date_str)

print('=' * 80)
print('채널별 계획/전년 데이터 추출 스크립트가 읽는 CSV 파일')
print('=' * 80)
print()

print('1. 계획 데이터 파일:')
plan_file = get_plan_file_path(year_month)
print(f'   경로: {plan_file}')
print(f'   파일명: plan_{year_month}_전처리완료.csv')
print(f'   위치: raw/{year_month}/plan/')
print()

print('2. 전년 데이터 파일:')
prev_file = get_previous_year_file_path(year_month, f'previous_rawdata_{year_month}_Shop.csv')
print(f'   경로: {prev_file}')
print(f'   파일명: previous_rawdata_{year_month}_Shop.csv')
print(f'   위치: raw/{year_month}/previous_year/')
print()

print('=' * 80)
print('읽는 컬럼:')
print('=' * 80)
print()
print('계획 파일 컬럼:')
print('  - 브랜드')
print('  - 채널')
print('  - 실판매액 [v+]')
print()
print('전년 파일 컬럼:')
print('  - 브랜드코드')
print('  - 채널명')
print('  - 부가세제외 실판매액')



