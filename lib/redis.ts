import { Redis } from '@upstash/redis';

/**
 * Upstash Redis 클라이언트
 * 환경 변수가 없으면 null 반환 (로컬 캐시로 폴백)
 */
export function getRedisClient(): Redis | null {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!url || !token) {
    console.log('[Redis] 환경 변수 미설정 - 파일 캐시 사용');
    return null;
  }

  try {
    return new Redis({ url, token });
  } catch (error) {
    console.error('[Redis] 클라이언트 생성 실패:', error);
    return null;
  }
}

/**
 * Redis에서 캐시 조회
 */
export async function getCache<T>(key: string): Promise<T | null> {
  const redis = getRedisClient();
  if (!redis) return null;

  try {
    const cached = await redis.get<T>(key);
    if (cached) {
      console.log(`[Redis 캐시 적중] ${key}`);
      return cached;
    }
    console.log(`[Redis 캐시 미스] ${key}`);
    return null;
  } catch (error) {
    console.error(`[Redis 조회 실패] ${key}:`, error);
    return null;
  }
}

/**
 * Redis에 캐시 저장
 * @param key 캐시 키
 * @param value 저장할 값
 * @param ttlSeconds TTL (초), 기본 24시간
 */
export async function setCache<T>(
  key: string,
  value: T,
  ttlSeconds: number = 86400 // 24시간
): Promise<boolean> {
  const redis = getRedisClient();
  if (!redis) return false;

  try {
    await redis.set(key, value, { ex: ttlSeconds });
    console.log(`[Redis 캐시 저장 완료] ${key} (TTL: ${ttlSeconds}초)`);
    return true;
  } catch (error) {
    console.error(`[Redis 저장 실패] ${key}:`, error);
    return false;
  }
}

/**
 * Redis 캐시 삭제
 */
export async function deleteCache(key: string): Promise<boolean> {
  const redis = getRedisClient();
  if (!redis) return false;

  try {
    await redis.del(key);
    console.log(`[Redis 캐시 삭제] ${key}`);
    return true;
  } catch (error) {
    console.error(`[Redis 삭제 실패] ${key}:`, error);
    return false;
  }
}

