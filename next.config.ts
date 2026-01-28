import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  // public 폴더의 정적 파일들이 제대로 서빙되도록 보장
  // Next.js는 기본적으로 public 폴더의 파일들을 자동으로 서빙합니다
  
  // Snowflake SDK는 서버 전용 패키지이므로 클라이언트 번들에서 제외
  // Next.js 16에서는 experimental에서 최상위 레벨로 이동
  serverExternalPackages: ['snowflake-sdk'],
  
  // Turbopack 설정 (빈 객체로 설정하여 경고 제거)
  turbopack: {},
};

export default nextConfig;
