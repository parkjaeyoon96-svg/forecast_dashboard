import { NextResponse } from 'next/server';

/**
 * Snowflake 환경변수 / Private Key 상태 진단 엔드포인트 (임시)
 *
 * 실제 키 내용은 반환하지 않고, 포맷 메타정보만 반환합니다.
 * 문제 해결 후 반드시 삭제하세요.
 *
 * 접근: GET /api/debug/snowflake?token=<DEBUG_TOKEN>
 *   - 환경변수 DEBUG_TOKEN 과 쿼리스트링 token 이 일치해야 동작
 *   - DEBUG_TOKEN 이 설정되지 않았다면 항상 403
 */
export async function GET(request: Request) {
  const expected = process.env.DEBUG_TOKEN;
  const { searchParams } = new URL(request.url);
  const provided = searchParams.get('token');

  if (!expected || !provided || expected !== provided) {
    return NextResponse.json({ error: 'forbidden' }, { status: 403 });
  }

  const rawBase64 = process.env.SNOWFLAKE_PRIVATE_KEY_BASE64;
  const rawPem = process.env.SNOWFLAKE_PRIVATE_KEY;

  type Info = {
    present: boolean;
    length: number | null;
    firstChars: string | null;
    lastChars: string | null;
    hasWhitespace: boolean | null;
    hasNewline: boolean | null;
    hasCarriageReturn: boolean | null;
  };

  function snapshot(v: string | undefined): Info {
    if (!v) return {
      present: false, length: null, firstChars: null, lastChars: null,
      hasWhitespace: null, hasNewline: null, hasCarriageReturn: null,
    };
    return {
      present: true,
      length: v.length,
      firstChars: v.slice(0, 20),
      lastChars: v.slice(-20),
      hasWhitespace: /\s/.test(v),
      hasNewline: v.includes('\n'),
      hasCarriageReturn: v.includes('\r'),
    };
  }

  let decoded: {
    attempted: boolean;
    ok: boolean;
    length: number | null;
    firstLine: string | null;
    lastLine: string | null;
    lineCount: number | null;
    hasBeginMarker: boolean | null;
    hasEndMarker: boolean | null;
    error: string | null;
  } = {
    attempted: false, ok: false, length: null, firstLine: null,
    lastLine: null, lineCount: null, hasBeginMarker: null, hasEndMarker: null,
    error: null,
  };

  if (rawBase64) {
    decoded.attempted = true;
    try {
      const str = Buffer.from(rawBase64.trim(), 'base64').toString('utf8');
      const lines = str.split('\n');
      decoded = {
        attempted: true,
        ok: true,
        length: str.length,
        firstLine: lines[0] ?? null,
        lastLine: lines[lines.length - 1] ?? null,
        lineCount: lines.length,
        hasBeginMarker: /-----BEGIN [^-]+-----/.test(str),
        hasEndMarker: /-----END [^-]+-----/.test(str),
        error: null,
      };
    } catch (e: any) {
      decoded.error = String(e?.message ?? e);
    }
  }

  return NextResponse.json({
    envPresence: {
      SNOWFLAKE_ACCOUNT: !!process.env.SNOWFLAKE_ACCOUNT,
      SNOWFLAKE_USERNAME: !!process.env.SNOWFLAKE_USERNAME,
      SNOWFLAKE_WAREHOUSE: !!process.env.SNOWFLAKE_WAREHOUSE,
      SNOWFLAKE_DATABASE: !!process.env.SNOWFLAKE_DATABASE,
      SNOWFLAKE_PRIVATE_KEY: !!process.env.SNOWFLAKE_PRIVATE_KEY,
      SNOWFLAKE_PRIVATE_KEY_BASE64: !!process.env.SNOWFLAKE_PRIVATE_KEY_BASE64,
      SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: !!process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
    },
    privateKeyBase64Raw: snapshot(rawBase64),
    privateKeyRaw: snapshot(rawPem),
    privateKeyDecodedFromBase64: decoded,
    nodeVersion: process.version,
    vercelEnv: process.env.VERCEL_ENV ?? null,
  });
}
