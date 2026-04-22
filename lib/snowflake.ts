/**
 * Snowflake 연결 및 쿼리 유틸리티
 * 
 * Node.js 환경에서 Snowflake 데이터베이스에 연결하고 쿼리를 실행합니다.
 * 인증 방식: 서비스 계정 + RSA Private Key (SNOWFLAKE_JWT)
 * Vercel에서 완벽하게 작동합니다.
 */

import snowflake from 'snowflake-sdk';

/**
 * PEM 문자열 정규화
 *
 * 다음과 같은 다양한 저장 형태의 Private Key를 표준 PEM 문자열로 변환합니다.
 *  1) 이미 줄바꿈이 포함된 정상 PEM
 *  2) `\n` / `\r\n` escape 시퀀스가 들어간 한 줄 문자열 (.env 저장 시 흔함)
 *  3) 공백으로 합쳐진 한 줄 문자열 (Vercel UI 붙여넣기 실패 케이스)
 *  4) BEGIN/END 없이 base64 body만 들어있는 문자열
 *
 * 양끝 공백/따옴표 및 BOM(\uFEFF)도 제거합니다.
 */
function normalizePem(raw: string): string {
  let key = raw.replace(/^\uFEFF/, '').trim();

  // 양끝에 감싸진 따옴표 제거
  if ((key.startsWith('"') && key.endsWith('"')) ||
      (key.startsWith("'") && key.endsWith("'"))) {
    key = key.slice(1, -1).trim();
  }

  // literal "\n" / "\r\n" 을 실제 개행으로 복원
  if (!key.includes('\n')) {
    key = key.replace(/\\r\\n/g, '\n').replace(/\\n/g, '\n');
  }

  // PEM 헤더/푸터 탐색
  const headerRe = /-----BEGIN [^-]+-----/;
  const footerRe = /-----END [^-]+-----/;
  const headerMatch = key.match(headerRe);
  const footerMatch = key.match(footerRe);

  if (headerMatch && footerMatch) {
    const header = headerMatch[0];
    const footer = footerMatch[0];
    const bodyStart = key.indexOf(header) + header.length;
    const bodyEnd = key.indexOf(footer);
    // 본문에서 모든 공백류(스페이스/개행/탭) 제거 → 순수 base64만 남김
    const body = key
      .slice(bodyStart, bodyEnd)
      .replace(/[\s\r\n]+/g, '');

    // base64 본문을 64자씩 개행으로 포맷팅 (표준 PEM)
    const wrapped = body.match(/.{1,64}/g)?.join('\n') ?? body;
    return `${header}\n${wrapped}\n${footer}\n`;
  }

  return key;
}

/**
 * SNOWFLAKE_PRIVATE_KEY 환경변수 값을 읽어 PEM 문자열로 반환.
 * 우선순위:
 *  1) SNOWFLAKE_PRIVATE_KEY_BASE64 — 전체 PEM을 base64로 인코딩해 한 줄로 저장한 경우
 *  2) SNOWFLAKE_PRIVATE_KEY — 원본 PEM (여러 줄 또는 \n escape 포함)
 */
function loadPrivateKey(): string {
  const base64 = process.env.SNOWFLAKE_PRIVATE_KEY_BASE64;
  if (base64 && base64.trim().length > 0) {
    try {
      const decoded = Buffer.from(base64.trim(), 'base64').toString('utf8');
      return normalizePem(decoded);
    } catch (e) {
      console.error('[Snowflake] SNOWFLAKE_PRIVATE_KEY_BASE64 디코딩 실패:', e);
      throw new Error('[Snowflake] SNOWFLAKE_PRIVATE_KEY_BASE64 값이 유효한 base64가 아닙니다.');
    }
  }

  const raw = process.env.SNOWFLAKE_PRIVATE_KEY;
  if (!raw) {
    throw new Error('[Snowflake] SNOWFLAKE_PRIVATE_KEY 환경변수가 설정되지 않았습니다.');
  }
  return normalizePem(raw);
}

/**
 * Snowflake 연결 생성 (Key-Pair / JWT 인증)
 */
export function createSnowflakeConnection(): snowflake.Connection {
  const privateKey = loadPrivateKey();
  const privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;

  const hasHeader = /-----BEGIN [^-]+-----/.test(privateKey);
  const hasFooter = /-----END [^-]+-----/.test(privateKey);
  const lineCount = privateKey.split('\n').length;

  console.log('[Snowflake] 연결 설정 확인:', {
    account: process.env.SNOWFLAKE_ACCOUNT ? '✓' : '✗',
    username: process.env.SNOWFLAKE_USERNAME ? '✓' : '✗',
    privateKey: privateKey ? '✓' : '✗',
    privateKeySource: process.env.SNOWFLAKE_PRIVATE_KEY_BASE64 ? 'BASE64' : 'PEM',
    privateKeyHasHeader: hasHeader,
    privateKeyHasFooter: hasFooter,
    privateKeyLineCount: lineCount,
    privateKeyPass: privateKeyPass ? '✓' : '(없음)',
    warehouse: process.env.SNOWFLAKE_WAREHOUSE ? '✓' : '✗',
    database: process.env.SNOWFLAKE_DATABASE ? '✓' : '✗',
  });

  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT!,
    username: process.env.SNOWFLAKE_USERNAME!,
    authenticator: 'SNOWFLAKE_JWT',
    privateKey,
    ...(privateKeyPass ? { privateKeyPass } : {}),
    warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
    database: process.env.SNOWFLAKE_DATABASE!,
    timeout: 60000, // 60초 타임아웃
    clientSessionKeepAlive: true, // 세션 유지
    clientSessionKeepAliveHeartbeatFrequency: 3600, // 1시간마다 heartbeat
  });

  return connection;
}

/**
 * Snowflake 연결 및 쿼리 실행
 * 
 * @param query SQL 쿼리 문자열
 * @returns 쿼리 결과 배열
 */
export async function executeSnowflakeQuery(query: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const connection = createSnowflakeConnection();

    connection.connect((err, conn) => {
      if (err) {
        console.error('[Snowflake] 연결 실패:', err);
        reject(err);
        return;
      }

      console.log('[Snowflake] 연결 성공');

      // 쿼리 타임아웃 설정 (60초)
      const timeout = setTimeout(() => {
        connection.destroy((err) => {
          if (err) {
            console.error('[Snowflake] 타임아웃 후 연결 종료 실패:', err);
          }
        });
        reject(new Error('[Snowflake] 쿼리 실행 타임아웃 (60초)'));
      }, 60000);

      conn.execute({
        sqlText: query,
        complete: (err, stmt, rows) => {
          clearTimeout(timeout);
          
          // 연결 종료
          connection.destroy((err) => {
            if (err) {
              console.error('[Snowflake] 연결 종료 실패:', err);
            }
          });

          if (err) {
            console.error('[Snowflake] 쿼리 실행 실패:', err);
            reject(err);
            return;
          }

          console.log(`[Snowflake] 쿼리 성공: ${rows?.length || 0}행 반환`);
          resolve(rows || []);
        },
      });
    });
  });
}

/**
 * 연결 종료
 * 
 * @param connection Snowflake 연결 객체
 */
export async function closeConnection(connection: snowflake.Connection): Promise<void> {
  return new Promise((resolve, reject) => {
    connection.destroy((err) => {
      if (err) {
        console.error('[Snowflake] 연결 종료 실패:', err);
        reject(err);
        return;
      }
      console.log('[Snowflake] 연결 종료 성공');
      resolve();
    });
  });
}

