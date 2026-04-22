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
 * .env / Vercel secret 등에서 한 줄 문자열로 저장된 PEM 키의 `\n` escape 시퀀스를
 * 실제 개행 문자로 복원하고, 앞뒤 공백/따옴표를 제거합니다.
 */
function normalizePem(raw: string): string {
  let key = raw.trim();

  // 양끝에 감싸진 따옴표 제거
  if ((key.startsWith('"') && key.endsWith('"')) ||
      (key.startsWith("'") && key.endsWith("'"))) {
    key = key.slice(1, -1);
  }

  // literal "\n" / "\r\n" 을 실제 개행으로 복원
  if (!key.includes('\n')) {
    key = key.replace(/\\r\\n/g, '\n').replace(/\\n/g, '\n');
  }

  return key;
}

/**
 * Snowflake 연결 생성 (Key-Pair / JWT 인증)
 */
export function createSnowflakeConnection(): snowflake.Connection {
  const rawPrivateKey = process.env.SNOWFLAKE_PRIVATE_KEY;
  if (!rawPrivateKey) {
    throw new Error('[Snowflake] SNOWFLAKE_PRIVATE_KEY 환경변수가 설정되지 않았습니다.');
  }

  const privateKey = normalizePem(rawPrivateKey);
  const privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;

  console.log('[Snowflake] 연결 설정 확인:', {
    account: process.env.SNOWFLAKE_ACCOUNT ? '✓' : '✗',
    username: process.env.SNOWFLAKE_USERNAME ? '✓' : '✗',
    privateKey: privateKey ? '✓' : '✗',
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

