/**
 * Snowflake 연결 및 쿼리 유틸리티
 * 
 * Node.js 환경에서 Snowflake 데이터베이스에 연결하고 쿼리를 실행합니다.
 * Vercel에서 완벽하게 작동합니다.
 */

import snowflake from 'snowflake-sdk';

/**
 * Snowflake 연결 생성
 */
export function createSnowflakeConnection(): snowflake.Connection {
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT!,
    username: process.env.SNOWFLAKE_USERNAME!,
    password: process.env.SNOWFLAKE_PASSWORD!,
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

