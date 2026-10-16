// Driver-matrix scenario with mysql2: execute() uses binary prepared statements.
const mysql = require('mysql2/promise');

function fail(what, got, want) {
  console.log(`node: FAIL ${what}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
  process.exit(1);
}
function expect(what, got, want) {
  if (JSON.stringify(got) !== JSON.stringify(want)) fail(what, got, want);
}

(async () => {
  const conn = await mysql.createConnection({
    host: process.env.DRV_HOST || '127.0.0.1', port: +(process.env.DRV_PORT || 3306),
    user: process.env.DRV_USER || 'root', password: process.env.DRV_PASSWORD || '',
    charset: 'utf8mb4', dateStrings: true, supportBigNumbers: true,
  });
  const db = process.env.DRV_DB || 'app';
  await conn.query(`CREATE DATABASE IF NOT EXISTS ${db}`);
  await conn.query(`USE ${db}`);
  await conn.query('DROP TABLE IF EXISTS drv_node');
  await conn.query(`CREATE TABLE drv_node (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2), created DATETIME, data BLOB, flag BOOL, UNIQUE KEY un (name))`);
  let [r] = await conn.execute('INSERT INTO drv_node (name, amount, created, data, flag) VALUES (?, ?, ?, ?, ?)',
    ['héllo 🌍', '12.34', '2026-01-02 03:04:05', Buffer.from([0, 1, 2, 255]), true]);
  expect('insertId', r.insertId, 1);
  [r] = await conn.execute('INSERT INTO drv_node (name, amount, created, data, flag) VALUES (?, NULL, NULL, NULL, NULL), (?, ?, ?, ?, ?)',
    ['nulls', 'second', '-0.50', '2026-01-02 03:04:05', Buffer.from('x'), false]);
  expect('affectedRows', r.affectedRows, 2);

  for (const [mode, run] of [['execute', conn.execute.bind(conn)], ['query', conn.query.bind(conn)]]) {
    const [rows] = await run('SELECT name, amount, created, data, flag FROM drv_node WHERE id = ?', [1]);
    const row = rows[0];
    expect(`${mode} utf8mb4 string`, row.name, 'héllo 🌍');
    expect(`${mode} decimal`, row.amount, '12.34');
    expect(`${mode} datetime`, row.created, '2026-01-02 03:04:05');
    expect(`${mode} blob`, [...row.data], [0, 1, 2, 255]);
    expect(`${mode} bool`, row.flag, 1);
    const [nulls] = await run('SELECT amount, created, data, flag FROM drv_node WHERE name = ?', ['nulls']);
    expect(`${mode} NULLs`, nulls[0], { amount: null, created: null, data: null, flag: null });
  }

  await conn.beginTransaction();
  await conn.execute('UPDATE drv_node SET amount = amount + 1 WHERE id = 1');
  await conn.rollback();
  await conn.beginTransaction();
  await conn.execute('UPDATE drv_node SET amount = amount + 1 WHERE id = 1');
  await conn.commit();
  const [after] = await conn.execute('SELECT amount FROM drv_node WHERE id = 1');
  expect('commit/rollback', after[0].amount, '13.34');

  try {
    await conn.execute('INSERT INTO drv_node (name) VALUES (?)', ['second']);
    fail('duplicate key error', 'no error', 1062);
  } catch (e) {
    expect('duplicate key error', e.errno, 1062);
  }
  await conn.end();
  console.log('node: PASS');
})().catch((e) => { console.log(`node: FAIL ${e.message}`); process.exit(1); });
