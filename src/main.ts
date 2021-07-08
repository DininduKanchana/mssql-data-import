/* eslint-disable @typescript-eslint/no-var-requires */
// import mssql from 'mssql';
// eslint-disable-next-line @typescript-eslint/no-var-requires
import * as mssql from 'mssql';
import * as fs from 'fs';
import * as split from 'split';
import * as prompt from 'prompt';
import * as firstline from 'firstline';
import * as dotenv from 'dotenv';

import { Table } from 'mssql';

dotenv.config();

const sqlConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_DATABASE,
  server: process.env.DB_SERVER,
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 10000,
  },
  options: {
    trustServerCertificate: true, // change to true for local dev / self-signed certs
  },
};

type UserInputProps = {
  fileName: string,
  tableName: string,
  seperator: string,
  firstLineColumns: string
}

function execute(data: UserInputProps): Promise<string> {
  const { fileName, tableName, seperator: sep, firstLineColumns } = data
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve: (value?: string) => void) => {
    if (!(fileName && tableName)) {
      resolve('missing inputs');
    }
    try {
      const seperator = sep || '|';
      // fileName = path.resolve(fileName);
      const table = new Table(tableName);
      table.create = true;

      // read first line to get column details
      const firstLine = await firstline(fileName);

      // adding the coulmns to table
      if (firstLineColumns.toLocaleLowerCase() === 'y') {
        const columns = firstLine.split(seperator);
        for (let i = 0; i < columns.length; i++) {
          const columnName = columns[i];
          table.columns.add(columnName + '-' + i, mssql.VarChar(200), {
            nullable: true,
          });
        }
      } else {
        const columnCount = firstLine.split(seperator).length;
        for (let i = 0; i < columnCount; i++) {
          table.columns.add('column-' + i.toString(), mssql.VarChar(200), {
            nullable: true,
          });
        }
      };

      // stablish connection
      const pool = new mssql.ConnectionPool(sqlConfig);
      await pool.connect();
      await pool.query`DROP TABLE IF EXISTS "${tableName}"`;

      let count = 0;

      const readStream = fs.createReadStream(fileName);
      const lineStream = readStream.pipe(split());

      lineStream.on('data', async function (data) {
        const values = data.split(seperator);
        if (values.length > 1) {
          try {
            count = count + 1;

            if (firstLineColumns.toLocaleLowerCase() === 'y' && count === 1) {
              // skip the first line in the file
              return;
            }
            table.rows.add(...values);
            if (count % 10000 === 0) {
              lineStream.pause();

              const request = pool.request();
              const results = await request.bulk(table);
              table.rows.splice(0, table.rows.length);

              lineStream.resume();
              console.log(`rows affected ${results.rowsAffected}`);
            }

          } catch (error) {
            console.log(error);
          }
        }
      });

      readStream.on('end', async function () {
        // the last remaining batch
        const request = pool.request();
        const results = await request.bulk(table);
        table.rows.splice(0, table.rows.length);
        console.log(`rows affected ${results.rowsAffected}`);

        resolve('data stored');
      });

    } catch (err) {
      console.log(err);
    }
  });
}

async function getUserInputs(): Promise<UserInputProps> {
  const schema = {
    properties: {
      fileName: {
        required: true
      },
      tableName: {
        required: true
      },
      seperator: {
        required: false
      },
      firstLineColumns: {
        required: false
      }
    }
  };
  prompt.start();
  const result: UserInputProps =  await prompt.get(schema as any);
  return result;
}

async function main() {
  const userInputs = await getUserInputs();
  await execute(userInputs);
}

main();
