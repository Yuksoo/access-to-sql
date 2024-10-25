const odbc = require('odbc');
const db = require('./mysql')
const cron = require('node-cron');
const dataTypeMapping = require('./dataTypes')
const escS = (str) => {
  if (str) {
    return `'${str.replace(/'/g, "''")}'`;
  } else {
    return 'NULL';
  }
};

const tablesNames = ['users', 'testdb']
const writeData = false // false pour tester si les tables sont correctement récupérées ( à mettre sur true pour écrire sur le mysql distant )

const getAccessTablesData = async () => {
  const connection = await odbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\killi\\Documents\\testodbc.accdb;').catch((err) => {
    console.error(err)
    return console.error(`[ODBC] Erreur de connexion.`)
  })


  const tables = []
  for (tbIndex in tablesNames) {
    try {
      const tablesContents = await connection.query(`SELECT * FROM ${tablesNames[tbIndex]}`)
      console.log(`[ODBC] Table récupérée : ${tablesNames[tbIndex]}.`)
      tables.push({ data: tablesContents.slice(0, tablesContents.length), columns: tablesContents.columns, tableName: tablesNames[tbIndex] })
    } catch (err) {
      console.error(err)
      return console.error(`[ODBC] Erreur de récupération de la table ${tablesNames[tbIndex]}.`)
    }
  }
  await connection.close();

  return tables
}

const checkTable = async (tableName, columns) => {

  const columnDefinitions = columns.map(column => {
    const mysqlType = dataTypeMapping[column.dataTypeName] || 'VARCHAR';
    const columnSize = mysqlType === 'VARCHAR' && column.columnSize ? `(${column.columnSize})` : '';
    const nullable = column.nullable ? 'NULL' : 'NOT NULL';
    return `\`${column.name}\` ${mysqlType}${columnSize} ${nullable}`;
  }).join(', ');

  const checkQuery = `SHOW TABLES LIKE '${tableName}'`;
  const createTableQuery = `
        CREATE TABLE ${tableName} (
            ${columnDefinitions}
        )
    `;

  const [queryExists] = await db.execute(checkQuery).catch((err) => {
    console.error(err)
    return console.error(`[MYSQL] Erreur de vérification des lignes distantes.`)
  })
  if (queryExists.length === 0) {
    try {
      await db.execute(createTableQuery)
    } catch (err) {
      console.error(err)
      return console.error(`[MYSQL] Erreur de création de la table ${tableName}.`)
    }
    return false;
  } else {
    return true;
  }


}

const pushIntoTable = async (data, columns) => {
  const { tableName } = data
  const totalData = data.data.length
  let erreurs = 0
  let succes = 0


  await db.execute(`DELETE from ${tableName} WHERE 1=1`).catch((err) => {
    console.error(err)
    return console.error(`[MYSQL] Erreur de suppression des lignes de la table ${tableName}.`)
  })
  console.log(`[] ------------------------------------------------------- ( ${tableName} ) ${totalData} lignes supprimées. -----------------------------------------------------`)


  const columnNames = columns.map(column => {
    return `${column.name}`
  }).join(', ');

  const formateValues = (localData) => {
    return columns.map(column => {
      const value = localData[column.name];
      const mysqlType = dataTypeMapping[column.dataTypeName] || 'VARCHAR'
      if (value === null || value === undefined) return 'NULL'; // gérer NULL

      switch (mysqlType) {
        case 'INT':
        case 'BIGINT':
        case 'FLOAT':
        case 'DOUBLE':
          return Number(value); // Convertir en nombre
        case 'DATE':
        case 'DATETIME':
          return escS(new Date(value)); // Convertir en objet Date
        case 'BOOLEAN':
          return value ? 1 : 0; // Convertir les booléens en 1 ou 0
        default:
          return escS(String(value)); // Convertir en chaîne pour VARCHAR, TEXT, etc.
      }
    }).join(', ');
  }


  for (const localDataIndex in data.data) {
    const localData = data.data[localDataIndex]
    const percentage = ((Number(localDataIndex) + 1) / totalData) * 100;

    try {
      const values = formateValues(localData)
      await db.execute(`INSERT INTO ${tableName} (${columnNames}) VALUES (${values})`)
      succes++;
      console.log(`[MYSQL] Ligne ${localDataIndex} insérée [ ${percentage.toFixed(2)}% ] ( ${tableName} )`)
    } catch (err) {
      erreurs++;
      console.log(`[MYSQL ERREUR] Ligne ${localDataIndex} insérée ( ${tableName} )`, err)
    }

  }



  console.log(`[] -----------------------------------------------------  ( ${tableName} ) ${erreurs} erreurs, ${succes} insérés. -----------------------------------------------------`)
}

const execute = async () => {
  const tablesData = await getAccessTablesData() // Récupérer les tables mises dans le tableau tablesNames
  //console.log(tablesData)


  if(!writeData) return;

  for (const tableDataIndex in tablesData) { // Boucle pour chaque table renvoyées
    const data = tablesData[tableDataIndex] // La table
    const columns = data.columns

    const tableExists = await checkTable(data.tableName, columns)
    if (!tableExists) {
      console.log(`[DATA] La table ${data.tableName} a été créee.`)
    }

    await pushIntoTable(data, columns)


  }

}


execute()

cron.schedule('0 0 * * *', () => {
  execute()
});
