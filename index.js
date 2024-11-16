const odbc = require('odbc');
const db = require('./mysql')
const cron = require('node-cron');
const dataTypeMapping = require('./dataTypes')
const escS = (str) => {
  if (str) {
    return `'${str.replace(/\\/g, '\\\\').replace(/'/g, "''").replace(/"/g, '\\"')}'`;
  } else {
    return 'NULL';
  }
};
const moment = require('moment')

let tablesNames = []
const writeData = false // false pour tester si les tables sont correctement récupérées ( à mettre sur true pour écrire sur le mysql distant )
const startTime = Date.now()

const getAccessTablesData = async () => {
  const connection = await odbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\\Users\\killi\\Documents\\DataPackageIC.accdb;').catch((err) => {
    console.error(err)
    return console.error(`[ODBC] Erreur de connexion.`);
  });

  try {
    [tablesNames] = await db.execute(`SELECT * FROM odbcImport WHERE actif=1 AND client=0`)
  } catch (error) {
    console.error(err);
    return console.error('[MYSQL] Erreur lors de la récupération de la liste des tables à importer.')
  }

  const tables = [];
  for (const table of tablesNames) {
    try {

      let query = `SELECT * FROM ${table.name}` //.replace(/{dateImport}/g, `#${formattedDate}#`);
      if (table.conditionSql && table.dateImport) {
        // Formatage de la date d'importation au format Access (MM/DD/YYYY)
        const formattedDateImport = moment.utc(table.dateImport).format('MM/DD/YYYY');
      
        // Conversion manuelle des dates DD/MM/YYYY présentes dans la conditionSql
        const conditionSqlAdjusted = table.conditionSql.replace(/(\d{2})\/(\d{2})\/(\d{4})/g, (match, day, month, year) => {
          return `#${month}/${day}/${year}#`; // Format pour Access
        });
      
        // Remplacement de {dateImport} dans la condition
        query += ` ${conditionSqlAdjusted}`.replace(/{dateImport}/g, `#${formattedDateImport}#`);
      }
      console.log(query)
      const tablesContents = await connection.query(query).catch((err) => {
        console.log(err)
      });
      console.log(`[ODBC] Table récupérée : ${table.name}.`);
      if(!tablesContents) return console.log(`[ODBC] ${table.name} : Aucune donnée à importer.`);

      tables.push({
        data: tablesContents.slice(0, tablesContents.length),
        columns: tablesContents.columns,
        tableName: table.newName 
      });
    } catch (err) {
      console.error(err);
      return console.error(`[ODBC] Erreur de récupération de la table ${table.name}.`);
    }
  }
  await connection.close();

  return tables;
};

const checkTable = async (tableName, columns) => {
  const columnDefinitions = columns.map(column => {
    const mysqlType = dataTypeMapping[column.dataTypeName] || 'VARCHAR';
    const columnSize = mysqlType === 'VARCHAR' && column.columnSize ? `(${column.columnSize})` : '';
    const nullable = column.nullable ? 'NULL' : 'NOT NULL';
    return `\`${column.name.replace(/ /g, '_').replace(/@/g, '')}\` ${mysqlType}${columnSize} ${nullable}`;
  }).join(', ');

  const checkQuery = `SHOW TABLES LIKE '${tableName}'`;
  const createTableQuery = `
    CREATE TABLE ${tableName} (
      ${columnDefinitions.replace('DUREE', 'DUREE2').replace('DATE_CONTRAT', 'DATE_CONTRAT2')}
    )
  `;

  const [queryExists] = await db.execute(checkQuery).catch((err) => {
    console.error(err);
    return console.error(`[MYSQL] Erreur de vérification des lignes distantes.`);
  });

  if (queryExists.length === 0) {
    try {
      await db.execute(createTableQuery);
    } catch (err) {
      console.error(err);
      return console.error(`[MYSQL] Erreur de création de la table ${tableName}.`);
    }
    return false;
  } else {
    return true;
  }
};

const pushIntoTable = async (data, columns) => {
  const { tableName } = data;
  const totalData = data.data.length;
  let erreurs = [];
  let succes = 0;

  await db.execute(`DELETE from ${tableName} WHERE 1=1`).catch((err) => {
    console.error(err);
    return console.error(`[MYSQL] Erreur de suppression des lignes de la table ${tableName}.`);
  });
  console.log(`[] ------------------------------------------------------- ( ${tableName} ) ${totalData} lignes supprimées. -----------------------------------------------------`);

  const columnNames = columns.map(column => {
    return `\`${column.name.replace(/ /g, '_').replace(/@/g, '')}\``;
  }).join(', ');

  const formateValues = (localData) => {
    return columns.map(column => {
      const value = localData[column.name];
      const mysqlType = dataTypeMapping[column.dataTypeName] || 'VARCHAR';
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
  };

  for (const localDataIndex in data.data) {
    const localData = data.data[localDataIndex];
    const percentage = ((Number(localDataIndex) + 1) / totalData) * 100;

    try {
      const values = formateValues(localData);
      await db.execute(`INSERT INTO ${tableName} (${columnNames.replace('DUREE', 'DUREE2').replace('DATE_CONTRAT', 'DATE_CONTRAT2')}) VALUES (${values})`);
      succes++;
      console.log(`[MYSQL] Ligne ${localDataIndex} insérée [ ${percentage.toFixed(2)}% ] ( ${tableName} )`);
    } catch (err) {
      erreurs.push(err);
      console.log(`[MYSQL ERREUR] Ligne ${localDataIndex} insérée ( ${tableName} )`, err);
    }
  }


  erreurs.forEach((errrr) => {
    console.log(errrr)
  })
  console.log(`[] -----------------------------------------------------  ( ${tableName} ) ${erreurs.length} erreurs, ${succes} insérés. -----------------------------------------------------`);
  

};


const execute = async () => {
  const tablesData = await getAccessTablesData() // Récupérer les tables mises dans le tableau tablesNames
  console.log(tablesData)


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

  const formatDuration = (milliseconds) => {
    const seconds = Math.floor(milliseconds / 1000) % 60;
    const minutes = Math.floor(milliseconds / (1000 * 60)) % 60;
    const hours = Math.floor(milliseconds / (1000 * 60 * 60)) % 24;
    const days = Math.floor(milliseconds / (1000 * 60 * 60 * 24));
  
    return [
      days ? `${days} jour${days > 1 ? 's' : ''}` : '',
      hours ? `${hours} heure${hours > 1 ? 's' : ''}` : '',
      minutes ? `${minutes} minute${minutes > 1 ? 's' : ''}` : '',
      seconds ? `${seconds} seconde${seconds > 1 ? 's' : ''}` : ''
    ].filter(Boolean).join(', ');
  };

  const timeTotal = Date.now() - startTime
  console.log(`[] -----------------------------------------------------  Durée : ${formatDuration(timeTotal)} -----------------------------------------------------`)


}

execute()