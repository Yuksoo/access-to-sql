const mysql = require('mysql');

// Créer un pool de connexions
const db = mysql.createPool({
    connectionLimit: 10, // Limite de connexions simultanées
    user: "atnadmin",
    password: "aTnaAdmin123",
    database: "atna",
    host: "gp243685-002.eu.clouddb.ovh.net",
    port: 35936,
    connectTimeout: 10000,
    acquireTimeout: 10000,
    waitForConnections: true,
    queueLimit: 0
});

// Vérifier si la connexion à la base de données est établie
db.getConnection((err, connection) => {
    if (err) {
        console.error('[MYSQL] Erreur lors de la connexion à la base de données.', err);
        return;
    }
    if (connection) connection.release();
    console.log(`[MYSQL] Connecté à l'instance mysql distante.`);
});

// Fonction asynchrone pour simplifier les requêtes
const dbQueryFunction = (query) => {
    return new Promise((resolve, reject) => {
        db.query(query, (error, results, columns) => {
            if (error) {
                return reject(error);
            }
            return resolve([results, columns]);
        });
    });
};

// Utiliser db.execute comme alias pour dbQueryFunction
db.execute = dbQueryFunction;

// Gestionnaire global des erreurs de connexion
db.on('error', (err) => {
    console.error('[MYSQL] Erreur dans le pool de connexions MySQL :', err);
});

module.exports = db;
