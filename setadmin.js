// setAdmin.js
const admin = require('firebase-admin');

// Replace with the path to your service account key JSON file
const serviceAccount = require('C:\\Users\\shamb\\Downloads\\flame-duplicates-firebase-adminsdk-fbsvc-1b9090f63d.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// Replace with your user's UID
const uid = 'yrYUsCggCFb9FZkBwsaheJ4Ywdu1';

admin.auth().setCustomUserClaims(uid, { admin: true })
  .then(() => {
    console.log(`Successfully set admin claim for user: ${uid}`);
    process.exit(0);
  })
  .catch(error => {
    console.error('Error setting admin claim:', error);
    process.exit(1);
  });
