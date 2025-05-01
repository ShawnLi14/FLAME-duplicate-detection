// setAdmin.js
const admin = require('firebase-admin');

// Replace with the path to your service account key JSON file
const serviceAccount = require('C:\\Users\\shamb\\Downloads\\flame-duplicates-firebase-adminsdk-fbsvc-1b9090f63d.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// Replace with your user's UID
const uid = '5Oe8NEmjBXa9uxK2Up2up4Jp2Sx2';

admin.auth().setCustomUserClaims(uid, { admin: true })
  .then(() => {
    console.log(`Successfully set admin claim for user: ${uid}`);
    process.exit(0);
  })
  .catch(error => {
    console.error('Error setting admin claim:', error);
    process.exit(1);
  });
