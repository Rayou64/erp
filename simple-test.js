// simple-test.js - Test simple de l'API
const express = require('express');
const app = express();
app.use(express.json());

app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body;
  console.log('Login attempt:', username, password);

  if (username === 'admin' && password === 'admin123') {
    res.json({ token: 'test-token', username: 'admin' });
  } else {
    res.status(401).json({ error: 'Identifiants invalides' });
  }
});

app.listen(4001, () => {
  console.log('Test server on port 4001');
});