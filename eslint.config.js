export default [
  {
    files: ['src/**/*.mjs', 'app.mjs'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        Buffer: 'readonly',
        console: 'readonly',
        setInterval: 'readonly'
      }
    }
  }
];
