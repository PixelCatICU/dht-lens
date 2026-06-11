import dhtLens from './src/index.mjs';

dhtLens({
  address: '0.0.0.0',
  port: 6881,
  nodesMaxSize: 4000
}, async ({ name }) => {
  console.log(new Date().getTimezoneOffset() + ' ' + name);
});
