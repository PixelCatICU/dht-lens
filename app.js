import p2pspider from './src/index.js'

p2pspider({
        address: '0.0.0.0',
        port: 6881,
        nodesMaxSize: 4000
    }, async ({ name, infohash, files, size }) => {
        console.log(new Date().getTimezoneOffset() + ' ' + name)
    })
