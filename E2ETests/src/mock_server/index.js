const mb = require('mountebank');
const settings = require('./settings');
const citiBikeService = require('./citiBikeService.js');

const mbServerInstance = mb.create({
    port: settings.port,
    pidfile: './logs/mb.pid',
    logfile: './logs/mb.log',
    protofile: '../protofile.json',
    ipWhitelist: ['*']
});

mbServerInstance.then(function() {
    citiBikeService.addService();
});