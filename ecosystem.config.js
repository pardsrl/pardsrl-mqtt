module.exports = {
  apps: [{
    name: 'MQTT server',
    script: './server.js',
    // docker env config
    watch: true,
    watch_options: {
      usePolling: true
    },
    env: {
      NODE_ENV: 'development',
      DEBUG: 'pardsrl:*'
    },
    // Set Env Vars if you are running on production
    env_production: {
      NODE_ENV: 'production'
    }
  }]
}
