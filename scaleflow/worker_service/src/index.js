require('dotenv').config();

console.log('Worker Service initialized and waiting for tasks...');

// Keep process active
process.on('SIGINT', () => {
  console.log('Worker Service shutting down...');
  process.exit(0);
});
