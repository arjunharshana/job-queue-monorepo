import 'dotenv/config';

async function measureConsistencyGap() {
  const start = Date.now();
  
  const postRes = await fetch('http://localhost:3000/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ queueName: 'timing_test', payload: {} })
  });
  
  const { id } = await postRes.json();
  console.log(`[0ms] Enqueued job ${id} (Buffered in Redis)`);

  let attempts = 0;
  const MAX_ATTEMPTS = 30;

  while (true) {
    attempts++;
    if (attempts >= MAX_ATTEMPTS) {
      console.error(`Gave up after ${attempts} attempts — job never appeared. Syncer may be broken.`);
      process.exit(1);
    }

    const getRes = await fetch(`http://localhost:3000/jobs/${id}`);
    const elapsed = Date.now() - start;

    if (getRes.status === 200) {
      console.log(`[${elapsed}ms] Job found in Postgres after ${attempts} attempts!`);
      break;
    }
    console.log(`[${elapsed}ms] Attempt ${attempts}: 404 Not Found (Still in syncer delay)`);
    await new Promise(r => setTimeout(r, 500));
  }
}

measureConsistencyGap();