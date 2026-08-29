import 'dotenv/config';
import { JobQueue } from '@jobqueue/core';

async function run() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL environment variable is missing');
  }

  const queue = new JobQueue({ connectionString });

  try {
    console.log('Enqueueing a test job...');
    const job = await queue.enqueue({
      queueName: 'email_notifications',
      payload: {
        userId: '12345',
        template: 'welcome_email',
        metadata: {
          browser: 'Firefox'
        }
      }
    });

    console.log('Job successfully enqueued!');
    console.log(job);
  } catch (error) {
    console.error('Failed to enqueue job:', error);
  } finally {
    await queue.close();
  }
}

run();